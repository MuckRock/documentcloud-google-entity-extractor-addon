"""
This is a proof of concept for using the new Entity API with DocumentCloud
"""

import logging
import os
import sys
from bisect import bisect
from tempfile import NamedTemporaryFile

from documentcloud.addon import AddOn
from documentcloud.exceptions import APIError
from documentcloud.exceptions import DoesNotExistError
from documentcloud.toolbox import grouper
from google.cloud import language_v1
from google.cloud.language_v1.types.language_service import AnalyzeEntitiesResponse
from wikimapper import WikiMapper

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)

BYTE_LIMIT = 1000000
BULK_LIMIT = 25


class GCPEntityExtractor(AddOn):
    """Extract entities using GCP NLP API"""

    def __init__(self):
        super().__init__()
        self.errors = 0
        self.successes = 0

    def setup_credential_file(self):
        """Sets up Google Cloud developer credential file"""
        credentials = os.environ["TOKEN"]
        # put the contents into a named temp file
        # and set the var to the name of the file
        with NamedTemporaryFile(delete=False) as gac:
            gac.write(credentials.encode("ascii"))
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gac.name

    def main(self):
        """Set up the credential file and extract entities for each document"""
        self.setup_credential_file()
        for document in self.get_documents():
            self.extract_entities(document)

    def get_existing_entities(self, document):
        """Fetch existing entities for the document"""
        try:
            resp = self.client.get(f"documents/{document.id}/entities/")
            return {entity["entity"] for entity in resp.json()["results"]}
        except APIError as api_error:
            logger.error("API Error while fetching existing entities: %s", api_error)
            return set()

    def extract_entities(self, document):
        """Coordinate the extraction of all of the entities"""
        try:
            all_page_text = document.get_json_text()
        except DoesNotExistError:
            self.set_message(
                f"The document {document.id}" 
                "has not been OCR'd recently and is missing a JSON txt file."
                "Apply OCR and try this Add-On again"
            )
            sys.exit(0)
        texts = []
        total_bytes = 0
        page_map = [0]
        character_offset = 0
        total_characters = 0
        entities = []

        logger.info(
            "Extracting entities for %s, %d pages",
            document,
            len(all_page_text["pages"]),
        )

        for page in all_page_text["pages"]:
            # page map is stored in unicode characters
            # we add the current page's length in characters to the beginning of the
            # last page, to get the start character of the next page
            page_chars = len(page["contents"]) + 2
            page_map.append(page_map[-1] + page_chars)
            # the API limit is based on byte size, so we use the length of the
            # content encoded into utf8
            page_bytes = len(page["contents"].encode("utf8"))
            if page_bytes > BYTE_LIMIT:
                logger.error("Single page too long for entity extraction")
                return

            if total_bytes + page_bytes > BYTE_LIMIT:
                # if adding another page would put us over the limit,
                # send the current chunk of text to be analyzed
                logger.info("Extracting to page %d", page["page"])
                entities.extend(
                    self.extract_entities_text("".join(texts), character_offset)
                )
                character_offset = total_characters
                texts = [page["contents"] + "\n\n"]
                total_bytes = page_bytes
                total_characters += page_chars
            else:
                # otherwise append the current page and accumulate the length
                texts.append(page["contents"] + "\n\n")
                total_bytes += page_bytes
                total_characters += page_chars

        # analyze the remaining text
        logger.info("Extracting to end")
        entities.extend(self.extract_entities_text("".join(texts), character_offset))

        self.create_entity_occurrences(entities, document, page_map)

    def extract_entities_text(self, text, character_offset):
        """Extract the entities from a given chunk of text from the document"""
        client = language_v1.LanguageServiceClient()
        language_document = language_v1.Document(
            content=text, type_=language_v1.Document.Type.PLAIN_TEXT
        )
        logger.info("Calling entity extraction API")
        response = client.analyze_entities(
            document=language_document, encoding_type="UTF32"
        )
        logger.info("Converting response to dictionary representatpution")
        entities = AnalyzeEntitiesResponse.to_dict(response)["entities"]

        # only get entities with Wikipedia URLs for now
        entities = [e for e in entities if "wikipedia_url" in e["metadata"]]

        # adjust for character offset
        for entity in entities:
            for mention in entity["mentions"]:
                mention["text"]["begin_offset"] += character_offset
        return entities

    def create_entity_occurrences(self, entities, document, page_map):
        """Create the entity occurrence objects in the database,
        linking the entities to the document
        """
        existing_entities = self.get_existing_entities(document)
        logger.info("Creating %d entities", len(entities))
        entity_map = self.get_or_create_entities(entities)
        # remove entities which still do not have a wikidata_id
        entities = [e for e in entities if e["metadata"]["wikidata_id"] is not None]

        logger.info("Collapse entity occurrences")
        collapsed_entities = {}
        for entity in entities:
            entity_id = entity_map[entity["metadata"]["wikidata_id"]]

            if entity_id in existing_entities:
                logger.warning("Duplicate entity found for ID %s. Skipping...", entity_id)
                continue

            if entity_id in collapsed_entities:
                collapsed_entities[entity_id]["mentions"].extend(entity["mentions"])
            else:
                collapsed_entities[entity_id] = entity

        logger.info("Create entity occurrence objects")
        occurrence_json = []
        occurrence_json = [
            {
                "entity": entity_id,
                "relevance": entity["salience"],
                "occurrences": self.transform_mentions(entity["mentions"], page_map),
            }
            for entity_id, entity in collapsed_entities.items()
            if entity_id not in existing_entities
        ]

        for group in grouper(occurrence_json, BULK_LIMIT):
            try:
                self.client.post(
                    f"documents/{document.id}/entities/",
                    json=[g for g in group if g is not None],
                )
            except APIError as api_error:
                logger.error("API Error: %s", api_error)
                error_code = api_error.status_code
                if error_code == 400:
                    self.set_message(
                        "Indexing error. Please try applying OCR to the document"
                        f"{document.id} and running this Add-On again."
                    )
                    logger.error(
                        "There is an indexing issue with posting entities to this document"
                    )
                    sys.exit(0)
                if error_code == 403:
                    logger.error(
                        "You do not have permission to create entities on document %s",
                        document.id,
                    )
                self.errors += 1

    def get_or_create_entities(self, entities):
        """Get or create the entities returned from the API in the database"""

        mapper = WikiMapper("data/index_enwiki-latest.db")
        for entity in entities:
            logger.info("Mapping entity for %s", entity["metadata"]["wikipedia_url"])
            entity["metadata"]["wikidata_id"] = mapper.url_to_id(
                entity["metadata"]["wikipedia_url"]
            )

        # limit these to a certain number at a time
        wikidata_ids = [
            e["metadata"]["wikidata_id"]
            for e in entities
            if e["metadata"]["wikidata_id"] is not None
        ]
        resp = self.client.get(
            "entities/", params={"wikidata_id__in": ",".join(wikidata_ids)}
        )

        # map from Wikidata ID -> DocumentCloud entity ID
        entity_map = {}
        for entity in resp.json()["results"]:
            entity_map[entity["wikidata_id"]] = entity["id"]

        # if missing from the entity map, that means the entity does not exist
        # on DocumentCloud yet
        missing_wikidata_ids = [q for q in wikidata_ids if q not in entity_map]
        for group in grouper(missing_wikidata_ids, BULK_LIMIT):
            resp = self.client.post(
                "entities/", json=[{"wikidata_id": q} for q in group if q is not None]
            )
            # TODO check resp status_code
            for entity in resp.json():
                entity_map[entity["wikidata_id"]] = entity["id"]
        return entity_map

    def transform_mentions(self, mentions, page_map):
        """Format mentions how we want to store them in our database
        Rename and flatten some fields and calculate page and page offset
        """
        occurrences = []
        for mention in mentions:
            occurrence = {}
            occurrence["content"] = mention["text"]["content"]
            # occurrence["kind"] = mention["type_"]

            offset = mention["text"]["begin_offset"]
            page = bisect(page_map, offset) - 1
            page_offset = offset - page_map[page]

            occurrence["offset"] = offset
            occurrence["page"] = page
            occurrence["page_offset"] = page_offset

            occurrences.append(occurrence)
        return occurrences


if __name__ == "__main__":
    GCPEntityExtractor().main()
