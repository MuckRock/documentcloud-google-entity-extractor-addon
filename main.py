"""
This is a proof of concept for using the new Entity API with DocumentCloud
"""

from documentcloud.addon import AddOn


class GCPEntityExtractor(AddOn):
    """Extract entities using GCP NLP API"""

    def main(self):
        for document in self.get_documents():
            pass


if __name__ == "__main__":
    GCPEntityExtractor().main()
