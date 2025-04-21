import json
import os

class JsonWriterPipeline:
    def open_spider(self, spider):
        # Create output file in the crawler directory
        output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'movies.json')
        self.file = open(output_path, 'w')
        spider.logger.info(f"Writing output to {output_path}")

    def close_spider(self, spider):
        self.file.close()
        spider.logger.info("Closed output file")

    def process_item(self, item, spider):
        line = json.dumps(dict(item)) + "\n"
        self.file.write(line)
        return item