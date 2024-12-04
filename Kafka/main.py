from kafka import KafkaProducer
import avro.schema
import avro.io
import io
import json
import os
from tqdm import tqdm

class KafkaScopus:
    def __init__(self, kafka_broker, topic, folder):
        schema_path = './scopus.avsc'
        with open(schema_path, 'r') as schema_file:
            schema_string = schema_file.read()
        self.schema = avro.schema.parse(schema_string)
        self.producer = KafkaProducer(bootstrap_servers=kafka_broker)
        self.folder = folder
        self.topic = topic
        self.number = int(self.folder + "00000")
        self.count = 0

    def serialize(self, schema, obj):
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer = avro.io.DatumWriter(schema)
        writer.write(obj, encoder)
        return bytes_writer.getvalue()

    def count_files_in_folder(self):
        folder_path = f'./Project/{self.folder}'
        return len([name for name in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, name))])

    def load_data(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)

    def convert_classification(self, data):

        result_data = []
        for item in data:
            result = {}
            result["name"] = item['@type']
            classifications = item['classification']

            if isinstance(classifications, list):
                result["value"] = [list(classification.values())[0]
                                   for classification in classifications]
            elif isinstance(classifications, dict):
                result["value"] = [list(classifications.values())[0]]
            else:
                result["value"] = [classifications]
            result_data.append(result)
        return result_data

    def convert_affiliation(self, data):
        if isinstance(data, list):
            return data
        else:
            return [data]

    def convert_references(self, data):
        if (data["abstracts-retrieval-response"]["item"]["bibrecord"]["tail"] == None):
            return []
        
        data = data["abstracts-retrieval-response"]["item"]["bibrecord"]["tail"]["bibliography"]["reference"]
        if isinstance(data, list):
            return [ref["ref-fulltext"] for ref in data]
        else:
            return [data["ref-fulltext"]]

    def convert_keywords(self, data):
        if (data == None):
            return []
        if isinstance(data["author-keyword"], list):
            return [keyword["$"] for keyword in data["author-keyword"]]
        else:
            return [data["author-keyword"]["$"]]
        
    def convert_date(self, data):
        return {
            "year": data["year"] if "year" in data else None,
            "month": data["month"] if "month" in data else None,
            "day": data["day"] if "day" in data else None
        }

    def extract_data(self, data):
        try:
            title = data["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]["citation-title"]
            abstract = data["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]["abstracts"]
            classifications = self.convert_classification(
                data["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]["enhancement"]["classificationgroup"]["classifications"])
            date_publication = self.convert_date(
                data["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]["source"]["publicationdate"])
            affiliation = self.convert_affiliation(
                data["abstracts-retrieval-response"]["affiliation"])
            references = self.convert_references(
                data)
            keywords = self.convert_keywords(
                data["abstracts-retrieval-response"]["authkeywords"])
        except KeyError as e:
            print(f"Key error: {e}")
            return None

        return {
            "title": title,
            "abstract": abstract,
            "classifications": classifications,
            "date_publication": date_publication,
            "affiliation": affiliation,
            "references": references,
            "keywords": keywords
        }

    def produce(self, data):
        serialized_data = self.serialize(self.schema, data)
        self.producer.send(self.topic, serialized_data)
        self.producer.flush()
        
    def run(self):
        len_files = self.count_files_in_folder()
        with tqdm(total=len_files, desc="Processing files", unit="file") as pbar:
            for i in range(len_files):
                file_path = f'./Project/{self.folder}/{self.number+i}'
                data = self.load_data(file_path)
                extracted_data = self.extract_data(data)
                if extracted_data:
                    self.produce(extracted_data)
                    self.count += 1
                pbar.update(1)
        