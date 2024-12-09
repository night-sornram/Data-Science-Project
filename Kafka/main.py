from kafka import KafkaProducer
import avro.schema
import avro.io
import io
import json
import os
import pandas as pd
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
        self.subject_areas_dict = scopus_subject_areas = {
            1000: "Multidisciplinary",
            1100: "Agricultural and Biological Sciences",
            1200: "Arts and Humanities",
            1300: "Biochemistry, Genetics and Molecular Biology",
            1400: "Business, Management, and Accounting",
            1500: "Chemical Engineering",
            1600: "Chemistry",
            1700: "Computer Science",
            1800: "Decision Sciences",
            1900: "Earth and Planetary Sciences",
            2000: "Economics, Econometrics and Finance",
            2100: "Energy",
            2200: "Engineering",
            2300: "Environmental Science",
            2400: "Immunology and Microbiology",
            2500: "Materials Science",
            2600: "Mathematics",
            2700: "Medicine",
            2800: "Neuroscience",
            2900: "Nursing",
            3000: "Pharmacology, Toxicology, and Pharmaceutics",
            3100: "Physics and Astronomy",
            3200: "Psychology",
            3300: "Social Sciences",
            3400: "Veterinary",
            3500: "Dentistry",
            3600: "Health Professions"
        }

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

    def title_convert(self, data):
        return data['abstracts-retrieval-response']['coredata'].get("dc:title", None)

    def abstract_convert(self, data):
        return data['abstracts-retrieval-response']['coredata'].get("dc:description", None)

    def publication_date_convert(self, data):
        publication = data['abstracts-retrieval-response']['item']['bibrecord']['head']['source']['publicationdate']

        year = publication.get("year", "Unknown")
        month = publication.get("month", "01")
        day = publication.get("day", "01")

        if year == "Unknown":
            return None
        else:
            month = f"{int(month):02}" if month.isdigit() else "01"
            day = f"{int(day):02}" if day.isdigit() else "01"

            # ISO 8601 format
            return f"{year}-{month}-{day}"

    def prism_type_convert(self, data):
        return data['abstracts-retrieval-response']['coredata'].get("prism:aggregationType", None)

    def keywords_convert(self, data):
        if data['abstracts-retrieval-response'].get('authkeywords') is not None:
            keywords = data['abstracts-retrieval-response']['authkeywords'].get(
                "author-keyword", None)
            if isinstance(keywords, list):
                k = ';'.join(keyword['$'].replace(' ', '')
                             for keyword in keywords)
                return k
            else:
                return None
        else:
            return None

    def subject_areas_convert(self, data):
        # array
        areas = data['abstracts-retrieval-response']['subject-areas'].get('subject-area', None)
        
        if areas is not None:
            a = ';'.join(self.subject_areas_dict[round(int(area['@code'])/100)*100]
                         for area in areas)
            return a

    def ref_count_convert(self, data):
        tail = data['abstracts-retrieval-response']['item']['bibrecord']['tail']

        if tail is not None:
            count = tail.get("bibliography").get("@refcount", None)
            if count is not None:
                return int(count)

    def publisher_convert(self, data):
        return data['abstracts-retrieval-response']['coredata'].get("dc:publisher", None)

    def affiliation_convert(self, data):
        affiliations = data['abstracts-retrieval-response']['affiliation']
        if isinstance(affiliations, list):
            af = ';'.join(
                f"{aff['affilname']}, {aff['affiliation-country']}"
                for aff in affiliations
            )

            return af

        else:
            return f"{affiliations['affilname']}, {affiliations['affiliation-country']}"

    def authors_convert(self, data):
        authors = data['abstracts-retrieval-response']['authors']['author']

        if isinstance(authors, list):

            s = ';'.join(
                f"{author['preferred-name']['ce:surname']} {author['preferred-name']['ce:given-name']}"
                for author in authors
            )

            return s
        else:
            return None

    def extract_data(self, data):
        title = self.title_convert(data)
        abstract = self.abstract_convert(data)
        publication_date = self.publication_date_convert(data)
        prism_type = self.prism_type_convert(data)
        keywords = self.keywords_convert(data)
        subject_areas = self.subject_areas_convert(data)
        ref_count = self.ref_count_convert(data)
        publisher = self.publisher_convert(data)
        affiliation = self.affiliation_convert(data)
        authors = self.authors_convert(data)

        return {
            "code": str(self.number),
            "title": title,
            "abstract": abstract,
            "publication_date": publication_date,
            "prism_type": prism_type,
            "keywords": keywords,
            "subject_area": subject_areas,
            "ref_count": ref_count,
            "publisher": publisher,
            "affiliation": affiliation,
            "authors": authors
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
