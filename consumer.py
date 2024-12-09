import os
import csv
from kafka import KafkaConsumer
import avro.schema
from avro.io import DatumReader, BinaryDecoder
import io

csv_file_path = 'scopus_messages.csv'

headers = [
    "code", "title", "abstract", "publication_date", "prism_type", "keywords",
    "subject_area", "ref_count", "publisher", "affiliation", "authors"
]

def deserialize(schema, raw_bytes):
    bytes_reader = io.BytesIO(raw_bytes)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    return reader.read(decoder)

kafka_broker = 'localhost:9092'

schema_file = 'scopus.avsc'
scopusschema = avro.schema.parse(open(schema_file).read())
            
scopusconsumer = KafkaConsumer(
    'scopus',
     bootstrap_servers=[kafka_broker],
     enable_auto_commit=True,
     value_deserializer=lambda x: deserialize(scopusschema, x))


with open(csv_file_path, mode='a', newline="", encoding='utf-8') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(headers)
    for message in scopusconsumer:
        data = message.value
        csv_writer.writerow(data.values())

print(f'Messages have been written to {csv_file_path}')