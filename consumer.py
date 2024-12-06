from kafka import KafkaConsumer
import avro.schema
import avro.io
import io
import csv

csv_file_path = 'scopus_messages.csv'

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

with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
    csv_writer = csv.writer(csv_file)
    header_written = False
    
    for message in scopusconsumer:
        message_value = message.value
        if not header_written and isinstance(message_value, dict):
            header = message_value.keys()
            csv_writer.writerow(header)
            header_written = True
        if isinstance(message_value, dict):
            csv_writer.writerow(message_value.values())
        else:
            csv_writer.writerow([message_value])

print(f'Messages have been written to {csv_file_path}')