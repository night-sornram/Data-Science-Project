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
    # Create a CSV writer object
    csv_writer = csv.writer(csv_file)
    
    # Write the header (if needed)
    # Assuming message.value is a dictionary, you can extract the keys for the header
    header_written = False
    
    # Iterate over the messages
    for message in scopusconsumer:
        # Get the message value
        message_value = message.value
        
        # If the message value is a dictionary, write the keys as the header
        if not header_written and isinstance(message_value, dict):
            header = message_value.keys()
            csv_writer.writerow(header)
            header_written = True
        
        # Write the message value to the CSV file
        if isinstance(message_value, dict):
            csv_writer.writerow(message_value.values())
        else:
            # If message_value is not a dictionary, write it as a single column
            csv_writer.writerow([message_value])

print(f'Messages have been written to {csv_file_path}')