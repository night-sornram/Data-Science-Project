from kafka import KafkaConsumer
import avro.schema
import avro.io
import io

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

for message in scopusconsumer:
    print(message.value)