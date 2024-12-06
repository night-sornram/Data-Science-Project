from Kafka.main import KafkaScopus

for i in range(2018, 2024):
    kafka_data = KafkaScopus('localhost:9092', 'scopus', str(i))
    kafka_data.run()
