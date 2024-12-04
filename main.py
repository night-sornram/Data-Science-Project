from Kafka.main import KafkaScopus

kafka_data1 = KafkaScopus('localhost:9092', 'scopus', '2023')
kafka_data1.run()

kafka_data2 = KafkaScopus('localhost:9092', 'scopus', '2021')
kafka_data2.run()
