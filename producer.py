from Kafka.main import KafkaScopus
import threading

def run_kafka(year):
    kafka_data = KafkaScopus('localhost:9092', 'scopus', str(year))
    kafka_data.run()

threads = []
for i in range(2018, 2024):
    thread = threading.Thread(target=run_kafka, args=(i,))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()