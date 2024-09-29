from confluent_kafka import Producer
import json
import time
import csv

# Kafka producer configuration
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to: {msg.topic()} with value: {msg.value().decode('utf-8')}')


def stream_Message(fn, topic):
    try:
        with open(fn, mode='r', newline='', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)

            for row in csv_reader:
                message = json.dumps(row)
                producer.poll(0)
                producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
                producer.flush()
                time.sleep(0.2)
    except Exception as e:
        print(f'Error occured: {e}')
    
    producer.flush()


if __name__ == '__main__':
    fn = 'Ice_Hockey_Challenge.csv'
    topic = 'Icehockey'

    stream_Message(fn, topic)
