from confluent_kafka import Producer
import json
import time
import csv
from datetime import datetime

# Kafka producer configuration
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        pass
        # Print message if needed
        #print(f'Message delivered to: {msg.topic()} with value: {msg.value().decode('utf-8')}')

def stream_Message(fn, topic):
    try:
        with open(fn, mode='r', newline='', encoding='utf-8-sig') as file:
            csv_reader = csv.DictReader(file)
            prev_time = None

            for row in csv_reader:
                event_timestamp = int(row['Timestamp'])
                event_time_seconds = event_timestamp / 1000.0

                # Timestamp from unix seconds to readable Timestamp
                Timestamp = datetime.fromtimestamp(event_time_seconds).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                row['Timestamp'] = Timestamp

                if prev_time is not None:
                    # calculate difference between Events to send messages in real-time
                    time_diff = event_timestamp - prev_time

                    if time_diff > 0:
                        time.sleep(time_diff / 1000)
                        print(time_diff / 1000)

                prev_time = event_timestamp
                message = json.dumps(row)

                producer.poll(0)
                producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
                producer.flush()
                
    except Exception as e:
        print(f'Error occured: {e}')
    
    producer.flush()


if __name__ == '__main__':
    fn = 'Ice_Hockey_Challenge.csv'
    topic = 'Event'

    stream_Message(fn, topic)
