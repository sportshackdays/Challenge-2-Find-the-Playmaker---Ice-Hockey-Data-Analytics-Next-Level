from confluent_kafka import Consumer
import json

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

topic = 'Icehockey'
consumer.subscribe([topic])

def consume_Message(consumer):
    try:
        while True:
            message=consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(f'Error: {message.error()}')
                continue
            else:
                data = message.value().decode('utf-8')
                tracks = json.loads(data)
                print(tracks)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == '__main__':
    consume_Message(consumer)