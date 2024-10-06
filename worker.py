from confluent_kafka import Consumer, Producer
import json

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

topic = 'Event'
consumer.subscribe([topic])

# Kafka producers configuration
producer_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_conf)

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
                pdf_Event = json.loads(data)
                
                #TODO: Some calculation for your worker

                # write results to next Topic
                output_topic = 'ShotPressure'
                producer.produce(output_topic, json.dumps(pdf_Event).encode('utf-8'))
                producer.flush()


    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.close()


if __name__ == '__main__':
    consume_Message(consumer)