from kafka import KafkaProducer
import json


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    while True:
        print('Create and generate new control messages')
        device_name = input('Enter device name: ')
        metric_name = input('Enter metric name: ')
        aggregation_name = input('Enter aggregation name(P99, AVERAGE, SUM, MIN, MAX): ')
        operator = input('Enter operator(GT, LT, GTE, LTE, EQ): ')
        threshold = input('Enter threshold: ')

        control = {
            'deviceName': device_name,
            'metricName': metric_name,
            'aggregationName': aggregation_name,
            'operator': operator,
            'threshold': threshold
        }
        print('Sending {} to kafka cluster'.format(control))
        producer.send('controls-topic', control)
