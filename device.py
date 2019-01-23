from kafka import KafkaProducer
import json
import argparse
import time
import random

METRIC_PROFILES = [
    ('cpuUsage', 0.0, 1.0),
    ('memUsage', 0.0, 1.0),
    ('tcpSent', 10.0, 1000.0),
    ('tcpReceived', 10.0, 1000.0),
    ('udpSent', 10.0, 1000.0),
    ('udpReceived', 10.0, 1000.0)
]

def generateMetricValues():
    for metric_name, lower, upper in METRIC_PROFILES:
        yield (metric_name, random.uniform(lower, upper))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Device that generates metrics to kafka')
    parser.add_argument('device_name', metavar='device_name', type=str,
                        help='device name')

    args = parser.parse_args()
    device_name = args.device_name
    
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    while True:
        for metric_name, value in generateMetricValues():
            device_metric = {
                'deviceName': device_name,
                'metricName': metric_name,
                'value': value
            }
            print('Sending {} to kafka cluster'.format(device_metric))
            producer.send('metrics-topic', device_metric)
        
        print('Sleeping for 1 second')
        time.sleep(1)
