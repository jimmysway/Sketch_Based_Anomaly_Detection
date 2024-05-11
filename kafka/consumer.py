#!/usr/bin/env python

import sys
import json
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from sketches import RobustCountMinSketch, RobustAMSSketch, RobustHyperLogLog
from collections import Counter

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    consumer = Consumer(config)

    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    topic = "python"
    consumer.subscribe([topic], on_assign=reset_offset)

    threshold_frequency = None
    threshold_variance = None
    threshold_cardinality = None

    cms_category = RobustCountMinSketch(1000, 10, 0.1, 1000)
    cms_amount_interval = RobustCountMinSketch(1000, 10, 0.1, 1000)
    cms_currency = RobustCountMinSketch(1000, 10, 0.1, 1000)
    ams = RobustAMSSketch(1000, 0.1, 1000)
    hll = RobustHyperLogLog(10, 0.01, 1000)

    count = 0
    data_collected = []
    frequency_data = Counter()



    # Continue processing
    try:
        while len(data_collected) < 200:
            msg = consumer.poll(0)
            if msg is None:
                print("Waiting for messages...")
            elif msg.error():
                print(f"ERROR: {msg.error()}")
            else:
                # Assuming the message is a JSON string
                print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

                data = json.loads(msg.value().decode('utf-8'))
                data_collected.append(data)
                cms_category.add(data['transaction_category'])
                cms_amount_interval.add(data['amount_interval'])
                cms_currency.add(data['currency'])
                ams.update(data['amount'])
                hll.update(data['user_id'])

        threshold_frequency = 3.5 * max(cms_category.count(x['transaction_category']) for x in data_collected)
        threshold_variance = 3.5 * ams.estimate()
        threshold_cardinality = 3.5 * hll.estimate()

        while True:
                msg = consumer.poll(1.0)
                if msg is not None and not msg.error():
                    value = json.loads(msg.value().decode('utf-8'))
                    cms_category.add(value['transaction_category'])
                    cms_amount_interval.add(value['amount_interval'])
                    cms_currency.add(value['currency'])
                    ams.update(value['amount'])
                    hll.update(value['user_id'])

                    # Anomaly detection
                    if cms_category.count(value['transaction_category']) > threshold_frequency:
                        print(f"High frequency anomaly detected for category: {value['transaction_category']}")
                    if cms_amount_interval.count(value['amount_interval']) > threshold_frequency:
                        print(f"High frequency anomaly detected for amount interval: {value['amount_interval']}")
                    if cms_currency.count(value['currency']) > threshold_frequency:
                        print(f"High frequency anomaly detected for currency: {value['currency']}")                                            
                    if ams.estimate() > threshold_variance:
                        print("Variance anomaly detected")
                    if hll.estimate() > threshold_cardinality:
                        print("Cardinality anomaly detected")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
