#!/usr/bin/env python

import sys
from confluent_kafka import Producer

import json
import time
import random

# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
def delivery_callback(err, msg):
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

# Produce data by selecting random values from these lists
        
def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

def get_amount_interval(amount):
    if amount <= 50:
        return '0-50'
    elif amount <= 100:
        return '51-100'
    elif amount <= 200:
        return '101-200'
    elif amount <= 500:
        return '201-500'
    elif amount <= 1000:
        return '501-1000'
    elif amount <= 5000:
        return '1001-5000'
    else:
        return 'Over 5000'
    

def generate_transaction():
    """Simulate generating a financial transaction."""
    currencies = [
    "USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "CNY", "SEK", "NZD",
    "MXN", "SGD", "HKD", "NOK", "KRW", "TRY", "RUB", "INR", "BRL", "ZAR",
    "PHP", "CZK", "PLN", "THB", "IDR", "HUF", "ILS", "DKK", "MYR", "SAR",
    "TWD", "CLP", "PEN", "COP", "ZAR", "AED", "ARS", "QAR", "RON", "EGP",
    "NGN", "PKR", "DZD", "KES", "UYU", "VND", "BGN", "HRK", "LBP", "OMR",
    "UAH", "MAD", "JOD", "BHD", "XOF", "LKR", "UZS", "BDT", "TZS", "IQD",
    "PAB", "ETH", "CRC", "TZS", "GTQ", "MZN", "AZN", "KHR", "BYN", "LAK",
    "MOP", "PYG", "LYD", "ISK", "HNL", "JMD", "AMW", "RSD", "TND", "BWP",
    "SYP", "UZS", "BIF", "YER", "MGA", "ANG", "ETB", "SDG", "MNT", "PGK",
    "XAF", "SCR", "NAD", "AFN", "NPR", "GIP", "MDL", "FJD", "MVR", "MWK",
    "KGS", "GEL", "XCD", "MKD", "ALL", "SBD", "AMD", "SLL", "SOS", "BBD",
    "AWG", "TJS", "BTN", "KZT", "SVC", "BSD", "HTG", "BMD", "KWD", "RWF",
    "MMK", "BZD", "GNF", "KYD", "MUR", "LSL", "LRD", "CVE", "DJF", "SZL",
    "GMD", "SHP", "VUV", "WST", "IQD", "TMT", "TMT", "TOP", "VEF", "SOS",
    "CUC", "ZWL", "SRD", "MOP", "FKP", "ERN", "SPL", "TVD", "IMP", "GGP",
    "JEP", "KMF", "FKP", "STN", "XPF", "MRU", "SVC", "ZMW", "GHS", "GGP",
    "SSP", "CUC", "KPW", "ERN", "VES", "CHE", "CHW", "CLF", "CNH", "XDR"
    ]

    transaction_categories = [
    "Groceries",
    "Utilities",
    "Rent",
    "Mortgage",
    "Dining Out",
    "Fast Food",
    "Public Transportation",
    "Gas/Fuel",
    "Vehicle Maintenance",
    "Clothing",
    "Healthcare",
    "Pharmacy",
    "Entertainment",
    "Travel",
    "Hotels",
    "Airfare",
    "Gifts",
    "Charity",
    "Electronics",
    "Books",
    "Education",
    "Personal Care",
    "Fitness",
    "Furniture",
    "Home Improvement",
    "Gardening",
    "Pet Supplies",
    "Subscriptions",
    "Insurance",
    "Taxes"
    ]

    rand_amount = round(random.paretovariate(2.7)*64, 2)


    return {
        'user_id': random.randint(1, 1500),
        'transaction_id': random.randint(1000, 99999),
        'transaction_category': random.choice(transaction_categories),
        'amount': rand_amount,
        'amount_interval': get_amount_interval(rand_amount),
        'currency': random.choice(currencies),
        'timestamp': time.time()
    }


def generate_anomalous_transaction():

    transaction_categories = [
    "Groceries",
    "Utilities",
    "Rent",
    "Mortgage"]
        
    sus_rand_amount = round(random.randint(5000,10000), 2)

    return {
        'user_id': random.randint(1500, 10000),
        'transaction_id': random.randint(1000, 99999),
        'transaction_category': random.choice(transaction_categories),
        'amount': sus_rand_amount,
        'amount_interval': get_amount_interval(sus_rand_amount),
        'currency': random.choice(['XDR', 'CHW']),
        'timestamp': time.time()
    }

def produce(config, topic, max_transactions=200):
    """Produces sample financial transactions to a given Kafka topic."""
    config = config
    producer = Producer(config)

    count = 0
    while count < max_transactions:
        transaction = generate_transaction()
        key = str(transaction['transaction_id'])
        value = json.dumps(transaction)

        producer.produce(topic, key=key, value=value, callback=delivery_callback)
        producer.poll(0)
        time.sleep(0)

        count += 1
    
    while count >= max_transactions:
        transaction = generate_anomalous_transaction()
        key = str(transaction['transaction_id'])
        value = json.dumps(transaction)

        producer.produce(topic, key=key, value=value, callback=delivery_callback)
        producer.poll(0)
        time.sleep(0)
        count += 1

        if count == 1300:
            break
        


    # Block until all messages are sent
    producer.flush()