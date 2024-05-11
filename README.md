# Sketch_Based_Anomaly_Detection

 Adversarially Robust Sketch-Based Anomaly Detection System for Data Streams Using Apache Kafka, CMS, AMS and HyperLogLog


# Kafka Financial Transactions Anomaly Detection

This Python project demonstrates a practical implementation of anomaly detection in financial transactions using Apache Kafka. It includes Kafka producer and consumer scripts that simulate transaction generation, transmit them over Kafka, and process them to detect anomalies based on frequency, variance, and cardinality.

## Prerequisites

- Python 3.6+
- Apache Kafka
- Confluent Kafka Python client

## Installation

Clone the repository and navigate into the project directory:

```bash
git clone [your-repository-url]
cd kafka
'''

Set up a Python virtual environment and activate it:

'''
python -m venv .venv
source .venv/bin/activate
'''

Install the required Python dependencies:
'''
pip install -r requirements.txt
'''


## Configuration
Edit `client.properties` to configure the Kafka server details and `getting_started.ini` for initial settings. The configuration files must be set up according to your Kafka setup.

## Running the Application
Run the main producer and consumer scripts concurrently to simulate the transaction flow and anomaly detection:

### Producer
To start the transaction producer, run (on windows):

'''
python main.py getting_started.ini
'''


### Consumer

To start the transaction consumer and begin anomaly detection, run:

'''
python consumer.py getting_started.ini --reset
'''
The --reset flag resets the consumer's offset to the beginning of the topic.

### Usage
The application simulates random financial transactions and checks for anomalies based on pre-defined thresholds. It uses sketches and other statistical algorithms to perform efficient anomaly detection on streaming data.

License
This project is licensed under the MIT License - see the LICENSE.md file for details.