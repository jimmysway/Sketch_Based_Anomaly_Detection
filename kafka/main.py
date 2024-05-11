from confluent_kafka import Producer, Consumer, OFFSET_BEGINNING

import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from producer import produce, read_config


if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])


    def main():
      topic = "python"
      produce(config, topic, max_transactions=1000)

    main()