#!/usr/bin/env python

import sys
import json
import trip_transformation as transformation
import trip_assertions as assertions
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from datetime import datetime
import trip_load_into_db as db_load

BUFFER_SIZE = 300000

def assert_and_transform(msg_list):
    assertions.run_assertions(current_user, msg_list)
    df = transformation.run_transformation(current_user, msg_list)
    print("After transformation:\n", df)
    msg_list.clear()
    db_load.load_db(df, current_user)


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
    config.update(config_parser['consumer'])

    env_var = dict(config_parser['env'])
    current_user = env_var['user.name']
    
    # Create Consumer instance
    consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    topic = "trimet-instance3"
    consumer.subscribe([topic], on_assign=reset_offset)

    #Create & open file for output
    today_date = datetime.now().strftime("%Y_%m_%d")
    #file_path = f'/home/{current_user}/DataEng-TriMet-Project/Data_Files/output_file_{today_date}.json'
    #output_file = open(file_path, "a+")
    msg_count = 0
    msg_list = []
    
    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if msg_count > 0:
                    assert_and_transform(msg_list)
                    msg_count=0
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:                
                json_val = json.loads(msg.value())
                msg_list.append(json_val)
                msg_count += 1
                #Output data to a file.
                #output_file.write(msg.value().decode('utf-8') + '\n')
                if msg_count == BUFFER_SIZE:
                    assert_and_transform(msg_list)
                    msg_count=0

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        #output_file.close()
        consumer.close()
        if msg_count == BUFFER_SIZE:
            assert_and_transform(msg_list)
            msg_count=0
        
