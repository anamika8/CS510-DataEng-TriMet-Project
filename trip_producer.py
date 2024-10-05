#!/usr/bin/env python3
from urllib.request import urlopen
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from datetime import datetime
import json  
import email_alert
import trip_data_collection

if __name__ == '__main__':

    #Adding Execution started command
    print("Starting Execution")
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    env_var = dict(config_parser['env'])
    current_user = env_var['user.name']
    email_username = env_var['email.username']
    email_password = env_var['email.password']
    email_receivers = env_var['email.receivers']

    # Calling trip_data_collection to create the JSON file
    filepath_load = trip_data_collection.gather_trip_data(current_user)
    
    #Create & open file for output
    date_time_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    file_path = '/home/' + current_user + '/DataEng-TriMet-Project/Data_Files/Trip_MetaData.txt'
    output_file = open(file_path, "a+")
   
    #Open the JSON file and read its contents
    with open(filepath_load) as json_file:
        json_data = json_file.read()

    #Parse the JSON data into a Python object
    data = json.loads(json_data)
    sensor_data_size = len(data)

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    num_errors = 0
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
            num_errors = 1

    # Produce data by selecting random values from these lists.
    topic = "trimet-instance3"
    

    count = 0
    for record in data:
        each_json = json.dumps(record)
        producer.produce(topic, each_json, str(count), callback=delivery_callback)
        count += 1
        if(count % 10000 == 0):
            producer.flush()
    
    producer.flush()
    print("Execution Finished")
    
    #Get stop time
    date_time_stop = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    #Trigger email send if error occurs
    if(num_errors == 1):
        subject = "ALERT2: Data Producer Failed"
        body = "The producer failed to run or finish running."
        email_alert.send(email_username, email_password, email_receivers, subject, body)
    else:
        # Trigger email with the same info written in file
        today_date = datetime.now().strftime("%Y_%m_%d")
        subject = "[Success]: Data Producer Completed Running for - " + today_date
        body = """
            Start:                 {0}
            End  :                 {1}
            Num of Sensor Records: {2}
            Num Messages:          {3}
        """.format(date_time_start, date_time_stop, str(sensor_data_size), str(count))
        email_alert.send(email_username, email_password, email_receivers, subject, body)
    
    #Output meta data to file.:w
    output_file.write("Start: " + date_time_start + '\n')
    output_file.write("Stop: " + date_time_stop + '\n')
    output_file.write("Num Sensor Records: " + str(sensor_data_size) + '\n')
    output_file.write("Num Messages: " + str(count) + '\n')
    output_file.write('\n' + '\n')
    output_file.close()