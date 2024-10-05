# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import time
import psycopg2
from datetime import datetime
import argparse
import re
import csv
import os

DB_NAME = "postgres"
DB_USER = "postgres"
DB_PWD = "wranglers"
STAGING_TABLE_NAME = "staging"
TRIP_TABLE = "trip"
BREADCRUMB_TABLE = "breadcrumb"


# connect to the database
def dbconnect(log_file):
	connection = psycopg2.connect(
		host="localhost",
		database=DB_NAME,
		user=DB_USER,
		password=DB_PWD,
	)
	connection.autocommit = False
	log_file.write(f"connect_db done \n")
	return connection


# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def create_staging_table(conn, log_file):
	with conn.cursor() as cursor:
		cursor.execute(f"""
		    DROP TABLE IF EXISTS {STAGING_TABLE_NAME};
			CREATE UNLOGGED TABLE {STAGING_TABLE_NAME} (
			    tstamp timestamp,
        		latitude float,
        		longitude float,
        		speed float,
        		trip_id integer,
				route_id integer NULL,
        		vehicle_id integer,
        		service_key service_type NULL,
        		direction tripdir_type NULL			
            );
        """)
		log_file.write(f"Created {STAGING_TABLE_NAME} \n")


def push_db(conn, df, log_file):
	with conn.cursor() as cursor:
		log_file.write(f"Dataframe consists of {len(df['trip_id'])} records...\n")
		start = time.perf_counter()
		TEMP_FILE = "temp.csv"
		df.to_csv(TEMP_FILE, index=False, header=False)
		file = open(TEMP_FILE, 'r')
		cursor.copy_from(file, 'staging', sep=",", columns=['tstamp', 'latitude', 'longitude', 'speed', 'trip_id', 'vehicle_id'])
		file.close()
		os.remove(TEMP_FILE)

		log_file.write("Copied temp.csv into staging table\n")

		cursor.execute(f"""
                INSERT INTO {TRIP_TABLE} (trip_id, vehicle_id) 
				SELECT DISTINCT trip_id, vehicle_id FROM staging
				ON CONFLICT (trip_id) DO NOTHING;				
        """)
		log_file.write(f"{TRIP_TABLE} table populated successfully\n")
	
		cursor.execute(f"""
				INSERT INTO {BREADCRUMB_TABLE} (tstamp, latitude, longitude, speed, trip_id) 
				SELECT tstamp, latitude, longitude, speed, trip_id FROM staging;
		""")
		log_file.write(f"{BREADCRUMB_TABLE} table populated successfully\n")

		cursor.execute(f"drop table {STAGING_TABLE_NAME} cascade;")
		log_file.write("Dropped staging table\n")
		conn.commit()
		elapsed = time.perf_counter() - start
		log_file.write(f"Loading successfully completed.\n")
		log_file.write(f"Elapsed Time: {elapsed:0.4} seconds\n")


def load_db(df, current_user):
	#Open Database_Load_Log.txt to write loading results.
	file_path = f'/home/{current_user}/DataEng-TriMet-Project/Data_Files/Database_Load_Log.txt'
	log_file = open(file_path, "a+")
	today_date = datetime.now().strftime("%Y_%m_%d")
	log_file.write(f"\n\n{today_date} \n")
	conn = dbconnect(log_file)
	create_staging_table(conn, log_file)
	push_db(conn, df, log_file)