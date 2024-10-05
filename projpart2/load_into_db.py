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
def dbconnect():
	connection = psycopg2.connect(
		host="localhost",
		database=DB_NAME,
		user=DB_USER,
		password=DB_PWD,
	)
	connection.autocommit = False
	print(f"connect_db done \n")
	return connection


# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def create_staging_table(conn):
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
		print(f"Created {STAGING_TABLE_NAME} \n")


def push_db(conn, df):
	with conn.cursor() as cursor:
		print(f"Dataframe consists of {len(df['trip_id'])} records...\n")
		start = time.perf_counter()
		TEMP_FILE = "temp.csv"
		df.to_csv(TEMP_FILE, index=False, header=False)
		file = open(TEMP_FILE, 'r')
		cursor.copy_from(file, 'staging', sep=",", columns=['tstamp', 'latitude', 'longitude', 'speed', 'trip_id', 'vehicle_id'])
		file.close()
		os.remove(TEMP_FILE)

		print("Copied temp.csv into staging table\n")

		cursor.execute(f"""
                INSERT INTO {TRIP_TABLE} (trip_id, vehicle_id) 
				SELECT DISTINCT trip_id, vehicle_id FROM staging
				ON CONFLICT (trip_id) DO NOTHING;				
        """)
		print(f"{TRIP_TABLE} table populated successfully\n")
	
		cursor.execute(f"""
				INSERT INTO {BREADCRUMB_TABLE} (tstamp, latitude, longitude, speed, trip_id) 
				SELECT tstamp, latitude, longitude, speed, trip_id FROM staging;
		""")
		print(f"{BREADCRUMB_TABLE} table populated successfully\n")

		cursor.execute(f"drop table {STAGING_TABLE_NAME} cascade;")
		print("Dropped staging table\n")
		conn.commit()
		elapsed = time.perf_counter() - start
		print(f"Loading successfully completed.\n")
		print(f"Elapsed Time: {elapsed:0.4} seconds\n")


def load_db(df, current_user):
	today_date = datetime.now().strftime("%Y_%m_%d")
	conn = dbconnect()
	create_staging_table(conn)
	push_db(conn, df)