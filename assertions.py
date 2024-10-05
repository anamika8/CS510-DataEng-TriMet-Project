#!/usr/bin/env python3

#Import libraries.
import pandas as pd
import numpy as np
from datetime import datetime
import json
from argparse import ArgumentParser, FileType
from configparser import ConfigParser

#Run Assertions function takes in user name variable as an argument from consumer.py file. 
#This function runs all data assertions and outputs the failures to Data_Files/Validation_Log.txt
def run_assertions(current_user, msg_list):
	
  df = pd.DataFrame(msg_list)
  #print(df)
  #Track number of failed assertions
  fail_count = 0

  #Create date variable for reading and writing data.
  today_date = datetime.now().strftime("%Y_%m_%d")

  #Open Data_Assertions_Log.txt to write assertion results.
  file_path = f'/home/{current_user}/DataEng-TriMet-Project/Data_Files/Validation_Log.txt'
  log_file = open(file_path, "a+")

  #Write todays date in file for new entry
  log_file.write(f"{today_date} '\n'")

    #ASSERTION #1
    #ACT_TIME field is not empty for all records.
    #Act time is required to know when the data was collected. Without the act time, we cannot use that record. 
  assertion1 = "Assertion: ACT_TIME has a value for all records"

  count_null_time = (df["ACT_TIME"] == '').sum()

  if count_null_time != 0:
    log_file.write(f"{assertion1} - FAILED '\n'")
    fail_count += 1


  #ASSERTION #2
    #Each EVENT_NO TRIP should have at least 1 EVENT_NO_STOP
    #If a trip has no stop, then it is not carrying passengers or there is an error in the data.

  assertion2 = "Assertion: Each EVENT_NO TRIP should have at least 1 EVENT_NO_STOP"

  stops_by_trip = df.groupby(['EVENT_NO_TRIP']).nunique()[['EVENT_NO_STOP']]
  count_no_stop = (stops_by_trip["EVENT_NO_STOP"] == 0).sum()

  if count_no_stop != 0:
    log_file.write(f"{assertion2} - FAILED '\n'")
    fail_count += 1


  #ASSERTION #3
    #METERS should not exceed 500,000 meters (310 miles)  on any day for any bus on any one trip.
    #This would indicate an error in the data that means the data is not reliable. 
  assertion3 = "Assertion: METERS should not exceed 500,000 meters (310 miles) on any day for any bus on any one trip"

  distance_per_trip = df.groupby(['EVENT_NO_TRIP', 'VEHICLE_ID'])['METERS'].max()
  #distance_per_trip

  count_assertion_trips = (distance_per_trip.iloc[3] > 500000).sum()

  if count_assertion_trips != 0:
    log_file.write(f"{assertion3} - FAILED '\n'")
    fail_count += 1


  #ASSERTION #4
    #GPS coordinates should be within the bounds of the Portland metro area (need to find these GPS coordinates).
  assertion4 = "Assertion: latitude and longitude coordinates are within the bounds of the Portland metro area"

  lat_long_df = df[(df['GPS_LATITUDE'] != '') & (df['GPS_LONGITUDE'] != '')]

  lat_max = float(lat_long_df['GPS_LATITUDE'].max())
  lat_min = float(lat_long_df['GPS_LATITUDE'].min())
  long_max = float(lat_long_df['GPS_LONGITUDE'].max())
  long_min = float(lat_long_df['GPS_LONGITUDE'].min())

  if ((lat_max > 46) & (lat_min < 45) & (long_min < -123.5) & (long_max > -122)):
    log_file.write(f"{assertion4} - FAILED '\n'")
    fail_count += 1


  #ASSERTION #5
    #Each EVENT_NO_TRIP should have only 1 VEHICLE_ID
  assertion5 = "Assertion: Each EVENT_NO_TRIP should have only 1 VEHICLE_ID"

  vehicle_by_trip = df.groupby(['EVENT_NO_TRIP']).nunique()[['VEHICLE_ID']]
  duplicate_vehicle = (vehicle_by_trip['VEHICLE_ID'] != 1).sum()

  if duplicate_vehicle != 0:
    log_file.write(f"{assertion5} - FAILED '\n'")
    fail_count += 1


  #ASSERTION #6
    #METERS field exists and is greater than or equal to 0 for all records. 
  assertion6 = "Assertion: METERS field exists and is greater than or equal to 0 for all records"

    #The 'METERS'column exists sand is greater than or equal to 0
  if 'METERS' not in df.columns or (df['METERS'] < 0).all(): 					###CHECK THIS CHANGE
    #do nothing
    log_file.write(f"{assertion6} - FAILED '\n'")
    fail_count += 1


  #ASSERTION #7
    #GPS_HDOP field is a positive floating point number.
  assertion7 = "Assertion: GPS_HDOP field is a positive floating point number"

  df['GPS_HDOP'] = pd.to_numeric(df['GPS_HDOP'])
  count_HDOP_errors = (df['GPS_HDOP'] <= 0).sum()

  if count_HDOP_errors != 0:
    log_file.write(f"{assertion7} - FAILED '\n'")
    fail_count += 1


  #ASSERTION #8
    #The OPD_DATE is not in the future & the year is not before 2022 or after 2023.
  assertion8 = "Assertion: The OPD_DATE is not in the future & the year is not before 2022 or after 2023"

    # Extract the year from the 'OPD_DATE' column
  opd_date_df = pd.to_datetime(df['OPD_DATE'], format='%d%b%Y:%H:%M:%S').dt.year

  if ((opd_date_df < 2022).any() or (opd_date_df > 2023).any()):
    log_file.write(f"{assertion8} - FAILED '\n'")
    fail_count += 1


  #ASSERTION #9
    #VEHICLE_ID field exists, and no non-numeric field will exist for all records.
  assertion9 = "Assertion: VEHICLE_ID field exists, and no non-numeric field will exist for all records"

    #Check that all values in the column are numeric
  check_numeric = pd.to_numeric(df['VEHICLE_ID'], errors='coerce').notnull().all()

  if ('VEHICLE_ID' not in df.columns or not check_numeric):						###CHECK THIS CHANGE
    log_file.write(f"{assertion9} - FAILED '\n'")
    fail_count += 1


  #ASSERTION #10
    #Check that OPD_DATE, ACT_TIME, and VEHICLE_ID fields are not null for all records
  assertion10 = "Assertion: Check that OPD_DATE, ACT_TIME, and VEHICLE_ID fields are not null for all records"

    # Define the column names to check
  column_names = ['OPD_DATE', 'ACT_TIME', 'VEHICLE_ID']
  try:
  # Loop through each column and check if it exists and has no null values
    for col in column_names:
      assert col in df.columns, f"{assertion10}: FAILED"
      assert df[col].notnull().all(), f"{assertion10}: FAILED"
  except AssertionError as err:
    log_file.write(err)
    log_file.write('\n')
    fail_count += 1


  #ASSERTION #11
    #EVENT_NO_TRIP field exists and is a 9 digit integer for all records
  assertion11 = "EVENT_NO_TRIP field exists and is a 9 digit integer for all records"

  try: 
    # Assert that the 'EVENT_NO_TRIP' column exists in the DataFrame
    assert 'EVENT_NO_TRIP' in df.columns, f"{assertion11}: FAIL"
    # Assert that all values in the 'EVENT_NO_TRIP' column are 9-digit integers
    assert df['EVENT_NO_TRIP'].apply(lambda x: isinstance(x, int) and len(str(x))==9).all(), f"{assertion11}: FAIL"
  except AssertionError as err:
    log_file.write(f"{assertion11} - FAILED '\n'")
    fail_count += 1


  #ASSERTION #12
    #EVENT_NO_STOP field exists and is a 9 digit integer for all records
  assertion12 = "EVENT_NO_STOP field exists and is a 9 digit integer for all records"

  try: 
    # Assert that the 'EVENT_NO_STOP' column exists in the DataFrame
    assert 'EVENT_NO_STOP' in df.columns, f"{assertion12}: FAIL"
    # Assert that all values in the 'EVENT_NO_TRIP' column are 9-digit integers
    assert df['EVENT_NO_STOP'].apply(lambda x: isinstance(x, int) and len(str(x))==9).all(), f"{assertion12}: FAIL"
  except AssertionError as err:
    log_file.write(f"{assertion12} - FAILED '\n'")
    fail_count += 1



  #ASSERTION #13
    #GPS_LATITUDE coordinate exists for all records
  assertion13 = "GPS_LATITUDE coordinate exists for all records"

    # check if the GPS_LATITUDE column exists for all records
  if not df['GPS_LATITUDE'].notnull().all():							###CHECK THIS
    log_file.write(f"{assertion13} - FAILED '\n'")
    fail_count += 1


  #ASSERTION #14
    #GPS_LONGITUDE  coordinate exists for all records
  assertion14 = "GPS_LONGITUDE  coordinate exists for all records"

    # check if the GPS_LONGITUDE column exists for all records			###CHECK THIS
  if not df['GPS_LONGITUDE'].notnull().all():
    log_file.write(f"{assertion14} - FAILED '\n'")
    fail_count += 1


  #ASSERTION #15
    #Check that ACT_TIME contains a seconds value between 0 to 36 hours
  assertion15 = "Check that ACT_TIME contains a seconds value between 0 to 36 hours"

    # convert the ACT_TIME column to a timedelta object
  df['ACT_TIME'] = pd.to_timedelta(df['ACT_TIME'], unit='s')

    # check if all ACT_TIME values are between 0 and 36 hours
  if ((df['ACT_TIME'] < pd.Timedelta(0)).all()) or ((df['ACT_TIME'] > pd.Timedelta(hours=36)).all()):
    log_file.write(f"{assertion9} - FAILED '\n'")
    fail_count += 1


  if fail_count == 0:
    log_file.write("ALL ASSERTIONS PASSED")
    log_file.write('\n')


  #Close files
  log_file.close()
