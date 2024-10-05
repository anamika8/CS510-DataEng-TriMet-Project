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
  file_path = f'/home/{current_user}/DataEng-TriMet-Project/Data_Files/Trip_Validation_Log.txt'
  log_file = open(file_path, "a+")

  #Write todays date in file for new entry
  log_file.write(f"{today_date} '\n'")


  #ASSERTION #1
  #Trip_id field exists and is a 9 digit integer for all records
  assertion1 = "Trip_id field exists and is a 9 digit integer for all records"

  try: 
    # Assert that the 'trip_id' column exists in the DataFrame
    assert 'trip_id' in df.columns, f"{assertion1}: FAIL"
    # Assert that all values in the 'trip_id' column are 9-digit integers
    assert df['trip_id'].apply(lambda x: len(str(x))==9).all(), f"{assertion1}: FAIL"
  except AssertionError as err:
    log_file.write(f"{assertion1} - FAILED '\n'")
    fail_count += 1

   #ASSERTION #2
   #All route_id are non-null, non-negative integers
    assertion2 = "All route_id are non-null, non-negative integers"
    
    try:
        assert "route_id" in df.columns, f"{assertion2}: FAIL"
        assert df["route_id"].notnull().all(), f"{assertion2}: FAIL"
        assert df["route_id"].dtype == int, f"{assertion2}: FAIL"       
        assert (df["route_id"] >= 0).all(), f"{assertion2}: FAIL"
    except AssertionError as err:
        log_file.write(f"{assertion2} - FAILED '\n'")
        fail_count += 1

   #ASSERTION #3
   #All route_id are non-null, non-negative integers
    assertion3 = "All vehicle_id are non-null, non-negative integers"
    
    try:
        assert "vehicle_id" in df.columns, f"{assertion3}: FAIL"
        assert df["vehicle_id"].notnull().all(), f"{assertion3}: FAIL"
        assert df["vehicle_id"].dtype == int, f"{assertion3}: FAIL"
        assert (df["vehicle_id"] >= 0).all(), f"{assertion3}: FAIL"
    except AssertionError as err:
        log_file.write(f"{assertion3} - FAILED '\n'")
        fail_count += 1

   #ASSERTION #4
   #All service_key fields are filled with values of either W, S, or U
    assertion4 = "All service_key fields are filled with values of either W, S, or U"
    allowed = {'W', 'S', 'U'}  # Use set for faster membership check
    
    try:
        assert "service_key" in df.columns, f"{assertion4}: FAIL"
        assert df["service_key"].notnull().all(), f"{assertion4}: FAIL"
        assert df["service_key"].apply(lambda x: x in allowed).all(), f"{assertion4}: FAIL"
    except AssertionError as err:
        log_file.write(f"{assertion4} - FAILED '\n'")
        fail_count += 1
   
   #ASSERTION #5
   #All directions are either 0 or 1
    assertion5 = "All directions are either 0 or 1"
    allowed = {'0', '1'}  # Use set for faster membership check
    
    try:
        assert "direction" in df.columns, f"{assertion5}: FAIL"
        df.dropna(subset=["direction"], inplace=True)
        assert df["direction"].apply(lambda x: x in allowed or x is None).all(), f"{assertion5}: FAIL"
    except AssertionError as err:
       log_file.write(f"{assertion5} - FAILED '\n'")
       fail_count += 1

  if fail_count == 0:
    log_file.write("ALL ASSERTIONS PASSED")
    log_file.write("\n")


  #Close files
  log_file.close()
