# -*- coding: utf-8 -*-

#Import required packages
import pandas as pd
import numpy as np
import re
import matplotlib.pyplot as plt
import seaborn as sns
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import datetime

def gather_trip_data(current_user):
  #Specify URL containing TriMet Data
  url = "http://www.psudataeng.com:8000/getStopEvents"
  html = urlopen(url)

  #Create a Beautiful Soup object from the html of URL.
  soup = BeautifulSoup(html, 'lxml')
  type(soup)

  #Get the DATE from the website. 
  date_header = soup.find_all('h1')
  date_header = str(date_header)

  #Use regex pattern to extract date. 
  pattern = r'\d{4}-\d{2}-\d{2}'
  match_date = re.search(pattern, date_header)

  #If match was found, return date, otherwise fill in filler date
  if(match_date):
    date = match_date.group(0)
  else:
    date = '0000-00-00'

  #Get STOP EVENT number
  stop_events = soup.find_all('h2')
  pattern = '-?\d+'

  list_events = []
  list_data = []

  for event in stop_events:
    #Get related table to event
    table = event.find_next_sibling("table")

    #Clean Event_ID
    event = str(event)
    clean = re.compile('<.*?>')
    clean2 = (re.sub(clean, '', event))
    stop = re.findall(pattern , clean2)
    
    #Table Level - find each row
    rows = table.find_all('tr')

    for row in rows:
      row_td = row.find_all('td')
      str_cells = str(row_td)  
      clean_text = BeautifulSoup(str_cells, "lxml").get_text()
      clean_text += ', ' + stop[0]
      list_data.append(clean_text)

  #Get data into dataframe
  df = pd.DataFrame(list_data)
  df1 = df[0].str.split(',', expand=True)
  df1[0] = df1[0].str.strip('[')
  df1[0] = df1[0].str.strip(']')
  df1[23] = df1[0].str.strip(']')

  #Get data headers
  col_labels = soup.find('tr')

  all_header = []

  for col in col_labels:
      col = str(col)
      cleantext = BeautifulSoup(col, "lxml").get_text()
      all_header.append(cleantext)

  list_header = []
  list_header.append(all_header)

  df2 = pd.DataFrame(list_header)

  #Concatenate two dataframes and drop rows with no data.
  frames = [df2, df1]
  df3 = pd.concat(frames)

  #Reconfigure DataFrame so first row is headers
  df4 = df3.rename(columns=df3.iloc[0])
  df4.rename(columns={df4.columns[24]: "trip_id"}, inplace=True)

  #Drop rows with NA for route number.
  df5 = df4.dropna(subset=['route_number'])

  #Remove first row
  df5 = df5.drop(0)
  df5 = df5.reset_index(drop=True)

  #Drop unnecessary columns
  columns_to_keep = ['trip_id', 'route_number', 'vehicle_number', 'service_key', 'direction']
  df6 = df5[columns_to_keep]

  #Rename Columns
  new_column_names = {'route_number': 'route_id', 'vehicle_number': 'vehicle_id'}
  df_clean = df6.rename(columns=new_column_names)

  #Append date to dataframe as column
  df_clean['event_date'] = date

  #Remove duplicate rows
  df_final = df_clean.drop_duplicates()
  df_final = df_final.reset_index(drop=True)
 
  #Create file where data will be saved.
  today_date = datetime.now().strftime("%Y_%m_%d")
  filepath = f'/home/{current_user}/DataEng-TriMet-Project/Data_Files/trip_data_{today_date}.json'
  output_file = open(filepath, "w+")
  output_file.close()

  # Strip whitespace from records in the original dataframe
  df_final = df_final.applymap(lambda x: x.strip() if isinstance(x, str) else x)

  #Save dataframe as a json file.
  df_final.to_json(filepath, orient='records', indent=0)
  return filepath
