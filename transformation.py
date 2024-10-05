import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta

def run_transformation(current_user, msg_list):
    df = pd.DataFrame(msg_list) 
    
    #Delete rows that are missing GPS_LATITUDE or GPS_LONGITUDE data
    df = df.dropna(subset=['GPS_LONGITUDE', 'GPS_LATITUDE'])

    #Make Timestamp from OPD_DATE and ACT_TIME
    def decodeTimestamp(opd_d, act_t):
        base = datetime.strptime(opd_d, '%d%b%Y:%H:%M:%S')
        delta = timedelta(seconds=act_t)
        finished_timestamp = base + delta
        return finished_timestamp

    #Apply timestamp function to every row in data frame.
    df["tstamp"] = df.apply(lambda x: decodeTimestamp(opd_d = x['OPD_DATE'], act_t=x['ACT_TIME']), axis=1)

    #Drop Un-needed columns
    df.drop(columns=["OPD_DATE", "ACT_TIME", "GPS_SATELLITES", "GPS_HDOP"], inplace=True)

    #Calculate the speed in Meters Per Hour
    df["Diff_Time"] = df['tstamp'].diff()
    df["Diff_Meters"] = df["METERS"].diff()

    #Extrapolate beginning values for diff_speed and diff_meters
    df.loc[0,"Diff_Time"] = df.loc[1, "Diff_Time"]
    df.loc[0,"Diff_Meters"] = df.loc[1, "Diff_Meters"]

    #Sort data by trip_id and then by timestamp.
    df = df.sort_values(['EVENT_NO_TRIP', 'tstamp'], ascending=[True, True])

    # Identify the first row index of each trip_id
    first_rows = df.groupby('EVENT_NO_TRIP').head(1).index

    # Set Diff_Meters value to 0 for the identified first rows
    df.loc[first_rows, 'Diff_Meters'] = 0

    #Function to calculate the speed in Meters Per Hour
    def calcSpeed(d_time, meters):
        #Convert timestamp difference to seconds
        seconds = d_time.total_seconds()

        speed =  meters / seconds
        speed = round(speed, 3)
        return speed


    #Apply speed function to all rows in data frame.
    df["speed"] = df.apply(lambda x: calcSpeed(d_time = x['Diff_Time'], meters=x['Diff_Meters']), axis=1)

    # Identify rows with negative speed
    negative_speed_rows = df[df['speed'] < 0].index

    # Drop rows with negative speed
    df.drop(negative_speed_rows, inplace=True)

    #Drop un-needed columns
    df.drop(columns=["EVENT_NO_STOP", "METERS", "Diff_Time", "Diff_Meters"], inplace=True)

    #Rename Columns
    df = df.rename(columns={'EVENT_NO_TRIP':'trip_id','VEHICLE_ID': 'vehicle_id','GPS_LONGITUDE': 'longitude', 'GPS_LATITUDE':'latitude'})

    #Reorder Data Frame Columns
    df = df[['tstamp', 'latitude', 'longitude', 'speed', 'trip_id','vehicle_id']]
    return df 
