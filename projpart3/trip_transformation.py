import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta

def run_transformation(current_user, msg_list):
    df = pd.DataFrame(msg_list) 
    print(df)

    #Drop the rows where trip_id has negative values
    df.drop(df[df['trip_id'].astype(int) < 0].index, axis=0, inplace=True)

    #drop the rows from a DataFrame where the route_id field is null or negative integers
    df.drop(df[(df['route_id'].isnull()) | (df['route_id'].astype(int) < 0)].index, inplace=True, axis=0)

    #drop the rows from a DataFrame where the vehicle_id field is null or negative integers
    df.drop(df[(df['vehicle_id'].isnull()) | (df['vehicle_id'].astype(int) < 0)].index, inplace=True, axis=0)

    #Map values in the service_key column to corresponding labels W, S, or U
    df['service_key'] = df['service_key'].apply(lambda x: ('Weekday' if x.strip() == 'W' else ('Saturday' if x.strip() == 'S' else ('Sunday' if x.strip() == 'U' else None))))

    #Map values in the direction column to corresponding labels in trip table 
    df['direction'] = df['direction'].apply(lambda x: 'Out' if x.strip() == '0' else ('Back' if x.strip() == '1' else None))

    #Drop un-needed columns
    df.drop(columns=["event_date"], inplace=True)

    return df 
