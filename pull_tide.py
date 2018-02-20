"""
This script pulls current tide level data from the NOAA API using the url shown. The values are extracted from the JSON
file and put in a dataframe. The data is then appended to the PostgreSQL database table with the connection and cursor.
"""

import pandas as pd
import requests
from sqlalchemy import create_engine
import json
from datetime import datetime

url = "https://tidesandcurrents.noaa.gov/api/datagetter?date=latest&station=8638610&product=water_level&datum=NAVD&units=english&time_zone=lst_ldt&format=json"
response = requests.get(url)
result = json.loads(response.text)
# print result

station_id = int(result['metadata']['id'])
station_name = str(result['metadata']['name'])
lat = float(result['metadata']['lat'])
lon = float(result['metadata']['lon'])
value = float(result['data'][0]['v'])
date_time_str = str(result['data'][0]['t'])
date_time = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M')

cols = ['station_id','station_name','latitude','longitude','date_time','value']
line = pd.DataFrame({'station_id': station_id,'station_name': station_name,'latitude': lat,
                     'longitude': lon,'date_time': date_time,'value': value}, columns=cols, index=[0])

engine = create_engine('postgresql+psycopg2://user:password@localhost:5432/tide_observations')  # put in user and password
conn = engine.raw_connection()

line.to_sql('tide', engine, if_exists='append', index=False)
# !!! put some exception here so that same tide value won't get appended if pulled for some reason

conn.close()