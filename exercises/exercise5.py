import pandas as pd
import sqlite3
import urllib.request
import zipfile
import os

# Step 1: Download GTFS Data
url = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
gtfs_zip_file = "GTFS.zip"
urllib.request.urlretrieve(url, gtfs_zip_file)

# Step 2: Unzip and Read Data
with zipfile.ZipFile(gtfs_zip_file, 'r') as zip_ref:
    zip_ref.extractall("GTFS_data")
stops_file = os.path.join("GTFS_data", "stops.txt")

# Step 3: Data Processing
stops_df = pd.read_csv(stops_file)
stops_df = stops_df[['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']]
stops_df = stops_df[stops_df['zone_id'] == 2001]
stops_df = stops_df[(stops_df['stop_lat'].between(-90, 90)) & (stops_df['stop_lon'].between(-90, 90))]

# Step 4: Write to SQLite Database
conn = sqlite3.connect('gtfs.sqlite')
stops_df.to_sql('stops', conn, if_exists='replace', index=False, dtype={
    'stop_id': 'BIGINT',
    'stop_name': 'TEXT',
    'stop_lat': 'FLOAT',
    'stop_lon': 'FLOAT',
    'zone_id': 'BIGINT'
})
conn.close()
