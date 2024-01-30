import pandas as pd
import sqlite3
import urllib.request
import zipfile
import os

# Initial step: Fetching GTFS dataset from the specified URL
download_url = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
local_zip_filename = "GTFS_Dataset.zip"
urllib.request.urlretrieve(download_url, local_zip_filename)

# Extracting files from the downloaded ZIP archive
with zipfile.ZipFile(local_zip_filename, 'r') as zip_file:
    zip_file.extractall("Extracted_GTFS")
stops_txt_path = os.path.join("Extracted_GTFS", "stops.txt")

# Processing the data from stops.txt
data_stops = pd.read_csv(stops_txt_path)
filtered_columns = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']
data_stops = data_stops.loc[data_stops['zone_id'] == 2001]
data_stops = data_stops[(data_stops['stop_lat'] >= -90) & (data_stops['stop_lat'] <= 90) & 
                        (data_stops['stop_lon'] >= -90) & (data_stops['stop_lon'] <= 90)]

# Storing the processed data into an SQLite database
database_connection = sqlite3.connect('gtfs_database.sqlite')
data_stops.to_sql(name='filtered_stops', con=database_connection, if_exists='replace', index=False, dtype={
    'stop_id': 'INTEGER',
    'stop_name': 'TEXT',
    'stop_lat': 'REAL',
    'stop_lon': 'REAL',
    'zone_id': 'INTEGER'
})
database_connection.close()
