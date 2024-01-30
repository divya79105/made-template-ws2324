import pandas as pd
import sqlite3
import urllib.request
import zipfile
import os

# Download the ZIP file
url = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
zip_path = "/mnt/data/GTFS.zip"
urllib.request.urlretrieve(url, zip_path)

# Extract stops.txt from the ZIP file
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extract("stops.txt", "/mnt/data")

# Load stops.txt into a pandas DataFrame
stops_path = "/mnt/data/stops.txt"
stops_df = pd.read_csv(stops_path, usecols=['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id'])

# Filter for stops in zone 2001
stops_df = stops_df[stops_df['zone_id'] == 2001]

# Validate data
# Ensure stop_name maintains German umlauts - this is inherently preserved by pandas read_csv
# Validate geographic coordinates for stop_lat and stop_lon
stops_df = stops_df[(stops_df['stop_lat'] >= -90) & (stops_df['stop_lat'] <= 90) &
                    (stops_df['stop_lon'] >= -90) & (stops_df['stop_lon'] <= 90)]

# Drop rows with invalid data (if any invalid data exists, it would have been filtered out by now)

# Create SQLite database and write data
conn = sqlite3.connect("/mnt/data/gtfs.sqlite")
stops_df.to_sql('stops', conn, if_exists='replace', index=False, dtype={
    'stop_id': 'TEXT',
    'stop_name': 'TEXT',
    'stop_lat': 'FLOAT',
    'stop_lon': 'FLOAT',
    'zone_id': 'BIGINT'
})

# Close connection to the database
conn.close()

# Print success message
print("GTFS data processing completed and stored in gtfs.sqlite database.")
