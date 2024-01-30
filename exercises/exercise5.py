import pandas as pd
import sqlite3
import urllib.request
import zipfile
import os

# Define paths
zip_path = "/mnt/data/GTFS.zip"
extracted_dir = "/mnt/data/"
database_path = "/mnt/data/gtfs.sqlite"

# Step 1: Download the ZIP file
url = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
print("Downloading GTFS.zip...")
urllib.request.urlretrieve(url, zip_path)
print("Download completed.")

# Step 2: Extract stops.txt from the ZIP file
print("Extracting stops.txt from GTFS.zip...")
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extract("stops.txt", extracted_dir)
print("Extraction completed.")

# Step 3: Load stops.txt into a pandas DataFrame
stops_path = os.path.join(extracted_dir, "stops.txt")
stops_df = pd.read_csv(stops_path, usecols=['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id'])
print("Loaded stops data.")

# Step 4: Filter and validate data
# Filter for stops in zone 2001
stops_df = stops_df[stops_df['zone_id'] == 2001]

# Validate geographic coordinates for stop_lat and stop_lon
stops_df = stops_df[(stops_df['stop_lat'] >= -90) & (stops_df['stop_lat'] <= 90) &
                    (stops_df['stop_lon'] >= -90) & (stops_df['stop_lon'] <= 90)]
print(f"Filtered and validated data. Rows remaining: {len(stops_df)}")

# Step 5: Write data into SQLite database
print("Writing data to SQLite database...")
conn = sqlite3.connect(database_path)
stops_df.to_sql('stops', conn, if_exists='replace', index=False, dtype={
    'stop_id': 'TEXT',
    'stop_name': 'TEXT',
    'stop_lat': 'FLOAT',
    'stop_lon': 'FLOAT',
    'zone_id': 'BIGINT'
})
conn.close()
print(f"GTFS data processing completed and stored in {database_path}.")
