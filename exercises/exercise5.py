import pandas as pd
import sqlite3
from urllib.request import urlretrieve
import zipfile
from os.path import join, exists

# Define constants for URLs and file paths
GTFS_ZIP_URL = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
ZIP_FILE_PATH = "Downloaded_GTFS.zip"
EXTRACTION_FOLDER = "GTFS_Files"
DB_NAME = 'gtfs.sqlite'

# Step 1: Download the GTFS dataset
print("Downloading GTFS dataset...")
urlretrieve(GTFS_ZIP_URL, ZIP_FILE_PATH)

# Step 2: Extract the dataset
print("Extracting files...")
with zipfile.ZipFile(ZIP_FILE_PATH, 'r') as zip_file:
    zip_file.extractall(EXTRACTION_FOLDER)

# Step 3: Load and process the stops data
stops_data_path = join(EXTRACTION_FOLDER, "stops.txt")
print("Processing stop data...")
stops = pd.read_csv(stops_data_path)
stops_filtered = stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']]
stops_filtered = stops_filtered.query("zone_id == 2001 and -90 <= stop_lat <= 90 and -90 <= stop_lon <= 90")

# Step 4: Save processed data to an SQLite database
print("Saving data to database...")
with sqlite3.connect(gtfs.sqlite) as conn:
    stops_filtered.to_sql(name='stops', con=conn, if_exists='replace', index=False, dtype={
        'stop_id': 'INTEGER',
        'stop_name': 'TEXT',
        'stop_lat': 'REAL',
        'stop_lon': 'REAL',
        'zone_id': 'INTEGER'
    })

# Step 5: Verify the database file creation
if exists(DB_NAME):
    print(f"Success: Database '{DB_NAME}' has been created and is ready for use.")
else:
    print(f"Error: Failed to create the database file '{DB_NAME}'. Please check the file path and permissions.")

