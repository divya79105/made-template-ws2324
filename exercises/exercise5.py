import pandas as pd
import sqlite3
from urllib.request import urlretrieve
import zipfile
from os.path import join, exists

# Constants
GTFS_ZIP_URL = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
ZIP_FILE_PATH = "Downloaded_GTFS.zip"
EXTRACTION_FOLDER = "GTFS_Files"
DB_FILE_PATH = 'gtfs.sqlite'

# Download GTFS data
print("Downloading GTFS dataset...")
try:
    urlretrieve(GTFS_ZIP_URL, ZIP_FILE_PATH)
    print("Download completed.")
except Exception as e:
    print(f"Failed to download the file: {e}")
    raise

# Extract the dataset
print("Extracting GTFS dataset...")
try:
    with zipfile.ZipFile(ZIP_FILE_PATH, 'r') as zip_ref:
        zip_ref.extractall(EXTRACTION_FOLDER)
    print("Extraction completed.")
except Exception as e:
    print(f"Failed to extract the file: {e}")
    raise

# Process the stops data
print("Processing stops data...")
stops_data_path = join(EXTRACTION_FOLDER, "stops.txt")
try:
    stops_df = pd.read_csv(stops_data_path)
    stops_df = stops_df[['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']]
    stops_df = stops_df[stops_df['zone_id'] == 2001]
    stops_df = stops_df[(stops_df['stop_lat'].between(-90, 90)) & (stops_df['stop_lon'].between(-90, 90))]
    print("Data processing completed.")
except Exception as e:
    print(f"Failed to process data: {e}")
    raise

# Write data to SQLite database
print("Writing data to SQLite database...")
try:
    with sqlite3.connect(DB_FILE_PATH) as conn:
        stops_df.to_sql('stops', conn, if_exists='replace', index=False, dtype={
            'stop_id': 'INTEGER',
            'stop_name': 'TEXT',
            'stop_lat': 'REAL',
            'stop_lon': 'REAL',
            'zone_id': 'INTEGER'
        })
    print(f"Data successfully written to {DB_FILE_PATH}.")
except Exception as e:
    print(f"Failed to write to the database: {e}")
    raise

# Verify the SQLite database file creation
if exists(DB_FILE_PATH):
    print(f"Verification successful: The database file '{DB_FILE_PATH}' exists.")
else:
    print(f"Verification failed: The database file '{DB_FILE_PATH}' does not exist. Check permissions and paths.")
