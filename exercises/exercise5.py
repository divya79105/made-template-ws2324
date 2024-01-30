import pandas as pd
import sqlite3
from urllib.request import urlretrieve
import zipfile
from os.path import join

# Define constants for URLs and file paths
GTFS_ZIP_URL = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
ZIP_FILE_PATH = "Downloaded_GTFS.zip"
EXTRACTION_FOLDER = "GTFS_Files"

# Downloading the GTFS dataset
print("Downloading GTFS dataset...")
urlretrieve(GTFS_ZIP_URL, ZIP_FILE_PATH)

# Extracting the dataset
print("Extracting files...")
with zipfile.ZipFile(ZIP_FILE_PATH, 'r') as zipObj:
    zipObj.extractall(EXTRACTION_FOLDER)

# Path to the extracted stops data
stops_data_path = join(EXTRACTION_FOLDER, "stops.txt")

# Loading and filtering stop data
print("Processing stop data...")
stops = pd.read_csv(stops_data_path)
relevant_fields = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']
stops_cleaned = stops[relevant_fields]
stops_cleaned = stops_cleaned.query("zone_id == 2001 and -90 <= stop_lat <= 90 and -90 <= stop_lon <= 90")

# Saving the cleaned data into an SQLite database
db_name = 'processed_gtfs.sqlite'
print("Saving data to database...")
with sqlite3.connect(db_name) as conn:
    stops_cleaned.to_sql('zone2001_stops', conn, if_exists='replace', index=False, dtype={
        'stop_id': 'INTEGER',
        'stop_name': 'TEXT',
        'stop_lat': 'REAL',
        'stop_lon': 'REAL',
        'zone_id': 'INTEGER'
    })

print(f"Data processing complete. Database '{db_name}' is ready.")
