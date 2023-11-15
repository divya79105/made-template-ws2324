#importing required libraries 
import pandas as pd
from sqlalchemy import create_engine, Integer, Text, Float

# Data source URL
url = 'https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv'
database_url = 'sqlite:///airports.sqlite'

# Fetch data from the URL
airports_data = pd.read_csv(url, sep=';')

# Define SQLite types for each column in the DataFrame
column_types = {
    "column_1": Integer(),
    "column_2": Text(),
    "column_3": Text(),
    "column_4": Text(),
    "column_5": Text(),
    "column_6": Text(),
    "column_7": Float(),
    "column_8": Float(),
    "column_9": Integer(),
    "column_10": Float(),
    "column_11": Text(),
    "column_12": Text(),
    "geo_punkt": Text()
}

# Create a database connection using SQLAlchemy
engine = create_engine(database_url)

# Write data to SQLite database
airports_data.to_sql('airports', engine, index=False, if_exists='replace', dtype=column_types)
print(f"Data written successfully to '{database_url}'.")

# closing the database connection
engine.dispose()
