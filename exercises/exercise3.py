import pandas as pd
import sqlite3

# Download data
url = "https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv"

# Read data while skipping metadata
df = pd.read_csv(url, sep=";", encoding='ISO-8859-1', skiprows=6, skipfooter=4, engine='python')

# Specify the columns to keep and their new names
columns_to_keep = {
    'A': 'date',
    'B': 'CIN',
    'C': 'name',
    'M': 'petrol',
    'W': 'diesel',
    'AG': 'gas',
    'AQ': 'electro',
    'BA': 'hybrid',
    'BK': 'plugInHybrid',
    'BU': 'others'
}

# Keep only specified columns and rename them
df = df[columns_to_keep.keys()]
df = df.rename(columns=columns_to_keep)

# Validate 'name' column as a string
df = df[df['name'].astype(str).str.isalpha()]

# Ensure 'CIN' column is a string of length 5
df['CIN'] = df['CIN'].astype(str).apply(lambda x: x.zfill(5) if x.isdigit() else x)

# Validate other columns as positive integers > 0
numeric_columns = ['petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']

for col in numeric_columns:
    df = df[df[col].astype(str).apply(lambda x: x.isdigit() and int(x) > 0)]

# Drop rows with missing or invalid values
df = df.dropna()

# Infer SQLite types for columns
sqlite_types = df.infer_objects().dtypes.apply(lambda x: 'TEXT' if x == 'object' else x).to_dict()

# Write data to SQLite database
db_path = "cars.sqlite"
table_name = "cars"

# Connect to SQLite database
conn = sqlite3.connect(db_path)

# Write DataFrame to SQLite table with inferred types
df.to_sql(table_name, conn, index=False, if_exists='replace', dtype=sqlite_types)

# Close the database connection
conn.close()

print(f"Data written to {db_path}, table: {table_name}")
