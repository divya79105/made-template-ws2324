import pandas as pd
import sqlite3

# Download data
url = "https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv"
df = pd.read_csv(url, sep=";", encoding='ISO-8859-1', skiprows=6, skipfooter=4, engine='python')

# Keep only selected columns and rename them
selected_columns = {
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

df = df.rename(columns=selected_columns)

# Data validation
name_column = [col for col in df.columns if 'name' in col.lower()]

if name_column:
    df = df[df[name_column[0]].astype(str).apply(lambda x: isinstance(x, str))]  # Validate 'name' as string

# Ensure 'CIN' column is a string of length 5
df['CIN'] = df['CIN'].astype(str).apply(lambda x: x.zfill(5) if x.isdigit() else x)

# Validate other columns as positive integers > 0
numeric_columns = ['petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']

for col in numeric_columns:
    df = df[df[col].astype(str).apply(lambda x: x.isdigit() and int(x) > 0)]

# Drop rows with missing or invalid values
df = df.dropna()

# Specify SQLite types for columns
sqlite_types = {
    'date': 'TEXT',
    'CIN': 'TEXT',
    'name': 'TEXT',
    'petrol': 'INTEGER',
    'diesel': 'INTEGER',
    'gas': 'INTEGER',
    'electro': 'INTEGER',
    'hybrid': 'INTEGER',
    'plugInHybrid': 'INTEGER',
    'others': 'INTEGER'
}

# Write data to SQLite database
db_path = "cars.sqlite"
table_name = "cars"

# Connect to SQLite database
conn = sqlite3.connect(db_path)

# Write DataFrame to SQLite table with specified types
df.to_sql(table_name, conn, index=False, if_exists='replace', dtype=sqlite_types)

# Close the database connection
conn.close()

print(f"Data written to {db_path}, table: {table_name}")
