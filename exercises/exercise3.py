import pandas as pd
import sqlite3

# Download data
url = "https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv"
df = pd.read_csv(url, encoding='latin1', skiprows=6, skipfooter=4, engine='python')

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

# Identify the actual column names
name_column = [col for col in df.columns if 'name' in col.lower()]
CIN_column = [col for col in df.columns if 'cin' in col.lower()]

# Data validation
if name_column:
    df = df[df[name_column[0]].astype(str).apply(lambda x: isinstance(x, str))]  # Validate 'name' as string

if CIN_column:
    df = df[df[CIN_column[0]].astype(str).apply(lambda x: len(x) == 5 and x.isdigit() or (x.isdigit() and x.startswith('0')))]  # Validate 'CIN'

# Drop rows with missing or invalid values
df = df.dropna()

# Specify SQLite types for columns
sqlite_types = {
    'date': 'TEXT',
    'CIN': 'TEXT',
    'name': 'TEXT',
    'petrol': 'FLOAT',
    'diesel': 'FLOAT',
    'gas': 'FLOAT',
    'electro': 'FLOAT',
    'hybrid': 'FLOAT',
    'plugInHybrid': 'FLOAT',
    'others': 'FLOAT'
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
