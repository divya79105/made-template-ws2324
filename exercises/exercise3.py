import pandas as pd
import sqlite3

# 1. Download data
url = "https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv"
df = pd.read_csv(url, sep=";", encoding='ISO-8859-1', skiprows=6, skipfooter=4, engine='python')

# 2. Reshape data structure
# Include all columns from the source data
new_column_names = ['date', 'CIN', 'name', 'petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']
df.columns = new_column_names

# 3. Validate data
df['CIN'] = df['CIN'].astype(str).str.zfill(5)

# Validate positive integers
numeric_columns = ['petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']
df = df.loc[(df[numeric_columns].apply(pd.to_numeric, errors='coerce') > 0).all(axis=1)]

# 4. Use fitting SQLite types for all columns
sqlite_types = {'date': 'TEXT', 'CIN': 'TEXT', 'name': 'TEXT',
                'petrol': 'INTEGER', 'diesel': 'INTEGER', 'gas': 'INTEGER',
                'electro': 'INTEGER', 'hybrid': 'INTEGER', 'plugInHybrid': 'INTEGER', 'others': 'INTEGER'}

# 5. Write data to SQLite database
db_path = "cars.sqlite"
table_name = "cars"

conn = sqlite3.connect(db_path)
df.to_sql(table_name, conn, index=False, if_exists='replace', dtype=sqlite_types)
conn.close()

print(f"Data written to {db_path}, table: {table_name}")
