import pandas as pd
import sqlite3

# 1. Download data
url = "https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv"
df = pd.read_csv(url, encoding='ISO-8859-1', skiprows=6, skipfooter=4, engine='python')

# 2. Reshape data structure
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

# 3. Validate data
# Validate 'name' as a string
df = df[df['name'].astype(str).apply(lambda x: isinstance(x, str))]

# Validate 'CIN' as a string with length 5 or starting with '0'
df = df[df['CIN'].astype(str).apply(lambda x: x.isdigit() and (len(x) == 5 or x.startswith('0')))]

# Validate positive integers > 0 for all other columns
other_columns = [col for col in df.columns if col not in ['date', 'CIN', 'name']]
for col in other_columns:
    df = df[df[col].astype(str).apply(lambda x: x.isdigit() and int(x) > 0)]

# Drop rows with missing or invalid values
df = df.dropna()

# Print for debugging
print("Data after reshaping and validation:")
print(df.head())

# 4. Use fitting SQLite types for all columns
sqlite_types = {'date': 'TEXT', 'CIN': 'TEXT', 'name': 'TEXT',
                'petrol': 'INTEGER', 'diesel': 'INTEGER', 'gas': 'INTEGER',
                'electro': 'INTEGER', 'hybrid': 'INTEGER', 'plugInHybrid': 'INTEGER', 'others': 'INTEGER'}

# 5. Write data to SQLite database
conn = sqlite3.connect('cars.sqlite')
df.to_sql('cars', conn, index=False, if_exists='replace', dtype=sqlite_types)
conn.close()

conn = sqlite3.connect(db_path)
df.to_sql(table_name, conn, index=False, if_exists='replace', dtype=sqlite_types)
conn.close()

print(f"Data written to {db_path}, table: {table_name}")
