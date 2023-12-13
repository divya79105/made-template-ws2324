import pandas as pd
import sqlite3

# 1. Download data
url = "https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv"
df = pd.read_csv(url, encoding='latin1', skiprows=6, skipfooter=4, engine='python')

# 2. Reshape data structure
columns_to_keep = ['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'Insgesamt', 'Insgesamt.1', 'Insgesamt.2', 'Insgesamt.3', 'Insgesamt.4', 'Insgesamt.5', 'Insgesamt.6']
new_column_names = ['date', 'CIN', 'name', 'petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']

column_mapping = dict(zip(columns_to_keep, new_column_names))
df.rename(columns=column_mapping, inplace=True)

# 3. Validate data
name_column = [col for col in df.columns if 'name' in col.lower()]
CIN_column = [col for col in df.columns if 'cin' in col.lower()]

# Additional validation for positive integers > 0 for all other columns
other_columns = [col for col in df.columns if col not in ['date', 'CIN', 'name']]
for col in other_columns:
    df = df[df[col].astype(str).apply(lambda x: x.isdigit() and int(x) > 0)]

if name_column:
    df = df[df[name_column[0]].astype(str).apply(lambda x: isinstance(x, str))]  # Validate 'name' as string

if CIN_column:
    df = df[df[CIN_column[0]].astype(str).apply(lambda x: len(x) == 5 and x.isdigit() or (x.isdigit() and x.startswith('0')))]  # Validate 'CIN'

# Drop rows with missing or invalid values
df = df.dropna()

# 4. Use fitting SQLite types for all columns
sqlite_types = {col: 'TEXT' if df[col].dtype == 'O' else 'REAL' for col in df.columns}

# 5. Write data to SQLite database
db_path = "cars.sqlite"
table_name = "cars"

conn = sqlite3.connect(db_path)
df.to_sql(table_name, conn, index=False, if_exists='replace', dtype=sqlite_types)
conn.close()

print(f"Data written to {db_path}, table: {table_name}")
