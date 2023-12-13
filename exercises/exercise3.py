import pandas as pd
import sqlite3

#1. Download the data
url = "https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv"
df = pd.read_csv(url, sep=";", encoding='ISO-8859-1', skiprows=6, skipfooter=4, engine='python')

#2. Reshape data structure
df = pd.read_csv(url, sep=";", encoding='ISO-8859-1', skiprows=6, skipfooter=4, engine='python')

# Define columns to keep and their new names
old_columns = ['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'Insgesamt', 'Insgesamt.1', 'Insgesamt.2', 'Insgesamt.3', 'Insgesamt.4', 'Insgesamt.5', 'Insgesamt.6']
new_columnss = ['date', 'CIN', 'name', 'petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']

# Rename and select columns
df = df[old_columns].rename(columns=dict(zip(old_columns, new_columns)))


#3. Validate data
# Validate CINs
df['CIN'] = df['CIN'].apply(lambda x: f'{x:0>5}')

# Validate positive integers
numeric_columns = ['petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']

df = df[df[numeric_columns].apply(pd.to_numeric, errors='coerce').gt(0).all(axis=1)]

# Drop rows with invalid values
df = df.dropna()
print(df)

#4. Use fitting SQLite types for all columns
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

#5. Write data into a SQLite database
conn = sqlite3.connect('cars.sqlite')
df.to_sql('cars', conn, index=False, if_exists='replace', dtype=sqlite_types)
conn.close()

