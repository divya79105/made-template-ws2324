import pandas as pd
import sqlite3

# Step 1: Download the data
url = "https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv"
# Specify the encoding to maintain German umlauts
df = pd.read_csv(url, sep=";", encoding='ISO-8859-1', skiprows=6, skipfooter=4, engine='python')

# Step 2: Reshape the data structure
columns_to_keep = ['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'Insgesamt', 'Insgesamt.1', 'Insgesamt.2', 'Insgesamt.3', 'Insgesamt.4', 'Insgesamt.5', 'Insgesamt.6']
new_column_names = ['date', 'CIN', 'name', 'petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']

column_mapping = dict(zip(columns_to_keep, new_column_names))

df.rename(columns=column_mapping, inplace=True)

# Select and reorder columns
df = df[new_column_names]

# Step 3: Validate data
# Validate alphanumeric characters in the 'name' column
df = df[df['name'].astype(str).str.isalpha()]

# Validate and fill leading zeros in the 'CIN' column
df['CIN'] = df['CIN'].astype(str).apply(lambda x: x.zfill(5) if x.isdigit() else x)

# Validate positive integers using pd.to_numeric
numeric_columns = ['petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']
for col in numeric_columns:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Drop rows with missing or invalid values
df = df.dropna()

# Print dimensions and inspect the DataFrame
print("DataFrame dimensions:", df.shape)
print(df)

# Step 4: Use fitting SQLite types
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

# Step 5: Write data to SQLite database
conn = sqlite3.connect('cars.sqlite')
df.to_sql('cars', conn, index=False, if_exists='replace', dtype=sqlite_types, encoding='utf-8')
conn.close()

