import pandas as pd
import requests
from io import StringIO
import sqlite3

# Step 1: Download the CSV file
url = "https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv"
response = requests.get(url)

# Check if the download was successful
if response.status_code == 200:
    # Step 2: Read the CSV content into a Pandas DataFrame, skipping metadata rows
    csv_content = StringIO(response.text)
    df = pd.read_csv(csv_content, skiprows=6, skipfooter=4, engine='python', sep=';', header=None)

    # Use the first row as column names
    df.columns = df.iloc[0]

    # Drop rows that contain metadata information
    df = df[df[';;;Euro 1'].notna()]

    # Drop the first row (as it is now column names)
    df = df.drop(0)

    # Validate and clean the data
    df['CIN'] = df['CIN'].astype(str).str.zfill(5)

    numeric_columns = ['Benzin', 'Diesel', 'Gas', 'Elektro', 'Hybrid', 'PlugInHybrid', 'Sonstige']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df[df[col] > 0]

    # Print the first 5 rows of the cleaned DataFrame
    print("Cleaned DataFrame:")
    print(df.head())

    # Store the cleaned data into an SQLite database with fitting types
    db_path = "cars.sqlite"
    table_name = "cars"

    # Define fitting SQLite types for each column
    column_types = {
        ';;;Euro 1': 'TEXT',  # Adjust this based on the actual column name
        'CIN': 'TEXT',
        'Name': 'TEXT',
        'Benzin': 'FLOAT',
        'Diesel': 'FLOAT',
        'Gas': 'FLOAT',
        'Elektro': 'FLOAT',
        'Hybrid': 'FLOAT',
        'PlugInHybrid': 'FLOAT',
        'Sonstige': 'FLOAT'
    }

    # Create a connection to the SQLite database
    conn = sqlite3.connect(db_path)

    # Write the cleaned DataFrame to the SQLite database with fitting types
    df.to_sql(table_name, conn, index=False, if_exists="replace", dtype=column_types)

    # Close the database connection
    conn.close()

    # Save the cleaned data to a CSV file with UTF-8 encoding
    csv_file_path = "cleaned_data.csv"
    df.to_csv(csv_file_path, index=False, encoding='utf-8')

    print(f"Data has been cleaned, written to {db_path}, table {table_name}, and CSV file {csv_file_path} successfully.")
else:
    print(f"Failed to download the file. Status code: {response.status_code}")

