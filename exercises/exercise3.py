import pandas as pd
import requests
from io import StringIO
from sqlalchemy import create_engine

# Step 1: Download the CSV data
url = "https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv"
response = requests.get(url)
data = StringIO(response.text)

# Step 2: Read the CSV data into a DataFrame
df = pd.read_csv(data, encoding='latin1', delimiter=';', skiprows=6, skipfooter=4, engine='python')

# Extract relevant columns
columns_to_keep = ['date', 'CIN', 'name', 'petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']
df = df.iloc[:, [0, 1, 2, 12, 22, 33, 42, 49, 54, 64]]
df.columns = columns_to_keep

# Step 3: Validate data and drop rows with invalid values
# Define validation functions
def validate_cin(cin):
    return isinstance(cin, str) and len(cin) == 5 and cin.isdigit()

def validate_positive_integer(value):
    return isinstance(value, int) and value > 0

# Apply validations and drop rows with invalid values
df = df[df['CIN'].apply(validate_cin)]
df = df[df.iloc[:, 3:].applymap(validate_positive_integer).all(axis=1)]

# Step 4: Write data to SQLite database
engine = create_engine('sqlite:///cars.sqlite')
df.to_sql('cars', engine, index=False, if_exists='replace')

print("Data written to SQLite database successfully.")
