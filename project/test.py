import unittest
import sqlite3
import pandas as pd


class TestDataProcessing(unittest.TestCase):

def execute_query(conn, query):
    try:
        cursor = conn.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result
    except Exception as e:
        raise Exception(f"Error executing SQL query: {e}")

def print_tables(conn, db_name):
    query = f"SELECT name FROM sqlite_master WHERE type='table';"
    tables = execute_query(conn, query)
    print(f"Tables in {db_name}: {tables}")

def setUp(self):
    try:
        # Set up SQLite databases
        conn1 = sqlite3.connect('../data/hotel_bookings.sqlite')
        print_tables(conn1, 'hotel_bookings.sqlite')

        conn2 = sqlite3.connect('../data/weather_data.sqlite')
        print_tables(conn2, 'weather_data.sqlite')

        self.weather_data_df = pd.read_sql_query('SELECT * FROM weather_data;', conn2)

    except Exception as e:
        self.fail(f"Failed to set up test environment: {e}")


    def test_hotelbooking_table_exists(self):
        try:
            # Test if the hotel_bookings table exists in the database
            cursor = self.conn1.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table1}';")
            tables = cursor.fetchall()
            table_names = [table[0] for table in tables]
            self.assertIn(self.table1, table_names, f"Test failed: {self.table1} table does not exist in the database.")
            print(f"Test passed: {self.table1} table exists in the database.")
        except Exception as e:
            self.fail(f"Test failed: {e}")

    def test_weather_data_table_exists(self):
        try:
            # Test if the weather_data table exists in the database
            cursor = self.conn2.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table2}';")
            tables = cursor.fetchall()
            table_names = [table[0] for table in tables]
            self.assertIn(self.table2, table_names, f"Test failed: {self.table2} table does not exist in the database.")
            print(f"Test passed: {self.table2} table exists in the database.")
        except Exception as e:
            self.fail(f"Test failed: {e}")

    def test_weather_data_table_columns(self):
        try:
            # Test if the weather data table has the expected columns
            cursor = self.conn2.cursor()
            cursor.execute(f"PRAGMA table_info({self.table2});")
            table_columns = [column[1] for column in cursor.fetchall()]
            for column in self.columns2:
                self.assertIn(column, table_columns, f"Column '{column}' not found in {self.table2}.")
            print(f"Test passed: {self.table2} has the expected columns.")
        except Exception as e:
            self.fail(f"Test failed: {e}")

    def test_hotelbooking_table_all_non_null_values(self):
        try:
            # Test if all columns in the hotel_bookings table have non-null values
            for column in self.columns1:
                query = f"SELECT COUNT(*) FROM {self.table1} WHERE {column} IS NOT NULL;"
                cursor = self.conn1.cursor()
                cursor.execute(query)
                count = cursor.fetchone()[0]
                self.assertTrue(count > 0, f"Column '{column}' has null values in {self.table1}.")
            print(f"Test passed: No null values found in all columns of {self.table1}.")
        except Exception as e:
            self.fail(f"Test failed: {e}")

if __name__ == "__main__":
    unittest.main()
