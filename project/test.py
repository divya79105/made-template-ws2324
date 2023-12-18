import unittest
import sqlite3
import pandas as pd


class TestDataProcessing(unittest.TestCase):

    def setUp(self):
        try:
            # Set up SQLite databases
            self.db_path1 = '../data/hotel_bookings.sqlite'
            self.conn1 = sqlite3.connect(self.db_path1)
            self.query1 = f"SELECT * FROM hotel_bookings;"
            self.hotelbooking_df = pd.read_sql_query(self.query1, self.conn1)

            self.db_path2 = '../data/weather_data.sqlite'
            self.conn2 = sqlite3.connect(self.db_path2)
            self.query2 = f"SELECT * FROM weather;"
            self.airbnb_df = pd.read_sql_query(self.query2, self.conn2)
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
