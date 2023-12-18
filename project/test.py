import unittest
import sqlite3
import pandas as pd
import opendatasets as od

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
            od.download("https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand")
            od.download("https://www.kaggle.com/datasets/muthuj7/weather-dataset")

            hotel_booking_data = pd.read_csv("hotel-booking-demand/hotel_bookings.csv")
            weather_data = pd.read_csv("weather-dataset/weatherHistory.csv")

            self.conn1 = sqlite3.connect("../data/hotel_booking.sqlite")
            hotel_booking_data.to_sql("hotel_booking", self.conn1, index=False, if_exists="replace")

            self.conn2 = sqlite3.connect("../data/weather_data.sqlite")
            weather_data.to_sql("weather_data", self.conn2, index=False, if_exists="replace")

            # Set the table and columns for hotel_bookings
            self.table1 = 'hotel_booking'
            self.columns1 = hotel_booking_data.columns.tolist()

            # Set the table and columns for weather_data
            self.table2 = 'weather_data'
            self.columns2 = weather_data.columns.tolist()

        except Exception as e:
            self.fail(f"Failed to set up test environment: {e}")

    def test_hotelbooking_table_exists(self):
        try:
            # Print available tables to help with debugging
            print(f"Tables in {self.db_path1}: {execute_query(self.conn1, 'SELECT name FROM sqlite_master WHERE type="table";')}")

            # Test if the hotel_booking table exists in the database
            cursor = self.conn1.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table1}';")
            tables = cursor.fetchall()
            table_names = [table[0] for table in tables]
            self.assertIn(self.table1, table_names, f"Test failed: {self.table1} table does not exist in the database.")
            print(f"Test passed: {self.table1} table exists in the database.")
        except Exception as e:
            self.fail(f"Test failed: {e}")

    # Add other test methods...

if __name__ == "__main__":
    unittest.main()
