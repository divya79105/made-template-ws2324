import pandas as pd
import sqlite3
import opendatasets as od

hotel_booking_url = "https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand/download?datasetVersionNumber=1"
weather_data_url = "https://www.kaggle.com/datasets/muthuj7/weather-dataset/download?datasetVersionNumber=1"

od.download(hotel_booking_url)
od.download(weather_data_url)

hotel_booking_path = "hotel-booking-demand/hotel_bookings.csv"
weather_data_path = "weather-dataset/weatherHistory.csv"

# Establish connections within a context manager (using 'with' statement)
with sqlite3.connect("../data/hotel_bookings.sqlite") as conn1:
    hotel_booking_data = pd.read_csv(hotel_booking_path)
    hotel_booking_data.to_sql("hotel_bookings", conn1, index=False, if_exists="replace")

with sqlite3.connect("../data/weather_data.sqlite") as conn2:
    weather_data = pd.read_csv(weather_data_path)
    weather_data.to_sql("weather_data", conn2, index=False, if_exists="replace")

print("Datasets are pulled and stored in /data directory")
