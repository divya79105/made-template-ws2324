import pandas as pd
import sqlite3
import opendatasets as od

# Dataset URLs
hotel_booking_url = "https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand"
weather_url = "https://www.kaggle.com/datasets/muthuj7/weather-dataset"

# Download datasets
od.download(hotel_booking_url)
od.download(weather_url)


hotel_booking_path = "hotel-booking-demand/hotel_bookings.csv"
weather_path = "weather-dataset/weatherHistory.csv"


hotel_booking_db_path = "../data/hotel_booking.sqlite"
weather_db_path = "../data/weather.sqlite"

# Establish connections within context managers (using 'with' statement)
with sqlite3.connect(hotel_booking_db_path) as conn1:
    hotel_booking_data = pd.read_csv(hotel_booking_path)
    hotel_booking_data.to_sql("hotel_booking", conn1, index=False, if_exists="replace")
    print("Database for hotel_booking created and stored at", hotel_booking_db_path)

with sqlite3.connect(weather_db_path) as conn2:
    weather_data = pd.read_csv(weather_path)
    weather_data.to_sql("weather", conn2, index=False, if_exists="replace")
    print("Database for weather created and stored at", weather_db_path)
