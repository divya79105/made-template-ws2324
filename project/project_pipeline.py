import pandas as pd
import sqlite3
import opendatasets as od

hotel_booking_url = "https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand"
weather_data_url = "https://www.kaggle.com/datasets/muthuj7/weather-dataset"

od.download(hotel_booking_url)
od.download(weather_data_url)

hotel_booking_path = "hotel-booking-demand/hotel_bookings.csv"
weather_data_path = "weather-dataset/weatherHistory.csv"

hotel_booking_data = pd.read_csv(hotel_booking_path)
weather_data = pd.read_csv(weather_data_path)

conn1 = sqlite3.connect("../data/hotel_booking.sqlite")
hotel_booking_data.to_sql("hotel_booking", conn1, index=False, if_exists="replace")
conn1.close()

conn2 = sqlite3.connect("../data/weather_data.sqlite")
weather_data.to_sql("weather_data", conn2, index=False, if_exists="replace")
conn2.close()

print("Datasets are pulled and stored in /data directory")
