import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def load_weather(data):
    conn = psycopg2.connect(
        host="localhost",
        dbname="weather_data",
        user="postgres",
        password=os.getenv("DB_PASS")
    )

    cursor = conn.cursor()

    insert_query = """
        INSERT INTO weather_logs (
            city, temperature, feels_like, temp_min, temp_max,
            humidity, pressure, wind_speed, wind_deg, visibility,
            clouds, weather_main, weather_description, weather_icon,
            sunrise, sunset, timestamp
        ) VALUES (
            %(city)s, %(temperature)s, %(feels_like)s, %(temp_min)s, %(temp_max)s,
            %(humidity)s, %(pressure)s, %(wind_speed)s, %(wind_deg)s, %(visibility)s,
            %(clouds)s, %(weather_main)s, %(weather_description)s, %(weather_icon)s,
            %(sunrise)s, %(sunset)s, %(timestamp)s
        )
    """

    cursor.execute(insert_query, data)
    conn.commit()
    cursor.close()
    conn.close()
