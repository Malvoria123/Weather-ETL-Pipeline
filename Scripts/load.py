import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def load_weather(data):
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    cursor = conn.cursor()

    # ✅ Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_logs (
            city TEXT,
            temperature REAL,
            feels_like REAL,
            temp_min REAL,
            temp_max REAL,
            humidity INT,
            pressure INT,
            wind_speed REAL,
            wind_deg REAL,
            visibility INT,
            clouds INT,
            weather_main TEXT,
            weather_description TEXT,
            weather_icon TEXT,
            sunrise TIMESTAMP,
            sunset TIMESTAMP,
            timestamp TIMESTAMP
        );
    """)

    # ✅ Prepare insert query
    insert_query = """
        INSERT INTO weather_logs (
            city, temperature, feels_like, temp_min, temp_max, humidity,
            pressure, wind_speed, wind_deg, visibility, clouds,
            weather_main, weather_description, weather_icon,
            sunrise, sunset, timestamp
        ) VALUES (
            %(city)s, %(temperature)s, %(feels_like)s, %(temp_min)s, %(temp_max)s,
            %(humidity)s, %(pressure)s, %(wind_speed)s, %(wind_deg)s,
            %(visibility)s, %(clouds)s, %(weather_main)s,
            %(weather_description)s, %(weather_icon)s,
            %(sunrise)s, %(sunset)s, %(timestamp)s
        )
    """

    cursor.executemany(insert_query, data)
    conn.commit()
    cursor.close()
    conn.close()
