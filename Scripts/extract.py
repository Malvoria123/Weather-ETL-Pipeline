import os
import requests
from dotenv import load_dotenv

load_dotenv()

def extract_weather():
    api_key = os.getenv("OPENWEATHER_API_KEY")
    city = "Jakarta"
    base_url = "http://api.openweathermap.org/data/2.5/weather"

    params = {
        "q": city,
        "appid": api_key,
        "units": "metric"
    }

    response = requests.get(base_url, params=params)
    response.raise_for_status()

    return response.json()
