from datetime import datetime

def transform_weather(data):
    main = data["main"]
    wind = data.get("wind", {})
    system_info = data.get("sys", {})
    weather = data["weather"][0]
    clouds = data.get("clouds", {}).get("all")
    visibility = data.get("visibility")

    return {
        "city": data["name"],
        "temperature": main["temp"],
        "feels_like": main["feels_like"],
        "temp_min": main["temp_min"],
        "temp_max": main["temp_max"],
        "humidity": main["humidity"],
        "pressure": main["pressure"],
        "wind_speed": wind.get("speed"),
        "wind_deg": wind.get("deg"),
        "visibility": visibility,
        "clouds": clouds,
        "weather_main": weather["main"],
        "weather_description": weather["description"],
        "weather_icon": weather["icon"],
        "sunrise": datetime.fromtimestamp(system_info["sunrise"]),
        "sunset": datetime.fromtimestamp(system_info["sunset"]),
        "timestamp": datetime.now()
    }
