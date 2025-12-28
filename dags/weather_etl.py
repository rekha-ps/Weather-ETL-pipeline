from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import os
import logging

# Retrieve API key from environment variable
API_KEY = os.getenv("OPENWEATHER_API_KEY")

# Function to fetch hourly weather data from OpenWeatherMap
def fetch_hourly_weather(lat, lon, city_name):
    # One Call API: hourly for 48 hours, exclude current/minutely/daily/alerts
    url = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={API_KEY}&units=metric&exclude=current,minutely,daily,alerts"
    response = requests.get(url)
    data = response.json()

    hourly_data = []
    for hour in data.get("hourly", []):
        timestamp = datetime.utcfromtimestamp(hour["dt"])
        hourly_data.append({
            "city": city_name,
            "temperature": hour["temp"],
            "feels_like": hour["feels_like"],
            "humidity": hour["humidity"],
            "pressure": hour["pressure"],
            "wind_speed": hour.get("wind_speed", 0),
            "wind_deg": hour.get("wind_deg", 0),
            "clouds": hour.get("clouds", 0),
            "rain_1h": hour.get("rain", {}).get("1h", 0),
            "sunrise": None,  # Will be updated separately daily
            "sunset": None,
            "description": hour["weather"][0]["description"],
            "date": timestamp.date(),
            "timestamp": timestamp
        })
    return hourly_data

# Function to fetch current weather (for sunrise/sunset once per day)
def fetch_current_weather(lat, lon):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()
    return {
        "sunrise": datetime.utcfromtimestamp(data["sys"]["sunrise"]),
        "sunset": datetime.utcfromtimestamp(data["sys"]["sunset"])
    }

# Store weather data in PostgreSQL
def store_weather():
    conn = psycopg2.connect(
        host="postgres",
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )
    cur = conn.cursor()

    # Fetch active cities
    cur.execute("SELECT city_name, lat, lon FROM cities WHERE active = TRUE")
    cities = [{"name": row[0], "lat": row[1], "lon": row[2]} for row in cur.fetchall()]

    for city in cities:
        try:
            # Fetch hourly weather
            hourly_weather = fetch_hourly_weather(city["lat"], city["lon"], city["name"])
            logging.info(f"Fetched {len(hourly_weather)} hourly records for {city['name']}")

            # Fetch sunrise/sunset for today (once per day)
            current = fetch_current_weather(city["lat"], city["lon"])
            for hour in hourly_weather:
                # Only set sunrise/sunset for the first record of the day
                if hour["timestamp"].hour == 0:
                    hour["sunrise"] = current["sunrise"]
                    hour["sunset"] = current["sunset"]

                # Upsert into DB
                cur.execute("""
                    INSERT INTO weather (
                        city, temperature, feels_like, humidity, pressure,
                        wind_speed, wind_deg, clouds, rain_1h,
                        sunrise, sunset, description, date, timestamp
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (city, timestamp) DO UPDATE SET
                        temperature = EXCLUDED.temperature,
                        feels_like = EXCLUDED.feels_like,
                        humidity = EXCLUDED.humidity,
                        pressure = EXCLUDED.pressure,
                        wind_speed = EXCLUDED.wind_speed,
                        wind_deg = EXCLUDED.wind_deg,
                        clouds = EXCLUDED.clouds,
                        rain_1h = EXCLUDED.rain_1h,
                        description = EXCLUDED.description,
                        sunrise = EXCLUDED.sunrise,
                        sunset = EXCLUDED.sunset
                """, (
                    hour["city"], hour["temperature"], hour["feels_like"], hour["humidity"],
                    hour["pressure"], hour["wind_speed"], hour["wind_deg"], hour["clouds"],
                    hour["rain_1h"], hour["sunrise"], hour["sunset"], hour["description"],
                    hour["date"], hour["timestamp"]
                ))

            logging.info(f"Inserted/Updated {len(hourly_weather)} hourly records for {city['name']}")

        except Exception as e:
            logging.error(f"Error fetching/inserting weather for {city['name']}: {e}")

    conn.commit()
    cur.close()
    conn.close()

# Define Airflow DAG
default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="weather_etl",
    schedule_interval="@daily",  # run every hour
    default_args=default_args,
    catchup=False
) as dag:

    store_weather_task = PythonOperator(
        task_id="store_weather",
        python_callable=store_weather
    )

    store_weather_task
