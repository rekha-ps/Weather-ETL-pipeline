from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import os
import logging

API_KEY = os.getenv("OPENWEATHER_API_KEY")

if not API_KEY:
    raise ValueError("OPENWEATHER_API_KEY not set")


# Fetch hourly forecast
def fetch_hourly_forecast(lat, lon, city_name):
    url = (
        f"https://api.openweathermap.org/data/2.5/onecall"
        f"?lat={lat}&lon={lon}&appid={API_KEY}"
        f"&units=metric&exclude=minutely,daily,alerts"
    )

    response = requests.get(url, timeout=10)
    data = response.json()

    if response.status_code != 200:
        logging.error(f"API error for {city_name}: {data}")
        return []

    now_utc = datetime.utcnow()
    records = []

    for hour in data.get("hourly", []):
        ts = datetime.utcfromtimestamp(hour["dt"])

        # KEEP ONLY NOW and FUTURE
        if ts < now_utc:
            continue

        records.append({
            "city": city_name,
            "temperature": hour["temp"],
            "feels_like": hour["feels_like"],
            "humidity": hour["humidity"],
            "pressure": hour["pressure"],
            "wind_speed": hour.get("wind_speed", 0),
            "wind_deg": hour.get("wind_deg", 0),
            "clouds": hour.get("clouds", 0),
            "rain_1h": hour.get("rain", {}).get("1h", 0),
            "description": hour["weather"][0]["description"],
            "timestamp": ts,
            "date": ts.date()
        })

    return records

# Store in PostgreSQL
def store_weather():
    conn = psycopg2.connect(
        host="postgres",
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor()

    cur.execute("""
        SELECT city_name, lat, lon
        FROM cities
        WHERE active = TRUE
    """)
    cities = cur.fetchall()

    for city_name, lat, lon in cities:
        hourly_data = fetch_hourly_forecast(lat, lon, city_name)
        logging.info(f"{city_name}: {len(hourly_data)} future records")

        for h in hourly_data:
            cur.execute("""
                INSERT INTO weather (
                    city, temperature, feels_like, humidity, pressure,
                    wind_speed, wind_deg, clouds, rain_1h,
                    description, date, timestamp
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (city, timestamp) DO UPDATE SET
                    temperature = EXCLUDED.temperature,
                    feels_like = EXCLUDED.feels_like,
                    humidity = EXCLUDED.humidity,
                    pressure = EXCLUDED.pressure,
                    wind_speed = EXCLUDED.wind_speed,
                    wind_deg = EXCLUDED.wind_deg,
                    clouds = EXCLUDED.clouds,
                    rain_1h = EXCLUDED.rain_1h,
                    description = EXCLUDED.description
            """, (
                h["city"], h["temperature"], h["feels_like"], h["humidity"],
                h["pressure"], h["wind_speed"], h["wind_deg"], h["clouds"],
                h["rain_1h"], h["description"], h["date"], h["timestamp"]
            ))

    conn.commit()
    cur.close()
    conn.close()

# DAG
default_args = {
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_etl",
    schedule_interval="@daily",  # or "0 */2 * * *" for bi-hourly
    catchup=False,
    default_args=default_args,
    tags=["weather", "forecast"],
) as dag:

    store_weather_task = PythonOperator(
        task_id="store_weather",
        python_callable=store_weather,
    )

    store_weather_task
