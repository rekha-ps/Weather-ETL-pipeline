from psycopg2 import connect, OperationalError
from psycopg2.extras import execute_batch
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import os
import logging


# Fetch hourly forecast
def fetch_hourly_forecast(lat, lon, city_name):
    """
    Fetch 3-hourly weather forecast from OpenWeather for a given city.
    Returns a list of dictionaries ready for PostgreSQL insertion.
    """
    API_KEY = os.getenv("OPENWEATHER_API_KEY")
    if not API_KEY:
        raise ValueError("OPENWEATHER_API_KEY not set")

    url = (
        f"https://api.openweathermap.org/data/2.5/forecast"
        f"?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
    )

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise exception for HTTP errors
        data = response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching forecast for {city_name}: {e}")
        return []

    records = []
    for item in data.get("list", []):
        ts = datetime.utcfromtimestamp(item["dt"])
        main = item.get("main", {})
        wind = item.get("wind", {})
        clouds = item.get("clouds", {})
        rain = item.get("rain", {})

        records.append({
            "city": city_name,
            "temperature": main.get("temp"),
            "feels_like": main.get("feels_like"),
            "humidity": main.get("humidity"),
            "pressure": main.get("pressure"),
            "wind_speed": wind.get("speed", 0),
            "wind_deg": wind.get("deg", 0),
            "clouds": clouds.get("all", 0),
            "rain_1h": rain.get("3h", 0),
            "description": item["weather"][0]["description"] if item.get("weather") else "",
            "timestamp": ts,
            "date": ts.date()
        })

    logging.info(f"{city_name}: fetched {len(records)} forecast records")
    return records

def store_weather():
    """
    Fetch forecast for all active cities and store in PostgreSQL.
    Uses batch inserts for better performance.
    """
    # Check API key
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY not set")

    # Connect to Postgres
    try:
        conn = connect(
            host="postgres",
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
    except OperationalError as e:
        logging.error(f"Database connection failed: {e}")
        return

    cur = conn.cursor()

    # Fetch active cities
    cur.execute("SELECT city_name, lat, lon FROM cities WHERE active = TRUE")
    cities = cur.fetchall()

    total_records = 0
    insert_query = """
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
    """

    for city_name, lat, lon in cities:
        try:
            hourly_data = fetch_hourly_forecast(lat, lon, city_name)
            if not hourly_data:
                logging.warning(f"No data fetched for {city_name}")
                continue

            # Prepare rows for batch insert
            rows = [
                (
                    h["city"], h["temperature"], h["feels_like"], h["humidity"],
                    h["pressure"], h["wind_speed"], h["wind_deg"], h["clouds"],
                    h["rain_1h"], h["description"], h["date"], h["timestamp"]
                ) for h in hourly_data
            ]

            execute_batch(cur, insert_query, rows, page_size=100)
            total_records += len(rows)
            logging.info(f"{city_name}: inserted/updated {len(rows)} records")

        except Exception as e:
            logging.error(f"Failed processing {city_name}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    logging.info(f"ETL complete: total records inserted/updated = {total_records}")

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
