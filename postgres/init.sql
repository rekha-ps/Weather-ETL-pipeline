CREATE TABLE IF NOT EXISTS weather (
    id SERIAL PRIMARY KEY,
    city VARCHAR(50),
    temperature FLOAT,
    feels_like FLOAT,
    humidity INTEGER,
    pressure INTEGER,
    wind_speed FLOAT,
    wind_deg INTEGER,
    clouds INTEGER,
    rain_1h FLOAT DEFAULT 0,
    sunrise TIMESTAMP,
    sunset TIMESTAMP,
    description TEXT,
    date DATE,
    timestamp TIMESTAMP,
    CONSTRAINT unique_city_date UNIQUE(city, timestamp)
);