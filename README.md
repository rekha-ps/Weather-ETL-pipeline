# 🌦️ Weather Data Pipeline with Airflow, Docker, Postgres & Metabase 

An automated data pipeline to collect, store, and visualize hourly weather data for UK cities using OpenWeatherMap, PostgreSQL, Airflow, and Metabase.
---

## ⚙️ Technology Stack

- Python & Airflow: Orchestrates ETL workflows, fetching and storing weather data automatically.

- PostgreSQL: Stores both hourly weather data and city metadata for all tracked UK cities.

- Docker Compose: Containerizes the entire pipeline for easy setup and deployment.

- Metabase: Provides interactive dashboards to visualize weather trends.

- pgAdmin: GUI for managing and querying the database.

---

### 🚀 Features

- Automated Weather Data ETL: Fetches and stores weather data for UK cities automatically.

- Hourly & Daily Forecast Storage: Maintains historical and upcoming weather records in PostgreSQL.

- Flexible Scheduling: DAGs can run daily or hourly with minimal configuration.

- Centralized Containerized Setup: All services run seamlessly via Docker Compose.

- Interactive Dashboards: Explore and visualize weather trends using Metabase.

- Database Management Made Easy: Manage tables, queries, and city metadata via pgAdmin.

- Upsert Logic: Prevents duplicate data for the same timestamp; updates existing records automatically.

- Extensible & Scalable: Easily add new cities or integrate additional weather APIs in the future.

🌐 Service Access

- Airflow UI: http://localhost:8080

- pgAdmin: http://localhost:5050

- Metabase: http://localhost:3000


Credits

Inspired by [Chiranjeevi Sagi]https://www.youtube.com/watch?v=w9Ke-BMettc
 (YouTube tutorial)
Developed and extended by Rekha Subramaniyam
