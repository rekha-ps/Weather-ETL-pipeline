# Weather 🌦️ Data Pipeline with Airflow, Docker, Postgres & Metabase 

An automated data pipeline to collect, store, and visualize hourly weather data for UK cities using OpenWeatherMap, PostgreSQL, Airflow, and Metabase.
---

## Technology ⚙️ Stack

Python & Airflow: Orchestrates ETL workflows, fetching and storing weather data automatically.

PostgreSQL: Stores both hourly weather data and city metadata for all tracked UK cities.

Docker Compose: Containerizes the entire pipeline for easy setup and deployment.

Metabase: Provides interactive dashboards to visualize weather trends.

pgAdmin: GUI for managing and querying the database.

---

### Features

Automated Weather Data ETL: Fetches and stores weather data for UK cities automatically using Airflow.

Hourly & Daily Forecast Storage: Maintains historical and upcoming weather records in PostgreSQL.

Flexible Scheduling: DAGs can be configured to run daily or hourly without extra code changes.

Centralized Containerized Setup: Full stack runs seamlessly with Docker Compose for Airflow, Postgres, pgAdmin, and Metabase.

Interactive Dashboards: Explore and visualize weather trends using Metabase dashboards.

Database Management Made Easy: Manage tables, queries, and cities metadata via pgAdmin.

Upsert Logic: Ensures no duplicate data for the same timestamp; updates existing records automatically.

Extensible & Scalable: Easy to add new cities or integrate additional weather APIs in the future.
