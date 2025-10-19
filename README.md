# UK Generation Mix Dashbaord
A modular data pipeline and interactive dashboard for analysing the UK National Electricity System Operator (NESO) historic generation mix dataset.

This project adopts clean, maintainable data engineering practices — including data ETL, and visualisation with logging — built entirely in Python, using Polars, SQLAlchemy, and Streamlit. Scheduler is implemented with APScheduler. Logging utilises with loguru.




## Project Overview
The application fetches half-hourly generation mix data from NESO’s public API ([Historic GB Generation Mix | NESO](https://www.neso.energy/data-portal/historic-generation-mix/historic_gb_generation_mix)):
```
https://api.neso.energy/api/3/action/datastore_search_sql
```
More inforation about this endpoint can be found here: 
Data are stored locally in a SQLite database (~300k rows) and visualised via an interactive Streamlit dashboard.

A background APScheduler job incrementally updates the dataset hourly.




## Architecture
### Diagram
![Flowchart](./harmony_energy_pipeline.svg)
### Project Structure
```
project/
│
├── src/
│   ├── app/               # Streamlit frontend
│   │   └── streamlit_app.py
│   ├── db/                # Database models and connection
│   │   ├── client.py
│   │   └── models.py
│   ├── ingest/            # NESO API ingestion logic
│   │   └── fetch_neso.py
│   ├── transform/         # Data cleaning, validation, enrichment
│   │   └── transform.py
│   ├── serve/             # Data loading for visualisation
│   │   ├── load.py        # Load data into database
│   │   └── run_history.py # Wrapper for tracking pipeline run
│   ├── scheduler/         # Background APScheduler setup
│   │   └── job.py
│   ├── utils/             # Shared config, logging utilities
│   │   ├── config.py
│   │   └── logger.py
│   └── pipeline/          # Orchestration (pipeline definition)
│       └── run.py
│
├── data/
│   └── app.db             # SQLite database storing processed data
│
├── conf/
│   └── config.json         # Central configuration (paths, schedule interval, etc.)
│
├── tests/
│   └── test_db_load.py     # Unit tests for DB loading
│
├── scripts/
│   └── run_local.sh         # Helper for local app development
│
└── requirements.txt
```



## Pipeline Logic
### 1. Ingest

* Fetches raw generation mix data from NESO API in 0.5-hour intervals.

* Handles pagination and deduplication of overlapping records.


### 2. Transform

* Cleans, validates, and enriches the data.

* Standardises timestamps and datatypes.

* Performs data quality checks (e.g. generation vs. mix % consistency).

* Deduplicates overlapping timestamps and id's, preferring latest entries.

* Produces a structured, schema-aligned Polars DataFrame ready for storage.


### 3. Load

* Persists transformed data into a SQLite database (`data/app.db`) via **SQLAlchemy**.

* Table schemas defined via ORM models (`generation`).


### 4. Serve

* DB data are read efficiently via Polars for Streamlit visualisation.

* Streamlit UI provides interactive filtering, date range selection, and aggregated visual summaries.


### 5. Scheduler

* The pipeline job runs automatically every **hour** using **APScheduler**.

* The job starts in the background when the Streamlit app first boots and runs independently of UI interactions.


### 6. Logging & Monitoring
* Uses Loguru for structured and colourful logging.

* Logs include:

   * Pipeline run status (start, success, failure, duration)

   * Data quality metrics (e.g. invalid rows, missing intervals)

   * Streamlit app events and scheduler activity

* Logs are written to:

   * `logs/pipeline.log`

   * A **DB table**: `pipeline_run_history` for persistent tracking and auditability.




## Design Assumptions & Rationale
|            **Design Choice**           |                         **Reason**                        |
|:--------------------------------------|:---------------------------------------------------------|
| **Polars** over **Pandas**                 | Faster columnar operations and efficient aggregation      |
| **SQLite** backend                     | Lightweight, portable serveless storage suitable for ≤300k rows     |
| **Loguru**-based logging               | Simplifies logging config and structured message  |
| **APScheduler**                        | Lightweight, in-process scheduler; ensures data freshness without manual triggers |
| **Config via JSON (conf/config.json)** | Centralised control of paths, intervals, and constants    |
| **Schema and data validation**                  | Prevents malformed data from entering the database        |
| **Modular structure**        | Easier testing, extension, maintenance, and CI/CD integration          |




## Dashboard Structure
* Fuel Mix (MW) – stacked area chart by generation source.

* Fuel Mix (%) – stacked area chart by generation source.

* Zero-Carbon vs Carbon Generation (MW) – comparative trend line plot.

* Zero-Carbon % vs Carbon Intensity – dual-axis chart comparing renewable share and emissions.

* Date Range Selector – dynamic date range and time interval control.

* Auto-refresh – hourly pipeline updates handled by background scheduler.




## Running the Project

### Local
To run and test the project locally, firstly setup a virtual environment. A helper script is provided:
```bash
scripts/setup.sh
```
To launch the app locally, run:
```bash
scripts/run_local.sh
```

### On Streamlit Cloud
Just push to GitHub — Streamlit Cloud will automatically run
```bash
streamlit run src/app/streamlit_app.py
```
The scheduler starts automatically when the app boots.




## Data & Logs
|       **Path**       |                      **Description**                      |
|:--------------------|:---------------------------------------------------------|
| `data/app.db`          | SQLite database for the project                 |
| `generation`           | DB table of transformed data                            |
| `pipeline_run_history` | DB table logging historical pipeline runs                 |
| `logs/pipeline.log`    | Text log with detailed run events                         |
| `conf/config.json`     | Central configuration for paths, intervals, and constants |
