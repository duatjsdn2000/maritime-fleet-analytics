Maritime Fuel Efficiency ETL Pipeline
End-to-end data engineering project built by a Chief Mate transitioning into data engineering — combining maritime domain expertise with modern data stack.

Project Overview
This project implements an automated ETL pipeline that processes ship fuel efficiency data, applies maritime KPIs, and loads structured results into a PostgreSQL data warehouse — fully orchestrated with Apache Airflow and containerized with Docker.

Tech Stack
Layer	Technology
Orchestration	Apache Airflow 3.0
Containerization	Docker, Docker Compose
Data Processing	Python, Pandas
Data Warehouse	PostgreSQL 14
Visualization	Power BI
Version Control	Git, GitHub
Architecture
CSV (Raw Data)
      │
      ▼
┌─────────────────────────────────────────┐
│           Apache Airflow DAG            │
│                                         │
│  create_tables → extract_data           │
│       → transform_data → load_to_postgres│
└─────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────┐
│         PostgreSQL (Star Schema)        │
│                                         │
│  ship_dimension   route_dimension       │
│  month_dimension ──→ fuel_fact          │
└─────────────────────────────────────────┘
      │
      ▼
  Power BI Dashboard
Docker Services
docker-compose
├── airflow-apiserver      (port 8080)
├── airflow-scheduler
├── airflow-dag-processor
├── airflow-triggerer
├── postgres               (Airflow metadata, port 5433)
└── postgres-portfolio     (Project data warehouse, port 5434)
Data Model (Star Schema)
ship_dimension          route_dimension
├── ship_id (PK)        └── route_id (PK)
├── ship_type
└── fuel_type

month_dimension
├── month_id (PK)
├── month
└── month_num

fuel_fact
├── id (PK, SERIAL)
├── ship_id (FK → ship_dimension)
├── route_id (FK → route_dimension)
├── month_id (FK → month_dimension)
├── distance
├── fuel_consumption
├── co2_emissions
├── weather_conditions
├── engine_efficiency
├── fuel_per_nm          ← derived KPI
├── co2_per_nm           ← derived KPI
├── eff_grade            ← A/B/C grading
└── co2_grade            ← A/B/C grading
Key Maritime KPIs
KPI	Description	Grading
(Standards were established using appropriate values ​​by referring to and analyzing the CSV data.)
Fuel per NM	Fuel consumption per nautical mile	A < 21.1 / B < 34.2 / C ≥ 34.2
CO₂ per NM	CO₂ emissions per nautical mile	A < 58.6 / B < 95.0 / C ≥ 95.0
Efficiency Grade	Overall fuel efficiency rating	A / B / C
CO₂ Grade	Emissions intensity rating	A / B / C
Grading thresholds are derived from operational experience as a Chief Mate and aligned with IMO emission intensity guidelines.

Airflow DAG Pipeline
[create_ship_dimension] ─┐
[create_route_dimension] ─┼─→ [create_fuel_fact] → [extract_data] → [transform_data] → [load_to_postgres]
[create_month_dimension] ─┘
Schedule: @daily
Executor: LocalExecutor
Task communication: XCom (dict serialization)
Error handling: try/except with raise on all tasks
Project Structure
maritime-fleet-analytics/
├── dags/
│   ├── maritime_fuel_efficiency_dag.py   # Airflow DAG
│   └── files/
│       └── ship_fuel_efficiency.csv      # Raw data source
├── docker-compose.yaml                   # Airflow + PostgreSQL services
├── .env.example                          # Environment variable template
├── .gitignore
└── README.md
Quick Start
Prerequisites
Docker Desktop
Git
1. Clone the repository
bash
git clone https://github.com/duatjsdn2000/maritime-fleet-analytics.git
cd maritime-fleet-analytics
2. Set up environment variables
bash
cp .env.example .env
# Edit .env with your own values
3. Start all services
bash
docker compose up -d
4. Access Airflow UI
URL: http://localhost:8080
Username: airflow
Password: airflow
5. Configure PostgreSQL Connection
Admin → Connections → Add
Connection Id: postgres
Connection Type: Postgres
Host: postgres-portfolio
Database: (your DB_NAME)
Login: (your DB_USER)
Password: (your DB_PASSWORD)
Port: 5432
6. Trigger the DAG
DAGs → maritime_fuel_efficiency → Trigger DAG ▶
Environment Variables
Copy .env.example to .env and fill in your values:

AIRFLOW_UID=50000
AIRFLOW_IMAGE_NAME=apache/airflow:3.0.0
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_NAME=your_db_name
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
Background
I am a Chief Mate (1st Officer) with years of experience operating vessels across international routes. During my time at sea, I worked directly with fuel efficiency reports, voyage performance analysis, and emission monitoring — the exact data this pipeline processes.

This project is part of my transition into data engineering, where I aim to bring maritime domain knowledge into the data infrastructure layer at companies like Kpler, Windward, or Vortexa.

Future Work
 Real-time AIS data ingestion via AISstream.io API
 CII (Carbon Intensity Indicator) Rating — IMO 2023 regulation compliance
 EEOI (Energy Efficiency Operational Index) calculation
 Kafka-based real-time streaming pipeline
 Migration to GCP BigQuery + Cloud Composer
 Spark-based distributed ETL for large-scale fleet data
 AI-based anomaly detection on fuel consumption patterns
 Predictive ETA model using weather and route data
Connect
LinkedIn: www.linkedin.com/in/seonyoo-yeom GitHub: (https://github.com/duatjsdn2000/maritime-fleet-analytics)