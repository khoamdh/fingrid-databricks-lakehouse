# Fingrid Databricks Lakehouse

An end-to-end lakehouse data engineering project using Finnish electricity
consumption data from Fingrid Open Data.

The project demonstrates REST API ingestion, Apache Spark processing,
Delta Lake storage, medallion architecture, data quality checks, and
SQL analytics in Databricks.

## Architecture

Fingrid REST API
→ JSON
→ PySpark DataFrame
→ Bronze Delta Table
→ Silver Delta Table
→ SQL Analytics

## Data Source

The project uses Fingrid Open Data:

- Dataset 124: Electricity consumption in Finland
- Source: Fingrid Open Data REST API

## Tech Stack

- Databricks
- Apache Spark
- PySpark
- Delta Lake
- Python
- SQL
- Fingrid Open Data API

## Pipeline

### 1. Ingestion

Electricity consumption data is retrieved from the Fingrid REST API
using Python.

The JSON response is converted into a Spark DataFrame.

### 2. Bronze Layer

Raw Fingrid data is stored as a Delta table.

Additional ingestion metadata is added:

- ingestion timestamp
- Fingrid dataset ID

### 3. Silver Layer

The Bronze data is transformed using PySpark.

Transformations include:

- timestamp conversion
- numeric type conversion
- date and time feature extraction
- null filtering
- invalid consumption filtering
- duplicate removal

The cleaned data is persisted as a Silver Delta table.

### 4. SQL Analytics

The Silver table can be queried using Databricks SQL.

Current analysis includes:

- minimum electricity consumption
- average electricity consumption
- maximum electricity consumption
- average consumption by hour

## Repository Structure

```text
fingrid-databricks-lakehouse/
├── README.md
├── .gitignore
├── requirements.txt
├── notebooks/
│   └── 01_fingrid_bronze_silver.py
└── sql/
    └── silver_analysis.sql
