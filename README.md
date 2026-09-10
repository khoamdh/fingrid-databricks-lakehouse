# fingrid-databricks-lakehouse
A data engineering portfolio project that builds a lakehouse pipeline
for Finnish electricity consumption data using Databricks, Apache Spark,
Delta Lake and Fingrid Open Data.

## Architecture

Fingrid REST API
→ JSON
→ PySpark
→ Bronze Delta
→ Silver Delta
→ SQL Analytics

## Current Features

- Fingrid REST API ingestion
- JSON processing with Python
- Apache Spark DataFrames
- Bronze Delta Lake layer
- Silver transformation using PySpark
- Basic data quality checks
- SQL analytics

## Data Source

Electricity consumption data is retrieved from Fingrid Open Data,
dataset 124.

## Tech Stack

- Databricks
- Apache Spark
- PySpark
- Delta Lake
- SQL
- Python
- Fingrid Open Data API

## Roadmap

- [x] Bronze ingestion
- [x] Silver transformation
- [x] Basic data quality
- [x] SQL analysis
- [ ] Gold analytical models
- [ ] dbt transformations
- [ ] Databricks Workflow
- [ ] Databricks SQL dashboard
- [ ] Weather data integration
- [ ] CI/CD
