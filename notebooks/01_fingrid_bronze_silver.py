# Databricks notebook source
# Fingrid Databricks Lakehouse
#
# Pipeline:
# Fingrid REST API -> Spark DataFrame -> Bronze Delta -> Silver Delta
#
# Dataset 124: Electricity consumption in Finland


# COMMAND ----------

# MAGIC %md
# MAGIC # Fingrid Electricity Consumption Pipeline
# MAGIC
# MAGIC This notebook retrieves Finnish electricity consumption data
# MAGIC from the Fingrid Open Data API and processes it through
# MAGIC Bronze and Silver layers using PySpark and Delta Lake.


# COMMAND ----------

import requests

from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    to_timestamp,
    year,
    month,
    dayofmonth,
    dayofweek,
    hour,
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fingrid API configuration
# MAGIC
# MAGIC The Fingrid API key should not be committed to source control.
# MAGIC Replace the placeholder when running locally, or use Databricks
# MAGIC secret management.


# COMMAND ----------

API_KEY = "YOUR_API_KEY_HERE"

DATASET_ID = 124

API_URL = (
    f"https://data.fingrid.fi/api/datasets/"
    f"{DATASET_ID}/data"
)

headers = {
    "x-api-key": API_KEY
}

params = {
    "startTime": "2026-09-01T00:00:00Z",
    "endTime": "2026-09-02T00:00:00Z"
}


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Retrieve data from Fingrid


# COMMAND ----------

response = requests.get(
    API_URL,
    headers=headers,
    params=params,
    timeout=30
)

response.raise_for_status()

print(f"HTTP status: {response.status_code}")


# COMMAND ----------

# Inspect the response structure

data = response.json()

print(type(data))

if isinstance(data, dict):
    print(data.keys())


# COMMAND ----------

# Extract measurement records from the API response.
#
# Fingrid returns the measurements inside the "data" property.

records = data["data"]

print(f"Records retrieved: {len(records)}")

if records:
    print(records[0])


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Spark DataFrame
# MAGIC
# MAGIC Convert the JSON records returned by Fingrid into a
# MAGIC distributed Spark DataFrame.


# COMMAND ----------

df_raw = spark.createDataFrame(records)

display(df_raw)


# COMMAND ----------

df_raw.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Bronze layer
# MAGIC
# MAGIC The Bronze layer preserves the source data while adding
# MAGIC ingestion metadata.


# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )
    .withColumn(
        "dataset_id",
        lit(DATASET_ID)
    )
)

display(df_bronze)


# COMMAND ----------

# Persist the Bronze layer as a Delta table.

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("fingrid_bronze")
)

print("Bronze Delta table created successfully.")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM fingrid_bronze
# MAGIC LIMIT 20;


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver transformation
# MAGIC
# MAGIC The Silver layer cleans and enriches the Bronze data.
# MAGIC
# MAGIC Transformations:
# MAGIC - Convert timestamps
# MAGIC - Convert consumption values to numeric values
# MAGIC - Extract date/time attributes
# MAGIC - Remove null records
# MAGIC - Remove invalid negative consumption
# MAGIC - Remove duplicate timestamps


# COMMAND ----------

df_silver = (
    df_bronze

    .withColumn(
        "timestamp",
        to_timestamp(col("startTime"))
    )

    .withColumn(
        "consumption_mw",
        col("value").cast("double")
    )

    .withColumn(
        "year",
        year(col("timestamp"))
    )

    .withColumn(
        "month",
        month(col("timestamp"))
    )

    .withColumn(
        "day",
        dayofmonth(col("timestamp"))
    )

    .withColumn(
        "weekday",
        dayofweek(col("timestamp"))
    )

    .withColumn(
        "hour",
        hour(col("timestamp"))
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Basic data quality checks


# COMMAND ----------

df_silver = (
    df_silver

    # Timestamp must exist
    .filter(
        col("timestamp").isNotNull()
    )

    # Consumption value must exist
    .filter(
        col("consumption_mw").isNotNull()
    )

    # Electricity consumption cannot be negative
    .filter(
        col("consumption_mw") >= 0
    )

    # One record per timestamp
    .dropDuplicates(
        ["timestamp"]
    )
)


# COMMAND ----------

# Select the columns required by the analytical layer.

df_silver = df_silver.select(
    "timestamp",
    "consumption_mw",
    "year",
    "month",
    "day",
    "weekday",
    "hour"
)

display(df_silver)


# COMMAND ----------

df_silver.printSchema()

print(f"Silver records: {df_silver.count()}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Persist Silver Delta table


# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("fingrid_silver")
)

print("Silver Delta table created successfully.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. SQL analysis


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM fingrid_silver
# MAGIC ORDER BY timestamp;


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MIN(consumption_mw) AS minimum,
# MAGIC     AVG(consumption_mw) AS average,
# MAGIC     MAX(consumption_mw) AS maximum
# MAGIC FROM fingrid_silver;


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     hour,
# MAGIC     AVG(consumption_mw) AS average_consumption_mw
# MAGIC FROM fingrid_silver
# MAGIC GROUP BY hour
# MAGIC ORDER BY hour;
