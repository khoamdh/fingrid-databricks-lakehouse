-- Fingrid Databricks Lakehouse
-- Silver layer exploratory analytics


-- Preview cleaned electricity consumption data

SELECT *
FROM fingrid_silver
ORDER BY timestamp
LIMIT 100;


-- Overall consumption statistics

SELECT
    MIN(consumption_mw) AS minimum_consumption_mw,
    AVG(consumption_mw) AS average_consumption_mw,
    MAX(consumption_mw) AS maximum_consumption_mw
FROM fingrid_silver;


-- Average electricity consumption by hour

SELECT
    hour,
    AVG(consumption_mw) AS average_consumption_mw
FROM fingrid_silver
GROUP BY hour
ORDER BY hour;


-- Number of Silver records

SELECT
    COUNT(*) AS record_count
FROM fingrid_silver;


-- Daily consumption statistics

SELECT
    DATE(timestamp) AS date,
    MIN(consumption_mw) AS minimum_consumption_mw,
    AVG(consumption_mw) AS average_consumption_mw,
    MAX(consumption_mw) AS maximum_consumption_mw
FROM fingrid_silver
GROUP BY DATE(timestamp)
ORDER BY date;
