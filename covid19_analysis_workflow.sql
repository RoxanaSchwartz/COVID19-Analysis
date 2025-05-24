/*
COVID-19 Data Analysis Workflow
================================
Author: Roxana Schwartz
Purpose: This script processes and cleans COVID-19 data from the public BigQuery dataset
         `bigquery-public-data.covid19_open_data.covid19_open_data` for a portfolio project.
         It performs data exploration, quality checks, and creates a cleaned dataset
         (`cleaned_covid_data_v6`) for visualization in Looker Studio.
Data Source: Google BigQuery Public Dataset (COVID-19 Open Data)
Time Period: January 1, 2020, to September 17, 2022
Output: Cleaned table `covid19-analysis-457510.covid19_us.cleaned_covid_data_v6`
        with country-level data for confirmed cases, deaths, vaccinations, and more.
Structure:
  - Step 1: Data Overview (volume, time range, distribution)
  - Step 2: Data Quality Checks (NULLs, negatives, duplicates, outliers, gaps)
  - Step 3: Create Cleaned Dataset (handle missing data, outliers, ensure consistency)
Usage: Run in Google BigQuery with appropriate permissions.
*/

-- Step 1: Data Overview
-- Purpose: Summarize dataset volume, time range, and distribution of records
-- Output: Metrics like total rows, date range, unique locations, and country-level rows
SELECT
  COUNT(*) AS total_rows, -- Total number of records in the dataset
  MIN(date) AS min_date, -- Earliest date in the dataset
  MAX(date) AS max_date, -- Latest date in the dataset
  COUNT(DISTINCT date) AS unique_dates, -- Number of unique dates
  APPROX_COUNT_DISTINCT(location_key) AS unique_location_keys, -- Approximate unique locations
  COUNT(DISTINCT CASE
    WHEN REGEXP_CONTAINS(location_key, '^[A-Z]{2}$')
    AND aggregation_level = 0
    THEN location_key
  END) AS country_keys, -- Number of unique country-level location keys
  SUM(CASE WHEN aggregation_level = 0 THEN 1 ELSE 0 END) AS country_level_rows, -- Rows at country level
  SUM(CASE WHEN aggregation_level > 0 THEN 1 ELSE 0 END) AS region_level_rows -- Rows at regional level
FROM
  `bigquery-public-data.covid19_open_data.covid19_open_data`
WHERE
  date BETWEEN '2020-01-01' AND '2022-09-17' -- Filter for analysis period
  AND location_key IS NOT NULL; -- Exclude records with missing location_key

-- Step 2: Data Quality Checks
-- Purpose: Assess data quality by checking for NULLs, negative values, duplicates, outliers, and gaps
-- Output: Aggregated metrics for data quality issues across key columns
WITH
  null_checks AS (
    -- Check for NULL values in critical columns
    SELECT
      COUNTIF(date IS NULL) AS null_date,
      COUNTIF(location_key IS NULL) AS null_location_key,
      COUNTIF(new_confirmed IS NULL) AS null_new_confirmed,
      COUNTIF(new_deceased IS NULL) AS null_new_deceased,
      COUNTIF(country_name IS NULL) AS null_country_name,
      COUNTIF(cumulative_confirmed IS NULL) AS null_cumulative_confirmed,
      COUNTIF(population IS NULL) AS null_population,
      COUNTIF(stringency_index IS NULL) AS null_stringency_index,
      COUNTIF(new_persons_vaccinated IS NULL) AS null_new_persons_vaccinated
    FROM
      `bigquery-public-data.covid19_open_data.covid19_open_data`
    WHERE
      date BETWEEN '2020-01-01' AND '2022-09-17'
  ),
  negative_checks AS (
    -- Check for negative values in metrics that should be non-negative
    SELECT
      COUNTIF(new_confirmed < 0) AS negative_new_confirmed,
      COUNTIF(new_deceased < 0) AS negative_new_deceased,
      COUNTIF(cumulative_confirmed < 0) AS negative_cumulative_confirmed,
      COUNTIF(new_persons_vaccinated < 0) AS negative_new_persons_vaccinated,
      MIN(new_confirmed) AS min_new_confirmed,
      MIN(new_deceased) AS min_new_deceased
    FROM
      `bigquery-public-data.covid19_open_data.covid19_open_data`
    WHERE
      date BETWEEN '2020-01-01' AND '2022-09-17'
  ),
  duplicate_checks AS (
    -- Check for duplicate records based on date and location_key
    SELECT
      COUNT(*) - COUNT(DISTINCT CONCAT(date, location_key)) AS duplicate_rows
    FROM
      `bigquery-public-data.covid19_open_data.covid19_open_data`
    WHERE
      date BETWEEN '2020-01-01' AND '2022-09-17'
      AND location_key IS NOT NULL
  ),
  outlier_checks AS (
    -- Identify extreme values in key metrics at country level
    SELECT
      MAX(new_confirmed) AS max_new_confirmed,
      COUNTIF(new_confirmed > 400000) AS extreme_new_confirmed, -- Threshold for extreme cases
      MAX(new_deceased) AS max_new_deceased,
      COUNTIF(new_deceased > 10000) AS extreme_new_deceased, -- Threshold for extreme deaths
      MAX(cumulative_confirmed) AS max_cumulative_confirmed,
      MAX(new_persons_vaccinated) AS max_new_persons_vaccinated
    FROM
      `bigquery-public-data.covid19_open_data.covid19_open_data`
    WHERE
      date BETWEEN '2020-01-01' AND '2022-09-17'
      AND aggregation_level = 0 -- Country-level only
  ),
  data_gaps AS (
    -- Check for missing dates per country (expecting 991 days)
    SELECT
      COUNTIF(count_dates < 991) AS countries_with_missing_dates,
      AVG(991 - count_dates) AS avg_missing_dates
    FROM (
      SELECT
        location_key,
        COUNT(DISTINCT date) AS count_dates
      FROM
        `bigquery-public-data.covid19_open_data.covid19_open_data`
      WHERE
        date BETWEEN '2020-01-01' AND '2022-09-17'
        AND aggregation_level = 0
        AND REGEXP_CONTAINS(location_key, '^[A-Z]{2}$') -- Country-level codes
      GROUP BY
        location_key
    )
  ),
  metadata_checks AS (
    -- Validate number of valid country keys
    SELECT
      COUNT(DISTINCT location_key) AS valid_country_keys
    FROM
      `bigquery-public-data.covid19_open_data.covid19_open_data`
    WHERE
      date BETWEEN '2020-01-01' AND '2022-09-17'
      AND aggregation_level = 0
      AND REGEXP_CONTAINS(location_key, '^[A-Z]{2}$')
  )
SELECT
  n.*, -- NULL checks
  neg.*, -- Negative value checks
  dup.duplicate_rows, -- Duplicate records
  o.*, -- Outlier checks
  g.countries_with_missing_dates, -- Countries with incomplete date coverage
  g.avg_missing_dates, -- Average missing days per country
  m.valid_country_keys -- Valid country keys
FROM
  null_checks n
CROSS JOIN
  negative_checks neg
CROSS JOIN
  duplicate_checks dup
CROSS JOIN
  outlier_checks o
CROSS JOIN
  data_gaps g
CROSS JOIN
  metadata_checks m;

-- Step 3: Create Cleaned Dataset
-- Purpose: Generate a cleaned country-level dataset for visualization
-- Logic: 
--   - Generate a complete date range (2020-01-01 to 2022-09-17) for all countries
--   - Join with raw data, handling NULLs and outliers
--   - Ensure consistent population values and remove duplicates
--   - Output: Table `cleaned_covid_data_v6` with metrics like cases, deaths, vaccinations
CREATE OR REPLACE TABLE `covid19-analysis-457510.covid19_us.cleaned_covid_data_v6` AS
SELECT
  g.date, -- Date from generated range
  g.location_key, -- Country code (e.g., 'US', 'DE')
  r.aggregation_level, -- Aggregation level (0 for country)
  g.country_name, -- Country name
  COALESCE(r.new_confirmed, 0) AS new_confirmed, -- Daily confirmed cases, NULLs as 0
  COALESCE(r.new_deceased, 0) AS new_deceased, -- Daily deaths, NULLs as 0
  COALESCE(r.cumulative_confirmed, 0) AS cumulative_confirmed, -- Cumulative cases, NULLs as 0
  g.population, -- Consistent population per country
  COALESCE(r.stringency_index, 0) AS stringency_index, -- Government response index, NULLs as 0
  COALESCE(r.new_persons_vaccinated, 0) AS new_persons_vaccinated, -- Daily vaccinations, NULLs as 0
  CASE WHEN r.new_confirmed IS NOT NULL THEN 1 ELSE 0 END AS record_exists -- Flag for existing records
FROM (
  -- Generate date range and cross-join with unique countries
  SELECT
    date,
    c.location_key,
    c.country_name,
    c.population
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2022-09-17', INTERVAL 1 DAY)) AS date
  CROSS JOIN (
    -- Select distinct countries with consistent population
    SELECT DISTINCT
      location_key,
      country_name,
      FIRST_VALUE(population) OVER (PARTITION BY location_key ORDER BY date) AS population
    FROM
      `bigquery-public-data.covid19_open_data.covid19_open_data`
    WHERE
      location_key IS NOT NULL
      AND REGEXP_CONTAINS(location_key, '^[A-Z]{2}$') -- Country-level codes
      AND aggregation_level = 0
      AND date BETWEEN '2020-01-01' AND '2022-09-17'
      AND population IS NOT NULL
  ) c
) g
LEFT JOIN (
  -- Aggregate raw data, handle outliers and duplicates
  SELECT
    date,
    location_key,
    aggregation_level,
    SUM(CASE
      WHEN new_confirmed >= 0 AND new_confirmed <= 400000
      THEN COALESCE(new_confirmed, 0)
      ELSE 0
    END) AS new_confirmed, -- Cap extreme confirmed cases
    SUM(CASE
      WHEN new_deceased >= 0 AND new_deceased <= 10000
      THEN COALESCE(new_deceased, 0)
      ELSE 0
    END) AS new_deceased, -- Cap extreme deaths
    SUM(CASE
      WHEN cumulative_confirmed >= 0
      THEN COALESCE(cumulative_confirmed, 0)
      ELSE 0
    END) AS cumulative_confirmed, -- Non-negative cumulative cases
    SUM(CASE
      WHEN new_persons_vaccinated >= 0
      THEN COALESCE(new_persons_vaccinated, 0)
      ELSE 0
    END) AS new_persons_vaccinated, -- Non-negative vaccinations
    AVG(COALESCE(stringency_index, 0)) AS stringency_index -- Average stringency index
  FROM
    `bigquery-public-data.covid19_open_data.covid19_open_data`
  WHERE
    date IS NOT NULL
    AND date BETWEEN '2020-01-01' AND '2022-09-17'
    AND location_key IS NOT NULL
    AND REGEXP_CONTAINS(location_key, '^[A-Z]{2}$')
    AND aggregation_level = 0
  GROUP BY
    date,
    location_key,
    aggregation_level
  HAVING
    COUNT(*) = 1 -- Remove duplicate records
) r
ON g.date = r.date
AND g.location_key = r.location_key
ORDER BY
  g.date ASC,
  g.location_key ASC;