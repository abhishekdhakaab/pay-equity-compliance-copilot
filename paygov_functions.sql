-- Databricks notebook source
USE CATALOG paygov


-- COMMAND ----------

USE SCHEMA hr_data


-- COMMAND ----------

CREATE OR REPLACE VIEW v_total_comp AS
SELECT
  anon_employee_id,
  job_family,
  job_level,
  location,
  gender,
  ethnicity,
  age_band,
  hire_year,
  rating,
  base_salary,
  bonus,
  stock,
  currency,
  comp_year,
  review_quarter,
  review_year,
  COALESCE(base_salary,0) + COALESCE(bonus,0) + COALESCE(stock,0) AS total_comp
FROM data_analyst_view


-- COMMAND ----------

CREATE OR REPLACE VIEW v_unadjusted_paygap_gender AS
SELECT
  comp_year,
  gender,
  ROUND(AVG(total_comp), 2) AS avg_total_comp,
  COUNT(*) AS employee_count
FROM v_total_comp
GROUP BY comp_year, gender


-- COMMAND ----------

SELECT * FROM v_unadjusted_paygap_gender ORDER BY comp_year, gender


-- COMMAND ----------

CREATE OR REPLACE VIEW v_with_tenure AS
SELECT
  *,
  CASE
    WHEN comp_year - hire_year BETWEEN 0 AND 2 THEN '0–2'
    WHEN comp_year - hire_year BETWEEN 3 AND 5 THEN '3–5'
    WHEN comp_year - hire_year BETWEEN 6 AND 9 THEN '6–9'
    ELSE '10+'
  END AS tenure_band
FROM v_total_comp


-- COMMAND ----------

CREATE OR REPLACE VIEW v_adjusted_gender_strata AS
SELECT
  comp_year,
  job_level,
  tenure_band,
  location,
  ROUND(AVG(CASE WHEN gender = 'Female' THEN total_comp END), 2) AS avg_female,
  ROUND(AVG(CASE WHEN gender = 'Male'   THEN total_comp END), 2) AS avg_male,
  COUNT(CASE WHEN gender = 'Female' THEN 1 END) AS n_female,
  COUNT(CASE WHEN gender = 'Male'   THEN 1 END) AS n_male
FROM v_with_tenure
GROUP BY comp_year, job_level, tenure_band, location


-- COMMAND ----------

CREATE OR REPLACE VIEW v_adjusted_gap_gender AS
SELECT
  comp_year,
  job_level,
  tenure_band,
  location,
  avg_female,
  avg_male,
  n_female,
  n_male,
  CASE
    WHEN avg_male IS NULL OR avg_male = 0 THEN NULL
    ELSE ROUND(avg_female / avg_male, 4)
  END AS gender_gap_ratio
FROM v_adjusted_gender_strata


-- COMMAND ----------

CREATE OR REPLACE VIEW v_equity_breakdown_tenure AS
SELECT
  tenure_band,
  comp_year,
  COUNT(*) AS employees,
  ROUND(AVG(total_comp), 2) AS avg_total_comp
FROM v_with_tenure
GROUP BY tenure_band, comp_year


-- COMMAND ----------

CREATE OR REPLACE VIEW v_equity_breakdown_role AS
SELECT
  job_level,
  comp_year,
  COUNT(*) AS employees,
  ROUND(AVG(total_comp), 2) AS avg_total_comp
FROM v_with_tenure
GROUP BY job_level, comp_year


-- COMMAND ----------

CREATE OR REPLACE VIEW v_equity_breakdown_location AS
SELECT
  location,
  comp_year,
  COUNT(*) AS employees,
  ROUND(AVG(total_comp), 2) AS avg_total_comp
FROM v_with_tenure
GROUP BY location, comp_year


-- COMMAND ----------

