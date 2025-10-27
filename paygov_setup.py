# Databricks notebook source
spark.sql("CREATE CATALOG IF NOT EXISTS paygov")
spark.sql("USE CATALOG paygov")
spark.sql("CREATE SCHEMA IF NOT EXISTS hr_data COMMENT 'Governed HR data for pay-equity analysis'")
spark.sql("USE paygov.hr_data")


# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql import functions as F

spark.sql("USE CATALOG paygov"); spark.sql("USE SCHEMA hr_data")

# --- employees (PII) ---
employees = [
    Row(employee_id="E001", first_name="Ava",  last_name="Ng",   email="ava.ng@corp.com",  ssn="111-22-3333", hire_date="2019-04-12"),
    Row(employee_id="E002", first_name="Ben",  last_name="Ortiz",email="ben.ortiz@corp.com",ssn="222-33-4444", hire_date="2021-01-20"),
    Row(employee_id="E003", first_name="Cara", last_name="Singh",email="cara.singh@corp.com",ssn="333-44-5555", hire_date="2017-09-03"),
    Row(employee_id="E004", first_name="Dan",  last_name="Kim",  email="dan.kim@corp.com",  ssn="444-55-6666", hire_date="2015-06-18"),
    Row(employee_id="E005", first_name="Eli",  last_name="Rao",  email="eli.rao@corp.com",  ssn="555-66-7777", hire_date="2022-03-11"),
]
spark.createDataFrame(employees).withColumn("hire_date", F.to_date("hire_date")).write.mode("overwrite").saveAsTable("employees")

# --- jobs (no PII) ---
jobs = [
    Row(employee_id="E001", job_family="Engineering", job_level="L4", location="AMER"),
    Row(employee_id="E002", job_family="Engineering", job_level="L3", location="EMEA"),
    Row(employee_id="E003", job_family="Finance",     job_level="L5", location="AMER"),
    Row(employee_id="E004", job_family="Engineering", job_level="L5", location="APAC"),
    Row(employee_id="E005", job_family="Legal",       job_level="L3", location="AMER"),  # we'll exclude Legal later
]
spark.createDataFrame(jobs).write.mode("overwrite").saveAsTable("jobs")

# --- compensation (no PII, but sensitive) ---
comp = [
    Row(employee_id="E001", base_salary=150000.0, bonus=15000.0, stock=25000.0, currency="USD", comp_year=2024),
    Row(employee_id="E002", base_salary=115000.0, bonus= 8000.0, stock=12000.0, currency="EUR", comp_year=2024),
    Row(employee_id="E003", base_salary=180000.0, bonus=20000.0, stock=30000.0, currency="USD", comp_year=2024),
    Row(employee_id="E004", base_salary=210000.0, bonus=35000.0, stock=50000.0, currency="USD", comp_year=2024),
    Row(employee_id="E005", base_salary=100000.0, bonus= 5000.0, stock= 8000.0, currency="USD", comp_year=2024),
]
spark.createDataFrame(comp).write.mode("overwrite").saveAsTable("compensation")

# --- performance (no PII) ---
perf = [
    Row(employee_id="E001", rating=4.3, review_quarter="Q4", review_year=2024),
    Row(employee_id="E002", rating=3.7, review_quarter="Q4", review_year=2024),
    Row(employee_id="E003", rating=4.6, review_quarter="Q4", review_year=2024),
    Row(employee_id="E004", rating=4.9, review_quarter="Q4", review_year=2024),
    Row(employee_id="E005", rating=3.2, review_quarter="Q4", review_year=2024),
]
spark.createDataFrame(perf).write.mode("overwrite").saveAsTable("performance")

# --- demographics (coarse buckets only) ---
demo = [
    Row(employee_id="E001", gender="Female", ethnicity="Asian",      age_band="30-39"),
    Row(employee_id="E002", gender="Male",   ethnicity="Hispanic",   age_band="20-29"),
    Row(employee_id="E003", gender="Female", ethnicity="Asian",      age_band="30-39"),
    Row(employee_id="E004", gender="Male",   ethnicity="White",      age_band="40-49"),
    Row(employee_id="E005", gender="Male",   ethnicity="Black",      age_band="20-29"),
]
spark.createDataFrame(demo).write.mode("overwrite").saveAsTable("demographics")

display(spark.sql("SHOW TABLES"))


# COMMAND ----------

spark.sql("ALTER TABLE employees    SET TBLPROPERTIES (classification='confidential', contains_pii='true')")
spark.sql("ALTER TABLE demographics SET TBLPROPERTIES (classification='confidential', contains_pii='true')")
spark.sql("ALTER TABLE jobs         SET TBLPROPERTIES (classification='internal',     contains_pii='false')")
spark.sql("ALTER TABLE compensation SET TBLPROPERTIES (classification='internal',     contains_pii='false')")
spark.sql("ALTER TABLE performance  SET TBLPROPERTIES (classification='internal',     contains_pii='false')")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1) Am I on Unity Catalog?
# MAGIC SELECT current_catalog();
# MAGIC
# MAGIC -- If this returns hive_metastore, you’re NOT on UC.
# MAGIC -- 2) What catalogs exist?
# MAGIC SHOW CATALOGS;
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, when, concat, lit, abs, hash, expr

df = spark.table("employees")
df = df.withColumn("user_role", lit("analyst"))
# For admin view, use: df = df.withColumn("user_role", lit("admins"))

masked_df = (
    df
    # SSN masking
    .withColumn(
        "masked_ssn",
        when(
            col("user_role") == "admins",
            expr('concat("***-**-", right(ssn, 4))')
        ).otherwise(lit("ANALYTICS_MASKED"))
    )
    # Email masking
    .withColumn(
        "masked_email",
        when(
            col("user_role") == "admins",
            col("email")
        ).otherwise(
            concat(
                lit("hidden+"),
                abs(hash(col("email"))).cast("string"),
                lit("@example.com")
            )
        )
    )
    # First name masking
    .withColumn(
        "masked_first_name",
        when(col("user_role") == "admins", col("first_name"))
        .otherwise(lit("REDACTED"))
    )
    # Last name masking
    .withColumn(
        "masked_last_name",
        when(col("user_role") == "admins", col("last_name"))
        .otherwise(lit("REDACTED"))
    )
)

display(
    masked_df.select(
        "masked_first_name",
        "masked_last_name",
        "masked_email",
        "masked_ssn"
    )
)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW data_analyst_view AS
WITH jo AS (
  SELECT e.employee_id,
         sha2(e.employee_id, 256) AS anon_employee_id,
         YEAR(e.hire_date)        AS hire_year,
         j.job_family, j.job_level, j.location,
         d.gender, d.ethnicity, d.age_band
  FROM employees e
  JOIN jobs j         ON e.employee_id = j.employee_id
  JOIN demographics d ON e.employee_id = d.employee_id
  WHERE j.job_family <> 'Legal'
),
comp AS (
  SELECT employee_id, base_salary, bonus, stock, currency, comp_year
  FROM compensation
),
perf AS (
  SELECT employee_id, rating, review_quarter, review_year
  FROM performance
)
SELECT
  jo.anon_employee_id,
  jo.job_family,
  jo.job_level,
  jo.location,
  jo.gender,
  jo.ethnicity,
  jo.age_band,
  jo.hire_year,
  perf.rating,
  comp.base_salary,
  comp.bonus,
  comp.stock,
  comp.currency,
  comp.comp_year,
  perf.review_quarter,
  perf.review_year
FROM jo
LEFT JOIN comp  ON jo.employee_id = comp.employee_id
LEFT JOIN perf  ON jo.employee_id = perf.employee_id
""")

display(spark.sql("SELECT * FROM data_analyst_view"))


# COMMAND ----------

display(spark.sql("SHOW TABLES IN paygov.hr_data"))


# COMMAND ----------

display(spark.sql("SELECT first_name,last_name,email,ssn FROM employees"))


# COMMAND ----------

display(spark.sql("SELECT * FROM data_analyst_view"))


# COMMAND ----------

