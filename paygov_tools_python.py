# Databricks notebook source
spark.sql("USE CATALOG paygov")
spark.sql("USE SCHEMA hr_data")


# COMMAND ----------

from pyspark.sql import functions as F

def unadjusted_gap_gender(job_family="*", job_level="*", location="*"):
    base = spark.table("v_total_comp")
    df = base
    if job_family != "*":
        df = df.filter(F.col("job_family") == job_family)
    if job_level != "*":
        df = df.filter(F.col("job_level") == job_level)
    if location != "*":
        df = df.filter(F.col("location") == location)
    return (df.groupBy("comp_year","gender")
              .agg(F.round(F.avg("total_comp"),2).alias("avg_total_comp"),
                   F.count("*").alias("employee_count"))
              .orderBy("comp_year","gender"))

def adjusted_gap_gender(job_family="*", location="*"):
    base = spark.table("v_with_tenure")
    df = base
    if job_family != "*":
        df = df.filter(F.col("job_family") == job_family)
    if location != "*":
        df = df.filter(F.col("location") == location)
    strata = (df.groupBy("comp_year","job_level","tenure_band","location")
                .agg(F.round(F.avg(F.when(F.col("gender")=="Female", F.col("total_comp"))),2).alias("avg_female"),
                     F.round(F.avg(F.when(F.col("gender")=="Male",   F.col("total_comp"))),2).alias("avg_male"),
                     F.sum(F.when(F.col("gender")=="Female", 1).otherwise(0)).alias("n_female"),
                     F.sum(F.when(F.col("gender")=="Male",   1).otherwise(0)).alias("n_male")))
    return (strata
            .withColumn("gender_gap_ratio",
                        F.when((F.col("avg_male").isNull()) | (F.col("avg_male")==0), F.lit(None))
                         .otherwise(F.round(F.col("avg_female")/F.col("avg_male"),4)))
            .orderBy("comp_year","job_level","tenure_band","location"))

def breakdown(by="tenure", job_family="*"):
    df = spark.table("v_with_tenure")
    if job_family != "*":
        df = df.filter(F.col("job_family")==job_family)
    if by == "tenure":
        key = "tenure_band"
    elif by == "role":
        key = "job_level"
    elif by == "location":
        key = "location"
    else:
        key = "tenure_band"
    return (df.groupBy(key,"comp_year")
              .agg(F.count("*").alias("employees"),
                   F.round(F.avg("total_comp"),2).alias("avg_total_comp"))
              .orderBy("comp_year", key))


# COMMAND ----------

display(unadjusted_gap_gender())


# COMMAND ----------

display(unadjusted_gap_gender(job_family="Engineering"))


# COMMAND ----------

