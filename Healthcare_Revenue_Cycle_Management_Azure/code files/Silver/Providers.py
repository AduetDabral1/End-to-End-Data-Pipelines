# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;

# COMMAND ----------

#Reading Hospital H1 departments data 
df_hos_h1 = spark.read.csv("/mnt/bronze/aduet-hospital-h1/providers.csv", header=True)
df_hos_h1 = df_hos_h1.withColumn("datasource", f.lit("hos-h1"))

#Reading Hospital B departments data 
df_hos_h2 =spark.read.csv("/mnt/bronze/aduet-hospital-h2/providers.csv", header=True)
df_hos_h2 = df_hos_h2.withColumn("datasource", f.lit("hos-h2"))

# COMMAND ----------

#union two departments dataframes
df_merged = df_hos_h1.unionByName(df_hos_h2)
display(df_merged)

# COMMAND ----------

df_merged.printSchema()

# COMMAND ----------

df_merged.createOrReplaceTempView("providers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.providers (
# MAGIC ProviderID string,
# MAGIC FirstName string,
# MAGIC LastName string,
# MAGIC Specialization string,
# MAGIC DeptID string,
# MAGIC NPI long,
# MAGIC datasource string,
# MAGIC is_quarantined boolean
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table silver.providers

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silver.providers
# MAGIC SELECT DISTINCT
# MAGIC     ProviderID,
# MAGIC     FirstName,
# MAGIC     LastName,
# MAGIC     Specialization,
# MAGIC     DeptID,
# MAGIC     CAST(NPI AS BIGINT) AS NPI,
# MAGIC     datasource,
# MAGIC     CASE
# MAGIC         WHEN ProviderID IS NULL OR DeptID IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM providers;

# COMMAND ----------

providers_df = spark.table("silver.providers")

# COMMAND ----------

display(providers_df)

# COMMAND ----------

providers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/silver/provider")