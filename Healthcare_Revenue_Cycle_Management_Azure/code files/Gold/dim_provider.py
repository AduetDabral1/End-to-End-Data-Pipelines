# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_provider
# MAGIC (
# MAGIC ProviderID string,
# MAGIC FirstName string,
# MAGIC LastName string,
# MAGIC DeptID string,
# MAGIC NPI long,
# MAGIC datasource string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gold.dim_provider
# MAGIC select 
# MAGIC ProviderID ,
# MAGIC FirstName ,
# MAGIC LastName ,
# MAGIC concat(DeptID,'-',datasource) deptid,
# MAGIC NPI ,
# MAGIC datasource 
# MAGIC  from silver.providers
# MAGIC  where is_quarantined=false

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.dim_provider

# COMMAND ----------

df = spark.table("gold.dim_provider")

# COMMAND ----------

df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/gold/dim_provider")