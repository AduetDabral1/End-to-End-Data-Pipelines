# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_department
# MAGIC (
# MAGIC Dept_Id string,
# MAGIC SRC_Dept_Id string,
# MAGIC Name string,
# MAGIC datasource string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gold.dim_department
# MAGIC select 
# MAGIC distinct
# MAGIC Dept_Id ,
# MAGIC SRC_Dept_Id ,
# MAGIC Name ,
# MAGIC datasource 
# MAGIC  from silver_departments
# MAGIC  where is_quarantined=false

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from gold.dim_department

# COMMAND ----------

df = spark.table("gold.dim_department")

# COMMAND ----------

df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/gold/dim_department")