# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS aduet_healthcare_delta_lake.audit;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE audit.load_logs
# MAGIC (
# MAGIC     id INT IDENTITY(1,1) PRIMARY KEY,
# MAGIC     pipeline_name NVARCHAR(100),
# MAGIC     data_source NVARCHAR(100),
# MAGIC     tablename NVARCHAR(100),
# MAGIC     load_type NVARCHAR(20),
# MAGIC     numberofrowscopied INT,
# MAGIC     watermarkcolumnname NVARCHAR(100),
# MAGIC     loaddate DATETIME2,
# MAGIC     status NVARCHAR(20)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table aduet_healthcare_delta_lake.audit.load_logs 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aduet_healthcare_delta_lake.audit.load_logs