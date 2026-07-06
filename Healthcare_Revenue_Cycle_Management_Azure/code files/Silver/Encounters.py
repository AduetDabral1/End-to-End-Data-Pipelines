# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

# COMMAND ----------

#Reading Hospital H1 encounters data 
df_hos_h1=spark.read.csv("/mnt/bronze/aduet-hospital-h1/encounters.csv", header=True)
df_hos_h1 = (
    df_hos_h1
    .withColumn("datasource", f.lit("hos-h1"))
    .withColumn("EncounterDate",f.to_date(f.col("EncounterDate"), "M/d/yyyy"))
    .withColumn("InsertedDate",f.to_date(f.col("InsertedDate"), "M/d/yyyy"))
    .withColumn("ModifiedDate",f.to_date(f.col("ModifiedDate"), "M/d/yyyy"))
    .withColumn("ProcedureCode", f.col("ProcedureCode").cast("int"))
)

#Reading Hospital H2 encounters data 
df_hos_h2=spark.read.csv("/mnt/bronze/aduet-hospital-h2/encounters.csv", header=True)
df_hos_h2 = (
    df_hos_h2
    .withColumn("datasource", f.lit("hos-h2"))
    .withColumn("InsertedDate",f.to_date(f.col("InsertedDate"), "M/d/yyyy"))
    .withColumn("ModifiedDate",f.to_date(f.col("ModifiedDate"), "M/d/yyyy"))
    .withColumn("EncounterDate",f.to_date(f.col("EncounterDate"), "M/d/yyyy"))
    .withColumn("ProcedureCode", f.col("ProcedureCode").cast("int"))
)

# COMMAND ----------

#union two departments dataframes
df_merged = df_hos_h1.unionAll(df_hos_h2)


# COMMAND ----------

df_merged.printSchema()

# COMMAND ----------

df_merged.createOrReplaceTempView("df_merged")

# COMMAND ----------

quality_checks = (
    df_merged
    .withColumn("SRC_EncounterID", f.concat(f.col("EncounterID"), f.lit("-"), f.col("datasource")))
    .withColumnRenamed("InsertedDate", "SRC_InsertedDate")
    .withColumnRenamed("ModifiedDate", "SRC_ModifiedDate")
    .withColumn(
        "is_quarantined",
        (f.col("EncounterID").isNull()) | (f.col("PatientID").isNull())
        )
)

quality_checks.createOrReplaceTempView("quality_checks")

# COMMAND ----------

quality_checks.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_encounters (
# MAGIC EncounterID string,
# MAGIC SRC_EncounterID string,
# MAGIC PatientID string,
# MAGIC EncounterDate date,
# MAGIC EncounterType string,
# MAGIC ProviderID string,
# MAGIC DepartmentID string,
# MAGIC ProcedureCode integer,
# MAGIC SRC_InsertedDate date,
# MAGIC SRC_ModifiedDate date,
# MAGIC datasource string,
# MAGIC is_quarantined boolean,
# MAGIC audit_insertdate timestamp,
# MAGIC audit_modifieddate timestamp,
# MAGIC is_current boolean
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update old record to implement SCD Type 2
# MAGIC MERGE INTO silver_encounters AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.EncounterID = source.EncounterID AND target.is_current = true
# MAGIC WHEN MATCHED AND (
# MAGIC     target.SRC_EncounterID != source.SRC_EncounterID OR
# MAGIC     target.PatientID != source.PatientID OR
# MAGIC     target.EncounterDate != source.EncounterDate OR
# MAGIC     target.EncounterType != source.EncounterType OR
# MAGIC     target.ProviderID != source.ProviderID OR
# MAGIC     target.DepartmentID != source.DepartmentID OR
# MAGIC     target.ProcedureCode != source.ProcedureCode OR
# MAGIC     target.SRC_InsertedDate != source.SRC_InsertedDate OR
# MAGIC     target.SRC_ModifiedDate != source.SRC_ModifiedDate OR
# MAGIC     target.datasource != source.datasource OR
# MAGIC     target.is_quarantined != source.is_quarantined
# MAGIC ) THEN
# MAGIC   UPDATE SET
# MAGIC     target.is_current = false,
# MAGIC     target.audit_modifieddate = current_timestamp()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert new record to implement SCD Type 2
# MAGIC MERGE INTO silver_encounters AS target USING quality_checks AS source ON target.EncounterID = source.EncounterID
# MAGIC AND target.is_current = true
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   (
# MAGIC     EncounterID,
# MAGIC     SRC_EncounterID,
# MAGIC     PatientID,
# MAGIC     EncounterDate,
# MAGIC     EncounterType,
# MAGIC     ProviderID,
# MAGIC     DepartmentID,
# MAGIC     ProcedureCode,
# MAGIC     SRC_InsertedDate,
# MAGIC     SRC_ModifiedDate,
# MAGIC     datasource,
# MAGIC     is_quarantined,
# MAGIC     audit_insertdate,
# MAGIC     audit_modifieddate,
# MAGIC     is_current
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     source.EncounterID,
# MAGIC     source.SRC_EncounterID,
# MAGIC     source.PatientID,
# MAGIC     source.EncounterDate,
# MAGIC     source.EncounterType,
# MAGIC     source.ProviderID,
# MAGIC     source.DepartmentID,
# MAGIC     source.ProcedureCode,
# MAGIC     source.SRC_InsertedDate,
# MAGIC     source.SRC_ModifiedDate,
# MAGIC     source.datasource,
# MAGIC     source.is_quarantined,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     true
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC select SRC_EncounterID,datasource,count(patientid) from  silver_encounters
# MAGIC group by all
# MAGIC order by 3 desc

# COMMAND ----------

df = spark.table("silver_encounters")

# COMMAND ----------

df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/silver/encounters")