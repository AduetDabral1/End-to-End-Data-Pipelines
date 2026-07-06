# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

# COMMAND ----------

#Reading Hospital H1 transactions data 
df_hos_h1=spark.read.csv("/mnt/bronze/aduet-hospital-h1/transactions.csv", header=True)
df_hos_h1 = (
    df_hos_h1
    .withColumn("datasource", f.lit("hos-h1"))
    .withColumn("VisitDate",f.to_date(f.col("VisitDate"), "M/d/yyyy"))
    .withColumn("ServiceDate",f.to_date(f.col("ServiceDate"), "M/d/yyyy"))
    .withColumn("PaidDate",f.to_date(f.col("PaidDate"), "M/d/yyyy"))
    .withColumn("InsertDate",f.to_date(f.col("InsertDate"), "M/d/yyyy"))
    .withColumn("ModifiedDate",f.to_date(f.col("ModifiedDate"), "M/d/yyyy"))
    .withColumn("PaidAmount", f.col("ProcedureCode").cast("double"))
    .withColumn("ProcedureCode", f.col("ProcedureCode").cast("int"))
)

#Reading Hospital H2 transactions data 
df_hos_h2=spark.read.csv("/mnt/bronze/aduet-hospital-h2/transactions.csv", header=True)
df_hos_h2 = (
    df_hos_h2
    .withColumn("datasource", f.lit("hos-h1"))
    .withColumn("VisitDate",f.to_date(f.col("VisitDate"), "M/d/yyyy"))
    .withColumn("ServiceDate",f.to_date(f.col("ServiceDate"), "M/d/yyyy"))
    .withColumn("PaidDate",f.to_date(f.col("PaidDate"), "M/d/yyyy"))
    .withColumn("InsertDate",f.to_date(f.col("InsertDate"), "M/d/yyyy"))
    .withColumn("ModifiedDate",f.to_date(f.col("ModifiedDate"), "M/d/yyyy"))
    .withColumn("PaidAmount", f.col("ProcedureCode").cast("double"))
    .withColumn("ProcedureCode", f.col("ProcedureCode").cast("int"))
)

# COMMAND ----------

#union two departments dataframes
df_merged = df_hos_h1.unionByName(df_hos_h2)


# COMMAND ----------

df_merged.printSchema()

# COMMAND ----------

df_merged.createOrReplaceTempView("df_merged")

# COMMAND ----------

quality_checks = (
    df_merged
    .withColumn("SRC_TransactionID", f.concat(f.col("TransactionID"), f.lit("-"), f.col("datasource")))
    .withColumnRenamed("InsertDate", "SRC_InsertDate")
    .withColumnRenamed("ModifiedDate", "SRC_ModifiedDate")
    .withColumn(
        "is_quarantined",
        (f.col("EncounterID").isNull()) | (f.col("PatientID").isNull()) | (f.col("SRC_TransactionID").isNull()) | (f.col("VisitDate").isNull())
        )
)

quality_checks.createOrReplaceTempView("quality_checks")

# COMMAND ----------

quality_checks.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_transactions (
# MAGIC   TransactionID string,
# MAGIC   SRC_TransactionID string,
# MAGIC   EncounterID string,
# MAGIC   PatientID string,
# MAGIC   ProviderID string,
# MAGIC   DeptID string,
# MAGIC   VisitDate date,
# MAGIC   ServiceDate date,
# MAGIC   PaidDate date,
# MAGIC   VisitType string,
# MAGIC   Amount double,
# MAGIC   AmountType string,
# MAGIC   PaidAmount double,
# MAGIC   ClaimID string,
# MAGIC   PayorID string,
# MAGIC   ProcedureCode integer,
# MAGIC   ICDCode string,
# MAGIC   LineOfBusiness string,
# MAGIC   MedicaidID string,
# MAGIC   MedicareID string,
# MAGIC   SRC_InsertDate date,
# MAGIC   SRC_ModifiedDate date,
# MAGIC   datasource string,
# MAGIC   is_quarantined boolean,
# MAGIC   audit_insertdate timestamp,
# MAGIC   audit_modifieddate timestamp,
# MAGIC   is_current boolean
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update old record to implement SCD Type 2
# MAGIC MERGE INTO silver_transactions AS target USING quality_checks AS source ON target.TransactionID = source.TransactionID
# MAGIC AND target.is_current = true
# MAGIC WHEN MATCHED
# MAGIC AND (
# MAGIC   target.SRC_TransactionID != source.SRC_TransactionID
# MAGIC   OR target.EncounterID != source.EncounterID
# MAGIC   OR target.PatientID != source.PatientID
# MAGIC   OR target.ProviderID != source.ProviderID
# MAGIC   OR target.DeptID != source.DeptID
# MAGIC   OR target.VisitDate != source.VisitDate
# MAGIC   OR target.ServiceDate != source.ServiceDate
# MAGIC   OR target.PaidDate != source.PaidDate
# MAGIC   OR target.VisitType != source.VisitType
# MAGIC   OR target.Amount != source.Amount
# MAGIC   OR target.AmountType != source.AmountType
# MAGIC   OR target.PaidAmount != source.PaidAmount
# MAGIC   OR target.ClaimID != source.ClaimID
# MAGIC   OR target.PayorID != source.PayorID
# MAGIC   OR target.ProcedureCode != source.ProcedureCode
# MAGIC   OR target.ICDCode != source.ICDCode
# MAGIC   OR target.LineOfBusiness != source.LineOfBusiness
# MAGIC   OR target.MedicaidID != source.MedicaidID
# MAGIC   OR target.MedicareID != source.MedicareID
# MAGIC   OR target.SRC_InsertDate != source.SRC_InsertDate
# MAGIC   OR target.SRC_ModifiedDate != source.SRC_ModifiedDate
# MAGIC   OR target.datasource != source.datasource
# MAGIC   OR target.is_quarantined != source.is_quarantined
# MAGIC ) THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   target.is_current = false,
# MAGIC   target.audit_modifieddate = current_timestamp()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert new record to implement SCD Type 2
# MAGIC MERGE INTO silver_transactions AS target USING quality_checks AS source ON target.TransactionID = source.TransactionID
# MAGIC AND target.is_current = true
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   (
# MAGIC     TransactionID,
# MAGIC     SRC_TransactionID,
# MAGIC     EncounterID,
# MAGIC     PatientID,
# MAGIC     ProviderID,
# MAGIC     DeptID,
# MAGIC     VisitDate,
# MAGIC     ServiceDate,
# MAGIC     PaidDate,
# MAGIC     VisitType,
# MAGIC     Amount,
# MAGIC     AmountType,
# MAGIC     PaidAmount,
# MAGIC     ClaimID,
# MAGIC     PayorID,
# MAGIC     ProcedureCode,
# MAGIC     ICDCode,
# MAGIC     LineOfBusiness,
# MAGIC     MedicaidID,
# MAGIC     MedicareID,
# MAGIC     SRC_InsertDate,
# MAGIC     SRC_ModifiedDate,
# MAGIC     datasource,
# MAGIC     is_quarantined,
# MAGIC     audit_insertdate,
# MAGIC     audit_modifieddate,
# MAGIC     is_current
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     source.TransactionID,
# MAGIC     source.SRC_TransactionID,
# MAGIC     source.EncounterID,
# MAGIC     source.PatientID,
# MAGIC     source.ProviderID,
# MAGIC     source.DeptID,
# MAGIC     source.VisitDate,
# MAGIC     source.ServiceDate,
# MAGIC     source.PaidDate,
# MAGIC     source.VisitType,
# MAGIC     source.Amount,
# MAGIC     source.AmountType,
# MAGIC     source.PaidAmount,
# MAGIC     source.ClaimID,
# MAGIC     source.PayorID,
# MAGIC     source.ProcedureCode,
# MAGIC     source.ICDCode,
# MAGIC     source.LineOfBusiness,
# MAGIC     source.MedicaidID,
# MAGIC     source.MedicareID,
# MAGIC     source.SRC_InsertDate,
# MAGIC     source.SRC_ModifiedDate,
# MAGIC     source.datasource,
# MAGIC     source.is_quarantined,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     true
# MAGIC   );

# COMMAND ----------

df = spark.table("silver_transactions")

# COMMAND ----------

df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/silver/transactions")