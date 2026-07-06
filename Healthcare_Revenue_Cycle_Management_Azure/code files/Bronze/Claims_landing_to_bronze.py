# Databricks notebook source
# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

claims_df=spark.read.csv("/mnt/landing/claims/*.csv",header=True)

# COMMAND ----------

claims_df = claims_df.withColumn(
    "datasource",
    f.when(f.input_file_name().contains("hospital1"), "hosh1").when(f.input_file_name().contains("hospital2"), "hosh2")
     .otherwise(None)
)

display(claims_df)

# COMMAND ----------

# Parquet file creation
claims_df.write.format("parquet").mode("overwrite").save("/mnt/bronze/claims/")

# COMMAND ----------

claims_df.createOrReplaceTempView("claims")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT 
# MAGIC CONCAT(ClaimID,'-', datasource) AS ClaimID,
# MAGIC ClaimID AS  SRC_ClaimID,
# MAGIC TransactionID,
# MAGIC PatientID,
# MAGIC EncounterID,
# MAGIC ProviderID,
# MAGIC DeptID,
# MAGIC cast(ServiceDate as date) ServiceDate,
# MAGIC cast(ClaimDate as date) ClaimDate,
# MAGIC PayorID,
# MAGIC ClaimAmount,
# MAGIC PaidAmount,
# MAGIC ClaimStatus,
# MAGIC PayorType,
# MAGIC Deductible,
# MAGIC Coinsurance,
# MAGIC Copay,
# MAGIC cast(InsertDate as date) as SRC_InsertDate,
# MAGIC cast(ModifiedDate as date) as SRC_ModifiedDate,
# MAGIC datasource,
# MAGIC     CASE 
# MAGIC         WHEN ClaimID IS NULL OR TransactionID IS NULL OR PatientID IS NULL or ServiceDate IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC         END AS is_quarantined
# MAGIC FROM claims

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from quality_checks