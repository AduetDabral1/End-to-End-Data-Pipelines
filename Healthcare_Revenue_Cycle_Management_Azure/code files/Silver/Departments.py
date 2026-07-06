# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

# COMMAND ----------

#Reading Hospital H1 departments data 
df_hos_h1=spark.read.csv("/mnt/bronze/aduet-hospital-h1/departments.csv", header=True)
df_hos_h1 = df_hos_h1.withColumn("datasource", f.lit("hos-h1"))

#Reading Hospital H2 departments data 
df_hos_h2=spark.read.csv("/mnt/bronze/aduet-hospital-h2/departments.csv", header=True)
df_hos_h2 = df_hos_h2.withColumn("datasource", f.lit("hos-h2"))

# COMMAND ----------

#union two departments dataframes
df_merged = df_hos_h1.unionByName(df_hos_h2)


# COMMAND ----------

df_merged.printSchema()

# COMMAND ----------

df_merged = (
    df_merged
    .withColumn("SRC_Dept_id", f.col("DeptID"))
    .withColumn(
        "Dept_id",
        f.concat(f.col("DeptID"), f.lit("-"), f.col("datasource"))
    )
    .drop("DeptID")
)

# COMMAND ----------

display(df_merged)

# COMMAND ----------

silver_departments_df = (
    df_merged
    .withColumn(
        "is_quarantined",
        (f.col("SRC_Dept_Id").isNull()) | (f.col("Name").isNull())
    )
)

# COMMAND ----------

silver_departments_df.createOrReplaceTempView("silver_departments_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_departments_df
# MAGIC

# COMMAND ----------

silver_departments_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/silver/department")