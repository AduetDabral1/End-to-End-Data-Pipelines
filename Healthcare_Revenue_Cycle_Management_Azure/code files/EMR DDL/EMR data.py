# Databricks notebook source
pip install faker

# COMMAND ----------

# Database details
server_name = "" 
database_name_h1 = "aduet-hospital-h1"
database_name_h2 = "aduet-hospital-h2"
username = ""
password = ""

# COMMAND ----------

import pandas as pd

# Define department data
departments = {
    "DeptID": [f"DEPT{str(i).zfill(3)}" for i in range(1, 21)],
    "Name": [
        "Emergency", "Cardiology", "Neurology", "Oncology", "Pediatrics", 
        "Orthopedics", "Dermatology", "Gastroenterology", "Urology", 
        "Radiology", "Anesthesiology", "Pathology", "Surgery", 
        "Pulmonology", "Nephrology", "Ophthalmology", "Gynecology", 
        "Psychiatry", "Endocrinology", "Rheumatology"
    ]
}

# Create DataFrame
departments_df = pd.DataFrame(departments)

# Convert Pandas DataFrame to a Spark DataFrame
spark_departments_df = spark.createDataFrame(departments_df)

table_departments = "dbo.departments"  # The target table name in Azure SQL

# COMMAND ----------

import pandas as pd
from faker import Faker
import random

# Initialize Faker
fake = Faker()

# Parameters for data generation
num_encounters = 10000  # Number of encounter records per hospital
encounter_types = ["Inpatient", "Outpatient", "Emergency", "Telemedicine", "Routine Checkup"]
cpt_codes = [str(random.randint(10000, 99999)) for _ in range(1000)]  # Sample CPT codes

# Generate Hospital 1 encounter data
hospital1_encounter_data = {
    "EncounterID": [f"ENC{str(i).zfill(6)}" for i in range(1, num_encounters + 1)],
    "PatientID": [f"HOSP1-{str(random.randint(1, 5000)).zfill(6)}" for _ in range(num_encounters)],
    "EncounterDate": [fake.date_this_decade(before_today=True, after_today=False) for _ in range(num_encounters)],
    "EncounterType": [random.choice(encounter_types) for _ in range(num_encounters)],
    "ProviderID": [f"PROV{str(random.randint(1, 500)).zfill(4)}" for _ in range(num_encounters)],
    "DepartmentID": [f"DEPT{str(random.randint(1, 20)).zfill(3)}" for _ in range(num_encounters)],
    "ProcedureCode": [random.choice(cpt_codes) for _ in range(num_encounters)],
    "InsertedDate": [fake.date_this_decade(before_today=True, after_today=False) for _ in range(num_encounters)],
    "ModifiedDate": [fake.date_this_decade(before_today=True, after_today=False) for _ in range(num_encounters)]
}

# Generate Hospital 2 encounter data
hospital2_encounter_data = {
    "EncounterID": [f"ENC{str(i).zfill(6)}" for i in range(1, num_encounters + 1)],
    "PatientID": [f"HOSP1-{str(random.randint(1, 5000)).zfill(6)}" for _ in range(num_encounters)],
    "EncounterDate": [fake.date_this_decade(before_today=True, after_today=False) for _ in range(num_encounters)],
    "EncounterType": [random.choice(encounter_types) for _ in range(num_encounters)],
    "ProviderID": [f"PROV{str(random.randint(1, 500)).zfill(4)}" for _ in range(num_encounters)],
    "DepartmentID": [f"DEPT{str(random.randint(1, 20)).zfill(3)}" for _ in range(num_encounters)],
    "ProcedureCode": [random.choice(cpt_codes) for _ in range(num_encounters)],
    "InsertedDate": [fake.date_this_decade(before_today=True, after_today=False) for _ in range(num_encounters)],
    "ModifiedDate": [fake.date_this_decade(before_today=True, after_today=False) for _ in range(num_encounters)]
}

# Create DataFrames
hospital1_encounters_df = pd.DataFrame(hospital1_encounter_data)
hospital2_encounters_df = pd.DataFrame(hospital2_encounter_data)

# Convert Pandas DataFrame to a Spark DataFrame
spark_encounters_h1_df = spark.createDataFrame(hospital1_encounters_df)
spark_encounters_h2_df = spark.createDataFrame(hospital2_encounters_df)

table_encounters = "dbo.encounters"  # The target table name in Azure SQL


# COMMAND ----------

import random
from datetime import datetime
from faker import Faker
import pandas as pd

# Initialize Faker
fake = Faker()

# Increase Faker's seed for reproducibility
Faker.seed(42)

# Generate 50,000 patient records for Hospital H1
num_records = 50000
hospital1_patient_data = {
    "PatientID": [f"HOSP{str(i).zfill(6)}" for i in range(1, num_records + 1)],
    "FirstName": [fake.first_name() for _ in range(num_records)],
    "LastName": [fake.last_name() for _ in range(num_records)],
    "MiddleName": [fake.random_letter().upper() for _ in range(num_records)],
    "SSN": [fake.ssn() for _ in range(num_records)],
    "PhoneNumber": [fake.phone_number() for _ in range(num_records)],
    "Gender": [random.choice(["Male", "Female"]) for _ in range(num_records)],
    "DOB": [fake.date_of_birth(minimum_age=0, maximum_age=100) for _ in range(num_records)],
    "Address": [fake.address().replace('\n', ', ') for _ in range(num_records)],
    "ModifiedDate": [fake.date_this_decade(before_today=True, after_today=False) for _ in range(num_records)]
}

hospital2_patient_data = {
    "ID": [f"HOSP{str(i).zfill(6)}" for i in range(1, num_records + 1)],
    "F_Name": [fake.first_name() for _ in range(num_records)],
    "L_Name": [fake.last_name() for _ in range(num_records)],
    "M_Name": [fake.random_letter().upper() for _ in range(num_records)],
    "SSN": [fake.ssn() for _ in range(num_records)],
    "PhoneNumber": [fake.phone_number() for _ in range(num_records)],
    "Gender": [random.choice(["Male", "Female"]) for _ in range(num_records)],
    "DOB": [fake.date_of_birth(minimum_age=0, maximum_age=100) for _ in range(num_records)],
    "Address": [fake.address().replace('\n', ', ') for _ in range(num_records)],
    "Updated_Date": [fake.date_this_decade(before_today=True, after_today=False) for _ in range(num_records)]
}

# Create DataFrame
hospital1_patient_df = pd.DataFrame(hospital1_patient_data)
hospital2_patient_df = pd.DataFrame(hospital2_patient_data)

# Convert Pandas DataFrame to a Spark DataFrame
spark_patients_h1_df = spark.createDataFrame(hospital1_patient_df)
spark_patients_h2_df = spark.createDataFrame(hospital2_patient_df)

table_patients = "dbo.patients"  # The target table name in Azure SQL

# COMMAND ----------

import pandas as pd
from faker import Faker
import random

# Initialize Faker
fake = Faker()

# Parameters for data generation
num_providers_hospital1 = 25  # Number of providers in Hospital 1
num_providers_hospital2 = 30  # Number of providers in Hospital 2
specializations = ["Cardiology", "Neurology", "Orthopedics", "General Surgery", 
                   "Pediatrics", "Radiology", "Dermatology", "Oncology", 
                   "Anesthesiology", "Emergency Medicine", "Psychiatry"]
departments = [f"DEPT{str(i).zfill(3)}" for i in range(1, 21)]  # 20 department IDs

# Generate Hospital 1 provider data
hospital1_provider_data = {
    "ProviderID": [f"H1-PROV{str(i).zfill(4)}" for i in range(1, num_providers_hospital1 + 1)],
    "FirstName": [fake.first_name() for _ in range(num_providers_hospital1)],
    "LastName": [fake.last_name() for _ in range(num_providers_hospital1)],
    "Specialization": [random.choice(specializations) for _ in range(num_providers_hospital1)],
    "DeptID": [random.choice(departments) for _ in range(num_providers_hospital1)],
    "NPI": [fake.unique.numerify("##########") for _ in range(num_providers_hospital1)]  # NPI as a 10-digit number
}

# Generate Hospital 2 provider data
hospital2_provider_data = {
    "ProviderID": [f"H2-PROV{str(i).zfill(4)}" for i in range(1, num_providers_hospital2 + 1)],
    "FirstName": [fake.first_name() for _ in range(num_providers_hospital2)],
    "LastName": [fake.last_name() for _ in range(num_providers_hospital2)],
    "Specialization": [random.choice(specializations) for _ in range(num_providers_hospital2)],
    "DeptID": [random.choice(departments) for _ in range(num_providers_hospital2)],
    "NPI": [fake.unique.numerify("##########") for _ in range(num_providers_hospital2)]  # NPI as a 10-digit number
}

# Create DataFrames
hospital1_providers_df = pd.DataFrame(hospital1_provider_data)
hospital2_providers_df = pd.DataFrame(hospital2_provider_data)

# Convert Pandas DataFrame to a Spark DataFrame
spark_providers_h1_df = spark.createDataFrame(hospital1_providers_df)
spark_providers_h2_df = spark.createDataFrame(hospital2_providers_df)

table_providers = "dbo.providers"  # The target table name in Azure SQL


# COMMAND ----------

import pandas as pd
from faker import Faker
import random

# Initialize Faker
fake = Faker()

# Parameters for data generation
num_transactions = 10000  # Number of transaction records per hospital
amount_types = ["Co-pay", "Insurance", "Self-pay", "Medicaid", "Medicare"]
visit_types = ["Routine", "Follow-up", "Emergency", "Consultation"]
line_of_business = ["Commercial", "Medicaid", "Medicare", "Self-Pay"]
icd_codes = [f"I{random.randint(10, 99)}.{random.randint(0, 9)}" for _ in range(100)]  # Sample ICD codes
cpt_codes = [str(random.randint(10000, 99999)) for _ in range(1000)]  # Sample CPT codes

# Generate Hospital 1 transaction data
hospital1_transaction_data = {
    "TransactionID": [f"TRANS{str(i).zfill(6)}" for i in range(1, num_transactions + 1)],
    "EncounterID": [f"ENC{str(random.randint(1, 10000)).zfill(6)}" for _ in range(num_transactions)],
    "PatientID": [f"HOSP1-{str(random.randint(1, 5000)).zfill(6)}" for _ in range(num_transactions)],
    "ProviderID": [f"PROV{str(random.randint(1, 500)).zfill(4)}" for _ in range(num_transactions)],
    "DeptID": [f"DEPT{str(random.randint(1, 20)).zfill(3)}" for _ in range(num_transactions)],
    "VisitDate": [fake.date_this_year(before_today=True, after_today=False) for _ in range(num_transactions)],
    "ServiceDate": [fake.date_this_year(before_today=True, after_today=False) for _ in range(num_transactions)],
    "PaidDate": [fake.date_this_year(before_today=True, after_today=False) for _ in range(num_transactions)],
    "VisitType": [random.choice(visit_types) for _ in range(num_transactions)],
    "Amount": [round(random.uniform(50, 1000), 2) for _ in range(num_transactions)],
    "AmountType": [random.choice(amount_types) for _ in range(num_transactions)],
    "PaidAmount": [round(random.uniform(20, 800), 2) for _ in range(num_transactions)],
    "ClaimID": [f"CLAIM{str(random.randint(100000, 999999))}" for _ in range(num_transactions)],
    "PayorID": [f"PAYOR{str(random.randint(1000, 9999))}" for _ in range(num_transactions)],
    "ProcedureCode": [random.choice(cpt_codes) for _ in range(num_transactions)],
    "ICDCode": [random.choice(icd_codes) for _ in range(num_transactions)],
    "LineOfBusiness": [random.choice(line_of_business) for _ in range(num_transactions)],
    "MedicaidID": [f"MEDI{str(random.randint(10000, 99999))}" for _ in range(num_transactions)],
    "MedicareID": [f"MCARE{str(random.randint(10000, 99999))}" for _ in range(num_transactions)],
    "InsertDate": [fake.date_this_decade(before_today=True, after_today=False) for _ in range(num_transactions)],
    "ModifiedDate": [fake.date_this_decade(before_today=True, after_today=False) for _ in range(num_transactions)]
}

# Generate Hospital 2 transaction data
hospital2_transaction_data = {
    "TransactionID": [f"TRANS{str(i).zfill(6)}" for i in range(1, num_transactions + 1)],
    "EncounterID": [f"ENC{str(random.randint(1, 10000)).zfill(6)}" for _ in range(num_transactions)],
    "PatientID": [f"HOSP1-{str(random.randint(1, 5000)).zfill(6)}" for _ in range(num_transactions)],
    "ProviderID": [f"PROV{str(random.randint(1, 500)).zfill(4)}" for _ in range(num_transactions)],
    "DeptID": [f"DEPT{str(random.randint(1, 20)).zfill(3)}" for _ in range(num_transactions)],
    "VisitDate": [fake.date_this_year(before_today=True, after_today=False) for _ in range(num_transactions)],
    "ServiceDate": [fake.date_this_year(before_today=True, after_today=False) for _ in range(num_transactions)],
    "PaidDate": [fake.date_this_year(before_today=True, after_today=False) for _ in range(num_transactions)],
    "VisitType": [random.choice(visit_types) for _ in range(num_transactions)],
    "Amount": [round(random.uniform(50, 1000), 2) for _ in range(num_transactions)],
    "AmountType": [random.choice(amount_types) for _ in range(num_transactions)],
    "PaidAmount": [round(random.uniform(20, 800), 2) for _ in range(num_transactions)],
    "ClaimID": [f"CLAIM{str(random.randint(100000, 999999))}" for _ in range(num_transactions)],
    "PayorID": [f"PAYOR{str(random.randint(1000, 9999))}" for _ in range(num_transactions)],
    "ProcedureCode": [random.choice(cpt_codes) for _ in range(num_transactions)],
    "ICDCode": [random.choice(icd_codes) for _ in range(num_transactions)],
    "LineOfBusiness": [random.choice(line_of_business) for _ in range(num_transactions)],
    "MedicaidID": [f"MEDI{str(random.randint(10000, 99999))}" for _ in range(num_transactions)],
    "MedicareID": [f"MCARE{str(random.randint(10000, 99999))}" for _ in range(num_transactions)],
    "InsertDate": [fake.date_this_decade(before_today=True, after_today=False) for _ in range(num_transactions)],
    "ModifiedDate": [fake.date_this_decade(before_today=True, after_today=False) for _ in range(num_transactions)]
}

# Create DataFrames
hospital1_transactions_df = pd.DataFrame(hospital1_transaction_data)
hospital2_transactions_df = pd.DataFrame(hospital2_transaction_data)

# Convert Pandas DataFrame to a Spark DataFrame
spark_transactions_h1_df = spark.createDataFrame(hospital1_transactions_df)
spark_transactions_h2_df = spark.createDataFrame(hospital2_transactions_df)

table_transactions = "dbo.transactions"  # The target table name in Azure SQL


# COMMAND ----------

# Construct the JDBC connection URL
jdbc_url_h1 = f"jdbc:sqlserver://{server_name}:1433;database={database_name_h1};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"


# Write the departments data to Hospital H1 DB
spark_departments_df.write \
    .format("jdbc") \
    .option("url", jdbc_url_h1) \
    .option("dbtable", table_departments) \
    .option("user", username) \
    .option("password", password) \
    .mode("append") \
    .save()

print("Department Data successfully loaded into Hospital 1 Database!")

# Write the encounters data to Hospital H1 DB
spark_encounters_h1_df.write \
    .format("jdbc") \
    .option("url", jdbc_url_h1) \
    .option("dbtable", table_encounters) \
    .option("user", username) \
    .option("password", password) \
    .mode("append") \
    .save()

print("Encounters Data successfully loaded into Hospital 1 Database!")

# Write the patients data to Hospital H1 DB
spark_patients_h1_df.write \
    .format("jdbc") \
    .option("url", jdbc_url_h1) \
    .option("dbtable", table_patients) \
    .option("user", username) \
    .option("password", password) \
    .mode("append") \
    .save()

print("Patients Data successfully loaded into Hospital 1 Database!")

# Write the providers data to Hospital H1 DB
spark_providers_h1_df.write \
    .format("jdbc") \
    .option("url", jdbc_url_h1) \
    .option("dbtable", table_providers) \
    .option("user", username) \
    .option("password", password) \
    .mode("append") \
    .save()

print("Providers Data successfully loaded into Hospital 1 Database!")

# Write the transactions data to Hospital H1 DB
spark_transactions_h1_df.write \
    .format("jdbc") \
    .option("url", jdbc_url_h1) \
    .option("dbtable", table_transactions) \
    .option("user", username) \
    .option("password", password) \
    .mode("append") \
    .save()

print("Transactions Data successfully loaded into Hospital 1 Database!")


# COMMAND ----------

# Construct the JDBC connection URL
jdbc_url_h2 = f"jdbc:sqlserver://{server_name}:1433;database={database_name_h2};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"


# Write the departments data to Hospital H2 DB
spark_departments_df.write \
    .format("jdbc") \
    .option("url", jdbc_url_h2) \
    .option("dbtable", table_departments) \
    .option("user", username) \
    .option("password", password) \
    .mode("append") \
    .save()

print("Department Data successfully loaded into Hospital 2 Database!")

# Write the encounters data to Hospital H2 DB
spark_encounters_h2_df.write \
    .format("jdbc") \
    .option("url", jdbc_url_h2) \
    .option("dbtable", table_encounters) \
    .option("user", username) \
    .option("password", password) \
    .mode("append") \
    .save()

print("Encounters Data successfully loaded into Hospital 2 Database!")

# Write the patients data to Hospital H2 DB
spark_patients_h2_df.write \
    .format("jdbc") \
    .option("url", jdbc_url_h2) \
    .option("dbtable", table_patients) \
    .option("user", username) \
    .option("password", password) \
    .mode("append") \
    .save()

print("Patients Data successfully loaded into Hospital 2 Database!")

# Write the providers data to Hospital H2 DB
spark_providers_h2_df.write \
    .format("jdbc") \
    .option("url", jdbc_url_h2) \
    .option("dbtable", table_providers) \
    .option("user", username) \
    .option("password", password) \
    .mode("append") \
    .save()

print("Providers Data successfully loaded into Hospital 2 Database!")

# Write the transactions data to Hospital H2 DB
spark_transactions_h2_df.write \
    .format("jdbc") \
    .option("url", jdbc_url_h2) \
    .option("dbtable", table_transactions) \
    .option("user", username) \
    .option("password", password) \
    .mode("append") \
    .save()

print("Transactions Data successfully loaded into Hospital 2 Database!")
