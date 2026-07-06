# Azure Platform for Healthcare Revenue Cycle Management (RCM) Data  Analytics

> Production-inspired Azure Data Engineering platform that ingests operational healthcare data from multiple sources into a Delta Lakehouse using metadata-driven pipelines, Medallion Architecture, and Slowly Changing Dimensions (SCD Type 2) for analytics-ready reporting.

---

## Overview

Healthcare organizations generate operational data from multiple disconnected systems including Electronic Medical Records (EMR), insurance claims, provider registries, and external healthcare standards.

This project demonstrates how a modern Azure Data Engineering platform can integrate these heterogeneous data sources into a centralized Lakehouse while preserving historical changes, improving data quality, and enabling business reporting.

The platform was designed around a real Revenue Cycle Management (RCM) use case where finance teams require reliable analytics for monitoring Accounts Receivable, payment collections, provider performance, and revenue trends.

---

# Business Problem

Hospital systems often store clinical, billing, and insurance data across multiple disconnected platforms. These inconsistencies make financial reporting, provider analytics, and revenue tracking difficult. Healthcare providers often operate multiple hospital systems that:

- Maintain different EMR databases
- Store claims as monthly flat files
- Consume external healthcare reference datasets
- Use inconsistent schemas across hospitals
- Require historical tracking for regulatory and analytical purposes

Without a centralized data platform, reporting becomes:

- Slow
- Inconsistent
- Difficult to audit
- Expensive to maintain

This project demonstrates how a modern Azure Lakehouse can consolidate heterogeneous healthcare data into a governed analytics platform capable of supporting operational reporting and downstream BI workloads.

---

# Solution Architecture

<p align="center">
<img width="1487" height="825" alt="Project Architecture" src="https://github.com/user-attachments/assets/bb78cf1d-b740-4854-8d67-15a888a8fa33" />
</p>

The solution ingests healthcare data from multiple operational systems into Azure Data Lake Storage Gen2 before progressively transforming it through Bronze, Silver, and Gold layers using Azure Databricks.

### Source Systems

- Azure SQL Database (Hospital EMR)
- Insurance Claims (CSV)
- CPT Code Files
- National Provider Identifier (NPI) Public API
- ICD Disease Classification API

### Azure Services

- Azure Data Factory
- Azure Databricks
- Azure SQL Database
- Azure Data Lake Storage Gen2
- Azure Key Vault
- Unity Catalog
- Delta Lake

---

# Business Workflow

The project models the **Accounts Receivable** side of Revenue Cycle Management.

```
Patient Registration
        │
        ▼
Medical Encounter
        │
        ▼
Charge Generated
        │
        ▼
Insurance Claim
        │
        ▼
Payment Processing
        │
        ▼
Financial Reporting
```

The resulting data warehouse enables downstream teams to analyze:

- Outstanding receivables
- Provider revenue
- Department revenue
- Insurance collections
- Payment aging
- Operational KPIs


---

# Technologies Used

| Category | Technologies |
|----------|--------------|
| Cloud | Microsoft Azure |
| Storage | Azure Data Lake Storage Gen2 |
| Orchestration | Azure Data Factory |
| Compute | Azure Databricks |
| Processing | Apache Spark |
| Storage Format | Delta Lake |
| Language | PySpark, SQL |
| Secrets | Azure Key Vault |
| Metadata | Unity Catalog |
| Source Systems | Azure SQL Database, CSV, REST APIs |

---

## Engineering Highlights

- Designed a metadata-driven ingestion framework supporting configurable Full and Incremental loads.
- Consolidated multiple heterogeneous healthcare data sources into a unified Lakehouse. using the Medallion Architecture (Bronze, Silver, Gold).
- Implemented Slowly Changing Dimension Type 2 for historical tracking.
- Designed a Star Schema optimized for analytical workloads.
- Standardized disparate EMR schemas into a Common Data Model.
- Automated orchestration using Azure Data Factory.
- Built Delta Lake tables optimized for analytical workloads.
- Secured secrets using Azure Key Vault.
- Automated orchestration using Azure Data Factory with reusable pipelines.

---

# Medallion Architecture

```
Landing
    │
    ▼
Bronze
    │
    ▼
Silver
    │
    ▼
Gold
```

## Landing

Stores raw incoming files exactly as received.

- Claims CSV
- CPT files

No transformations are performed.

---

## Bronze Layer

Purpose:

Preserve source data in a standardized format.

Characteristics:

- Parquet
- Immutable source copy
- Source of truth
- Schema preservation

Data Sources

- EMR
- Claims
- CPT
- ICD
- NPI

---

## Silver Layer

Business transformations occur here.

Implemented features include:

- Common Data Model (CDM)
- Schema standardization
- Quality validation
- Quarantine handling
- SCD Type 2
- Delta Lake tables

Historical changes are preserved for entities such as:

- Patients
- Encounters
- Transactions
- Claims

---

## Gold Layer

Business-ready star schema optimized for analytics.

Contains:

- Fact tables
- Dimension tables
- Curated datasets
- Reporting views

Designed for:

- Power BI
- SQL Analytics
- Machine Learning
- Ad-hoc SQL

---

# Data Model

<p align="center">
<img width="1177" height="806" alt="ERD Diagram (Gold Layer)" src="https://github.com/user-attachments/assets/4e466494-bb3f-4e86-94cf-ad5ea8c34859" />
</p>

The Gold layer follows a Star Schema.

## Fact

- Fact Transactions

## Dimensions

- Patients
- Providers
- Departments
- Diagnosis
- NPI

This design minimizes join complexity while supporting analytical workloads.

---

# End-to-End Data Pipeline

<p align="center">
<img width="1903" height="745" alt="Pipeline_End-to-End_adhoc" src="https://github.com/user-attachments/assets/e0b3f767-07dc-43c7-b055-ded30b89a990" />
</p>

The orchestration layer consists of multiple Azure Data Factory pipelines.

## 1. EMR → Bronze

- Reads metadata-driven configuration
- Detects Full vs Incremental loads
- Archives previous extracts
- Loads Parquet files into Bronze

---

## 2. Landing → Bronze

Processes:

- Claims
- CPT

---

## 3. API → Bronze

Fetches

- NPI
- ICD

and stores standardized datasets.

---

## 4. Bronze → Silver

Performs:

- Cleansing
- Standardization
- SCD Type 2
- Data Quality Checks
- Common Data Model transformation

---

## 5. Silver → Gold

Builds

- Fact tables
- Dimension tables
- Reporting datasets

using Databricks notebooks.

---

# Metadata-Driven Ingestion

Instead of building separate pipelines for every table, ingestion is configuration-driven.

Configuration includes:

- Source database
- Source table
- Target path
- Watermark column
- Load type
- Active flag

Benefits:

- Easily onboard new tables
- Minimal pipeline changes
- Improved maintainability
- Reduced duplication

---

# Data Engineering Features

## Incremental Loading

Supports watermark-based ingestion for transactional tables.

---

## Full Load Support

Reference datasets are refreshed through complete reloads where appropriate.

---

## Slowly Changing Dimension Type 2

Historical versions are preserved using:

- Insert Timestamp
- Modified Timestamp
- Current Record Flag

allowing complete change history for business entities.

---

## Data Quality

Validation rules identify invalid records before loading into Silver.

Bad records are quarantined rather than discarded.

---

## Common Data Model

Hospital systems with different schemas are standardized into a unified model using surrogate keys and common naming conventions.

---

## Delta Lake

Used for:
- ACID Transactions
- Schema Enforcement
- Reliable Updates
- Time Travel Support
- Efficient Analytics

---

# Azure Data Factory

## Linked Services

- Azure SQL Database
- Azure Data Lake Storage Gen2
- Azure Databricks
- Azure Key Vault
- Delta Lake

<p align="center">
<img width="1286" height="835" alt="Linked Services" src="https://github.com/user-attachments/assets/991c8a79-55cf-48c4-9c65-3c35474ed013" />
</p>

---

## Pipeline Highlights

### Metadata Driven Pipeline

- Lookup
- ForEach
- File Detection
- Archive
- Conditional Loading

### Incremental Processing

- Watermark detection
- Full/Incremental branching

### Pipeline Chaining

Master pipeline orchestrates complete workflow from ingestion through Gold layer generation.

---

# Security

Sensitive credentials are not embedded in pipelines.

Implemented using:

- Azure Key Vault
- Managed Linked Services
- Unity Catalog

---

# Engineering Decisions

✔ Metadata-driven ingestion

✔ Medallion Architecture

✔ Delta Lake

✔ SCD Type 2

✔ Common Data Model

✔ Configuration-based pipelines

✔ Incremental loading

✔ Parallel pipeline execution

✔ Historical tracking

✔ Centralized secrets management

---

# Repository Structure

```
azure-healthcare-data-platform/

│
├── adf/
│   ├── pipelines/
│   ├── datasets/
│   └── linked_services/
│
├── databricks/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── utilities/
│
├── configs/
│
├── architecture/
│
├── images/
│
└── README.md
```

---

# Future Enhancements

- CI/CD using Azure DevOps
- Infrastructure as Code using Terraform
- Automated Data Quality Framework
- Data Observability
- Unity Catalog Governance Policies
- Partition Optimization
- Delta Live Tables
- Event-driven ingestion using Event Grid
- Real-time streaming with Azure Event Hubs

---

# Disclaimer

This project is intended for educational and portfolio purposes.

All datasets are synthetically generated using the Faker library and do not contain real patient information.

