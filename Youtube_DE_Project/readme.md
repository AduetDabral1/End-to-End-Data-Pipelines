# YouTube Data Engineering Analysis Project

---

## 🚀 Project Overview

This project builds an end-to-end data engineering pipeline to process, cleanse, and analyze YouTube's trending video data (Kaggle dataset).

The architecture follows a **Medallion (Lakehouse) Architecture** — transitioning data from Raw to Cleansed and finally to an Analytics-ready layer.

---

## 🏗️ Architecture

<img width="1280" height="720" alt="image" src="https://github.com/user-attachments/assets/f5e5bba4-eed4-43a7-a2c6-d1d36705900f" />

The project is built entirely on **AWS**, ensuring a serverless and scalable infrastructure.

### Core Components

- **Ingestion:** AWS CLI (Local to S3 Landing Area)
- **Storage:** Amazon S3 (Raw, Cleansed/Enriched, and Analytics/Reporting buckets)
- **Data Processing:**
  - **AWS Lambda:** Handles semi-structured JSON flattening
  - **AWS Glue (PySpark):** Handles large-scale CSV transformation and schema enforcement
- **Data Cataloging:** AWS Glue Data Catalog & Crawlers for metadata management
- **Analytics:** Amazon Athena for SQL-based analytical data access

---

## 🛠️ Tech Stack

| Category | Services / Tools |
|---|---|
| **Compute** | AWS Lambda, AWS Glue (PySpark) |
| **Storage** | Amazon S3 |
| **Discovery** | AWS Glue Data Catalog & Crawlers |
| **Query Engine** | Amazon Athena |
| **Languages** | Python, PySpark, SQL |

---

## 🔧 Key Engineering Steps

---

### 1. Partitioned Ingestion & Environment Setup

- Configured AWS CLI and IAM users with programmatic access
- Uploaded data to S3 using **Hive-style partitioning** (`region=xx`) to optimize query costs and performance

<img width="1916" height="767" alt="image" src="https://github.com/user-attachments/assets/db0fcfe9-4b89-45d5-88c6-33bed5d5dc57" />


#### Example Command

```bash
aws s3 cp CAvideos.csv s3://yt-de-project-raw-dev/youtube/raw_statistics/region=ca/
```

---

### 2. Semi-Structured ETL (AWS Lambda)

- Resolved `HIVE_CURSOR_ERROR` in Athena caused by nested JSON structures
- Configured Lambda to trigger automatically on `S3:ObjectCreated`
<img width="969" height="811" alt="image" src="https://github.com/user-attachments/assets/42ef335b-66c1-47e1-b426-b97bf329dec9" />

<img width="1624" height="366" alt="image" src="https://github.com/user-attachments/assets/2fab313f-e6a0-4d8b-80e8-d5093f1414b6" />


#### Transformation Logic

- Used `pd.json_normalize` to flatten nested JSON structures
- Cast `id` to `bigint` for join compatibility

---

### 3. Distributed Processing (AWS Glue)

<img width="1283" height="236" alt="Screenshot 2026-05-09 170457" src="https://github.com/user-attachments/assets/73638592-a5b5-4298-bf92-4d191e90c41f" />


For regional CSVs, an AWS Glue Job processes bulk data using PySpark.

<img width="1619" height="417" alt="Screenshot 2026-05-09 171011" src="https://github.com/user-attachments/assets/d3927859-f1aa-40cd-85d7-e8df116b89d1" />


#### Key Optimizations

##### 🔹 Predicate Pushdown

Filters specific regions (`ca`, `gb`, `us`) at the S3 source level to reduce scan costs.

##### 🔹 Data Quality

Implemented `EvaluateDataQuality` transforms to ensure row integrity.

##### 🔹 File Management

Used `.coalesce(1)` to optimize Parquet file sizing and reduce the **Small File Problem**.

---

### 4. Analytics Layer (Gold Layer)

<img width="1323" height="748" alt="Screenshot 2026-05-09 170559" src="https://github.com/user-attachments/assets/2b89417c-f644-438a-beb0-9c65f06e8f4b" />


A final Glue Job joins the Cleansed Statistics (Fact) and Cleansed Category Reference (Dimension) datasets to create a fully enriched analytical layer.

#### Processing Details

- **Join Logic:**  
  Merged datasets on `category_id` and `id`

- **Destination:**  
  Final data stored in the `yt-de-project-analytics-dev` bucket for Athena querying

<img width="1615" height="323" alt="Screenshot 2026-05-09 171002" src="https://github.com/user-attachments/assets/e06eaa63-58e2-4d6e-ba24-46864eaa58dd" />

---

---

## 💡 Engineering Decisions
This project prioritizes the "Architecture of Trade-offs," choosing tools based on specific data characteristics rather than a "one size fits all" approach.

### Why Parquet over CSV?
While CSV is human-readable, it is highly inefficient for large-scale analytics. 
* **Columnar Storage:** Parquet allows Athena to read only the specific columns required for a query, drastically reducing the amount of data scanned.
* **Compression:** Parquet with Snappy compression reduced the storage footprint by ~50%, leading to faster I/O and lower S3 storage costs.

### Why Partitioning was Chosen?
We implemented Hive-style partitioning (`region=xx`) to enable **Partition Pruning**. 
* **Query Optimization:** Without partitions, Athena would scan the entire bucket for every query. 
* **Cost Efficiency:** By limiting the scan to specific folders (regions), we stay within the AWS Free Tier limits and ensure sub-second query performance.

### Why Lambda for JSON instead of Glue?
* **Complexity vs. Scale:** The JSON files were small but highly nested. AWS Lambda is cheaper and faster for "light" event-driven transformations using Python/Pandas. 
* **Real-time Ingestion:** Lambda allowed us to create a trigger-based system where data is cleansed the second it lands, whereas Glue is better suited for high-volume, scheduled batch processing.

### Why Athena instead of Redshift?
* **Serverless Cost Model:** Redshift requires a provisioned cluster (hourly cost), whereas Athena is serverless—you only pay for the data scanned during a query.
* **Zero Infrastructure:** For this volume of data, the "Pay-as-you-go" model of Athena is far more cost-effective for an analytical sandbox than maintaining a dedicated data warehouse.

### Why `awswrangler` over Boto3?
While Boto3 is the standard AWS SDK, `awswrangler` (AWS SDK for Pandas) was chosen because it provides high-level abstractions for writing Parquet files directly to S3 and automatically updating the Glue Data Catalog in a single step.

---


## 🧪 Troubleshooting & Optimization

### 🔹 Schema Mismatch

Resolved `Malformed Parquet file` errors by explicitly casting reference IDs to `bigint` during source transformation instead of query-time casting.

### 🔹 Event-Driven Automation

Configured S3 triggers and Glue Crawlers to automate metadata synchronization with the Glue Data Catalog.

---

## 📈 Project Results & Insights

Through this pipeline, approximately **40,000 daily trending records per region** were transformed into a high-performance analytical dataset.

### Key Insights

#### 📊 Category Performance

Identify which video categories remain on trending lists the longest.
<img width="1425" height="841" alt="image" src="https://github.com/user-attachments/assets/810f2c0e-ec82-48f8-802e-0df8384b76b0" />


#### 🌍 Regional Trends

Compare engagement metrics such as Likes-to-Views ratios across countries.
<img width="1253" height="843" alt="image" src="https://github.com/user-attachments/assets/11e3b6af-bdbf-46c4-a6ab-c075952e8a64" />


#### ⚡ System Efficiency

Reduced storage costs by approximately **50%** by converting CSV/JSON into Snappy-compressed Parquet.

---

## 🚀 Future Improvements

To further evolve the architecture, the following enhancements are planned:

### 🔹 Orchestration

Implement **AWS Step Functions** to manage dependencies between Glue jobs.

### 🔹 Real-Time Analytics

Integrate **AWS Kinesis** for streaming ingestion instead of batch uploads.

### 🔹 Data Visualization

Build **QuickSight** or **Power BI** dashboards directly on top of Athena.

---

## 🤝 Acknowledgments

This Kaggle dataset contains statistics (CSV files) on daily popular YouTube videos across multiple regions over several months.

### Dataset Includes

- Video title
- Channel title
- Publication time
- Tags
- Views
- Likes/dislikes
- Description
- Comment count
- Region-specific `category_id` mappings in JSON format

### Dataset Source

[Kaggle Dataset - YouTube Trending Videos](https://www.kaggle.com/datasets/datasnaek/youtube-new)

### Inspiration

Inspired by best practices in AWS Cloud Data Engineering for building scalable serverless data lakes.
