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

#### Example Command

```bash
aws s3 cp CAvideos.csv s3://yt-de-project-raw-dev/youtube/raw_statistics/region=ca/
```

---

### 2. Semi-Structured ETL (AWS Lambda)

- Resolved `HIVE_CURSOR_ERROR` in Athena caused by nested JSON structures
- Configured Lambda to trigger automatically on `S3:ObjectCreated`

#### Transformation Logic

- Used `pd.json_normalize` to flatten nested JSON structures
- Cast `id` to `bigint` for join compatibility

---

### 3. Distributed Processing (AWS Glue)

For regional CSVs, an AWS Glue Job processes bulk data using PySpark.

#### Key Optimizations

##### 🔹 Predicate Pushdown

Filters specific regions (`ca`, `gb`, `us`) at the S3 source level to reduce scan costs.

##### 🔹 Data Quality

Implemented `EvaluateDataQuality` transforms to ensure row integrity.

##### 🔹 File Management

Used `.coalesce(1)` to optimize Parquet file sizing and reduce the **Small File Problem**.

---

### 4. Analytics Layer (Gold Layer)

A final Glue Job joins the Cleansed Statistics (Fact) and Cleansed Category Reference (Dimension) datasets to create a fully enriched analytical layer.

#### Processing Details

- **Join Logic:**  
  Merged datasets on `category_id` and `id`

- **Destination:**  
  Final data stored in the `yt-de-project-analytics-dev` bucket for Athena querying

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

#### 🌍 Regional Trends

Compare engagement metrics such as Likes-to-Views ratios across countries.

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
