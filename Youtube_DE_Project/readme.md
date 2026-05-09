# YouTube Data Engineering Analysis Project

## 🚀 Project Overview
This project builds an end-to-end data engineering pipeline to process, cleanse, and analyze YouTube's trending video data (Kaggle dataset). The architecture follows a **Medallion (Lakehouse) Architecture**—transitioning data from Raw to Cleansed and finally to an Analytics-ready layer.

## 🏗️ Architecture
<img width="1280" height="720" alt="image" src="https://github.com/user-attachments/assets/f5e5bba4-eed4-43a7-a2c6-d1d36705900f" />


The project is built entirely on **AWS**, ensuring a serverless and scalable infrastructure:
- **Ingestion:** AWS CLI (Local to S3 Landing Area).
- **Storage:** Amazon S3 (Raw, Cleansed/Enriched, and Analytics/Reporting buckets).
- **Data Processing:** 
    - **AWS Lambda:** Handles semi-structured JSON flattening.
    - **AWS Glue (PySpark):** Handles large-scale CSV transformation and schema enforcement.
- **Data Cataloging:** AWS Glue Data Catalog & Crawlers for metadata management.
- **Analytics:** Amazon Athena for SQL-based analytical data access.

## 🛠️ Tech Stack
- **Compute:** AWS Lambda, AWS Glue (PySpark).
- **Storage:** Amazon S3.
- **Discovery:** AWS Glue Data Catalog & Crawlers.
- **Query Engine:** Amazon Athena.
- **Language:** Python, PySpark, SQL.

---

## 🔧 Key Engineering Steps

### 1. Partitioned Ingestion & Environment Setup
* Configured AWS CLI and IAM users with programmatic access.
* Uploaded data to S3 using **Hive-style partitioning** (`region=xx`) to optimize query costs and performance.
  ```bash
  aws s3 cp CAvideos.csv s3://yt-de-project-raw-dev/youtube/raw_statistics/region=ca/

### 2. Semi-Structured ETL (AWS Lambda)
* To resolve HIVE_CURSOR_ERROR in Athena caused by nested JSON, a Lambda function triggers on S3:ObjectCreated.
* **Logic:** Uses pd.json_normalize to flatten nested items and casts id to bigint for join compatibility.


### 3. Distributed Processing (AWS Glue)
For regional CSVs, an AWS Glue Job processes bulk data using PySpark.
* **Predicate Pushdown:** Filters specific regions (e.g., `ca`, `gb`, `us`) at the S3 source level to reduce scan costs.
* **Data Quality:** Implemented `EvaluateDataQuality` transforms to ensure row integrity.
* **File Management:** Used `.coalesce(1)` to group data into optimal Parquet file sizes, avoiding the "Small File Problem".

### 4. Analytics Layer (The Gold Layer)
A final Glue Job joins the Cleansed Statistics (Fact) and Cleansed Category Reference (Dimension) datasets. A high-performance, fully enriched dataset ready for complex analytical queries and BI dashboarding.
* **Join Logic:** Merged on `category_id` and `id` to create a unified view.
* **Destination:** Data is stored in the `yt-de-project-analytics-dev` bucket for final querying via Athena.

## 🧪 Troubleshooting & Optimization
* **Schema Mismatch:** Resolved `Malformed Parquet file` errors by explicitly casting reference IDs to `bigint` at the source transformation level rather than the query level.
* **Event-Driven Automation:** Configured S3 triggers and Glue Crawlers to automate the synchronization of physical data with the metadata catalog.


## 📈 Project Results & Insights
Through this pipeline, we successfully transformed approximately 40,000 daily trending records per region into a high-performance analytical dataset. Key insights include:
* **Category Performance:** Identifying which video categories (e.g., Entertainment vs. Tech) stay on the trending list the longest.
* **Regional Trends:** Comparing engagement metrics like "Likes-to-Views" ratios across different countries.
* **System Efficiency:** Reduced storage costs by ~50% by converting raw CSV/JSON to Snappy-compressed Parquet.

## 🚀 Future Improvements
To further evolve this architecture, the following enhancements are planned:
* **Orchestration:** Implementing **AWS Step Functions** to manage the dependency between the Cleansed and Analytics Glue jobs.
* **Real-time Analytics:** Integrating **AWS Kinesis** to handle live streaming data rather than batch-based S3 uploads.
* **Data Visualization:** Building a **QuickSight** or **Power BI** dashboard to visualize the trends directly from the Athena 'Gold' layer.

## 🤝 Acknowledgments
* This Kaggle dataset contains statistics (CSV files) on daily popular YouTube videos over the course of many months. There are up to 200 trending videos published every day for many locations. The data for each region is in its own file. The video title, channel title, publication time, tags, views, likes and dislikes, description, and comment count are among the items included in the data. A category_id field, which differs by area, is also included in the JSON file linked to the region.
https://www.kaggle.com/datasets/datasnaek/youtube-new.
* Inspired by best practices in AWS Cloud Data Engineering for building serverless data lakes.
