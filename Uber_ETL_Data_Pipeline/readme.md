# 🚕 Uber Analytics Pipeline on Google Cloud | ETL and Analytics | Modern ETL amd Analytics Pipeline

> Transforming raw NYC taxi trip data into business-ready insights using Google Cloud, Python, Mage AI, BigQuery, and Google Data Studio.

![GCP](https://img.shields.io/badge/Google%20Cloud-GCP-blue)
![BigQuery](https://img.shields.io/badge/Data%20Warehouse-BigQuery-orange)
![Mage](https://img.shields.io/badge/Orchestration-Mage%20AI-purple)
![Python](https://img.shields.io/badge/Python-Data%20Engineering-yellow)
![Data Studio](https://img.shields.io/badge/BI-Data%20Studio-green)

---

## Executive Summary

Organizations generate massive amounts of operational data every day, but raw data alone does not drive business decisions.

This project demonstrates how a complete cloud-based analytics platform can convert raw transportation data into meaningful business insights. Using Google Cloud Platform and modern data engineering tools, the solution automates data ingestion, transformation, warehousing, and visualization to provide stakeholders with actionable insights on revenue, customer behavior, trip patterns, and operational performance.

The result is a scalable analytics solution capable of answering key business questions such as:

- Which payment methods generate the most revenue?
- When are peak revenue hours?
- Which trip categories are most profitable?
- How does trip distance influence revenue?
- Where are the busiest pickup and drop-off locations?
- What customer patterns can be observed across trips?

---

# Business Problem

Ride-sharing and transportation companies generate millions of trip records containing information about fares, locations, passengers, and payments.

Without a centralized analytics platform, stakeholders face challenges such as:

- Data scattered across multiple systems
- Slow reporting processes
- Limited visibility into customer behavior
- Difficulty identifying revenue opportunities
- Lack of operational insights

This project addresses these challenges by creating a centralized analytics platform that delivers trusted, business-ready data for reporting and decision-making.

---

# Solution Overview

The solution follows a modern cloud analytics architecture:

```text
Raw Data
    │
    ▼
Google Cloud Storage
    │
    ▼
Mage AI ETL Pipeline
    │
    ▼
BigQuery Data Warehouse
    │
    ▼
Data Studio Dashboard
    │
    ▼
Business Insights
```

### Data Flow

1. Raw trip records are stored in Google Cloud Storage.
2. Mage AI orchestrates the data pipeline.
3. Python transformations clean and structure the data.
4. BigQuery stores analytics-ready datasets.
5. Data Studio provides interactive dashboards for business users.

---

# Architecture

<img width="960" height="540" alt="architecture" src="https://github.com/user-attachments/assets/2e279f7c-7d8f-48f6-8b75-cab78bbd1f02" />


## Cloud Components

| Component | Purpose |
|------------|------------|
| Google Cloud Storage | Stores raw source files |
| Compute Engine | Hosts Mage AI |
| Mage AI | Automates ETL workflows |
| Python | Data transformation and processing |
| BigQuery | Enterprise analytics warehouse |
| Data Studio | Interactive reporting and dashboards |

---

# Dataset

This project uses publicly available New York City Taxi and Limousine Commission (TLC) trip records.

### Source

**NYC TLC Trip Record Data**

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

### Dataset Includes

- Pickup and dropoff timestamps
- Passenger counts
- Trip distances
- Fare amounts
- Tips
- Taxes and surcharges
- Payment methods
- Pickup and dropoff coordinates
- Vendor information

These attributes provide an excellent foundation for operational and revenue analytics.

---

# Business Value Delivered

The analytics platform enables stakeholders to:

### Revenue Monitoring

- Track total revenue trends
- Analyze fare performance
- Identify top-performing trip categories

### Customer Analytics

- Understand passenger patterns
- Analyze payment preferences
- Measure customer spending behavior

### Operational Insights

- Identify peak demand periods
- Evaluate trip distance distributions
- Monitor service performance

### Geographic Analysis

- Discover high-demand areas
- Visualize ride concentration
- Support location-based decision making

---

# Technology Stack

| Category | Technology |
|-----------|------------|
| Cloud Platform | Google Cloud Platform |
| Storage | Google Cloud Storage |
| Processing | Python |
| Workflow Automation | Mage AI |
| Data Warehouse | BigQuery |
| Analytics | SQL |
| Visualization | Data Studio |
| Version Control | Git & GitHub |

---

# Pipeline Design

<img width="1917" height="872" alt="Mage Pipeline UI" src="https://github.com/user-attachments/assets/b3c301a0-8e3f-4631-b367-d790808a985c" />


The pipeline consists of three stages:

## 1. Data Ingestion

Raw taxi trip data is collected from cloud storage and prepared for processing.

### Activities

- Source validation
- Schema verification
- Initial quality checks

---

## 2. Data Transformation

Business-ready datasets are created from raw records.

### Activities

- Data cleansing
- Date and time enrichment
- Revenue calculations
- Payment categorization
- Location processing
- Business-friendly modeling

---

## 3. Data Delivery

Transformed data is loaded into BigQuery for analytics and reporting.

### Outputs

- Fact tables
- Dimension tables
- Analytics views
- Dashboard-ready datasets

---

# Data Model

To support fast and flexible analytics, the solution uses a dimensional model.

<img width="1760" height="1206" alt="data_model" src="https://github.com/user-attachments/assets/95d1e21f-158c-4e8e-9ae5-ce9ddf44be57" />


## Core Business Dimensions

### Date & Time

Supports:

- Revenue by hour
- Revenue by day
- Seasonal trends
- Peak demand analysis

### Passenger Information

Supports:

- Passenger distribution analysis
- Customer segmentation

### Trip Distance

Supports:

- Distance-based performance analysis
- Revenue optimization studies

### Payment Type

Supports:

- Cash vs card analysis
- Customer payment behavior

### Rate Code

Supports:

- Fare category analysis
- Profitability comparisons

### Pickup & Dropoff Locations

Supports:

- Geographic reporting
- Demand hotspot identification

---

# Dashboard Highlights

The final dashboard provides a business-focused view of operational performance.

<img width="832" height="621" alt="Data Studio Report Snapshot" src="https://github.com/user-attachments/assets/f9f79870-1c37-4dad-9dcb-6c83a194b6d8" />

## Executive KPIs

The dashboard tracks metrics such as:

- Total Revenue
- Total Trips
- Average Revenue Per Trip
- Average Fare Amount
- Average Tip Amount
- Total Taxes Collected

---

## Revenue Analytics

Business users can explore:

- Revenue by payment type
- Revenue by trip category
- Revenue trends throughout the day
- High-value trips

### Key Finding


Credit card transactions generate the largest share of revenue while also producing significantly higher tip percentages.

---

## Customer Analytics

Insights include:

- Passenger distribution
- Ride frequency
- Payment behavior
- Fare spending patterns

### Key Finding

Most trips involve one passenger, representing the majority of ride volume.

---

## Operational Analytics

Insights include:

- Peak pickup hours
- Trip distance distribution
- Revenue by trip distance
- Top-performing trips

### Key Finding

Short-distance trips dominate ride volume, while longer trips contribute disproportionately higher revenue.

---

## Geographic Analytics

Interactive maps help identify:

- High-demand pickup zones
- Popular dropoff destinations
- Revenue concentration areas

These insights can support resource allocation and operational planning.

---

# Example Business Questions Answered

### Revenue Performance

- Which payment method generates the most revenue?
- Which fare category is most profitable?
- What hours generate the highest revenue?

### Customer Behavior

- How many passengers typically travel together?
- Which payment methods are preferred?

### Operations

- What distance range accounts for most trips?
- Which trips generate the highest revenue?

### Geographic Trends

- Where are rides concentrated?
- Which locations generate the most activity?

---

# Key Achievements

### Built an End-to-End Analytics Platform

Designed and implemented a complete analytics workflow from raw data ingestion to executive reporting.

### Automated Data Processing

Reduced manual effort through automated pipeline orchestration using Mage AI.

### Centralized Business Reporting

Created a single source of truth for operational and revenue analytics.

### Enabled Self-Service Analytics

Delivered interactive dashboards that allow stakeholders to explore data without technical expertise.

### Leveraged Cloud-Native Services

Implemented a scalable solution using managed Google Cloud services.

---

# Skills Demonstrated

### Business & Analytics

- Business Intelligence
- KPI Development
- Revenue Analytics
- Customer Analytics
- Dashboard Design
- Data Storytelling

### Data Engineering

- ETL Development
- Data Modeling
- Data Warehousing
- Cloud Data Platforms
- Workflow Automation
- SQL Analytics

### Cloud Technologies

- Google Cloud Storage
- Compute Engine
- BigQuery
- Looker Studio

### Programming

- Python
- SQL
- Data Processing
- Data Transformation

---

# Why This Project Matters

This project demonstrates the ability to bridge the gap between technical implementation and business outcomes.

Rather than focusing solely on moving data, the solution focuses on delivering measurable business value through:

- Better visibility into revenue performance
- Faster access to operational insights
- Improved decision-making capabilities
- Scalable cloud-based analytics architecture

It showcases the complete lifecycle of modern analytics engineering—from raw data to executive dashboards.

---

## Author

**Aduet Dabral**
Data Engineering • Analytics Engineering • Cloud Analytics • Business Intelligence

If you found this project useful, consider giving it a ⭐.
