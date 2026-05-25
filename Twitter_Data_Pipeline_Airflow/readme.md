# Airflow ETL Pipeline on AWS EC2

## 📖 Project Overview
This repository outlines the architecture and deployment strategy for a modern Apache Airflow orchestration environment hosted on an AWS EC2 instance (Ubuntu). The core objective is an automated ETL pipeline that processes data and securely uploads the transformed dataset to an Amazon S3 bucket.

Rather than a simple proof-of-concept, this project serves as a robust blueprint for securely deploying Airflow on a bare-metal cloud instance. It resolves several common, undocumented pitfalls related to modern Airflow architecture, Python environment isolation, and strict AWS IAM & IMDSv2 security policies.

<img width="694" height="856" alt="image" src="https://github.com/user-attachments/assets/713f8b96-93d5-45a1-86a4-cf518b039569" />



---


## 📂 Core Components

* **`x_dag.py`**: The main DAG definition file defining the schedule, retry logic, and task dependencies.
* **`x_etl.py`**: The extraction, transformation, and loading script. Contains the logic to process the data and the explicit `boto3` configuration required to bypass Airflow worker credential caching.
* **Cloud Infrastructure**: AWS EC2 instance (Ubuntu), custom Security Group (TCP Port 8080 open to required IPs), and an isolated Amazon S3 bucket protected by a dedicated IAM role.


---


## 🛠️ Tech Stack

**Cloud & Infrastructure**
* **AWS EC2:** Host compute instance (Ubuntu Linux).
* **AWS S3:** Scalable cloud object storage for the final transformed datasets.

**Orchestration & Data Processing**
* **Apache Airflow (3.0+):** Modern orchestration utilizing the new API-server architecture for scheduling and task dependency management.
* **Python 3:** Core programming language, strictly isolated using native `venv` to protect OS-level dependencies.
* **Pandas:** In-memory data transformation, cleaning, and manipulation.

**Integrations & Security**
* **Boto3 & S3FS:** AWS SDK for Python and S3 file system interfaces, explicitly configured to handle IAM role handshakes and bypass worker caching.
* **Tweepy:** API integration for external data extraction.
* **AWS IAM & IMDSv2:** Least-privilege role-based access control and strict EC2 metadata security enforcement.

---


## 🏗️ Architecture & Engineering Decisions

### 1. Python Virtual Environments for System Stability
To prevent conflicts with critical OS-level Python tools (such as `apt` and `ufw`), all Airflow components and data science libraries (`pandas`, `s3fs`, `boto3`) are quarantined inside an isolated virtual environment (`venv`). This ensures the underlying Ubuntu system remains stable and secure.

### 2. Modern Airflow Architecture (v3.0+)
This project utilizes the latest Airflow architecture, intentionally abandoning legacy, deprecated components:
* **API Server over Webserver:** Migrated from the deprecated Flask `airflow webserver` to the modern FastAPI-based `airflow api-server` (and unified `standalone` deployments).
* **Consolidated Scheduling:** Replaced the legacy `schedule_interval` with the unified `schedule` parameter, enabling future integration with Airflow Datasets.
* **Modern Imports:** Updated core operator imports (e.g., utilizing `airflow.operators.python` instead of the legacy `python_operator` and dropping dynamic `days_ago` in favor of static, predictable datetimes).

### 3. AWS Security & IAM Roles
Instead of hardcoding AWS credentials into Python files or utilizing dangerously broad access policies, this pipeline implements strict, production-ready AWS security:
* **Least Privilege IAM:** The EC2 instance operates under an IAM role restricted explicitly to `s3:PutObject` on the target bucket array (`arn:aws:s3:::<bucket-name>/*`).
* **IMDSv2 Compatibility:** The EC2 Instance Metadata Service is configured to allow secure credential handshakes between the instance profile and the `boto3` library running inside the Airflow workers.
* **Overcoming Worker Caching:** To prevent Airflow worker processes and the `s3fs` library from falling back to "anonymous" AWS states, the ETL script dynamically extracts active credentials via `boto3` and explicitly injects them into the S3 client payload. This guarantees secure, authenticated uploads regardless of Airflow's internal caching mechanics.


---

## 💡 Key Learnings & Resolutions

During the development of this pipeline, several critical infrastructure challenges were diagnosed and resolved:
1. **Network & Binding Configuration:** Resolved UI access timeouts by properly configuring the Airflow server to listen on `0.0.0.0` and mapping inbound AWS Security Group rules.
2. **S3FS & Pandas Credential Retrieval:** Identified an edge-case where `pandas` and `s3fs` fail to securely negotiate IMDSv2 metadata within an Airflow worker process. Resolved this by bypassing the default handlers and directly supplying frozen `boto3` credentials to the S3 client.
3. **Cache Invalidation:** Managed Airflow's aggressive Python file caching during development to successfully deploy critical security patches to the ETL script without full environment rebuilds.
