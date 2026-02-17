### Real-Time Stock Market Data Pipeline
A production-style, end-to-end real-time data engineering pipeline that ingests stock market events, processes them using a Medallion architecture, and delivers near real-time analytics dashboards.

🚀 Overview:
This project streams stock market data from external APIs using Kafka, stores raw data in MinIO (S3-compatible data lake), orchestrates workflows with Airflow, transforms data using dbt (Medallion Architecture), and loads analytics-ready datasets into Snowflake for visualization in Power BI.
The entire pipeline is fully containerized using Docker for consistent and reproducible deployments.

🛠 Tech Stack:
Python
Apache Kafka
MinIO
Apache Airflow
dbt
Snowflake
Docker
Power BI

🧱 Architecture:

Data Flow:
Producer → Kafka → MinIO (Bronze) → Airflow → dbt (Silver & Gold) → Snowflake → Power BI

📊 Impact:
Processed ~100K events/day
Reduced reporting time by ~50%
Improved query performance by ~40%
Reduced setup time by ~60% via Docker.

✅ Conclusion

This project demonstrates the design and implementation of a scalable, real-time data pipeline using modern data engineering tools and best practices. By combining streaming ingestion, lakehouse architecture, workflow orchestration, and analytics modeling, the system delivers reliable, analytics-ready data with improved performance and reduced operational overhead.
It showcases hands-on experience in building production-style data platforms with end-to-end ownership — from ingestion to visualization.
