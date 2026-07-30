# Stock Data Ingestion and Transformation Pipeline

A modern ELT pipeline for Nepal stock market data that combines Apache Airflow, dbt, PostgreSQL, and AWS analytics services. The project ingests market data from the Nepalipaisa API, loads raw records into PostgreSQL, transforms them through dbt models, and optionally publishes analytics-ready datasets to Amazon S3, Glue, and Athena using Iceberg tables.

## Overview

This repository automates the full data lifecycle:

1. Fetch company metadata and share-price data from the Nepalipaisa API.
2. Load the raw data into PostgreSQL tables under the raw schema.
3. Transform the data with dbt through staging, intermediate, and mart layers.
4. Orchestrate the process with Apache Airflow and trigger downstream AWS loading jobs.

## Architecture

The system is organized around four main layers:

- Data Ingestion: Airflow DAGs fetch data from external APIs.
- Data Storage: PostgreSQL stores the raw and transformed datasets.
- Data Transformation: dbt builds the warehouse logic in layered models.
- Data Delivery: AWS S3, Glue, and Athena are used to expose curated data via Iceberg tables.

## Project Structure

```text
.
├── dags/
│   ├── aws_load_dag.py
│   ├── dbt_class_dag.py
│   ├── injestion_dag.py
│   ├── master_dag.py
│   └── scrape_broker_holding_dag.py
├── dbt/
│   └── dbt_stock_transformation/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── macros/
│       ├── models/
│       │   ├── staging/
│       │   ├── intermediate/
│       │   └── marts/
│       ├── snapshots/
│       └── tests/
├── aws-simple-pipeline/
│   ├── db_utils.py
│   ├── load_broker_summary.py
│   ├── load_live_share.py
│   ├── run_both.py
│   ├── setup_aws_infra.py
│   └── iam_policy.json
├── tests/
│   └── dags/
├── Dockerfile
├── requirements.txt
├── packages.txt
├── airflow_settings.yaml
└── README.md
```

## Data Flow

```text
Nepalipaisa API
    ↓
Airflow ingestion DAG
    ↓
PostgreSQL (raw schema)
    ↓
dbt models
    ├── staging
    ├── intermediate
    └── marts
    ↓
AWS S3 / Glue / Athena
    ↓
Iceberg tables for analytics
```

### End-to-End Workflow

1. The master workflow triggers the ingestion DAG.
2. Company and share-price data are fetched and written into PostgreSQL raw tables.
3. The dbt DAG runs the transformation pipeline.
4. The AWS loading DAG exports the transformed data into Iceberg tables for warehouse-style analytics.

## Main Components

### Airflow DAGs

- master_dag.py: orchestrates the full pipeline.
- injestion_dag.py: fetches data from the source API and loads it into PostgreSQL.
- dbt_class_dag.py: runs the dbt project using Astronomer Cosmos.
- aws_load_dag.py: loads selected intermediate tables into AWS services.

### dbt Project

The dbt project is located in dbt/dbt_stock_transformation and contains:

- staging models for initial cleaning and standardization
- intermediate models for business-ready transformations
- mart models for analytics and reporting
- tests to validate assumptions and data quality

### AWS Pipeline

The aws-simple-pipeline folder contains Python scripts to:

- create AWS infrastructure such as S3 buckets, Glue databases, and Athena workgroups
- load PostgreSQL query results into S3 CSV files
- publish those datasets as Iceberg tables in Athena

## Prerequisites

Before running the pipeline, make sure you have:

- Python 3.10+ and pip
- Docker and the Astro CLI (recommended for Airflow development)
- PostgreSQL access for the warehouse database
- AWS credentials with permissions for S3, Glue, and Athena
- A configured environment file for AWS and database settings

## Setup

1. Clone the repository.
2. Install Python dependencies:

```bash
pip install -r requirements.txt
```

3. Configure environment variables for PostgreSQL and AWS access.
4. Start the Airflow environment (if using Astro):

```bash
astro dev start
```

5. Trigger the DAGs from the Airflow UI or CLI.

## Running the Pipeline

The recommended execution order is:

1. Run the ingestion DAG to populate PostgreSQL raw tables.
2. Run the dbt DAG to build the transformed models.
3. Run the AWS load DAG to publish data into S3 / Glue / Athena.

You can also trigger the master workflow to run the full sequence automatically.

## Expected Outputs

After a successful run, the project produces:

- raw tables in PostgreSQL for source data
- staging, intermediate, and mart tables in the warehouse layer
- Iceberg-backed tables in AWS for analytical querying

## Notes

- The project is designed to support both local development and container-based Airflow execution.
- The dbt layer is structured to separate raw ingestion logic from downstream business transformations.
- The AWS ingestion path is useful when you want to make the transformed datasets available in Athena for SQL-based analytics.

## License

This project is intended for educational and analytical use within the capstone workflow.