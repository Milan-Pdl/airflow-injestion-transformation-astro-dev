Stock Data Ingestion & Transformation
====================================

Overview
--------
This repository contains an ELT pipeline that ingests Nepali stock market data, stores raw data in PostgreSQL, and transforms it using dbt into staging, intermediate, and marts layers. Workflows are orchestrated with Apache Airflow.

Prerequisites
-------------
- Python 3.10+ and pip
- dbt (install via pip or use the provided Dockerfile)
- PostgreSQL instance and connection credentials

Quick local setup
-----------------

Windows example (PowerShell):

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

Set your dbt profile in `dbt/dbt_stock_transformation/profiles.yml` or provide credentials via environment variables.

Common commands
---------------

Compile dbt models:

```bash
dbt compile --project-dir dbt/dbt_stock_transformation
```

Run a single model:

```bash
dbt run --models intermediate.company_not_in_ordinary_market --project-dir dbt/dbt_stock_transformation
```

Run dbt tests:

```bash
dbt test --project-dir dbt/dbt_stock_transformation
```

Start Airflow locally (Astronomer/Astro):

```bash
astro dev start
```

Project layout
--------------

- `dags/` — Airflow DAGs and operators (master, ingestion, dbt runner).
- `dbt/dbt_stock_transformation/` — dbt project: models, macros, and profiles.
- `Dockerfile`, `requirements.txt`, `packages.txt` — runtime and dependency manifests.

Notes
-----
- See `dbt/dbt_stock_transformation/models/intermediate/intermediate_company.sql` for deduplication and data-quality logic used by the intermediate models.
- If you change database credentials, update both the dbt profile and Airflow connections used by the DAGs.

Contributing
------------
Open issues and PRs. For changes to dbt models, include tests and a short description of the data-change rationale.

License
-------
Add a `LICENSE` file if you plan to publish or share this repository.

Contact
-------
For questions about setup or running the pipeline, open an issue or contact the project owner.
