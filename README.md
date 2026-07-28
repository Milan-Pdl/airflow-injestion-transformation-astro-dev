# Stock Data Ingestion & Transformation
A modern ELT pipeline that ingests Nepali stock market data, stores it in PostgreSQL, and transforms it with dbt.

---

## Getting started

Prerequisites:
- Python 3.10+ and pip
- dbt (installed via `pip install -r requirements.txt` or in Docker)
- PostgreSQL instance and connection credentials

Local setup (example):

```bash
python -m venv .venv
source .venv/Scripts/activate   # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
```

Set your dbt/profile credentials in `dbt/dbt_stock_transformation/profiles.yml` or via environment variables.

---

## Common commands

- Compile dbt models:

```bash
dbt compile --project-dir dbt/dbt_stock_transformation
```

- Run a single model:

```bash
dbt run --models intermediate.company_not_in_ordinary_market --project-dir dbt/dbt_stock_transformation
```

- Run tests:

```bash
dbt test --project-dir dbt/dbt_stock_transformation
```

- Start Airflow locally (Astro dev):

```bash
astro dev start
```

---

## Project layout

The key folders:

- [dags](dags): Airflow DAGs and operators.
- [dbt/dbt_stock_transformation](dbt/dbt_stock_transformation): dbt project with staging, intermediate, and marts models.
- `Dockerfile`, `requirements.txt`, `packages.txt`: runtime and dependency manifests.

---

## Notes & contributions
- The dbt intermediate models implement deduplication and basic data quality filters (see `intermediate/intermediate_company.sql`).
- If you change database credentials, update `dbt/dbt_stock_transformation/profiles.yml` and the Airflow connections used by the DAGs.

Contributions welcome — open an issue or a PR with a clear description of the change.

---

## License
This repository is provided as-is. Add a license file if you plan to share publicly.

---

File references: see [dags](dags) and [dbt/dbt_stock_transformation](dbt/dbt_stock_transformation).
├── dbt/
│   └── dbt_stock_transformation/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       └── models/
│           ├── staging/
│           ├── intermediate/
│           └── marts/
├── Dockerfile
├── requirements.txt
└── packages.txt