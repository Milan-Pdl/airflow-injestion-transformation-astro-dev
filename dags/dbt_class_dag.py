from pathlib import Path
from pendulum import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import TestBehavior  # <-- Import the proper Enum for test behavior

DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/dbt_stock_transformation")  # matches the dbt_project.yml location in the Dockerfile

# Configured to run staging -> intermediate -> marts and run tests after each model completes
render_config = RenderConfig(
    emit_datasets=False,
    test_behavior=TestBehavior.AFTER_EACH,  # <-- Use the proper constant here
    select=["path:models/staging", "path:models/intermediate", "path:models/marts"] 
)

profile_config = ProfileConfig(
    profile_name="dbt_stock_transformation",  # matches dbt_project.yml + profiles.yml
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)

dbt_stock_transformation_dag = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",  # isolated venv from the Dockerfile
    ),
    render_config=render_config,  # <-- CRITICAL: You must pass it here so Airflow reads it!
    schedule=None,  # <-- This DAG is triggered by the master_workflow DAG
    start_date=datetime(2026, 7, 9),
    catchup=False,
    dag_id="dbt_stock_transformation_cosmos",
    tags=["dbt", "cosmos"],
)