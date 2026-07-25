from airflow.models.dag import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime

with DAG(
    "master_workflow",
    start_date=datetime(2026, 7, 9),
    schedule="0 23 * * 1-5",
    catchup=False,
) as dag:
    
    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_api_ingestion",
        trigger_dag_id="companies_ingestion", 
        wait_for_completion=True,              
        poke_interval=30,
        deferrable=True  # <-- THIS FIXES THE DEADLOCK
    )

    trigger_warehouse_build = TriggerDagRunOperator(
        task_id="trigger_dbt_transformations",
        trigger_dag_id="dbt_stock_transformation_cosmos",
        wait_for_completion=True,
        deferrable=True  # <-- ALSO GOOD TO HAVE HERE, 
        #free your worker while waiting for dbt to finish
    )

    trigger_aws_load = TriggerDagRunOperator(
        task_id="trigger_aws_ecosystem_load",
        trigger_dag_id="aws_ecosystem_load",
        wait_for_completion=True,
        deferrable=True
    )

    trigger_ingestion >> trigger_warehouse_build >> trigger_aws_load