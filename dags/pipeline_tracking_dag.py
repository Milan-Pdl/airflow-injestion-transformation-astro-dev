# from __future__ import annotations
# import requests
# from airflow.decorators import dag, task
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from pendulum import datetime
# from airflow.models import DagRun
# from airflow.utils.session import provide_session
# from airflow.utils.state import DagRunState
# from airflow.decorators import task
# from airflow.providers.http.hooks.http import HttpHook
# from datetime import date,timedelta
# POSTGRES_CONN_ID = "postgres_dwh"
# staging_schema="staging"
# staging_table="stg_liveShare"
# intermediate_schema="intermediate"
# intermediate_table="intermediate_liveShare"
# # Define your database credentials    



# # @task
# # @provide_session
# # def get_latest_master_dag_info(session=None):
# #     # Query the latest finished run of master_dag
# #     latest_run = (
# #         session.query(DagRun)
# #         .filter(
# #             DagRun.dag_id == "master_dag",
# #             DagRun.state.in_([DagRunState.SUCCESS, DagRunState.FAILED]),
# #         )
# #         .order_by(DagRun.execution_date.desc())
# #         .first()
# #     )

# #     if latest_run:
# #         run_id = latest_run.run_id
# #         start_date = latest_run.start_date
# #         end_date = latest_run.end_date
# #         state = latest_run.state

# #         print(
# #             f"Run ID: {run_id} | Start: {start_date} | End: {end_date} | State: {state}"
# #         )

# #         # Return or insert into your database...
# #         return {
# #             "run_id": run_id,
# #             "start_date": start_date.isoformat() if start_date else None,
# #             "end_date": end_date.isoformat() if end_date else None,
# #             "status": state,
# #         }

# #     return None
# @dag(
        
#     dag_id="pipeline_tracking_dag",
#     schedule=None,
#     start_date=datetime(2026, 8, 4),
#     catchup=False,
#     tags=["tracking", "nepalipaisa_pipeline"],
# )
# def pipeline_tracking_dag():

#     @task()
#     def compute_today_date():
#         """
#         returns today date
#         in case of saturday and sunday returns the closest data when the market is open which is friday
        
#         """
#         today_date=date.today()
#         print(today_date.weekday())
#         if today_date.day==6:
#             current_processing_date=today_date-timedelta(days=1)
#             print(current_processing_date)
#             return current_processing_date
#         if today_date.day==5:
#             current_processing_date=today_date-timedelta(days=2)
#             print(current_processing_date)
#             return current_processing_date
#         else:
#             # print(type(today_date))
#             return today_date
            
#     @task()
#     def get_stging_lastest_trade_date()->date:
#         """this functions returns the latest trade date from staging table
#         keyword arguments:
#         query -- a sql query to fetch the record
#         table_name -- name of the table where the record is at 
#         """
#         hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
#         sql = f'select max(trade_date) from "{staging_schema}"."{staging_table}"'
#         cur = hook.get_conn().cursor()
#         cur.execute(sql)
#         max_date=cur.fetchone()[0]
#         # print(f"Max date is : {type(max_date)}")
#         # print(max_date)
#         return max_date


#     @task()
#     def get_staging_row_count():
#         hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
#         cur = hook.get_conn().cursor()
#         sql=f"""with cet as(
#                 select 
#                 *,
#                 row_number() over(partition by stock_symbol,trade_date order by trade_date) as rnk
#                 from {staging_schema}."{staging_table}"
#                 where trade_date=(select max(trade_date) from {staging_schema}."{staging_table}")
#                 )
                
#                 select count(*) as cnt from cet
#                 where rnk=1"""
#         cur.execute(sql)
#         count=cur.fetchone()[0]
#         # print(count)
#         return count

#     @task()
#     def get_intermediate_lastest_trade_date()->date:
#         """this functions returns the latest trade date from staging table
#         keyword arguments:
#         query -- a sql query to fetch the record
#         table_name -- name of the table where the record is at 
#         """
#         hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
#         cur = hook.get_conn().cursor()
#         sql = f'select max(trade_date) from "{intermediate_schema}"."{intermediate_table}"'
#         cur.execute(sql)
#         max_date=cur.fetchone()[0]
#         # print(f"Max date is : {type(max_date)}")
#         # print(max_date)
#         return max_date

#     @task()
#     def get_intermediate_row_count()->int:
#         hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
#         cur = hook.get_conn().cursor()
#         sql=f'select count(*) as cnt from "{intermediate_schema}"."{intermediate_table}"'
#         cur.execute(sql)
#         count=cur.fetchone()[0]
#         # print(count)
#         return count
    
#     @task()
#     def get_latest_master_dag_info(session=None):
#         # Query the latest finished run of master_dag
#         http_hook=HttpHook(method="GET",http_conn_id="airflow_api")
#         url="api/v2/dags/master_workflow/dagRuns?order_by=-end_date&limit=10"
#         # response=requests.get(url)
#         response=http_hook.run(url)
#         data=response.json()
#         dag_runs = data.get("dag_runs", [])
#         completed_run=[
#            dag_run_status for dag_run_status in dag_runs
#            if dag_run_status.get("status") in ["success","failure"]
#         ]
#         if completed_run:
#             lasest_run=completed_run[0]
#             run_id=dag_runs.get("run_id")
#             start_at=dag_runs.get("start_date")
#             end_at=dag_runs.get("end_date")
#             status=lasest_run.get("status")

#             return {
#                 run_id,
#                 start_at,
#                 end_at,
#                 status
#             }
#         return None

#     @task()
#     def dump_into_sql():
#         hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
#             # Query the latest finished run of master_dag

#         payload={
#             "run_id":get_latest_master_dag_info().get("run_id"),
#             "started_at":get_latest_master_dag_info().get("start_at"),
#             "end_at":get_latest_master_dag_info().get("end_at"),
#             "latest_staging_liveshare_row_count":get_staging_row_count(),
#             "latest_intermediate_liveshare_row_count":get_intermediate_row_count(),
#             "current_staging_trade_date":get_stging_lastest_trade_date(),
#             "current_intermediate_trade_date":get_intermediate_lastest_trade_date(),
#             "status":get_latest_master_dag_info().get("status")

#         }
#         hook.insert_rows(table="tracking.pipeline_tracking", rows=[payload], target_fields=list(payload.keys()), commit_every=1)
#         print(payload)
#         return payload

#     get_latest_master_dag_info() >> get_stging_lastest_trade_date() >> get_staging_row_count() >> get_intermediate_lastest_trade_date() >> get_intermediate_row_count() >> compute_today_date() >> dump_into_sql()

# pipeline_tracking_dag()

from __future__ import annotations

from datetime import date, timedelta
from pendulum import datetime

from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests

POSTGRES_CONN_ID = "postgres_dwh"
staging_schema = "staging"
staging_table = "stg_liveShare"
intermediate_schema = "intermediate"
intermediate_table = "intermediate_liveShare"


@dag(
    dag_id="pipeline_tracking_dag",
    schedule=None,
    start_date=datetime(2026, 8, 4),
    catchup=False,
    tags=["tracking", "nepalipaisa_pipeline"],
)
def pipeline_tracking_dag():

    @task()
    def compute_today_date():
        """Returns today's date. Adjusts for weekend market closure."""
        today_date = date.today()
        # weekday() returns 5 for Saturday, 6 for Sunday
        if today_date.weekday() == 6:  # Sunday
            return today_date - timedelta(days=2)
        elif today_date.weekday() == 5:  # Saturday
            return today_date - timedelta(days=1)
        else:
            return today_date

    @task()
    def get_staging_latest_trade_date() -> str:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = f'select max(trade_date) from "{staging_schema}"."{staging_table}"'
        cur = hook.get_conn().cursor()
        cur.execute(sql)
        max_date = cur.fetchone()[0]
        return str(max_date) if max_date else None

    @task()
    def get_staging_row_count() -> int:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        cur = hook.get_conn().cursor()
        sql = f"""
            with cet as (
                select 
                    *,
                    row_number() over(partition by stock_symbol, trade_date order by trade_date) as rnk
                from {staging_schema}."{staging_table}"
                where trade_date = (select max(trade_date) from {staging_schema}."{staging_table}")
            )
            select count(*) as cnt from cet where rnk = 1
        """
        cur.execute(sql)
        return cur.fetchone()[0]

    @task()
    def get_intermediate_latest_trade_date() -> str:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        cur = hook.get_conn().cursor()
        sql = f'select max(trade_date) from "{intermediate_schema}"."{intermediate_table}"'
        cur.execute(sql)
        max_date = cur.fetchone()[0]
        return str(max_date) if max_date else None

    @task()
    def get_intermediate_row_count() -> int:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        cur = hook.get_conn().cursor()
        sql = f'select count(*) as cnt from "{intermediate_schema}"."{intermediate_table}"'
        cur.execute(sql)
        return cur.fetchone()[0]

    @task()
    def get_latest_master_dag_info():
        # Cleaned up API call via HttpHook

        token = "eyJhbGciOiJIUzUxMiIsImtpZCI6Im5vdC11c2VkIiwidHlwIjoiSldUIn0.eyJzdWIiOiJBbm9ueW1vdXMiLCJyb2xlIjoiQURNSU4iLCJ0ZWFtcyI6W10sImp0aSI6IjhjYjU2MGZjOTllMTQ2MzRhNDA3Y2EzYjYyOWE2YTMxIiwiYXVkIjoiYXBhY2hlLWFpcmZsb3ciLCJuYmYiOjE3ODU4Mzc1NTAsImV4cCI6MTc4NTkyMzk1MCwiaWF0IjoxNzg1ODM3NTUwfQ.lnrxwIzFLIJGlJuj36JyKyeLSmzS_4J6oU8SF7dI55rm9g8kV5p3HJGOi33DhzFgaVUyY9KBhelH9UAO2aAnvQ"

        headers = {
            "Authorization": f"Bearer {token}"
        }

        url = "http://api-server:8080/api/v2/dags/master_workflow/dagRuns?order_by=-end_date&limit=10"

        try:
            response = requests.get(url, headers=headers)
            date=response.json()
            dag_runs = date.get("dag_runs", [])
            print(dag_runs)


                # Filter for completed runs
            completed_runs = [
                    run for run in dag_runs if run.get("state") in ["success", "failed"]
                ]

            if completed_runs:
                    latest_run = completed_runs[0]
                    print(f"Latest Master DAG Run: {latest_run.get('dag_run_id')} | "
                        f"Start: {latest_run.get('start_date')} | "
                        f"End: {latest_run.get('end_date')} | "
                        f"Status: {latest_run.get('state')}")
                    
                    return {
                        "run_id": latest_run.get("dag_run_id"),
                        "start_at": latest_run.get("start_date"),
                        "end_at": latest_run.get("end_date"),
                        "status": latest_run.get("state"),
                    }
        except Exception as e:
            print(f"Error fetching master DAG metrics via API: {e}")

        return {
            "run_id": None,
            "start_at": None,
            "end_at": None,
            "status": "UNKNOWN",
        }

    @task()
    def dump_into_sql(
        master_info: dict,
        stg_count: int,
        inter_count: int,
        stg_date: str,
        inter_date: str,
    ):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        payload = {
            "run_id": master_info.get("run_id"),
            "started_at": master_info.get("start_at"),
            "end_at": master_info.get("end_at"),
            "latest_staging_liveshare_row_count": stg_count,
            "latest_intermediate_liveshare_row_count": inter_count,
            "current_staging_trade_date": stg_date,
            "current_intermediate_trade_date": inter_date,
            "status": master_info.get("status"),
        }

        # Extract target column names and row values explicitly
        target_fields = list(payload.keys())
        row_values = [tuple(payload.values())]  # <-- FIX: Convert values to tuple

        hook.insert_rows(
            table="tracking.pipeline_tracking",
            rows=row_values,
            target_fields=target_fields,
            commit_every=1,
        )

        print("Pipeline Metrics Inserted:", payload)
        return payload

    # Execute and pass XCom values naturally
    master_info = get_latest_master_dag_info()
    stg_date = get_staging_latest_trade_date()
    stg_count = get_staging_row_count()
    inter_date = get_intermediate_latest_trade_date()
    inter_count = get_intermediate_row_count()
    _ = compute_today_date()

    dump_into_sql(master_info, stg_count, inter_count, stg_date, inter_date)


pipeline_tracking_dag()