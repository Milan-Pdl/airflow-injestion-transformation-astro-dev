from __future__ import annotations

import io
import os
from datetime import date
from pathlib import Path

import awswrangler as wr
import boto3
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv
from pendulum import datetime

load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")
load_dotenv()

POSTGRES_CONN_ID = "postgres_dwh"


def _get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


BUCKET = _get_env("S3_BUCKET")
REGION = _get_env("AWS_REGION")
GLUE_DATABASE = _get_env("GLUE_DATABASE")
ATHENA_WORKGROUP = _get_env("ATHENA_WORKGROUP")
PG_SCHEMA_INTERMEDIATE = _get_env("PG_SCHEMA_intermediate")
PG_SCHEMA_BROKER = _get_env("PG_SCHEMA_fct_historical_weekly_broker_holding_summary")


def _load_dataframe_to_aws(
    query: str,
    *,
    table_name: str,
    partition_col: str,
    s3_prefix: str,
) -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    df = hook.get_pandas_df(sql=query)

    if df.empty:
        print(f"No rows returned for {table_name}; skipping S3/Iceberg load.")
        return
    # making the partition as per the trade_date
    # today = date.today().isoformat()
    
    df[partition_col] = df[partition_col].astype(str)
    partition_value = str(df['trade_date'].iloc[0])

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)

    session = boto3.Session(region_name=REGION)
    s3_client = session.client("s3")
    csv_key = f"{s3_prefix}/date={partition_value}/{table_name}_{partition_value}.csv"
    s3_client.put_object(
        Bucket=BUCKET,
        Key=csv_key,
        Body=buffer.getvalue().encode("utf-8"),
    )
    print(f"Uploaded raw CSV to s3://{BUCKET}/{csv_key}")

    latest_csv = wr.s3.read_csv(
        path=f"s3://{BUCKET}/{s3_prefix}/date={partition_value}/{table_name}_{partition_value}.csv",
        boto3_session=session,
    )
    wr.athena.to_iceberg(
        df=latest_csv,
        database=GLUE_DATABASE,
        table=table_name,
        table_location=f"s3://{BUCKET}/iceberg-warehouse/{table_name}/",
        temp_path=f"s3://{BUCKET}/iceberg-warehouse/_tmp/{table_name}/",
        partition_cols=[partition_col],
        mode="overwrite_partitions",
        workgroup=ATHENA_WORKGROUP,
        boto3_session=session,
        keep_files=False,
    )
    print(f"Loaded {len(df)} rows into {GLUE_DATABASE}.{table_name}")


@dag(
    dag_id="aws_ecosystem_load",
    schedule=None,
    start_date=datetime(2026, 7, 9),
    catchup=False,
    tags=["aws", "s3", "athena", "iceberg", "warehouse"],
)
def aws_ecosystem_load_dag():
    @task()
    def load_live_share_to_aws() -> None:
        query = f"""
            SELECT
                stock_date_key, stock_symbol, company_name, no_of_transactions,
                max_price, min_price, opening_price, closing_price, amount,
                previous_closing, difference_rs, percent_change, volume, ltv,
                as_of_date, as_of_date_string, trade_date, loaded_at
            FROM {PG_SCHEMA_INTERMEDIATE}."intermediate_liveShare"
        """
        _load_dataframe_to_aws(
            query,
            table_name="live_share_iceberg",
            partition_col="trade_date",
            s3_prefix="raw/live_share",
        )

    @task()
    def load_broker_summary_to_aws() -> None:
        query = f"""
            SELECT
                scraped_date,
                symbol,
                period_range,
                total_buy_qty,
                total_sell_qty,
                weekly_buy_pressure_pct,
                weekly_sell_pressure_pct,
                rank_1_buyer,
                rank_2_buyer,
                rank_3_buyer,
                rank_1_dumper,
                rank_2_dumper,
                rank_3_dumper,
                sentiment_status
            FROM {PG_SCHEMA_BROKER}.fct_weekly_broker_holding_analysis_summary
        """
        _load_dataframe_to_aws(
            query,
            table_name="weekly_broker_summary_iceberg",
            partition_col="scraped_date",
            s3_prefix="raw/weekly_broker_summary",
        )

    load_live_share_to_aws() >> load_broker_summary_to_aws()


aws_ecosystem_load_dag()
