"""
Loads today's rows from Postgres table "intermediate_weekly_broker_summary"
into:
  1. S3, as a CSV file (raw backup / audit trail)
  2. An Iceberg table in Glue/Athena (the actual queryable table)

Run it:
    python load_broker_summary.py

Safe to re-run on the same day (same reasons as load_live_share.py).
"""
import os
import io
from datetime import date

import awswrangler as wr
import boto3
from dotenv import load_dotenv

from db_utils import get_dataframe

load_dotenv()

BUCKET = os.environ["S3_BUCKET"]
REGION = os.environ["AWS_REGION"]
GLUE_DATABASE = os.environ["GLUE_DATABASE"]
ATHENA_WORKGROUP = os.environ["ATHENA_WORKGROUP"]
PG_SCHEMA = os.environ["PG_SCHEMA_fct_historical_weekly_broker_holding_summary"]

TABLE_NAME = "weekly_broker_summary_iceberg"   # name it will have in Athena
PARTITION_COL = "scraped_date"                  # column used for copy-on-write

session = boto3.Session(region_name=REGION)

QUERY = f'''
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
    FROM {PG_SCHEMA}.fct_weekly_broker_holding_analysis_summary
'''


def main():
    today = date.today().isoformat()

    # ---- step 1: read from Postgres -----------------------------------
    print("Step 1: reading from Postgres...")
    df = get_dataframe(QUERY)
    print(f"  -> got {len(df)} rows for scraped_date = {today}")

    if len(df) == 0:
        print("  !! No rows found for today. Nothing to load.")
        print("  !! This is the most common reason a table doesn't show up")
        print("     in Glue - the query above returned 0 rows, so the script")
        print("     stops here before ever creating the Iceberg table.")
        print("  !! Check: does intermediate_weekly_broker_summary really")
        print("     have a row where scraped_date = today's date? Run the")
        print("     query above directly in psql/DBeaver to confirm.")
        return

    df[PARTITION_COL] = df[PARTITION_COL].astype(str)

    # ---- step 2: upload raw CSV backup to S3 ---------------------------
    print("Step 2: uploading raw CSV to S3...")
    csv_key = f"raw/weekly_broker_summary/date={today}/weekly_broker_summary_{today}.csv"
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    s3 = session.client("s3")
    s3.put_object(Bucket=BUCKET, Key=csv_key, Body=buffer.getvalue().encode("utf-8"))
    print(f"  -> s3://{BUCKET}/{csv_key}")

    # ---- step 3: write to the Iceberg table -----------------------------
    print("Step 3: writing to Iceberg table in Glue/Athena...")
    wr.athena.to_iceberg(
        df=df,
        database=GLUE_DATABASE,
        table=TABLE_NAME,
        table_location=f"s3://{BUCKET}/iceberg-warehouse/weekly_broker_summary/",
        temp_path=f"s3://{BUCKET}/iceberg-warehouse/_tmp/weekly_broker_summary/",
        partition_cols=[PARTITION_COL],
        mode="overwrite_partitions",
        workgroup=ATHENA_WORKGROUP,
        boto3_session=session,
        keep_files=False,
    )
    print(f"  -> done. Table: {GLUE_DATABASE}.{TABLE_NAME}")


if __name__ == "__main__":
    main()
