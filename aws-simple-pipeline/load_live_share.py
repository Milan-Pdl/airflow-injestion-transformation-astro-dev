"""
Loads today's rows from Postgres table "intermediate_liveShare" into:
  1. S3, as a CSV file (raw backup / audit trail)
  2. An Iceberg table in Glue/Athena (the actual queryable table)

Run it:
    python load_live_share.py

If you run this twice on the same day, step 1 just overwrites the same
CSV file, and step 2 just overwrites today's partition in the Iceberg
table - so it is always safe to re-run.
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
PG_SCHEMA = os.environ["PG_SCHEMA_intermediate"]

TABLE_NAME = "live_share_iceberg"          # name it will have in Athena
PARTITION_COL = "trade_date"                # column used for copy-on-write

session = boto3.Session(region_name=REGION)

# NOTE: the source table name has mixed case ("intermediate_liveShare"),
# so it MUST be double-quoted in Postgres or the query will fail.
QUERY = f'''
    SELECT
        stock_date_key, stock_symbol, company_name, no_of_transactions,
        max_price, min_price, opening_price, closing_price, amount,
        previous_closing, difference_rs, percent_change, volume, ltv,
        as_of_date, as_of_date_string, trade_date, loaded_at
    FROM {PG_SCHEMA}."intermediate_liveShare"
'''


def main():
    today = date.today().isoformat()

    # ---- step 1: read from Postgres -----------------------------------
    print("Step 1: reading from Postgres...")
    df = get_dataframe(QUERY)
    print(f"  -> got {len(df)} rows for trade_date = {today}")

    if len(df) == 0:
        print("  !! No rows found for today. Nothing to load.")
        print("  !! Check: did dbt actually run today? Does trade_date in the")
        print("     table really match today's date?")
        return

    df[PARTITION_COL] = df[PARTITION_COL].astype(str)

    # ---- step 2: upload raw CSV backup to S3 ---------------------------
    print("Step 2: uploading raw CSV to S3...")
    csv_key = f"raw/live_share/date={today}/live_share_{today}.csv"
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
        table_location=f"s3://{BUCKET}/iceberg-warehouse/live_share/",
        temp_path=f"s3://{BUCKET}/iceberg-warehouse/_tmp/live_share/",
        partition_cols=[PARTITION_COL],
        mode="overwrite_partitions",   # <- this is what makes reruns safe
        workgroup=ATHENA_WORKGROUP,
        boto3_session=session,
        keep_files=False,
    )
    print(f"  -> done. Table: {GLUE_DATABASE}.{TABLE_NAME}")


if __name__ == "__main__":
    main()
