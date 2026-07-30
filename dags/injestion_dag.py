from __future__ import annotations

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
from psycopg2.extras import execute_values

COMPANIES_URL = "https://nepalipaisa.com/api/GetCompanies"
SHARE_PRICE_URL = "https://nepalipaisa.com/api/GetTodaySharePrice"

API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://nepalipaisa.com/",
    "X-Requested-With": "XMLHttpRequest",
}

POSTGRES_CONN_ID = "postgres_dwh"
RAW_SCHEMA = "raw"
COMPANY_TABLE = f"{RAW_SCHEMA}.company"
STOCK_TABLE = f"{RAW_SCHEMA}.stock_market_data"


@dag(
    dag_id="companies_ingestion",
    schedule=None,
    start_date=datetime(2026, 7, 9),
    catchup=False,
    tags=["ingestion", "nepalipaisa"],
)
def companies_ingestion_dag():

    @task()
    def prepare_database_schema() -> None:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};")
        print(f"Schema '{RAW_SCHEMA}' initialized cleanly.")

    @task()
    def fetch_companies() -> list[dict]:
        response = requests.post(
            COMPANIES_URL,
            json=[],
            headers=API_HEADERS,
            timeout=60,
        )
        response.raise_for_status()
        companies = response.json().get("result", [])
        print(f"Fetched {len(companies)} companies")
        return companies

    @task()
    def load_companies(companies: list[dict]) -> None:
        if not companies:
            print("No company data to load.")
            return

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        hook.run(f"""
            CREATE TABLE IF NOT EXISTS {COMPANY_TABLE} (
                company_id INT,
                company_name VARCHAR,
                stock_symbol VARCHAR,
                sector_id INT,
                sector_name VARCHAR
            );
        """)
        hook.run(f"TRUNCATE TABLE {COMPANY_TABLE};")

        target_fields = ["company_id", "company_name", "stock_symbol", "sector_id", "sector_name"]
        rows = [
            (
                c.get("companyId"),
                c.get("companyName"),
                c.get("stockSymbol"),
                c.get("sectorId"),
                c.get("sectorName"),
            )
            for c in companies
        ]

        hook.insert_rows(table=COMPANY_TABLE, rows=rows, target_fields=target_fields)
        print(f"Inserted {len(rows)} companies into {COMPANY_TABLE}")

    @task()
    def fetch_share_prices() -> list[dict]:
        response = requests.get(
            SHARE_PRICE_URL,
            params={"stockSymbol": ""},
            headers=API_HEADERS,
            timeout=60,
        )
        response.raise_for_status()
        stocks = response.json().get("result", {}).get("stocks", [])
        print(f"Fetched {len(stocks)} stocks")
        return stocks

    @task()
    def load_share_prices(stocks: list[dict]) -> None:
        if not stocks:
            print("No share price data to load.")
            return

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        hook.run(f"""
            CREATE TABLE IF NOT EXISTS {STOCK_TABLE} (
                stock_symbol VARCHAR,
                company_name VARCHAR,
                no_of_transactions VARCHAR,
                max_price VARCHAR,
                min_price VARCHAR,
                opening_price VARCHAR,
                closing_price VARCHAR,
                amount VARCHAR,
                previous_closing VARCHAR,
                difference_rs VARCHAR,
                percent_change VARCHAR,
                volume VARCHAR,
                ltv VARCHAR,
                as_of_date VARCHAR,
                as_of_date_string VARCHAR,
                trade_date VARCHAR,
                data_type VARCHAR,
                loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)

        append_query = f"""
            INSERT INTO {STOCK_TABLE}
            (
                stock_symbol, company_name, no_of_transactions, max_price, min_price,
                opening_price, closing_price, amount, previous_closing, difference_rs,
                percent_change, volume, ltv, as_of_date, as_of_date_string, trade_date, data_type
            )
            VALUES %s;
        """

        rows = [
            (
                s.get("stockSymbol"), s.get("companyName"), s.get("noOfTransactions"), 
                s.get("maxPrice"), s.get("minPrice"), s.get("openingPrice"), 
                s.get("closingPrice"), s.get("amount"), s.get("previousClosing"), 
                s.get("differenceRs"), s.get("percentChange"), s.get("volume"), 
                s.get("ltv"), s.get("asOfDate"), s.get("asOfDateString"), 
                s.get("tradeDate"), s.get("dataType")
            )
            for s in stocks
        ]

        # Use context manager to prevent connection leaks
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                execute_values(cur, append_query, rows)
                conn.commit()

        print(f"Appended {len(rows)} records into {STOCK_TABLE}.")

    # --- CLEAN TASKFLOW DEPENDENCY ORCHESTRATION ---
    schema_task = prepare_database_schema()

    # 1. Pipeline for Companies
    companies_raw = fetch_companies()
    schema_task >> companies_raw
    load_companies(companies_raw)

    # 2. Pipeline for Daily Share Prices
    prices_raw = fetch_share_prices()
    schema_task >> prices_raw
    load_share_prices(prices_raw)


# Instantiate DAG
companies_ingestion_dag()