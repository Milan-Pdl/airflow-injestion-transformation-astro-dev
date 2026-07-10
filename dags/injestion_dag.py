from __future__ import annotations

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime

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
company_table = f"{RAW_SCHEMA}.company"
stock_table = f"{RAW_SCHEMA}.stock_market_data"

@dag(
    dag_id="companies_ingestion",
    schedule=None,
    start_date=datetime(2026, 7, 9),       
    catchup=False,
    tags=["ingestion", "nepalipaisa"],
)
def companies_ingestion_dag():

    # NEW INITIALIZATION TASK: Creates the schema safely before parallel workers begin
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
        companies = response.json()["result"]
        print(f"Fetched {len(companies)} companies")
        return companies

    @task()
    def load_companies(companies: list[dict]) -> None:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Removed CREATE SCHEMA from here to prevent duplicate worker race conditions
        hook.run(f"""
            CREATE TABLE IF NOT EXISTS {company_table} (
                company_id INT,
                company_name VARCHAR,
                stock_symbol VARCHAR,
                sector_id INT,
                sector_name VARCHAR
            );
        """)
        hook.run(f"TRUNCATE TABLE {company_table};")

        target_fields = ["company_id", "company_name", "stock_symbol", "sector_id", "sector_name"]
        rows = [
            (c["companyId"], c["companyName"], c["stockSymbol"], c["sectorId"], c["sectorName"])
            for c in companies
        ]

        hook.insert_rows(table=company_table, rows=rows, target_fields=target_fields)
        print(f"Inserted {len(rows)} companies")

    @task()
    def fetch_share_prices() -> list[dict]:
        response = requests.get(
            SHARE_PRICE_URL,
            params={"stockSymbol": ""},
            headers=API_HEADERS,
            timeout=60,
        )
        response.raise_for_status()
        stocks = response.json()["result"]["stocks"]
        print(f"Fetched {len(stocks)} stocks")
        return stocks    
    
    @task()
    def load_share_prices(stocks: list[dict]) -> None:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Removed CREATE SCHEMA from here to prevent duplicate worker race conditions
        hook.run(f"""
            CREATE TABLE IF NOT EXISTS {stock_table} (
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

        hook.run(f"""
            ALTER TABLE {stock_table} 
            DROP CONSTRAINT IF EXISTS unique_stock_trade_date;
        """)

        append_query = f"""
            INSERT INTO {stock_table}
            (
                stock_symbol, company_name, no_of_transactions, max_price, min_price,
                opening_price, closing_price, amount, previous_closing, difference_rs,
                percent_change, volume, ltv, as_of_date, as_of_date_string, trade_date, data_type
            )
            VALUES %s;
        """

        rows = [
            (
                s["stockSymbol"], s["companyName"], s["noOfTransactions"], s["maxPrice"], s["minPrice"],
                s["openingPrice"], s["closingPrice"], s["amount"], s["previousClosing"], s["differenceRs"],
                s["percentChange"], s["volume"], s["ltv"], s["asOfDate"], s["asOfDateString"], s["tradeDate"],
                s["dataType"]
            )
            for s in stocks
        ]

        from psycopg2.extras import execute_values
        conn = hook.get_conn()
        with conn.cursor() as cur:
            execute_values(cur, append_query, rows)
            conn.commit()
        conn.close()

        print(f"Appended {len(rows)} historical records into the raw table.")
    
    # --- FIXED TASK DEPENDENCIES WITH SEQUENTIAL SCHEMA INIT ---
    init_schema = prepare_database_schema()

    # Company processing pipeline
    company_data = fetch_companies()
    init_schema >> company_data >> load_companies(company_data)

    # Share price processing pipeline
    share_data = fetch_share_prices()
    init_schema >> share_data >> load_share_prices(share_data)
    

# Instantiate the DAG
companies_ingestion_dag()