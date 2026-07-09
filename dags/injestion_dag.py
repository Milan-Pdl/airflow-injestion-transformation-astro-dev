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
def companies_ingestion_dag():  # Fixed naming consistency

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
        
        hook.run(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};")
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

        # Extract target fields and tuples
        target_fields = ["company_id", "company_name", "stock_symbol", "sector_id", "sector_name"]
        rows = [
            (c["companyId"], c["companyName"], c["stockSymbol"], c["sectorId"], c["sectorName"])
            for c in companies
        ]

        # Use Airflow's built-in bulk inserter for plain inserts
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

        hook.run(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};")
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
                loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_stock_trade_date UNIQUE (stock_symbol, trade_date)
            );
        """)

        # Fast bulk upsert using execute_values from psycopg2 via Hook connection
        upsert_query = f"""
            INSERT INTO {stock_table}
            (
                stock_symbol, company_name, no_of_transactions, max_price, min_price,
                opening_price, closing_price, amount, previous_closing, difference_rs,
                percent_change, volume, ltv, as_of_date, as_of_date_string, trade_date, data_type
            )
            VALUES %s
            ON CONFLICT (stock_symbol, trade_date) 
            DO UPDATE SET
                company_name = EXCLUDED.company_name,
                no_of_transactions = EXCLUDED.no_of_transactions,
                max_price = EXCLUDED.max_price,
                min_price = EXCLUDED.min_price,
                opening_price = EXCLUDED.opening_price,
                closing_price = EXCLUDED.closing_price,
                amount = EXCLUDED.amount,
                previous_closing = EXCLUDED.previous_closing,
                difference_rs = EXCLUDED.difference_rs,
                percent_change = EXCLUDED.percent_change,
                volume = EXCLUDED.volume,
                ltv = EXCLUDED.ltv,
                as_of_date = EXCLUDED.as_of_date,
                as_of_date_string = EXCLUDED.as_of_date_string,
                data_type = EXCLUDED.data_type,
                loaded_at = CURRENT_TIMESTAMP
            WHERE 
                ({stock_table}.closing_price IS DISTINCT FROM EXCLUDED.closing_price OR
                {stock_table}.volume IS DISTINCT FROM EXCLUDED.volume OR
                {stock_table}.no_of_transactions IS DISTINCT FROM EXCLUDED.no_of_transactions);
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

        # Use execute_values for efficient, native Postgres array-based bulk upserts
        from psycopg2.extras import execute_values
        conn = hook.get_conn()
        with conn.cursor() as cur:
            execute_values(cur, upsert_query, rows)
            conn.commit()
        conn.close()

        print(f"Processed {len(rows)} records into the raw table (inserted or updated changes).")
    
    # --- TASK DEPENDENCIES (Both pipelines will run in parallel) ---
    company_data = fetch_companies()
    load_companies(company_data)

    share_data = fetch_share_prices()
    load_share_prices(share_data)
    

# Instantiate the DAG
companies_ingestion_dag()