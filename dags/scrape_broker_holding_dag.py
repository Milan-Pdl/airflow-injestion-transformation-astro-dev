from datetime import datetime, timedelta
import os

from bs4 import BeautifulSoup

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from playwright.sync_api import sync_playwright

# Configuration Constants
POSTGRES_CONN_ID = "postgres_dwh"
RAW_SCHEMA = "raw"
BROKER_HOLDING_TABLE = "broker_holding"
DATA_DIR = "/usr/local/airflow/include/data"


# --- Helper Functions for Scraper ---


def get_all_available_symbols(page) -> list:
    """Extracts stock symbols from the Select2 dropdown."""
    page.wait_for_selector("span.select2-selection--single")
    page.click("span.select2-selection--single")

    page.wait_for_selector(
        "ul.select2-results__options li.select2-results__option", timeout=15000
    )

    symbols = page.evaluate("""
        () => {
            const listItems = document.querySelectorAll('ul.select2-results__options li.select2-results__option');
            return Array.from(listItems)
                .map(li => li.textContent.trim())
                .filter(sym => sym && !sym.includes('Select Symbol'));
        }
    """)

    page.keyboard.press("Escape")
    return symbols


def parse_dynamic_table(html_content: str, action_type: str) -> pd.DataFrame:
    """Parses scraped HTML table into a clean pandas DataFrame."""
    soup = BeautifulSoup(html_content, "html.parser")
    data_dict = {}

    for row in soup.find_all("tr"):
        header_el = row.find("th")
        if header_el:
            header_name = header_el.get_text(strip=True)
            values = [td.get_text(strip=True) for td in row.find_all("td")]
            data_dict[header_name] = values

    df = pd.DataFrame(data_dict)

    if not df.empty:
        if "Broker" in df.columns:
            df["Broker"] = df["Broker"].astype(str)
        if "Quantity" in df.columns:
            df["Quantity"] = (
                df["Quantity"].astype(str).str.replace(",", "", regex=False)
            )
            df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
        df["Type"] = action_type

    return df


# --- DAG Definition ---

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
}


@dag(
    dag_id="nepse_broker_holding_ingestion",
    default_args=default_args,
    description="Scrapes NepseAlpha broker holdings and ingests into Postgres using PostgresHook",
    schedule="0 18 * * 5",  # Runs every Friday at 6:00 PM UTC
    start_date=datetime(2026, 7, 25),
    catchup=False,
    tags=["nepse", "scraper", "broker_holding"],
)
def broker_holding_pipeline():

    @task()
    def scrape_broker_holdings() -> str:
        """Scrapes weekly broker holdings via Playwright and saves output to local CSV."""
        os.makedirs(DATA_DIR, exist_ok=True)
        all_data = []

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--window-size=1920,1080",
                ],
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
            )

            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )

            page = context.new_page()
            page.goto(
                "https://nepsealpha.com/broker-holding",
                wait_until="domcontentloaded",
            )

            page.wait_for_selector("#report-types", state="attached", timeout=15000)
            page.select_option("#report-types", value="W")

            print("Fetching stock symbols...")
            symbols = get_all_available_symbols(page)
            print(f"Discovered {len(symbols)} symbols to scrape.")

            for index, symbol in enumerate(symbols, start=1):
                print(f"[{index}/{len(symbols)}] Scraper running for symbol: {symbol}")
                try:
                    # 1. Click to open Select2 dropdown
                    page.wait_for_selector("span.select2-selection--single", timeout=5000)
                    page.click("span.select2-selection--single")

                    # 2. Wait specifically for the ACTIVE visible search input
                    search_input = page.wait_for_selector(
                        ".select2-container--open input.select2-search__field",
                        state="visible",
                        timeout=5000
                    )
                    
                    # 3. Type symbol and select matching result directly
                    search_input.fill(symbol)
                    
                    # Wait for filtered option to appear and click it
                    option_selector = f"ul.select2-results__options li.select2-results__option:has-text('{symbol}')"
                    page.wait_for_selector(option_selector, timeout=3000)
                    page.click(option_selector)

                    # 4. Explicitly click Filter button and wait for AJAX response
                    page.click("button:has-text('Filter')", force=True)
                    page.wait_for_load_state("networkidle", timeout=5000)

                    # 5. Wait for tables to appear
                    page.wait_for_selector(
                        "#broker_holder_buyer_div table",
                        state="visible",
                        timeout=8000,
                    )
                    page.wait_for_selector(
                        "#broker_holder_seller_div table",
                        state="visible",
                        timeout=8000,
                    )

                    date_range_text = (
                        page.locator("div.card-header .card-title b")
                        .first.inner_text()
                    )

                    buyer_html = page.locator(
                        "#broker_holder_buyer_div div.table-responsive"
                    ).inner_html()
                    seller_html = page.locator(
                        "#broker_holder_seller_div div.table-responsive"
                    ).inner_html()

                    df_buyers = parse_dynamic_table(buyer_html, action_type="Buy")
                    df_sellers = parse_dynamic_table(seller_html, action_type="Sell")

                    combined_df = pd.concat([df_buyers, df_sellers], ignore_index=True)

                    if not combined_df.empty:
                        combined_df["Symbol"] = symbol
                        combined_df["Period_Range"] = date_range_text.strip()
                        combined_df["Scraped_At"] = pd.Timestamp.now().isoformat()
                        all_data.append(combined_df)
                        print(f"  Successfully extracted data for {symbol}")

                except Exception as err:
                    print(f"  Skipped symbol '{symbol}' (Error: {err})")
                    # Press Escape to reset open dropdown state before next symbol
                    page.keyboard.press("Escape")
                    continue

            context.close()
            browser.close()

        if not all_data:
            raise ValueError("Failed to capture any broker holding data. Aborting pipeline.")

        master_df = pd.concat(all_data, ignore_index=True)
        current_date = datetime.now().strftime("%Y-%m-%d")
        csv_file_path = os.path.join(DATA_DIR, f"broker_holding_{current_date}.csv")
        master_df.to_csv(csv_file_path, index=False)
        print(f"Dataset successfully created: {csv_file_path}")

        return csv_file_path

    @task()
    def ingest_csv_to_db(csv_file_path: str):
        """Loads CSV data into Postgres schema using standard PostgresHook."""
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"Missing file: {csv_file_path}")

        df = pd.read_csv(csv_file_path)
        if df.empty:
            print("CSV file is empty. Ingestion skipped.")
            return

        df.columns = [
            col.strip().lower().replace(" ", "_") for col in df.columns
        ]

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        table_qualified = f"{RAW_SCHEMA}.{BROKER_HOLDING_TABLE}"

        create_table_sql = f"""
            CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};
            CREATE TABLE IF NOT EXISTS {table_qualified} (
                broker VARCHAR,
                quantity NUMERIC,
                type VARCHAR,
                symbol VARCHAR,
                period_range VARCHAR,
                scraped_at VARCHAR
            );
        """
        pg_hook.run(create_table_sql)

        rows_to_insert = [
            (
                str(row.get("broker", "")),
                row.get("quantity") if pd.notna(row.get("quantity")) else None,
                str(row.get("type", "")),
                str(row.get("symbol", "")),
                str(row.get("period_range", "")),
                str(row.get("scraped_at", "")),
            )
            for _, row in df.iterrows()
        ]

        pg_hook.insert_rows(
            table=table_qualified,
            rows=rows_to_insert,
            target_fields=[
                "broker",
                "quantity",
                "type",
                "symbol",
                "period_range",
                "scraped_at",
            ],
            commit_every=1000,
        )

        print(
            f"Successfully inserted {len(rows_to_insert)} records into `{table_qualified}`."
        )

    csv_path = scrape_broker_holdings()
    ingest_csv_to_db(csv_path)


dag_instance = broker_holding_pipeline()