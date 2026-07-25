# Nepal Stock Market → Iceberg Pipeline (simple version)

Loads two Postgres tables (built by dbt) into Iceberg tables on S3, run
straight from your VS Code terminal. No Airflow, no fancy abstractions —
just plain Python scripts you can read top to bottom.

## How it works

```
Postgres (dbt intermediate tables)
        |
        |  1. SELECT today's rows
        v
S3 raw CSV  (raw/live_share/date=2026-07-25/...)
        |
        |  2. awswrangler writes it as an Iceberg table
        v
Glue Data Catalog  (database: nepal_stock_market_db)
        |
        |  3. queryable
        v
Athena  (SQL, time travel, etc.)
```

- `intermediate_liveShare` → Iceberg table `live_share_iceberg`
- `intermediate_weekly_broker_summary` → Iceberg table `weekly_broker_summary_iceberg`

Both tables live in the **same Glue database** (`nepal_stock_market_db`) —
that's expected. A Glue "database" is just a folder/namespace; it normally
holds many tables. If you only see one table under it, that means only
one of the two scripts actually finished writing — see the debugging
section at the bottom, it's a very common first-run issue.

**Re-running safely:** each script partitions its Iceberg table by date
(`trade_date` / `scraped_date`) and writes with
`mode="overwrite_partitions"`. That means running the same script twice
today just overwrites today's data — it never creates duplicates, and it
never touches any other day's data.

## Files

| File | What it does |
|---|---|
| `db_utils.py` | one function: run a SQL query against Postgres, return a DataFrame |
| `load_live_share.py` | Postgres → S3 CSV → Iceberg, for live share prices |
| `load_broker_summary.py` | Postgres → S3 CSV → Iceberg, for broker holding |
| `run_both.py` | just calls both of the above, one after the other |
| `setup_aws_infra.py` | one-time: creates the S3 bucket, Glue database, Athena workgroup |
| `iam_policy.json` | permissions to paste into your IAM user |
| `.env.example` | copy to `.env` and fill in your own values |

## Setup (do this once)

**1. Install dependencies**
```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # Mac/Linux
pip install -r requirements.txt
```

**2. Create your `.env` file**
```bash
cp .env.example .env
```
Open `.env` and fill in your Postgres username/password, and pick a
globally-unique S3 bucket name.

**3. Create an IAM user in AWS (console, 2 minutes)**
- Go to IAM → Users → Create user → name it `nepal-stock-pipeline`
- Skip the group/permissions step for now, click "Create user"
- Open the new user → "Add permissions" → "Create inline policy" → click
  the **JSON** tab → paste the contents of `iam_policy.json` (replace
  `REPLACE_BUCKET_NAME` with your actual bucket name first) → save
- Go to "Security credentials" tab → "Create access key" → choose
  "Application running outside AWS" → copy the Access key ID and Secret
  access key into your `.env` file

**4. Create the S3 bucket / Glue database / Athena workgroup**
```bash
python setup_aws_infra.py
```
You should see three `[create]` lines the first time, and `[ok]` lines if
you run it again.

## Running the pipeline

```bash
python load_live_share.py
python load_broker_summary.py

# or both at once:
python run_both.py
```

Every step prints what it's doing and how many rows it found, so if
something looks wrong you'll see it immediately in the terminal — nothing
fails silently.

## Checking the result

AWS Console → Athena → make sure the workgroup dropdown (top left) is set
to `nepal-stock-market-wg`, then:

```sql
SELECT * FROM nepal_stock_market_db.live_share_iceberg LIMIT 10;
SELECT * FROM nepal_stock_market_db.weekly_broker_summary_iceberg LIMIT 10;
```

## Debugging "only one table got created"

This almost always means one script printed `No rows found for today` and
stopped before creating its table. Look back at your terminal output:

- If `load_live_share.py` never printed `Step 3: writing to Iceberg
  table...`, it means `SELECT ... WHERE trade_date = CURRENT_DATE`
  returned 0 rows. Run that same query directly in psql/DBeaver and check
  whether `trade_date` in the table actually equals today's date (common
  cause: dbt ran with a different date, or `trade_date` was loaded as a
  different day than "today" from the API).
- Same idea for `load_broker_summary.py` and `scraped_date`.
- If it DID print "Step 3" but you still don't see the table in Glue,
  scroll up for a Python traceback / AWS error — awswrangler will raise
  an exception (not fail silently) if the Athena query itself errors out,
  e.g. a permissions issue. Common cause: the IAM policy wasn't attached
  correctly, or the Athena workgroup isn't on engine version 3.

You can also just query Postgres directly first, before touching AWS at
all, to confirm both tables actually have rows for today:
```sql
SELECT count(*) FROM intermediate."intermediate_liveShare" WHERE trade_date = CURRENT_DATE;
SELECT count(*) FROM intermediate.intermediate_weekly_broker_summary WHERE scraped_date = CURRENT_DATE;
```

## Later, if you want to automate this on a schedule

You don't need Airflow for something this small. A simple cron job (Mac/
Linux) or Windows Task Scheduler entry that runs `python run_both.py`
once a day is enough:

```bash
# crontab -e
30 15 * * 0-4 cd /path/to/this/project && .venv/bin/python run_both.py >> run.log 2>&1
```
