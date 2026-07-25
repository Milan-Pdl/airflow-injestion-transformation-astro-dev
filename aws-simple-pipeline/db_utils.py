"""
Tiny helper shared by both load_*.py scripts - just runs a SQL query
against local Postgres and returns a pandas DataFrame.
"""
# import os
# import pandas as pd
# from sqlalchemy import create_engine, text
# from dotenv import load_dotenv

# load_dotenv()


# def get_dataframe(query: str) -> pd.DataFrame:
#     url = (
#         f"postgresql+psycopg2://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}"
#         f"@{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DB']}"
#     )
#     engine = create_engine(url)
#     with engine.connect() as conn:
#         df = pd.read_sql(text(query), conn)
#     return df

import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def get_dataframe(query: str) -> pd.DataFrame:
    conn = psycopg2.connect(
        host=os.environ["PG_HOST"],
        port=os.environ["PG_PORT"],
        dbname=os.environ["PG_DB"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )
    try:
        df = pd.read_sql(query, conn)   # query is just a plain SQL string
    finally:
        conn.close()
    return df