import logging
import os
from typing import List, Dict, Any
from dotenv import load_dotenv
import psycopg
from psycopg.rows import tuple_row

logger = logging.getLogger(__name__)

load_dotenv()

# ----------------- Client -----------------
def get_pg_conn() -> psycopg.Connection:
    """
    Open a new PostgreSQL connection using environment variables.
    """
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
        autocommit=True,
        row_factory=tuple_row,
    )


# ----------------- Staging helpers -----------------
def truncate_candles_stage(conn: psycopg.Connection) -> None:
    """Truncate the staging table before a fresh load."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE public.candles_stage")
    logger.info("Truncated public.candles_stage")


def insert_candles_batch(conn: psycopg.Connection, candles: List[Dict[str, Any]]) -> int:
    """
    Bulk-insert a list of candle dicts into public.candles_stage.
    Returns the number of rows inserted.
    """
    if not candles:
        return 0

    sql = """
        INSERT INTO public.candles_stage
            (exchange, symbol, timeframe, open_time, close_time,
             base_currency, quote_currency, open, high, low, close, volume)
        VALUES
            (%(exchange)s, %(symbol)s, %(timeframe)s, %(open_time)s, %(close_time)s,
             %(base_currency)s, %(quote_currency)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
    """
    with conn.cursor() as cur:
        cur.executemany(sql, candles)

    logger.info(f"Inserted {len(candles)} candles into public.candles_stage")
    return len(candles)


def merge_stage_to_candles(conn: psycopg.Connection) -> None:
    """
    Execute DB-side merge procedure to copy missing records from stage to main.
    Requires stored procedure: public.merge_candles_stage_to_candles().
    """
    with conn.cursor() as cur:
        cur.execute("CALL public.merge_candles_stage_to_candles()")
    logger.info("Executed public.merge_candles_stage_to_candles()")

