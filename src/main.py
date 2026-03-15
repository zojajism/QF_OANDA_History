import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv
from logger_config import setup_logger

from telegram_notifier import (
    notify_telegram,
    ChatType,
    start_telegram_notifier,
    close_telegram_notifier,
)

from exchange_oanda import (
    get_oanda_candles_history,
    OandaEnv,
)

from db_general import (
    get_pg_conn,
    truncate_candles_stage,
    insert_candles_batch,
    merge_stage_to_candles,
)

CONFIG_PATH = Path("/data/config.yaml")
if not CONFIG_PATH.exists():
    CONFIG_PATH = Path(__file__).resolve().parent / "data" / "config.yaml"

DB_INSERT_BATCH = 1000   # rows per executemany call


def to_oanda_instrument(symbol_str: str) -> str:
    return symbol_str.strip().upper().replace("/", "_")


def _parse_config_dt(value) -> datetime:
    """Parse a YAML datetime value (or ISO string) to UTC-aware datetime."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def main():
    try:
        # Load .env (Docker volume first, then local)
        env_path = Path("/data/.env")
        if not env_path.exists():
            env_path = Path(__file__).resolve().parent / "data" / ".env"
        load_dotenv(dotenv_path=env_path)

        logger = setup_logger()
        logger.info(json.dumps({"EventCode": 0, "Message": "Starting QF_OANDA_History ..."}))

        await start_telegram_notifier()
        notify_telegram("QF_OANDA_History App started ...", ChatType.ALERT)

        # -- Load config --
        if not CONFIG_PATH.exists():
            raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

        with CONFIG_PATH.open("r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f) or {}

        symbols    = [str(s) for s in config_data.get("symbols", [])]
        timeframes = [str(t).lower() for t in config_data.get("timeframes", [])]
        start_dt   = _parse_config_dt(config_data["Strat_Date_Time"])
        end_dt     = _parse_config_dt(config_data["End_Date_Time"])

        if not symbols or not timeframes:
            raise ValueError("config.yaml must define at least one symbol and one timeframe")

        logger.info(json.dumps({
            "Message": "Config loaded",
            "symbols": symbols,
            "timeframes": timeframes,
            "start": start_dt.isoformat(),
            "end": end_dt.isoformat(),
        }))

        # -- OANDA credentials --
        env_flag    = (os.getenv("OANDA_ENV") or "live").strip().lower()
        oanda_env   = OandaEnv.LIVE if env_flag == "live" else OandaEnv.PRACTICE
        oanda_token = os.getenv("OANDA_API_TOKEN")
        if not oanda_token:
            raise RuntimeError("Missing OANDA_API_TOKEN in .env")

        # -- Database connection --
        conn = get_pg_conn()

        # Truncate staging table ONCE before the run
        truncate_candles_stage(conn)
        notify_telegram("Staging table truncated", ChatType.ALERT)

        # -- Fetch & insert each symbol x timeframe --
        total_inserted = 0

        for symbol in symbols:
            instrument = to_oanda_instrument(symbol)
            for tf in timeframes:
                logger.info(json.dumps({
                    "Message": f"Fetching {symbol} {tf}",
                    "instrument": instrument,
                }))

                candles = await get_oanda_candles_history(
                    display_symbol=symbol,
                    instrument=instrument,
                    timeframe=tf,
                    price_mode="M",
                    token=oanda_token,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    env=oanda_env,
                )

                if not candles:
                    logger.warning(f"No candles returned for {symbol} {tf}")
                    continue

                # Insert in batches
                for i in range(0, len(candles), DB_INSERT_BATCH):
                    batch = candles[i : i + DB_INSERT_BATCH]
                    inserted = insert_candles_batch(conn, batch)
                    total_inserted += inserted

                logger.info(json.dumps({
                    "Message": f"Done {symbol} {tf}",
                    "candles_fetched": len(candles),
                }))
                notify_telegram(
                    f"{symbol} {tf}: {len(candles)} candles inserted", ChatType.ALERT
                )

            # Merge stage into main candles table (missing records only)
            merge_stage_to_candles(conn)
            notify_telegram("candles_stage merged into candles", ChatType.ALERT)

        conn.close()

        logger.info(json.dumps({
            "EventCode": 0,
            "Message": "Run complete",
            "total_inserted": total_inserted,
        }))
        notify_telegram(
            f"QF_OANDA_History finished - {total_inserted} total candles inserted.",
            ChatType.ALERT,
        )

    except Exception as e:
        notify_telegram(f"QF_OANDA_History ERROR: {e}", ChatType.ALERT)
        raise
    finally:
        notify_telegram("QF_OANDA_History App stopped.", ChatType.ALERT)
        await close_telegram_notifier()


if __name__ == "__main__":
    asyncio.run(main())