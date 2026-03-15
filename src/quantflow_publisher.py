from __future__ import annotations
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional
import logging

# --------- small utilities ---------------------------------------------------
logger = logging.getLogger(__name__)
    
def _ensure_utc_iso(dt: datetime) -> str:
    """Return ISO 8601 string with millisecond precision (UTC)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    # Format with microseconds, then truncate to milliseconds
    return dt.isoformat(timespec="milliseconds")#.replace("+00:00", "Z")


def _symbol_for_subject(base_currency: str, quote_currency: str) -> str:
    """Subject-safe symbol (no slash). Example: BTC + USDT -> BTCUSDT"""
    return f"{base_currency.upper()}{quote_currency.upper()}"


def _symbol_for_payload(base_currency: str, quote_currency: str) -> str:
    """Human-friendly payload symbol. Example: BTC/USDT"""
    return f"{base_currency.upper()}/{quote_currency.upper()}"


def _require_keys(d: Dict[str, Any], keys: list[str], ctx: str) -> None:
    missing = [k for k in keys if k not in d]
    if missing:
        #raise KeyError(f"{ctx}: missing required key(s): {', '.join(missing)}")
        logger.error(f"{ctx}: missing required key(s): {', '.join(missing)}")


# --------- candle publisher --------------------------------------------------

async def publish_candle(
    candle: Optional[Dict[str, Any]] = None,
    **kwargs,
):
    """
    Stub for candle data — NATS removed. DB insert will go here (step 2).

    Usage:
      await publish_candle(candle=candle_dict)

    Required fields (either in `candle` or kwargs):
      exchange, base_currency, quote_currency, timeframe,
      open_time, close_time, open, high, low, close, volume
    Optional:
      trade_count, extra_fields (dict)
    """
    data = candle.copy() if candle else kwargs

    _require_keys(
        data,
        [
            "exchange", "base_currency", "quote_currency", "timeframe",
            "open_time", "close_time", "open", "high", "low", "close", "volume",
        ],
        "publish_candle",
    )

    exchange = str(data["exchange"])
    base_currency = str(data["base_currency"])
    quote_currency = str(data["quote_currency"])
    timeframe = str(data["timeframe"])

    subj_symbol = _symbol_for_subject(base_currency, quote_currency)
    payload_symbol = _symbol_for_payload(base_currency, quote_currency)

    open_time_iso = _ensure_utc_iso(data["open_time"])
    close_time_iso = _ensure_utc_iso(data["close_time"])

    body: Dict[str, Any] = {
        "exchange": exchange,
        "symbol": payload_symbol,
        "base_currency": base_currency.upper(),
        "quote_currency": quote_currency.upper(),
        "timeframe": timeframe,
        "open_time": open_time_iso,
        "close_time": close_time_iso,
        "open": float(data["open"]),
        "high": float(data["high"]),
        "low": float(data["low"]),
        "close": float(data["close"]),
        "volume": float(data["volume"]),
        # Publisher insert timestamp (UTC ISO)
        "insert_ts": _ensure_utc_iso(datetime.now(timezone.utc)),
    }
    if "trade_count" in data and data["trade_count"] is not None:
        body["trade_count"] = int(data["trade_count"])

    # allow passthrough of any extras
    extra_fields = data.get("extra_fields")
    if isinstance(extra_fields, dict):
        body.update(extra_fields)

# TODO: insert into database (step 2)
    logger.info(json.dumps({"EventType": "Candle", **{k: str(v) for k, v in body.items()}}))
    return body
    

    
    


# --------- tick publisher ----------------------------------------------------

async def publish_tick(
    tick: Optional[Dict[str, Any]] = None,
    **kwargs,
):
    """
    Stub for tick data — NATS removed. DB insert will go here (step 2).

    Usage:
      await publish_tick(tick=tick_dict)

    Required:
      exchange, base_currency, quote_currency, tick_time, price, quantity
    Optional:
      side ("buy"|"sell"), extra_fields (dict)
    """
    data = tick.copy() if tick else kwargs

    _require_keys(
        data,
        ["exchange", "tick_time", "symbol", "base_currency", "quote_currency", "bid", "ask", "last_price", "high", "low", "volume"],
        "publish_candle",
    )
    exchange = str(data["exchange"])
    base_currency = str(data["base_currency"])
    quote_currency = str(data["quote_currency"])

    subj_symbol = _symbol_for_subject(base_currency, quote_currency)
    payload_symbol = _symbol_for_payload(base_currency, quote_currency)

    event_time_iso = _ensure_utc_iso(data["tick_time"])

    body: Dict[str, Any] = {
        "exchange": exchange,
        "symbol": payload_symbol,
        "tick_time": event_time_iso,
        "base_currency": base_currency.upper(),
        "quote_currency": quote_currency.upper(),
        "ask": float(data["ask"]),
        "bid": float(data["bid"]),
        "last_price": float(data["last_price"]),
        "high": float(data["high"]),
        "low": float(data["low"]),
        "volume": float(data["volume"]),
        "insert_ts": _ensure_utc_iso(datetime.now(timezone.utc)),
    }
    if "side" in data and data["side"]:
        body["side"] = str(data["side"]).lower()

    extra_fields = data.get("extra_fields")
    if isinstance(extra_fields, dict):
        body.update(extra_fields)

    # TODO: insert into database (step 2)
    logger.info(json.dumps({"EventType": "Tick", **{k: str(v) for k, v in body.items()}}))
    return body

