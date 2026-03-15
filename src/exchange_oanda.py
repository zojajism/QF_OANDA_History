import asyncio
import json
import logging
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional
import datetime as dt
import aiohttp
from telegram_notifier import notify_telegram, ChatType
from quantflow_publisher import publish_candle

logger = logging.getLogger(__name__)

# --------------------
# Tunables (defaults; can be overridden via env if you want)
# --------------------
# Base grace used when computing dynamic grace; actual grace = clamp(period_s * GRACE_RATIO, GRACE_MIN, GRACE_MAX)
GRACE_RATIO = 0.20      # 20% of the timeframe length
GRACE_MIN   = 6         # seconds
GRACE_MAX   = 20        # seconds
# Confirm window scales too (how far back we "double-check" before synthesizing)
CONFIRM_RATIO = 0.60    # 60% of the timeframe length
CONFIRM_MIN   = 20      # seconds
CONFIRM_MAX   = 90      # seconds


def _grace_for(period_s: int) -> int:
    g = int(period_s * GRACE_RATIO)
    return max(GRACE_MIN, min(g, GRACE_MAX))


def _confirm_window_for(period_s: int) -> int:
    c = int(period_s * CONFIRM_RATIO)
    return max(CONFIRM_MIN, min(c, CONFIRM_MAX))


# =========================
#   ENV / HOSTS
# =========================
class OandaEnv:
    PRACTICE = "practice"
    LIVE = "live"


def _stream_host(env):
    return "https://stream-fxpractice.oanda.com" if env == OandaEnv.PRACTICE else "https://stream-fxtrade.oanda.com"


def _api_host(env):
    return "https://api-fxpractice.oanda.com" if env == OandaEnv.PRACTICE else "https://api-fxtrade.oanda.com"


# =========================
#   SMALL HELPERS
# =========================
async def _reconnect_backoff(delay):
    jitter = random.uniform(0, 0.5)
    await asyncio.sleep(delay + jitter)
    return min(delay * 2.0, 60.0)


def _parse_rfc3339(ts):
    # Example: "2025-01-01T12:34:56.123456789Z"
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def _display_split(symbol_str):
    parts = symbol_str.strip().upper().split("/")
    return (parts[0], parts[1]) if len(parts) == 2 else (symbol_str.strip().upper(), "")


def _tf_label_to_oanda(tf_label: str) -> str:
    s = tf_label.strip().lower()
    if s.endswith("m"):
        return f"M{int(s[:-1])}"
    if s.endswith("h"):
        return f"H{int(s[:-1])}"
    if s.endswith("d"):
        return "D"
    return "M1"


def _tf_label_to_seconds(tf_label: str) -> int:
    s = tf_label.strip().lower()
    if s.endswith("m"):
        return int(s[:-1]) * 60
    if s.endswith("h"):
        return int(s[:-1]) * 3600
    if s.endswith("d"):
        return 86400
    return 60


def _floor_to_period_utc(dt_utc, period_s):
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    seconds = int((dt_utc - epoch).total_seconds())
    floored = seconds - (seconds % period_s)
    return epoch + timedelta(seconds=floored)


# =========================
#   LIGHTWEIGHT FX CALENDAR  (all times UTC)
# =========================
def _is_fx_open(now_utc: dt.datetime) -> bool:
    """
    FX market hours in UTC:
      Opens  : Sunday    22:00 UTC
      Closes : Friday    22:00 UTC
    Saturday is always closed.
    """
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=dt.timezone.utc)
    else:
        now_utc = now_utc.astimezone(dt.timezone.utc)

    wd = now_utc.weekday()   # 0=Mon … 6=Sun
    t  = now_utc.time()

    if wd == 5:               # Saturday — always closed
        return False
    if wd == 6:               # Sunday — open from 22:00 UTC
        return t >= dt.time(22, 0)
    if wd in (0, 1, 2, 3):   # Mon–Thu — always open
        return True
    if wd == 4:               # Friday — closes at 22:00 UTC
        return t < dt.time(22, 0)
    return False


# =========================
#   HISTORICAL CANDLES FETCH
# =========================
async def get_oanda_candles_history(
    display_symbol: str,
    instrument: str,
    timeframe: str,
    price_mode: str,
    token: str,
    start_dt: datetime,
    end_dt: datetime,
    env=OandaEnv.LIVE,
    batch_size: int = 5000,
) -> list:
    """
    Fetch all completed candles for instrument/timeframe between start_dt and end_dt.
    Paginates automatically in chunks of batch_size (max 5000).
    Returns a list of candle dicts ready for DB insertion.
    """
    base, quote = _display_split(display_symbol)
    host = _api_host(env)
    granularity = _tf_label_to_oanda(timeframe)
    period_s = _tf_label_to_seconds(timeframe)
    pk = {"M": "mid", "B": "bid", "A": "ask"}[price_mode.upper()]
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{host}/v3/instruments/{instrument}/candles"

    results = []
    from_dt = start_dt

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        while from_dt < end_dt:
            # OANDA rejects requests that have both 'from'/'to' AND 'count'.
            # Paginate using 'from' + 'count' only; filter out candles beyond end_dt.
            params = {
                "granularity": granularity,
                "price": price_mode.upper(),
                "from": from_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "count": batch_size,
            }
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"OANDA candles HTTP {resp.status}: {await resp.text()}")
                data = await resp.json()

            candles = data.get("candles", [])
            if not candles:
                break

            last_time = None
            for c in candles:
                t_open = _parse_rfc3339(c["time"])
                t_close = t_open + timedelta(seconds=period_s)
                if t_close > end_dt:
                    break
                last_time = t_open
                if not c.get("complete"):
                    continue
                ohlc_raw = c.get(pk)
                if not ohlc_raw:
                    continue
                results.append({
                    "exchange": "OANDA",
                    "symbol": display_symbol,
                    "base_currency": base,
                    "quote_currency": quote,
                    "timeframe": timeframe,
                    "open_time": t_open,
                    "close_time": t_close,
                    "open": float(ohlc_raw["o"]),
                    "high": float(ohlc_raw["h"]),
                    "low": float(ohlc_raw["l"]),
                    "close": float(ohlc_raw["c"]),
                    "volume": float(c.get("volume", 0.0)),
                })

            if last_time is None:
                break
            next_from = last_time + timedelta(seconds=period_s)
            if len(candles) < batch_size or next_from >= end_dt:
                break
            from_dt = next_from

    logger.info(f"Fetched {len(results)} candles for {display_symbol} {timeframe} "
                f"[{start_dt.isoformat()} → {end_dt.isoformat()}]")
    return results


# =========================
#   TICK STREAM (unchanged)
# =========================
async def get_oanda_tick_stream(
    instrument,
    display_symbol,
    account_id,
    token,
    env=OandaEnv.LIVE,
    tick_queue: asyncio.Queue = None,
):
    base, quote = _display_split(display_symbol)
    headers = {"Authorization": f"Bearer {token}"}
    params = {"instruments": instrument}
    url = f"{_stream_host(env)}/v3/accounts/{account_id}/pricing/stream"

    delay = 1.0
    while True:
        try:
            timeout = aiohttp.ClientTimeout(sock_connect=20, sock_read=None)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=headers, params=params) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"OANDA stream HTTP {resp.status}: {await resp.text()}")
                    delay = 1.0
                    async for raw in resp.content:
                        if not raw:
                            continue
                        line = raw.decode().strip()
                        if not line:
                            continue
                        try:
                            msg = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if msg.get("type") != "PRICE":
                            continue
                        bids = msg.get("bids") or []
                        asks = msg.get("asks") or []
                        if not bids or not asks:
                            continue
                        bid = float(bids[0]["price"])
                        ask = float(asks[0]["price"])
                        mid = (bid + ask) / 2.0
                        t = _parse_rfc3339(msg["time"])
                        tick_data = {
                            "exchange": "OANDA",
                            "tick_time": t,
                            "symbol": display_symbol,
                            "base_currency": base,
                            "quote_currency": quote,
                            "bid": bid,
                            "ask": ask,
                            "last_price": mid,
                            "high": mid,
                            "low": mid,
                            "volume": 0.0,
                        }
                        await asyncio.sleep(0)
        except Exception as e:
            logger.warning(f"OANDA tick stream error: {e}")
            delay = await _reconnect_backoff(delay)


# =========================
#   CANDLES VIA REST (race-free publishing)
# =========================
async def _fetch_last_completed(
    session: aiohttp.ClientSession,
    host: str,
    instrument: str,
    granularity: str,
    price_mode: str,
    token: str,
) -> tuple[Optional[datetime], Optional[tuple[float, float, float, float]], float]:
    """
    Fetch most recent completed candle for instrument/granularity/price_mode.
    Returns (open_time_utc, (o,h,l,c), volume)
    """
    url = f"{host}/v3/instruments/{instrument}/candles"
    params = {"granularity": granularity, "price": price_mode, "count": 2}
    headers = {"Authorization": f"Bearer {token}"}
    async with session.get(url, headers=headers, params=params) as resp:
        if resp.status != 200:
            raise RuntimeError(f"OANDA candles HTTP {resp.status}: {await resp.text()}")
        data = await resp.json()
        candles = [c for c in data.get("candles", []) if c.get("complete")]
        if not candles:
            return None, None, 0.0
        last = candles[-1]
        t_open = _parse_rfc3339(last["time"])
        pk = {"M": "mid", "B": "bid", "A": "ask"}[price_mode]
        ohlc_raw = last.get(pk)
        if not ohlc_raw:
            return None, None, 0.0
        ohlc = (float(ohlc_raw["o"]), float(ohlc_raw["h"]), float(ohlc_raw["l"]), float(ohlc_raw["c"]))
        vol = float(last.get("volume", 0.0))
        return t_open, ohlc, vol


async def get_oanda_candles_rest(
    display_symbol: str,
    instrument: str,
    timeframes,
    price_modes,
    token: str,
    env=OandaEnv.LIVE,
    poll_interval_sec: int = 2,
):
    """
    Race-free policy:
      - Never publish a candle until it is fully closed + dynamic grace.
      - When considering a synthetic, *re-confirm* with REST first.
      - Exactly one candle published per (tf, price_mode, open_time) — no later replacements.
    """
    base, quote = _display_split(display_symbol)
    host = _api_host(env)
    tf_labels = [str(t).lower() for t in timeframes]
    price_modes = [m.upper() for m in price_modes]

    last_open_map: Dict[Tuple[str, str], datetime] = {}
    last_close_px_map: Dict[Tuple[str, str], float] = {}

    delay = 1.0
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            if not _is_fx_open(now_utc):
                await asyncio.sleep(10)
                continue

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
                for tf in tf_labels:
                    og = _tf_label_to_oanda(tf)
                    period_s = _tf_label_to_seconds(tf)
                    GRACE = _grace_for(period_s)
                    CONFIRM_WINDOW = _confirm_window_for(period_s)

                    for pm in price_modes:
                        # Fetch newest completed (if any)
                        real_last_open, real_last_ohlc, real_last_vol = await _fetch_last_completed(
                            session, host, instrument, og, pm, token
                        )

                        key = (tf, pm)
                        last_open = last_open_map.get(key)
                        last_close_px = last_close_px_map.get(key)

                        # Seed with the newest real candle if we don't have any yet
                        if last_open is None:
                            if real_last_open is None or real_last_ohlc is None:
                                continue
                            o, h, l, c = real_last_ohlc
                            await publish_candle(candle={
                                "exchange": "OANDA",
                                "symbol": display_symbol,
                                "base_currency": base,
                                "quote_currency": quote,
                                "timeframe": tf,
                                "open_time": real_last_open,
                                "open": o, "high": h, "low": l, "close": c,
                                "volume": real_last_vol,
                                "close_time": real_last_open + timedelta(seconds=period_s),
                                "price_mode": pm,
                            })

                            last_open_map[key] = real_last_open
                            last_close_px_map[key] = c
                            continue

                        # If we already have a newer real candle, publish it immediately (no grace for real bars)
                        if real_last_open and real_last_open > last_open and real_last_ohlc:
                            # Fill any fully-closed bars BEFORE that (respect grace)
                            expected = last_open + timedelta(seconds=period_s)
                            cutoff = now_utc - timedelta(seconds=GRACE)
                            while expected + timedelta(seconds=period_s) <= cutoff and expected < real_last_open:
                                px = last_close_px
                                open_time = expected
                                await publish_candle(candle={
                                    "exchange": "OANDA",
                                    "symbol": display_symbol,
                                    "base_currency": base,
                                    "quote_currency": quote,
                                    "timeframe": tf,
                                    "open_time": open_time,
                                    "open": px, "high": px, "low": px, "close": px,
                                    "volume": 0.0,
                                    "close_time": open_time + timedelta(seconds=period_s),
                                    "price_mode": pm,
                                })

                                last_open_map[key] = open_time
                                last_close_px_map[key] = px
                                expected += timedelta(seconds=period_s)

                            # Publish the real newest bar now
                            o, h, l, c = real_last_ohlc
                            await publish_candle(candle={
                                "exchange": "OANDA",
                                "symbol": display_symbol,
                                "base_currency": base,
                                "quote_currency": quote,
                                "timeframe": tf,
                                "open_time": real_last_open,
                                "open": o, "high": h, "low": l, "close": c,
                                "volume": real_last_vol,
                                "close_time": real_last_open + timedelta(seconds=period_s),
                                "price_mode": pm,
                            })

                            last_open_map[key] = real_last_open
                            last_close_px_map[key] = c
                            continue

                        # No newer real bar yet → consider fully-closed bars older than GRACE
                        expected = last_open + timedelta(seconds=period_s)
                        cutoff = now_utc - timedelta(seconds=GRACE)
                        while expected + timedelta(seconds=period_s) <= cutoff:
                            # If the bar just closed recently, double-check before synthesizing
                            recently_closed = (now_utc - (expected + timedelta(seconds=period_s))).total_seconds() <= CONFIRM_WINDOW
                            if recently_closed:
                                conf_open, conf_ohlc, conf_vol = await _fetch_last_completed(
                                    session, host, instrument, og, pm, token
                                )
                                if conf_open and conf_open >= expected and conf_ohlc:
                                    # Fill any older full gaps (if any) before conf_open
                                    fill_ptr = last_open + timedelta(seconds=period_s)
                                    while fill_ptr < conf_open and fill_ptr + timedelta(seconds=period_s) <= cutoff:
                                        px = last_close_px
                                        await publish_candle(candle={
                                            "exchange": "OANDA",
                                            "symbol": display_symbol,
                                            "base_currency": base,
                                            "quote_currency": quote,
                                            "timeframe": tf,
                                            "open_time": fill_ptr,
                                            "open": px, "high": px, "low": px, "close": px,
                                            "volume": 0.0,
                                            "close_time": fill_ptr + timedelta(seconds=period_s),
                                            "price_mode": pm,
                                        })

                                        last_open_map[key] = fill_ptr
                                        last_close_px_map[key] = px
                                        fill_ptr += timedelta(seconds=period_s)

                                    # Publish the confirmed real bar
                                    o, h, l, c = conf_ohlc
                                    await publish_candle(candle={
                                        "exchange": "OANDA",
                                        "symbol": display_symbol,
                                        "base_currency": base,
                                        "quote_currency": quote,
                                        "timeframe": tf,
                                        "open_time": conf_open,
                                        "open": o, "high": h, "low": l, "close": c,
                                        "volume": conf_vol,
                                        "close_time": conf_open + timedelta(seconds=period_s),
                                        "price_mode": pm,
                                    })

                                    last_open_map[key] = conf_open
                                    last_close_px_map[key] = c
                                    expected = conf_open + timedelta(seconds=period_s)
                                    continue

                            # Still no real bar → publish a single synthetic (flat) for this minute
                            px = last_close_px
                            await publish_candle(candle={
                                "exchange": "OANDA",
                                "symbol": display_symbol,
                                "base_currency": base,
                                "quote_currency": quote,
                                "timeframe": tf,
                                "open_time": expected,
                                "open": px, "high": px, "low": px, "close": px,
                                "volume": 0.0,
                                "close_time": expected + timedelta(seconds=period_s),
                                "price_mode": pm,
                            })

                            last_open_map[key] = expected
                            last_close_px_map[key] = px
                            expected += timedelta(seconds=period_s)

            await asyncio.sleep(poll_interval_sec)
            delay = 1.0

        except (aiohttp.ClientConnectionError, aiohttp.ServerDisconnectedError, asyncio.TimeoutError, OSError) as e:
            notify_telegram("❌ DataCollectorApp-Candle (OANDA REST)\n" + str(e), ChatType.ALERT)
            logger.warning(f"OANDA REST candles error ({display_symbol}): {e}; retrying in ~{delay:.1f}s")
            delay = await _reconnect_backoff(delay)
        except Exception as e:
            notify_telegram("❌ DataCollectorApp-Candle (OANDA REST)\n" + str(e), ChatType.ALERT)
            logger.exception(f"Unexpected OANDA REST candle error ({display_symbol}): {e}")
            delay = await _reconnect_backoff(delay)
