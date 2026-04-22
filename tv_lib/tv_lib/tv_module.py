"""
TradingView WebSocket Client
Production-grade implementation with historical data + real-time streaming.
"""

from __future__ import annotations

import json
import logging
import random
import re
import string
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, List, Optional

import pandas as pd
import websocket

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("TradingViewWS")

# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────
WS_URL  = "wss://data.tradingview.com/socket.io/websocket"
HEADERS = {
    "Origin":     "https://www.tradingview.com",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

# ─────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────
class TVError(Exception):
    """Base exception for TradingView client."""

class TVConnectionError(TVError):
    """Raised when WebSocket connection fails."""

class TVParseError(TVError):
    """Raised when server data cannot be parsed."""

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
@dataclass
class TVConfig:
    """All tunable parameters in one place."""
    ws_url:              str            = WS_URL
    connect_timeout:     int            = 10       # seconds
    recv_timeout:        int            = 15       # seconds per recv call
    max_reconnect:       int            = 5
    reconnect_base_delay: float         = 2.0      # exponential backoff base
    auth_token:          str            = "unauthorized_user_token"
    headers:             dict           = field(default_factory=lambda: dict(HEADERS))

# ─────────────────────────────────────────────
# Protocol helpers  (pure functions, no state)
# ─────────────────────────────────────────────
def _encode(msg: dict) -> str:
    """Wrap a dict in TradingView wire format: ~m~{len}~m~{json}"""
    body = json.dumps(msg, separators=(",", ":"))
    return f"~m~{len(body)}~m~{body}"

def _decode(raw: str) -> List[dict]:
    """Extract all JSON objects from a TradingView wire-format string."""
    results = []
    for part in raw.split("~m~"):
        part = part.strip()
        if not part or part.isdigit():
            continue
        try:
            results.append(json.loads(part))
        except json.JSONDecodeError:
            pass
    return results

def _heartbeat_reply(raw: str) -> Optional[str]:
    """
    If message contains a heartbeat token (~h~N), return the correct
    pong string. Otherwise return None.
    """
    m = re.search(r"~h~(\d+)", raw)
    if m:
        token = m.group(1)
        return f"~m~{len('~h~' + token)}~m~~h~{token}"
    return None

def _new_session() -> str:
    return "qs_" + uuid.uuid4().hex[:12]

# ─────────────────────────────────────────────
# OHLCV row parser
# ─────────────────────────────────────────────
_BAR_RE = re.compile(r'"s":\[(.*?)\]', re.DOTALL)

def _parse_ohlcv(raw: str) -> pd.DataFrame:
    """
    Extract OHLCV bars from raw WebSocket accumulation string.
    Returns a DataFrame indexed by datetime, or empty DataFrame on failure.
    """
    rows = []
    for match in _BAR_RE.finditer(raw):
        try:
            bars = json.loads(f"[{match.group(1)}]")
        except json.JSONDecodeError:
            continue
        for bar in bars:
            v = bar.get("v", [])
            if len(v) < 6:
                continue
            rows.append({
                "datetime": datetime.fromtimestamp(v[0]),
                "open":     float(v[1]),
                "high":     float(v[2]),
                "low":      float(v[3]),
                "close":    float(v[4]),
                "volume":   float(v[5]),
            })

    if not rows:
        return pd.DataFrame()

    df = (
        pd.DataFrame(rows)
        .drop_duplicates(subset="datetime")
        .sort_values("datetime")
        .set_index("datetime")
    )
    return df

# ─────────────────────────────────────────────
# Historical data client  (blocking, one-shot)
# ─────────────────────────────────────────────
class TVHistorical:
    """
    Fetch historical OHLCV candles from TradingView.

    Usage:
        tv = TVHistorical("BINANCE", "BTCUSDT")
        df = tv.get(interval="1", n_bars=200)
    """

    def __init__(
        self,
        market:   str = "BINANCE",
        ticker:   str = "BTCUSDT",
        config:   TVConfig = TVConfig(),
    ):
        self.symbol = f"{market.upper()}:{ticker.upper()}"
        self.cfg    = config

    def get(self, interval: str = "1", n_bars: int = 50) -> pd.DataFrame:
        """
        Fetch `n_bars` candles of `interval` length.

        interval: "1","3","5","15","30","60","120","240","D","W","M"
        """
        try:
            ws = websocket.create_connection(
                self.cfg.ws_url,
                timeout=self.cfg.connect_timeout,
                header=self.cfg.headers,
            )
        except Exception as e:
            raise TVConnectionError(f"Could not connect: {e}") from e

        chart_sess = _new_session()
        raw_buf    = ""

        try:
            # Consume server banner
            ws.recv()

            # Handshake
            ws.send(_encode({"m": "set_auth_token",       "p": [self.cfg.auth_token]}))
            ws.send(_encode({"m": "chart_create_session", "p": [chart_sess, ""]}))
            ws.send(_encode({"m": "resolve_symbol", "p": [
                chart_sess, "symbol_1",
                f'={{"symbol":"{self.symbol}","adjustment":"splits"}}',
            ]}))
            ws.send(_encode({"m": "create_series", "p": [
                chart_sess, "s1", "s1", "symbol_1", interval, n_bars,
            ]}))

            ws.settimeout(self.cfg.recv_timeout)

            while True:
                try:
                    msg = ws.recv()
                except websocket.WebSocketTimeoutException:
                    log.warning("recv timeout — using data collected so far")
                    break

                # Heartbeat
                pong = _heartbeat_reply(msg)
                if pong:
                    ws.send(pong)
                    continue

                raw_buf += msg

                if "series_completed" in msg:
                    break

        except Exception as e:
            log.error("Error during data fetch: %s", e)
        finally:
            ws.close()

        df = _parse_ohlcv(raw_buf)
        if df.empty:
            raise TVParseError(
                f"No OHLCV data parsed for {self.symbol}. "
                "Check symbol/market or try again."
            )
        return df

# ─────────────────────────────────────────────
# Real-time streaming client
# ─────────────────────────────────────────────
OnQuote = Callable[[str, dict], None]   # (symbol, price_fields) -> None

class TVStreamer:
    """
    Real-time quote streaming with auto-reconnect.

    Usage:
        def on_tick(symbol, data):
            print(symbol, data.get("lp"))   # last price

        stream = TVStreamer(["BINANCE:BTCUSDT", "OANDA:EURUSD"], on_tick)
        stream.start()
        ...
        stream.stop()

    Context manager:
        with TVStreamer(symbols, on_tick) as s:
            time.sleep(60)
    """

    def __init__(
        self,
        symbols:   List[str],
        on_quote:  OnQuote,
        config:    TVConfig = TVConfig(),
    ):
        self.symbols    = [s.upper() for s in symbols]
        self.on_quote   = on_quote
        self.cfg        = config

        self._lock              = threading.Lock()
        self._session: str      = ""
        self._connected: bool   = False
        self._stopping: bool    = False
        self._reconnect_count   = 0
        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread]   = None

    # ── public API ────────────────────────────

    def start(self) -> None:
        """Start streaming in a background thread."""
        self._stopping = False
        self._connect()

    def stop(self) -> None:
        """Gracefully stop streaming."""
        self._stopping = True
        if self._ws:
            self._ws.close()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        log.info("Streamer stopped.")

    def subscribe(self, symbol: str) -> None:
        """Subscribe to an additional symbol at runtime."""
        symbol = symbol.upper()
        with self._lock:
            if symbol not in self.symbols:
                self.symbols.append(symbol)
        if self._connected and self._session:
            self._send({"m": "quote_add_symbols", "p": [self._session, symbol]})

    def unsubscribe(self, symbol: str) -> None:
        """Unsubscribe from a symbol at runtime."""
        symbol = symbol.upper()
        with self._lock:
            if symbol in self.symbols:
                self.symbols.remove(symbol)
        if self._connected and self._session:
            self._send({"m": "quote_remove_symbols", "p": [self._session, symbol]})

    def is_alive(self) -> bool:
        return (
            self._connected
            and self._ws is not None
            and self._ws.sock is not None
            and self._ws.sock.connected
        )

    # ── context manager ───────────────────────

    def __enter__(self) -> TVStreamer:
        self.start()
        return self

    def __exit__(self, *_) -> None:
        self.stop()

    # ── internals ─────────────────────────────

    def _send(self, msg: dict) -> bool:
        if not self._connected or not self._ws:
            return False
        try:
            self._ws.send(_encode(msg))
            return True
        except Exception as e:
            log.warning("Send failed: %s", e)
            return False

    def _connect(self) -> None:
        self._ws = websocket.WebSocketApp(
            self.cfg.ws_url,
            header=self.cfg.headers,
            on_open=    self._on_open,
            on_message= self._on_message,
            on_error=   self._on_error,
            on_close=   self._on_close,
        )
        self._thread = threading.Thread(
            target=self._ws.run_forever,
            kwargs={"ping_interval": 0},   # we handle heartbeat manually
            daemon=True,
        )
        self._thread.start()

    def _on_open(self, ws) -> None:
        log.info("Connected to TradingView")
        with self._lock:
            self._connected     = True
            self._reconnect_count = 0
            self._session       = _new_session()
            session             = self._session
            symbols             = list(self.symbols)

        ws.send(_encode({"m": "set_auth_token",       "p": [self.cfg.auth_token]}))
        ws.send(_encode({"m": "quote_create_session", "p": [session]}))
        ws.send(_encode({"m": "set_data_quality",     "p": ["low"]}))

        for sym in symbols:
            ws.send(_encode({"m": "quote_add_symbols", "p": [session, sym]}))
            log.info("Subscribed: %s", sym)

    def _on_message(self, ws, raw: str) -> None:
        # Heartbeat
        pong = _heartbeat_reply(raw)
        if pong:
            ws.send(pong)
            return

        for msg in _decode(raw):
            if not isinstance(msg, dict):
                continue

            m_type = msg.get("m")

            if m_type == "qsd":
                params = msg.get("p", [])
                if len(params) < 2:
                    continue
                symbol_data = params[1]
                if symbol_data.get("s") == "ok":
                    symbol = symbol_data.get("n", "UNKNOWN")
                    values = symbol_data.get("v", {})
                    if values:
                        try:
                            self.on_quote(symbol, values)
                        except Exception as e:
                            log.error("on_quote callback error: %s", e)

            elif m_type == "error":
                err = msg.get("p", ["unknown"])[0]
                log.error("Server error: %s", err)

    def _on_error(self, ws, error) -> None:
        log.error("WebSocket error: %s", error)

    def _on_close(self, ws, code, msg) -> None:
        with self._lock:
            self._connected = False
            self._session   = ""

        log.info("Connection closed (%s: %s)", code, msg)

        if not self._stopping:
            self._schedule_reconnect()

    def _schedule_reconnect(self) -> None:
        with self._lock:
            self._reconnect_count += 1
            attempt = self._reconnect_count

        if attempt > self.cfg.max_reconnect:
            log.error("Max reconnect attempts reached. Giving up.")
            return

        delay = min(self.cfg.reconnect_base_delay ** attempt, 60.0)
        log.info("Reconnecting in %.0fs (attempt %d/%d)...", delay, attempt, self.cfg.max_reconnect)
        t = threading.Timer(delay, self._connect)
        t.daemon = True
        t.start()


# ─────────────────────────────────────────────
# Example usage
# ─────────────────────────────────────────────
if __name__ == "__main__":

    # ── Example 1: Historical OHLCV ──────────
    print("=" * 50)
    print("HISTORICAL DATA")
    print("=" * 50)

    tv = TVHistorical("BINANCE", "BTCUSDT")
    try:
        df = tv.get(interval="1", n_bars=10)
        print(df.tail())
    except TVError as e:
        print(f"Error: {e}")

    # ── Example 2: Real-time streaming ───────
    print("\n" + "=" * 50)
    print("REAL-TIME STREAM (30 seconds)")
    print("=" * 50)

    def on_tick(symbol: str, data: dict) -> None:
        price  = data.get("lp", "N/A")
        change = data.get("chp", "N/A")
        ts     = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] {symbol:25s}  price={price}  chg={change}%")

    symbols = ["BINANCE:BTCUSDT", "BINANCE:ETHUSDT"]

    with TVStreamer(symbols, on_tick) as stream:
        time.sleep(30)
