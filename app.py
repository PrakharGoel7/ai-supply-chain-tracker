from flask import Flask, render_template, request
import yfinance as yf
import pandas as pd
import requests as http_requests
import time
import threading
import math
import json
import calendar
import os
import sqlite3
import boto3
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)

TICKERS = [
    # AI Chips
    "NVDA", "AMD", "INTC", "AVGO", "MRVL", "ARM",
    # Memory
    "MU", "000660.KS", "005930.KS",
    # Chip Manufacturing
    "TSM", "ASML", "AMAT", "LRCX",
    # Servers & Hardware
    "SMCI", "DELL", "HPE",
    # Networking
    "CSCO", "ANET",
    # Storage
    "PSTG", "NTAP",
    # Cloud Platforms
    "AMZN", "MSFT", "GOOGL", "META", "ORCL",
    # Data Center REITs
    "EQIX", "DLR",
    # Power & Cooling
    "VRT", "ETN", "GEV", "SU.PA", "ABBN.SW", "SIE.DE",
    # Utilities
    "CEG", "VST", "D", "SO", "AEP",
    # Energy
    "FSLR", "ENPH", "XOM", "CVX", "TSLA",
]

_cache = {}
_cache_time = 0
_detail_cache = {}
_monthly_cache = {}
_trends_cache = {}
_gpu_cache = {}
_eia_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL = 1800   # 30 min — reduces Yahoo Finance request frequency on cloud IPs
DETAIL_TTL = 3600

# ── EIA grid region config ────────────────────────────────────────────────────
EIA_REGIONS = {
    "PJM": "PJM (Virginia / Mid-Atlantic)",
    "CAL": "CAISO (California)",
    "TEX": "ERCOT (Texas)",
    "MISO": "MISO (Midwest)",
}

# ── GPU instance definitions ──────────────────────────────────────────────────
GPU_INSTANCES = {
    "p3.2xlarge":    {"gpu": "V100 16GB",  "count": 1},
    "p3.8xlarge":    {"gpu": "V100 16GB",  "count": 4},
    "p3.16xlarge":   {"gpu": "V100 16GB",  "count": 8},
    "p4d.24xlarge":  {"gpu": "A100 40GB",  "count": 8},
    "p4de.24xlarge": {"gpu": "A100 80GB",  "count": 8},
    "p5.48xlarge":   {"gpu": "H100 80GB",  "count": 8},
    "g5.xlarge":     {"gpu": "A10G 24GB",  "count": 1},
    "g5.12xlarge":   {"gpu": "A10G 24GB",  "count": 4},
}

CATEGORY_TICKERS = {
    "AI Chips":           ["NVDA", "AMD", "INTC", "AVGO", "MRVL", "ARM"],
    "Memory":             ["MU", "000660.KS", "005930.KS"],
    "Chip Manufacturing": ["TSM", "ASML", "AMAT", "LRCX"],
    "Servers & Hardware": ["SMCI", "DELL", "HPE"],
    "Networking":         ["CSCO", "ANET", "AVGO", "MRVL"],
    "Storage":            ["PSTG", "NTAP"],
    "Cloud Platforms":    ["AMZN", "MSFT", "GOOGL", "META", "ORCL"],
    "Data Center REITs":  ["EQIX", "DLR"],
    "Power & Cooling":    ["VRT", "ETN", "GEV", "SU.PA", "ABBN.SW", "SIE.DE"],
    "Utilities":          ["CEG", "VST", "NEE", "D", "SO", "AEP"],
    "Energy":             ["FSLR", "ENPH", "XOM", "CVX", "TSLA"],
}

PERIOD_MAP = {
    "1D": ("1d",  "15m"),
    "5D": ("5d",  "1h"),
    "1M": ("1mo", "1d"),
    "3M": ("3mo", "1d"),
    "1Y": ("1y",  "1d"),
}

# ── GPU price database ────────────────────────────────────────────────────────
# Production: uses Turso cloud SQLite over its HTTP pipeline API (no extra deps).
# Local dev:  falls back to a plain sqlite3 file.
_TURSO_URL   = os.environ.get("TURSO_DB_URL", "").strip()
_TURSO_TOKEN = os.environ.get("TURSO_AUTH_TOKEN", "").strip()
_LOCAL_DB    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gpu_prices.db")


def _decode_turso(v):
    t = v.get("type")
    val = v.get("value")
    if t == "null" or val is None:
        return None
    if t == "integer":
        return int(val)
    if t == "float":
        return float(val)
    return val


class _TursoCursor:
    def __init__(self, result):
        self._rows = []
        if result and "rows" in result:
            for row in result["rows"]:
                self._rows.append(tuple(_decode_turso(v) for v in row))

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class _TursoDB:
    """Minimal sqlite3-compatible wrapper over the Turso HTTP pipeline API."""
    _CHUNK = 200

    def __init__(self, url, token):
        self._url = url.replace("libsql://", "https://") + "/v2/pipeline"
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def _pipeline(self, stmts):
        body = {
            "requests": [{"type": "execute", "stmt": s} for s in stmts]
                        + [{"type": "close"}]
        }
        r = http_requests.post(self._url, headers=self._headers, json=body, timeout=20)
        r.raise_for_status()
        return r.json()["results"]

    @staticmethod
    def _args(params):
        out = []
        for p in params:
            if p is None:
                out.append({"type": "null"})
            elif isinstance(p, float):
                out.append({"type": "float", "value": p})
            elif isinstance(p, int):
                out.append({"type": "integer", "value": str(p)})
            else:
                out.append({"type": "text", "value": str(p)})
        return out

    def execute(self, sql, params=()):
        stmt = {"sql": sql}
        if params:
            stmt["args"] = self._args(params)
        results = self._pipeline([stmt])
        if results[0]["type"] != "ok":
            raise Exception(f"Turso error: {results[0]}")
        return _TursoCursor(results[0]["response"]["result"])

    def executemany(self, sql, params_list):
        stmts = [{"sql": sql, "args": self._args(p)} for p in params_list]
        for i in range(0, len(stmts), self._CHUNK):
            self._pipeline(stmts[i:i + self._CHUNK])

    def commit(self):
        pass  # Turso HTTP API auto-commits each statement

    def close(self):
        pass


def _db_connect():
    if _TURSO_URL and _TURSO_TOKEN:
        return _TursoDB(_TURSO_URL, _TURSO_TOKEN)
    return sqlite3.connect(_LOCAL_DB)


# ── helpers ──────────────────────────────────────────────────────────────────

def clean(v):
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None


def sanitize(obj):
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [sanitize(v) for v in obj]
    return obj


def safe_json(data):
    raw = json.dumps(sanitize(data))
    return app.response_class(response=raw, mimetype="application/json")


def parse_news(news_raw):
    articles = []
    for n in (news_raw or [])[:10]:
        try:
            if "content" in n and isinstance(n["content"], dict):
                c = n["content"]
                title = c.get("title", "")
                pub = c.get("provider", {})
                publisher = pub.get("displayName", "") if isinstance(pub, dict) else ""
                lu = c.get("canonicalUrl", {})
                link = lu.get("url", "") if isinstance(lu, dict) else ""
                raw_date = c.get("pubDate", "")
                try:
                    t = int(datetime.fromisoformat(raw_date.replace("Z", "+00:00")).timestamp())
                except Exception:
                    t = 0
            else:
                title = n.get("title", "")
                publisher = n.get("publisher", "")
                link = n.get("link", "")
                t = int(n.get("providerPublishTime", 0) or 0)
            if title:
                articles.append({"title": title, "publisher": publisher, "link": link, "time": t})
        except Exception:
            continue
    return articles


# ── main price cache ──────────────────────────────────────────────────────────
# Uses yf.download() — a single batched request — instead of per-ticker .info
# calls, which hit Yahoo's quoteSummary API and get rate-limited on cloud IPs.

def _finnhub_pe(ticker):
    """Return trailing PE for one ticker from Finnhub, or None."""
    api_key = os.environ.get("FINNHUB_API_KEY", "").strip()
    if not api_key:
        return None
    symbol = ticker.split(".")[0] if "." in ticker else ticker
    try:
        m = http_requests.get(
            "https://finnhub.io/api/v1/stock/metric",
            params={"symbol": symbol, "metric": "all"},
            headers={"X-Finnhub-Token": api_key},
            timeout=6,
        ).json().get("metric", {})
        return clean(m.get("peTTM"))
    except Exception:
        return None


@app.route("/api/stocks")
def get_stocks():
    global _cache, _cache_time
    now = time.time()
    with _cache_lock:
        if _cache and now - _cache_time < CACHE_TTL:
            return safe_json({"stocks": _cache, "last_updated": _cache_time, "cached": True})

    try:
        raw = yf.download(
            " ".join(TICKERS),
            period="5d",
            interval="1d",
            progress=False,
            auto_adjust=True,
        )
        closes = raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw
        results = {}
        for ticker in TICKERS:
            try:
                series = closes[ticker].dropna() if ticker in closes.columns else pd.Series(dtype=float)
                current = clean(series.iloc[-1]) if len(series) >= 1 else None
                prev    = clean(series.iloc[-2]) if len(series) >= 2 else None
                first   = clean(series.iloc[0])  if len(series) >= 1 else None
                results[ticker] = {
                    "price":         round(current, 2) if current is not None else None,
                    "pe_ratio":      None,
                    "daily_change":  round((current - prev) / prev * 100, 2) if current and prev else None,
                    "weekly_change": round((current - first) / first * 100, 2) if current and first else None,
                }
            except Exception:
                results[ticker] = {"price": None, "pe_ratio": None, "daily_change": None, "weekly_change": None}
    except Exception as e:
        results = {t: {"price": None, "pe_ratio": None, "daily_change": None, "weekly_change": None, "error": str(e)}
                   for t in TICKERS}

    # Fetch PE ratios from Finnhub concurrently (one request per ticker)
    with ThreadPoolExecutor(max_workers=5) as ex:
        pe_futures = {ex.submit(_finnhub_pe, t): t for t in TICKERS}
        for future in as_completed(pe_futures):
            t = pe_futures[future]
            pe = future.result()
            if t in results:
                results[t]["pe_ratio"] = pe

    with _cache_lock:
        _cache = results
        _cache_time = time.time()
    return safe_json({"stocks": results, "last_updated": _cache_time, "cached": False})


# ── chart endpoint ────────────────────────────────────────────────────────────

@app.route("/api/stock/chart")
def get_chart():
    ticker = request.args.get("ticker", "")
    period_key = request.args.get("period", "1M")
    if not ticker:
        return safe_json({"candles": [], "error": "ticker required"})

    yf_period, yf_interval = PERIOD_MAP.get(period_key, ("1mo", "1d"))
    is_intraday = yf_interval not in ("1d", "1wk", "1mo")

    try:
        hist = yf.Ticker(ticker).history(period=yf_period, interval=yf_interval)
        candles = []
        seen = set()

        for dt, row in hist.iterrows():
            if is_intraday:
                try:
                    time_val = int(dt.timestamp())
                except Exception:
                    time_val = int(dt.value // 1_000_000_000)
            else:
                time_val = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)[:10]

            if time_val in seen:
                continue
            seen.add(time_val)

            c = clean(row.get("Close"))
            if c is None:
                continue
            o = clean(row.get("Open")) or c
            h = clean(row.get("High")) or c
            lo = clean(row.get("Low")) or c
            v = row.get("Volume", 0)
            vol = int(v) if v and not (isinstance(v, float) and math.isnan(v)) else 0

            candles.append({"time": time_val, "open": o, "high": h, "low": lo, "close": c, "volume": vol})

        return safe_json({"candles": candles, "intraday": is_intraday})
    except Exception as e:
        return safe_json({"candles": [], "intraday": is_intraday, "error": str(e)})


# ── details endpoint ──────────────────────────────────────────────────────────

def _finnhub_fundamentals(ticker):
    """
    Fetch PE, EPS, beta, dividend yield, market cap, and company profile from
    Finnhub (free tier, 60 req/min, no cloud-IP rate limiting).
    Returns a dict of fundamentals, or None if the key is missing or the ticker
    isn't covered (e.g. some international symbols).
    """
    api_key = os.environ.get("FINNHUB_API_KEY", "").strip()
    if not api_key:
        return None

    # Finnhub uses plain symbols without exchange suffixes for most tickers.
    # e.g. "SU.PA" → "SU", "005930.KS" → "005930"
    symbol = ticker.split(".")[0] if "." in ticker else ticker

    try:
        base = "https://finnhub.io/api/v1"
        headers = {"X-Finnhub-Token": api_key}

        profile = http_requests.get(
            f"{base}/stock/profile2", params={"symbol": symbol},
            headers=headers, timeout=6
        ).json()

        if not profile.get("name"):
            return None  # ticker not found in Finnhub

        m = http_requests.get(
            f"{base}/stock/metric", params={"symbol": symbol, "metric": "all"},
            headers=headers, timeout=6
        ).json().get("metric", {})

        market_cap_raw = profile.get("marketCapitalization")  # Finnhub returns millions
        return {
            "shortName":    profile.get("name", ticker),
            "sector":       profile.get("finnhubIndustry", ""),
            "industry":     profile.get("finnhubIndustry", ""),
            "currency":     profile.get("currency", "USD"),
            "marketCap":    market_cap_raw * 1_000_000 if market_cap_raw else None,
            "trailingPE":   clean(m.get("peTTM")),
            "forwardPE":    clean(m.get("peNormalizedAnnual")),
            "trailingEPS":  clean(m.get("epsTTM")),
            "beta":         clean(m.get("beta")),
            "dividendYield": clean(m.get("dividendYieldIndicatedAnnual")),
        }
    except Exception:
        return None


@app.route("/api/stock/details")
def get_details():
    ticker = request.args.get("ticker", "")
    if not ticker:
        return safe_json({"metrics": {}, "news": [], "error": "ticker required"})

    now = time.time()
    with _cache_lock:
        cached = _detail_cache.get(ticker)
        if cached and now - cached["time"] < DETAIL_TTL:
            return safe_json(cached["data"])

    try:
        t = yf.Ticker(ticker)
        fi = t.fast_info   # chart endpoint — never rate-limited
        articles = parse_news(t.news)

        # Price/volume/52wk range from fast_info (always available)
        price_metrics = {
            "fiftyTwoWeekHigh": clean(getattr(fi, "fifty_two_week_high", None)),
            "fiftyTwoWeekLow":  clean(getattr(fi, "fifty_two_week_low", None)),
            "averageVolume":    getattr(fi, "three_month_average_volume", None),
            "volume":           getattr(fi, "last_volume", None),
            "currentPrice":     clean(getattr(fi, "last_price", None)),
            "previousClose":    clean(getattr(fi, "previous_close", None)),
        }

        # Fundamentals: Finnhub first, fall back to yfinance .info
        fund = _finnhub_fundamentals(ticker)
        if fund is None:
            fund = {}
            try:
                info       = t.info
                fund = {
                    "shortName":    info.get("shortName") or info.get("longName", ticker),
                    "sector":       info.get("sector", ""),
                    "industry":     info.get("industry", ""),
                    "currency":     info.get("currency", getattr(fi, "currency", "USD") or "USD"),
                    "marketCap":    info.get("marketCap"),
                    "trailingPE":   clean(info.get("trailingPE")),
                    "forwardPE":    clean(info.get("forwardPE")),
                    "trailingEPS":  clean(info.get("trailingEPS")),
                    "beta":         clean(info.get("beta")),
                    "dividendYield": clean(info.get("dividendYield")),
                }
            except Exception:
                fund = {
                    "shortName": ticker, "sector": "", "industry": "",
                    "currency": getattr(fi, "currency", "USD") or "USD",
                    "marketCap": getattr(fi, "market_cap", None),
                    "trailingPE": None, "forwardPE": None, "trailingEPS": None,
                    "beta": None, "dividendYield": None,
                }

        metrics = {**fund, **price_metrics}
        result = {"metrics": metrics, "news": articles}
        with _cache_lock:
            _detail_cache[ticker] = {"data": result, "time": time.time()}

        return safe_json(result)
    except Exception as e:
        return safe_json({"metrics": {}, "news": [], "error": str(e)})


# ── category trends ───────────────────────────────────────────────────────────

@app.route("/api/category-trends")
def get_category_trends():
    period = request.args.get("period", "2y")

    now = time.time()
    with _cache_lock:
        cached = _trends_cache.get(period)
        if cached and now - cached["time"] < 3600:
            return safe_json(cached["data"])

    try:
        raw = yf.download(
            " ".join(TICKERS),
            period=period,
            interval="1mo",
            progress=False,
            auto_adjust=True,
        )
        closes = raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw

        # Index each ticker to its first available price → % change since period start
        first_prices = closes.bfill().iloc[0]
        cumulative = (closes.divide(first_prices) - 1) * 100

        result = {}
        for cat, cat_tickers in CATEGORY_TICKERS.items():
            valid = [t for t in cat_tickers if t in cumulative.columns]
            if not valid:
                continue
            avg = cumulative[valid].mean(axis=1)
            series = []
            for dt, val in avg.items():
                if not (isinstance(val, float) and math.isnan(val)):
                    series.append({
                        "time": f"{dt.year}-{dt.month:02d}-01",
                        "value": round(float(val), 2),
                    })
            if series:
                result[cat] = series

        data = {"categories": result, "period": period}
        with _cache_lock:
            _trends_cache[period] = {"data": data, "time": time.time()}
        return safe_json(data)
    except Exception as e:
        return safe_json({"categories": {}, "error": str(e)})


# ── monthly performance ───────────────────────────────────────────────────────

@app.route("/api/monthly")
def get_monthly():
    month = request.args.get("month", "")
    if not month or len(month) != 7:
        return safe_json({"error": "Use format YYYY-MM"})

    now = time.time()
    current_month = datetime.now().strftime("%Y-%m")
    cache_ttl = 300 if month >= current_month else 86400  # past months cached 24h

    with _cache_lock:
        cached = _monthly_cache.get(month)
        if cached and now - cached["time"] < cache_ttl:
            return safe_json(cached["data"])

    try:
        year, mon = int(month[:4]), int(month[5:7])
        start = f"{year}-{mon:02d}-01"
        last_day = calendar.monthrange(year, mon)[1]
        end = (datetime(year, mon, last_day) + timedelta(days=1)).strftime("%Y-%m-%d")

        raw = yf.download(
            " ".join(TICKERS),
            start=start,
            end=end,
            progress=False,
            auto_adjust=True,
        )

        closes = raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw

        results = []
        for ticker in TICKERS:
            try:
                if ticker not in closes.columns:
                    continue
                series = closes[ticker].dropna()
                if len(series) < 2:
                    continue
                s = float(series.iloc[0])
                e = float(series.iloc[-1])
                if s > 0 and not math.isnan(s) and not math.isnan(e):
                    results.append({
                        "ticker": ticker,
                        "change": round((e - s) / s * 100, 2),
                        "start_price": round(s, 2),
                        "end_price": round(e, 2),
                    })
            except Exception:
                continue

        results.sort(key=lambda x: x["change"], reverse=True)

        result = {
            "month": month,
            "top5": results[:5],
            "bottom5": list(reversed(results[-5:])) if len(results) >= 5 else list(reversed(results)),
            "total": len(results),
        }
        with _cache_lock:
            _monthly_cache[month] = {"data": result, "time": time.time()}

        return safe_json(result)
    except Exception as e:
        return safe_json({"error": str(e), "month": month})


# ── GPU spot price history ────────────────────────────────────────────────────

def _db_init():
    """Create the GPU price table if it doesn't exist."""
    conn = _db_connect()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpu_daily_prices (
            date       TEXT NOT NULL,
            gpu_model  TEXT NOT NULL,
            median_price REAL NOT NULL,
            PRIMARY KEY (date, gpu_model)
        )
    """)
    conn.commit()
    conn.close()


def _fetch_and_store_gpu_prices(days=2):
    """Pull AWS spot price history for `days` days and upsert daily medians."""
    try:
        ec2 = boto3.client(
            "ec2",
            region_name="us-east-1",
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        )
        instance_types = list(GPU_INSTANCES.keys())
        all_prices = []
        paginator = ec2.get_paginator("describe_spot_price_history")
        for page in paginator.paginate(
            InstanceTypes=instance_types,
            ProductDescriptions=["Linux/UNIX"],
            StartTime=datetime.utcnow() - timedelta(days=days),
            EndTime=datetime.utcnow(),
        ):
            all_prices.extend(page["SpotPriceHistory"])

        daily: dict = defaultdict(lambda: defaultdict(list))
        for entry in all_prices:
            inst = entry["InstanceType"]
            if inst not in GPU_INSTANCES:
                continue
            info = GPU_INSTANCES[inst]
            gpu_model = info["gpu"]
            price_per_gpu = float(entry["SpotPrice"]) / info["count"]
            date = entry["Timestamp"].strftime("%Y-%m-%d")
            daily[gpu_model][date].append(price_per_gpu)

        rows = []
        for gpu_model, dates in daily.items():
            for date, vals in dates.items():
                vals.sort()
                median = vals[len(vals) // 2]
                rows.append((date, gpu_model, round(median, 4)))

        conn = _db_connect()
        conn.executemany(
            "INSERT OR REPLACE INTO gpu_daily_prices (date, gpu_model, median_price) VALUES (?, ?, ?)",
            rows,
        )
        conn.commit()
        conn.close()
        return len(rows)
    except Exception as e:
        print(f"[gpu-poller] Error fetching GPU prices: {e}")
        return 0


def _gpu_poller():
    """Background thread: 90-day seed on first run, then hourly incremental updates."""
    conn = _db_connect()
    count = conn.execute("SELECT COUNT(*) FROM gpu_daily_prices").fetchone()[0]
    conn.close()

    if count == 0:
        print("[gpu-poller] DB empty — seeding with 90-day history...")
        n = _fetch_and_store_gpu_prices(days=90)
        print(f"[gpu-poller] Seeded {n} daily price records")
    else:
        print(f"[gpu-poller] DB has {count} records — skipping initial seed")

    while True:
        time.sleep(3600)
        n = _fetch_and_store_gpu_prices(days=2)
        print(f"[gpu-poller] Hourly update: stored {n} records")
        with _cache_lock:
            _gpu_cache.clear()


@app.route("/api/gpu-prices")
def get_gpu_prices():
    now_t = time.time()
    with _cache_lock:
        cached = _gpu_cache.get("prices")
        if cached and now_t - cached["time"] < 3600:
            return safe_json(cached["data"])

    try:
        conn = _db_connect()
        rows = conn.execute(
            "SELECT date, gpu_model, median_price FROM gpu_daily_prices ORDER BY date"
        ).fetchall()
        conn.close()

        result: dict = defaultdict(list)
        for date, gpu_model, median_price in rows:
            result[gpu_model].append({"time": date, "value": median_price})

        data = {"gpus": dict(result), "updated": time.time(), "region": "us-east-1"}
        with _cache_lock:
            _gpu_cache["prices"] = {"data": data, "time": time.time()}
        return safe_json(data)
    except Exception as e:
        return safe_json({"gpus": {}, "error": str(e)})


# ── EIA electricity demand ────────────────────────────────────────────────────

def _eia_fetch_region(api_key, respondent, days):
    """Return list of raw hourly records from EIA API for one grid region."""
    end_dt   = datetime.utcnow()
    start_dt = end_dt - timedelta(days=days)
    url = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
    params = {
        "api_key":             api_key,
        "data[0]":             "value",
        "facets[type][]":      "D",
        "facets[respondent][]": respondent,
        "frequency":           "hourly",
        "start":               start_dt.strftime("%Y-%m-%dT%H"),
        "end":                 end_dt.strftime("%Y-%m-%dT%H"),
        "sort[0][column]":     "period",
        "sort[0][direction]":  "asc",
        "length":              5000,
        "offset":              0,
    }
    records = []
    while True:
        r = http_requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        body    = r.json()
        page    = body.get("response", {}).get("data", [])
        total   = body.get("response", {}).get("total", 0)
        records.extend(page)
        if len(records) >= int(total):
            break
        params["offset"] = len(records)
    return records


def _eia_to_daily(records):
    """Aggregate hourly MW demand records to daily averages."""
    by_date = defaultdict(list)
    for r in records:
        period = r.get("period", "")   # "2024-01-15T10"
        date   = period[:10]           # "2024-01-15"
        val    = r.get("value")
        if val is not None:
            try:
                by_date[date].append(float(val))
            except (TypeError, ValueError):
                pass
    series = []
    for date in sorted(by_date):
        vals = by_date[date]
        series.append({"time": date, "value": round(sum(vals) / len(vals), 1)})
    return series


def _rolling_avg(series, window=7):
    out = []
    for i, pt in enumerate(series):
        chunk = series[max(0, i - window + 1): i + 1]
        avg   = sum(p["value"] for p in chunk) / len(chunk)
        out.append({"time": pt["time"], "value": round(avg, 1)})
    return out


@app.route("/api/eia-demand")
def get_eia_demand():
    api_key = os.environ.get("EIA_API_KEY", "").strip()
    if not api_key:
        return safe_json({"regions": {}, "error": "EIA_API_KEY not configured"})

    now_t = time.time()
    with _cache_lock:
        cached = _eia_cache.get("demand")
        if cached and now_t - cached["time"] < 21600:   # 6-hour cache
            return safe_json(cached["data"])

    try:
        result = {}
        for respondent, label in EIA_REGIONS.items():
            records = _eia_fetch_region(api_key, respondent, days=365)
            daily   = _eia_to_daily(records)
            smooth  = _rolling_avg(daily, window=7)

            # YoY baseline: same calendar week last year → % deviation
            yoy_signals = []
            if len(daily) >= 370:
                for i in range(364, len(daily)):
                    curr = daily[i]["value"]
                    prev = daily[i - 364]["value"]
                    if prev:
                        yoy_signals.append(round((curr - prev) / prev * 100, 2))

            # Trend: slope of last 30 days (MWh/day change)
            trend_pct = None
            if len(smooth) >= 30:
                recent  = smooth[-1]["value"]
                month_ago = smooth[-30]["value"]
                if month_ago:
                    trend_pct = round((recent - month_ago) / month_ago * 100, 2)

            result[respondent] = {
                "label":      label,
                "smooth":     smooth,
                "trend_pct":  trend_pct,
                "latest_mw":  smooth[-1]["value"] if smooth else None,
                "yoy_avg":    round(sum(yoy_signals) / len(yoy_signals), 2) if yoy_signals else None,
            }

        data = {"regions": result, "updated": time.time()}
        with _cache_lock:
            _eia_cache["demand"] = {"data": data, "time": time.time()}
        return safe_json(data)
    except Exception as e:
        return safe_json({"regions": {}, "error": str(e)})


# ── serve frontend ────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


# ── startup: init DB and launch background GPU poller ─────────────────────────
_db_init()
threading.Thread(target=_gpu_poller, daemon=True, name="gpu-poller").start()


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    debug = os.environ.get("FLASK_ENV") != "production"
    print(f"Starting AI Supply Chain Stock Tracker at http://localhost:{port}")
    app.run(debug=debug, port=port, host="0.0.0.0")
