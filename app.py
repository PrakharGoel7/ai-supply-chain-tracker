from flask import Flask, render_template, request
import yfinance as yf
import time
import threading
import math
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

app = Flask(__name__)

TICKERS = [
    "MU", "000660.KS", "005930.KS",
    "GOOGL", "NVDA", "AMD", "AMZN", "MSFT",
    "INTC", "DELL", "HPE",
    "AVGO", "MRVL", "CSCO", "ANET",
    "PSTG", "NTAP", "ORCL",
    "EQIX", "DLR", "META",
    "FSLR", "GEV", "NEE", "ENPH",
    "XOM", "CVX", "TSLA",
    "VRT", "SU.PA", "ABBN.SW", "ETN", "SIE.DE"
]

_cache = {}
_cache_time = 0
_detail_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL = 300
DETAIL_TTL = 3600

PERIOD_MAP = {
    "1D": ("1d",  "15m"),
    "5D": ("5d",  "1h"),
    "1M": ("1mo", "1d"),
    "3M": ("3mo", "1d"),
    "1Y": ("1y",  "1d"),
}


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

def fetch_ticker(ticker):
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="5d")
        info = t.info

        current_price = (
            clean(info.get("currentPrice"))
            or clean(info.get("regularMarketPrice"))
            or (clean(hist["Close"].iloc[-1]) if len(hist) > 0 else None)
        )
        prev_close = (
            clean(info.get("previousClose"))
            or clean(info.get("regularMarketPreviousClose"))
            or (clean(hist["Close"].iloc[-2]) if len(hist) >= 2 else None)
        )
        pe_ratio = clean(info.get("trailingPE"))

        daily_change = None
        if current_price and prev_close and prev_close != 0:
            daily_change = round((current_price - prev_close) / prev_close * 100, 2)

        weekly_change = None
        if len(hist) >= 2:
            wo = clean(hist["Close"].iloc[0])
            wc = clean(hist["Close"].iloc[-1])
            if wo and wc and wo != 0:
                weekly_change = round((wc - wo) / wo * 100, 2)

        return ticker, {
            "price": round(current_price, 2) if current_price is not None else None,
            "pe_ratio": round(pe_ratio, 2) if pe_ratio is not None else None,
            "daily_change": daily_change,
            "weekly_change": weekly_change,
        }
    except Exception as e:
        return ticker, {"price": None, "pe_ratio": None, "daily_change": None, "weekly_change": None, "error": str(e)}


@app.route("/api/stocks")
def get_stocks():
    global _cache, _cache_time
    now = time.time()
    with _cache_lock:
        if _cache and now - _cache_time < CACHE_TTL:
            return safe_json({"stocks": _cache, "last_updated": _cache_time, "cached": True})

    results = {}
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = {executor.submit(fetch_ticker, t): t for t in TICKERS}
        for future in as_completed(futures):
            ticker, data = future.result()
            results[ticker] = data

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
        info = t.info
        articles = parse_news(t.news)

        metrics = {
            "shortName": info.get("shortName") or info.get("longName", ticker),
            "sector": info.get("sector", ""),
            "industry": info.get("industry", ""),
            "currency": info.get("currency", "USD"),
            "marketCap": info.get("marketCap"),
            "trailingPE": clean(info.get("trailingPE")),
            "forwardPE": clean(info.get("forwardPE")),
            "trailingEPS": clean(info.get("trailingEPS")),
            "beta": clean(info.get("beta")),
            "dividendYield": clean(info.get("dividendYield")),
            "fiftyTwoWeekHigh": clean(info.get("fiftyTwoWeekHigh")),
            "fiftyTwoWeekLow": clean(info.get("fiftyTwoWeekLow")),
            "averageVolume": info.get("averageVolume"),
            "volume": info.get("regularMarketVolume") or info.get("volume"),
            "currentPrice": clean(info.get("currentPrice") or info.get("regularMarketPrice")),
            "previousClose": clean(info.get("previousClose") or info.get("regularMarketPreviousClose")),
        }

        result = {"metrics": metrics, "news": articles}
        with _cache_lock:
            _detail_cache[ticker] = {"data": result, "time": time.time()}

        return safe_json(result)
    except Exception as e:
        return safe_json({"metrics": {}, "news": [], "error": str(e)})


# ── serve frontend ────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    debug = os.environ.get("FLASK_ENV") != "production"
    print(f"Starting AI Supply Chain Stock Tracker at http://localhost:{port}")
    app.run(debug=debug, port=port, host="0.0.0.0")
