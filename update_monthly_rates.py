#!/usr/bin/env python3
"""
Фиксация среднемесячного курса за прошлый месяц.
Запускается 1-го числа каждого месяца — перезаписывает слот YYYY-MM-01 прошлого месяца.

Логика (аналогична backfill_rates.py но для одного месяца):
  - Запрашивает курсы на 4 даты прошлого месяца: 1-е, 8-е, 15-е, 22-е
  - Считает среднее по каждой валюте
  - Перезаписывает строку YYYY-MM-01 с source='monthly_avg'
"""

import sqlite3
import json
import ssl
import time
import logging
from datetime import date, timedelta, datetime, timezone
from urllib.request import urlopen
from pathlib import Path

log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "data" / "app.db"

FIAT_CODES = ["RUB", "VND", "CNY", "HKD", "THB", "MYR", "EUR"]

COINGECKO_IDS = {
    "BTC":  "bitcoin",
    "ETH":  "ethereum",
    "SOL":  "solana",
    "TRX":  "tron",
    "USDT": "tether",
}

MOEX_TICKERS = ["BELU", "IRAO", "MAGN", "MGNT", "NVTK", "OZON", "PLZL", "SBER", "SIBN", "X5", "YDEX"]

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


def fetch_json(url: str, retries: int = 3, pause: float = 2.0) -> dict:
    for attempt in range(retries):
        try:
            with urlopen(url, timeout=15, context=_SSL_CTX) as resp:
                return json.loads(resp.read())
        except Exception as e:
            if attempt < retries - 1:
                log.warning("fetch attempt %d failed: %s", attempt + 1, e)
                time.sleep(pause)
            else:
                raise


def prev_month(today: date | None = None) -> tuple[int, int]:
    d = today or date.today()
    m, y = d.month - 1, d.year
    if m == 0:
        m, y = 12, y - 1
    return y, m


def sample_dates(year: int, month: int) -> list[date]:
    """4 точки для усреднения: 1-е, 8-е, 15-е, 22-е."""
    return [date(year, month, day) for day in [1, 8, 15, 22]]


def fetch_fiat_on_date(d: date) -> dict[str, float]:
    url = f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{d.isoformat()}/v1/currencies/usd.min.json"
    data = fetch_json(url)
    rates_from_usd = data.get("usd", {})
    return {
        code: 1.0 / rates_from_usd[code.lower()]
        for code in FIAT_CODES
        if rates_from_usd.get(code.lower(), 0) > 0
    }


def fetch_crypto_on_date(coin_id: str, d: date) -> float | None:
    date_str = d.strftime("%d-%m-%Y")
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/history?date={date_str}&localization=false"
    try:
        data = fetch_json(url, pause=2.0)
        return data["market_data"]["current_price"]["usd"]
    except Exception:
        return None


def fetch_moex_month_avg(ticker: str, year: int, month: int) -> list[float]:
    """Все цены закрытия за месяц с MOEX — для настоящего среднемесячного."""
    from_d = date(year, month, 1).isoformat()
    if month == 12:
        till_d = (date(year + 1, 1, 1) - timedelta(days=1)).isoformat()
    else:
        till_d = (date(year, month + 1, 1) - timedelta(days=1)).isoformat()
    url = (
        f"https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR"
        f"/securities/{ticker}.json?from={from_d}&till={till_d}"
    )
    data = fetch_json(url, pause=0.5)
    rows = data["history"]["data"]
    cols = data["history"]["columns"]
    close_idx = cols.index("CLOSE")
    return [r[close_idx] for r in rows if r[close_idx] is not None]


def upsert_rate(cur: sqlite3.Cursor, code: str, rate_date: str, rate: float, source: str):
    now = datetime.now(timezone.utc).isoformat()
    cur.execute("""
        INSERT INTO currency_rates (currency_code, rate_date, rate_to_usd, source, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(currency_code, rate_date) DO UPDATE SET
            rate_to_usd = excluded.rate_to_usd,
            source      = excluded.source
    """, (code, rate_date, rate, source, now))


def run(db_path: Path = DB_PATH, reference_date: date | None = None) -> int:
    """
    Считает среднемесячный курс за прошлый месяц и перезаписывает слот YYYY-MM-01.
    Возвращает количество сохранённых валют.
    """
    year, month = prev_month(reference_date)
    rate_date = f"{year}-{month:02d}-01"
    dates = sample_dates(year, month)
    label = f"{year}-{month:02d}"

    log.info("Обрабатываем месяц %s (%d точек)", label, len(dates))

    rates: dict[str, list[float]] = {}

    # ── Фиат: 4 точки ──
    for d in dates:
        try:
            daily = fetch_fiat_on_date(d)
            for code, val in daily.items():
                rates.setdefault(code, []).append(val)
            time.sleep(0.3)
        except Exception as e:
            log.warning("Fiat %s: %s", d, e)

    rates["USD"] = [1.0]

    # ── Крипта: 4 точки ──
    for code, coin_id in COINGECKO_IDS.items():
        for d in dates:
            try:
                p = fetch_crypto_on_date(coin_id, d)
                if p is not None:
                    rates.setdefault(code, []).append(p)
            except Exception as e:
                log.warning("Crypto %s %s: %s", code, d, e)
            time.sleep(1.2)  # CoinGecko free tier rate limit

    # ── MOEX: все торговые дни за месяц ──
    rub_vals = rates.get("RUB", [])
    rub_avg = sum(rub_vals) / len(rub_vals) if rub_vals else 0
    if rub_avg:
        for ticker in MOEX_TICKERS:
            try:
                prices_rub = fetch_moex_month_avg(ticker, year, month)
                if prices_rub:
                    avg_rub = sum(prices_rub) / len(prices_rub)
                    rates[ticker] = [avg_rub * rub_avg]
                time.sleep(0.3)
            except Exception as e:
                log.warning("MOEX %s: %s", ticker, e)
    else:
        log.warning("MOEX пропущен — нет курса RUB")

    # ── Сохраняем средние в БД ──
    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    saved = 0
    for code, vals in rates.items():
        if vals:
            avg = sum(vals) / len(vals)
            upsert_rate(cur, code, rate_date, avg, "monthly_avg")
            log.info("  %s: %.6f USD (n=%d)", code, avg, len(vals))
            saved += 1
    con.commit()
    con.close()

    log.info("monthly_rates %s: сохранено %d валют в слот %s", label, saved, rate_date)
    return saved


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
