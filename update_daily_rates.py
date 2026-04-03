#!/usr/bin/env python3
"""
Обновление актуальных курсов для текущего месяца.
Запускается ежедневно — перезаписывает одну строку на валюту с датой YYYY-MM-01 текущего месяца.

Источники:
  - Фиат (RUB, VND, THB, MYR, EUR): fawazahmed0/currency-api (@latest)
  - Крипта (BTC, ETH, SOL, TRX, USDT): CoinGecko /simple/price (один запрос на все)
  - Акции РФ: MOEX ISS /marketdata (текущие цены, один запрос на все тикеры)
"""

import sqlite3
import json
import ssl
import time
import logging
from datetime import date, datetime, timezone
from urllib.request import urlopen
from pathlib import Path

log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "data" / "app.db"

FIAT_CODES = ["RUB", "VND", "THB", "MYR", "EUR"]

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
                log.warning("fetch %s attempt %d failed: %s", url, attempt + 1, e)
                time.sleep(pause)
            else:
                raise


def fetch_fiat_rates() -> dict[str, float]:
    url = "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.min.json"
    data = fetch_json(url)
    rates_from_usd = data.get("usd", {})
    return {
        code: 1.0 / rates_from_usd[code.lower()]
        for code in FIAT_CODES
        if rates_from_usd.get(code.lower(), 0) > 0
    }


def fetch_crypto_rates() -> dict[str, float]:
    """Один запрос для всех монет через /simple/price."""
    ids = ",".join(COINGECKO_IDS.values())
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd"
    data = fetch_json(url)
    return {
        code: float(data[coin_id]["usd"])
        for code, coin_id in COINGECKO_IDS.items()
        if data.get(coin_id, {}).get("usd")
    }


def fetch_moex_rates(rub_to_usd: float) -> dict[str, float]:
    """
    Текущие цены акций через MOEX ISS /securities (один запрос).
    LAST из marketdata = цена последней сделки (None если рынок закрыт).
    PREVPRICE из securities = предыдущее закрытие (всегда доступно).
    """
    tickers = ",".join(MOEX_TICKERS)
    url = (
        f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR"
        f"/securities.json?securities={tickers}&iss.meta=off"
    )
    data = fetch_json(url)

    md_cols = data["marketdata"]["columns"]
    md_rows = data["marketdata"]["data"]
    sec_cols = data["securities"]["columns"]
    sec_rows = data["securities"]["data"]

    md_secid = md_cols.index("SECID")
    md_last  = md_cols.index("LAST")
    sec_secid = sec_cols.index("SECID")
    sec_prev  = sec_cols.index("PREVPRICE")

    # Собираем PREVPRICE из securities как fallback
    prev_prices = {row[sec_secid]: row[sec_prev] for row in sec_rows if row[sec_prev]}

    result = {}
    for row in md_rows:
        ticker = row[md_secid]
        price_rub = row[md_last] or prev_prices.get(ticker)
        if price_rub:
            result[ticker] = float(price_rub) * rub_to_usd
    return result


def upsert_rate(cur: sqlite3.Cursor, code: str, rate_date: str, rate: float, source: str):
    now = datetime.now(timezone.utc).isoformat()
    cur.execute("""
        INSERT INTO currency_rates (currency_code, rate_date, rate_to_usd, source, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(currency_code, rate_date) DO UPDATE SET
            rate_to_usd = excluded.rate_to_usd,
            source      = excluded.source
    """, (code, rate_date, rate, source, now))


def run(db_path: Path = DB_PATH) -> dict[str, float]:
    """
    Получает текущие курсы и перезаписывает строку YYYY-MM-01 текущего месяца.
    Возвращает словарь {code: rate_to_usd}.
    """
    today = date.today()
    rate_date = today.strftime("%Y-%m-01")  # слот текущего месяца

    rates: dict[str, float] = {"USD": 1.0}

    try:
        rates.update(fetch_fiat_rates())
        log.info("Fiat OK: %d курсов", len(rates) - 1)
    except Exception as e:
        log.error("Fiat failed: %s", e)

    try:
        crypto = fetch_crypto_rates()
        rates.update(crypto)
        log.info("Crypto OK: %d курсов", len(crypto))
    except Exception as e:
        log.error("Crypto failed: %s", e)

    rub = rates.get("RUB")
    if rub:
        try:
            moex = fetch_moex_rates(rub)
            rates.update(moex)
            log.info("MOEX OK: %d курсов", len(moex))
        except Exception as e:
            log.error("MOEX failed: %s", e)
    else:
        log.warning("MOEX пропущен — нет курса RUB")

    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    for code, rate in rates.items():
        upsert_rate(cur, code, rate_date, rate, "daily")
    con.commit()
    con.close()

    log.info("daily_rates: обновлено %d валют, слот %s", len(rates), rate_date)
    return rates


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = run()
    print(f"\nКурсы на {date.today().strftime('%Y-%m-01')}:")
    for code, rate in sorted(result.items()):
        print(f"  {code:8s} {rate:.6f} USD")