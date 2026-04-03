#!/usr/bin/env python3
"""
Бэкфил currency_rates: среднемесячные курсы за последние 7 месяцев.

Источники:
  - Фиат (RUB, VND, THB, MYR, EUR, USD): fawazahmed0/currency-api на jsDelivr
    (бесплатно, все валюты, исторические данные по дате в URL)
  - Крипта (BTC, ETH, SOL, TRX, USDT): CoinGecko free API
  - Акции РФ (SBER, OZON, ...): MOEX ISS API (цены в RUB, конвертируем через RUB/USD)

Среднемесячная = среднее по 4 точкам: 1-е, 8-е, 15-е, 22-е число месяца.
Для дней когда рынок/API не вернул данные — точка пропускается.
"""

import sqlite3
import json
import time
import ssl
from datetime import date, timedelta, datetime, timezone
from urllib.request import urlopen
from urllib.error import URLError
from pathlib import Path

# Отключаем проверку SSL — нужно для сетей с MITM-прокси (VPN, корпоративные сети)
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

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


# ── Вспомогательные ──────────────────────────────────────────────────────────

def fetch_json(url: str, retries: int = 3, pause: float = 1.0) -> dict:
    for attempt in range(retries):
        try:
            with urlopen(url, timeout=15, context=_SSL_CTX) as resp:
                return json.loads(resp.read())
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(pause)
            else:
                raise


def last_n_months(n: int) -> list[tuple[int, int]]:
    """Возвращает [(year, month), ...] последних n завершённых месяцев (без текущего)."""
    today = date.today()
    result = []
    for i in range(n, 0, -1):
        month = today.month - i
        year = today.year
        while month <= 0:
            month += 12
            year -= 1
        result.append((year, month))
    return result


def sample_dates(year: int, month: int) -> list[date]:
    """4 точки внутри месяца для усреднения."""
    dates = []
    for day in [1, 8, 15, 22]:
        try:
            dates.append(date(year, month, day))
        except ValueError:
            pass
    return dates


# ── Источники данных ─────────────────────────────────────────────────────────

def get_fiat_rates_on_date(d: date) -> dict[str, float]:
    """
    Курс к USD для всех фиатных валют на дату.
    Использует https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@DATE/v1/currencies/usd.json
    Возвращает {CODE: rate_to_usd} (1 единица валюты = ? USD).
    """
    url = f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{d.isoformat()}/v1/currencies/usd.min.json"
    data = fetch_json(url)
    rates_from_usd = data.get("usd", {})  # {code_lower: units_per_usd}
    result = {}
    for code in FIAT_CODES:
        key = code.lower()
        if key in rates_from_usd and rates_from_usd[key] > 0:
            result[code] = 1.0 / rates_from_usd[key]
    return result


def get_crypto_price_usd(coin_id: str, d: date) -> float | None:
    """Цена крипты в USD на дату через CoinGecko /coins/{id}/history."""
    date_str = d.strftime("%d-%m-%Y")
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/history?date={date_str}&localization=false"
    try:
        data = fetch_json(url, pause=2.0)
        return data["market_data"]["current_price"]["usd"]
    except Exception:
        return None


def get_moex_close_prices(ticker: str, year: int, month: int) -> list[float]:
    """
    Цены закрытия акции на MOEX за месяц (в RUB).
    Берём первые ~5 торговых дней в наших 4 точках (1, 8, 15, 22).
    """
    # Запрашиваем весь месяц сразу
    from_d = date(year, month, 1).isoformat()
    # последний день месяца
    if month == 12:
        till_d = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        till_d = date(year, month + 1, 1) - timedelta(days=1)
    till_str = till_d.isoformat()

    url = (
        f"https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR"
        f"/securities/{ticker}.json?from={from_d}&till={till_str}"
    )
    try:
        data = fetch_json(url, pause=0.5)
        rows = data["history"]["data"]
        cols = data["history"]["columns"]
        if not rows:
            return []
        close_idx = cols.index("CLOSE")
        prices = [r[close_idx] for r in rows if r[close_idx] is not None]
        return prices
    except Exception:
        return []


# ── БД ──────────────────────────────────────────────────────────────────────

def upsert_rate(cur: sqlite3.Cursor, code: str, rate_date: str, rate: float, source: str):
    now = datetime.now(timezone.utc).isoformat()
    cur.execute("""
        INSERT INTO currency_rates (currency_code, rate_date, rate_to_usd, source, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(currency_code, rate_date) DO UPDATE SET
            rate_to_usd = excluded.rate_to_usd,
            source      = excluded.source
    """, (code, rate_date, rate, source, now))


def monthly_rate_date(year: int, month: int) -> str:
    """Храним как 1-е число месяца — маркер «среднемесячный курс»."""
    return f"{year}-{month:02d}-01"


# ── Основная логика ──────────────────────────────────────────────────────────

def main():
    con = sqlite3.connect(str(DB_PATH))
    cur = con.cursor()

    months = last_n_months(7)
    print(f"Обрабатываем {len(months)} месяцев: {months[0]} → {months[-1]}\n")

    total_ok = 0
    total_err = 0

    for year, month in months:
        label = f"{year}-{month:02d}"
        print(f"{'='*50}")
        print(f"  Месяц: {label}")
        rate_date = monthly_rate_date(year, month)
        dates = sample_dates(year, month)

        # ── 1. Фиат ──
        fiat_samples: dict[str, list[float]] = {c: [] for c in FIAT_CODES}
        for d in dates:
            try:
                daily = get_fiat_rates_on_date(d)
                for code in FIAT_CODES:
                    if code in daily:
                        fiat_samples[code].append(daily[code])
                time.sleep(0.3)
            except Exception as e:
                print(f"    [fiat {d}] ОШИБКА: {e}")

        rub_avg = None
        for code in FIAT_CODES:
            vals = fiat_samples[code]
            if vals:
                avg = sum(vals) / len(vals)
                upsert_rate(cur, code, rate_date, avg, "fawazahmed0")
                print(f"    {code}: {avg:.8f} USD  (n={len(vals)})")
                if code == "RUB":
                    rub_avg = avg
                total_ok += 1
            else:
                print(f"    {code}: нет данных")
                total_err += 1

        # USD всегда 1.0
        upsert_rate(cur, "USD", rate_date, 1.0, "backfill")

        # ── 2. Крипта ──
        print(f"  --- Крипта ---")
        for code, coin_id in COINGECKO_IDS.items():
            prices = []
            for d in dates:
                p = get_crypto_price_usd(coin_id, d)
                if p is not None:
                    prices.append(p)
                time.sleep(1.2)  # CoinGecko free tier: ~10 rpm
            if prices:
                avg = sum(prices) / len(prices)
                upsert_rate(cur, code, rate_date, avg, "coingecko")
                print(f"    {code}: {avg:.2f} USD  (n={len(prices)})")
                total_ok += 1
            else:
                print(f"    {code}: нет данных")
                total_err += 1

        # ── 3. Акции MOEX ──
        print(f"  --- MOEX акции ---")
        if rub_avg is None:
            print("    RUB курс недоступен — акции MOEX пропускаем")
        else:
            for ticker in MOEX_TICKERS:
                prices_rub = get_moex_close_prices(ticker, year, month)
                if prices_rub:
                    avg_rub = sum(prices_rub) / len(prices_rub)
                    avg_usd = avg_rub * rub_avg
                    upsert_rate(cur, ticker, rate_date, avg_usd, "moex")
                    print(f"    {ticker}: {avg_rub:.2f} RUB = {avg_usd:.4f} USD  (n={len(prices_rub)})")
                    total_ok += 1
                else:
                    print(f"    {ticker}: нет данных на MOEX")
                    total_err += 1
                time.sleep(0.3)

        con.commit()
        print(f"  Сохранено. Пауза 2с...")
        time.sleep(2)

    con.close()
    print(f"\n{'='*50}")
    print(f"Готово: успешно {total_ok}, ошибок {total_err}")


if __name__ == "__main__":
    main()