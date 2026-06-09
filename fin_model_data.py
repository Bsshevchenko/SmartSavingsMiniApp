#!/usr/bin/env python3
"""
Данные для дивидендной фин-модели.

Два слоя:
  1. Дивиденды с dohod.ru (форвард, доходность, история, календарь выплат) —
     парсятся РЕДКО (1-го числа месяца шедулером) и кладутся в кэш
     data/dividends_cache.json. См. refresh_dividend_cache() / run().
  2. Портфель, цены, расходы, факт стоимости по месяцам — считаются на лету
     из БД под конкретного user_id. См. build_fin_model().

Запуск парсинга вручную:  python fin_model_data.py
"""
from __future__ import annotations

import html
import json
import logging
import re
import sqlite3
import ssl
import time
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import metrics

log = logging.getLogger(__name__)

# macOS-сборки Python иногда без CA-бандла → CERTIFICATE_VERIFY_FAILED.
try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except Exception:
    SSL_CTX = ssl._create_unverified_context()

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "data" / "app.db"
CACHE_PATH = ROOT / "data" / "dividends_cache.json"

STOCK_CATEGORY = "Акции"
ETF_TICKERS = {"TRUR"}        # ETF: цена есть, дивидендов нет (реинвест внутри фонда)
SKIP_AS_CASH = {"RUB"}        # «бумага» RUB в категории Акции — это кэш
TAX = 0.13
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


# ────────────────────────── dohod.ru: парсинг ──────────────────────────

def _flatten_table(table_html: str) -> str:
    text = re.sub(r"<[^>]+>", "|", table_html)
    text = html.unescape(text)
    text = re.sub(r"\|+", "|", text)
    rows = [r.strip(" |") for r in text.split("\n") if r.strip(" |")]
    return re.sub(r"\s+", " ", " ".join(rows))


def _parse_metrics(html_src: str) -> dict:
    out = {"forward_div": 0.0, "current_yield": 0.0, "payout": None, "dsi": None, "history": {}}
    tables = re.findall(r"<table.*?</table>", html_src, re.S | re.I)
    flat = [_flatten_table(t) for t in tables]
    for t in flat:
        if "текущая доходность" in t:
            pcts = re.findall(r"(\d+[.,]?\d*)%", t)
            nums = re.findall(r"(\d+[.,]?\d+)", t)
            if pcts:
                out["current_yield"] = float(pcts[0].replace(",", "."))
            if len(pcts) >= 2:
                out["payout"] = float(pcts[1].replace(",", "."))
            for n in nums:
                v = float(n.replace(",", "."))
                if 0 <= v <= 1.0:
                    out["dsi"] = v
            break
    for t in flat:
        if "прогноз" in t.lower() and "Дивиденд" in t:
            m = re.search(r"прогноз\)\s*\|?\s*(\d+[.,]?\d*)", t)
            if m:
                out["forward_div"] = float(m.group(1).replace(",", "."))
            for ym, val in re.findall(r"(19\d{2}|20\d{2})\s*\|?\s*(\d+[.,]?\d*)", t):
                out["history"][int(ym)] = float(val.replace(",", "."))
            break
    return out


def _parse_calendar(html_src: str) -> list[dict]:
    """Календарь выплат: [{record_date(iso), per_share, declared}]."""
    events = []
    for t in re.findall(r"<table.*?</table>", html_src, re.S | re.I):
        if "Дата закрытия реестра" not in t:
            continue
        for tr in re.findall(r"<tr.*?</tr>", t, re.S | re.I):
            tds = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S | re.I)
            cells = [re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", c))).strip() for c in tds]
            if len(cells) < 4:
                continue
            m = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", cells[1])
            if not m:
                continue
            try:
                per = float(cells[3].replace(",", "."))
            except ValueError:
                continue
            iso = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
            declared = "прогноз" not in (cells[0] + cells[1] + cells[2]).lower()
            events.append({"record_date": iso, "per_share": per, "declared": declared})
        break
    return events


def _hist_cagr(history: dict) -> float | None:
    """history: {int_year: value}. Среднегодовой рост за последние ~5 лет."""
    if not history:
        return None
    ys = sorted(y for y, v in history.items() if v and v > 0)
    if len(ys) < 2:
        return None
    recent = [y for y in ys if y >= ys[-1] - 6]
    if len(recent) < 2:
        return None
    first, last, n = history[recent[0]], history[recent[-1]], recent[-1] - recent[0]
    if first <= 0 or n <= 0:
        return None
    return round(((last / first) ** (1 / n) - 1) * 100, 1)


def _fetch_dohod(ticker: str) -> dict | None:
    url = f"https://www.dohod.ru/ik/analytics/dividend/{ticker.lower()}"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=25, context=SSL_CTX) as resp:
            if resp.status != 200:
                return None
            src = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        log.warning("dohod %s failed: %s", ticker, e)
        return None
    m = _parse_metrics(src)
    return {
        "forward_div": m["forward_div"], "current_yield": m["current_yield"],
        "payout": m["payout"], "dsi": m["dsi"], "history": m["history"],
        "div_cagr": _hist_cagr(m["history"]), "calendar": _parse_calendar(src),
        "source": "dohod.ru",
    }


def _stock_tickers(con) -> list[str]:
    cur = con.cursor()
    cur.execute("""
        SELECT DISTINCT c.code AS code
        FROM entries e
        JOIN currencies c   ON c.id  = e.currency_id
        JOIN categories cat ON cat.id = e.category_id
        WHERE e.mode='asset' AND cat.name=?
    """, (STOCK_CATEGORY,))
    return [r[0] for r in cur.fetchall()
            if r[0] not in ETF_TICKERS and r[0] not in SKIP_AS_CASH]


def refresh_dividend_cache(db_path: Path = DB_PATH, cache_path: Path = CACHE_PATH) -> dict:
    """Парсит dohod по всем тикерам-акциям портфеля и пишет кэш JSON."""
    con = sqlite3.connect(str(db_path))
    tickers = _stock_tickers(con)
    con.close()
    out = {}
    for t in tickers:
        d = _fetch_dohod(t)
        if d:
            out[t] = d
        time.sleep(0.4)
    payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "tickers": out}
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("dividend cache refreshed: %d tickers", len(out))
    return payload


def load_dividend_cache(cache_path: Path = CACHE_PATH) -> dict:
    if cache_path.exists():
        try:
            return json.loads(cache_path.read_text(encoding="utf-8")).get("tickers", {})
        except Exception as e:
            log.warning("dividend cache read failed: %s", e)
    return {}


def run():
    """Точка для шедулера/CLI."""
    refresh_dividend_cache()


# ────────────────────────── вспомогательное ──────────────────────────

def _prev_business_day(iso_date: str) -> str:
    y, m, d = map(int, iso_date.split("-"))
    dt = date(y, m, d) - timedelta(days=1)
    while dt.weekday() >= 5:
        dt -= timedelta(days=1)
    return dt.isoformat()


def _shares_at(history: list, iso_date: str) -> float:
    qty = 0.0
    for d, q in history or []:
        if d <= iso_date:
            qty = q
        else:
            break
    return qty


# ────────────────────────── сборка payload ──────────────────────────

def build_fin_model(user_id: int, db_path: Path = DB_PATH, cache_path: Path = CACHE_PATH) -> dict:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    year = int(today[:4])

    # Текущее кол-во по тикеру (последний снапшот)
    cur.execute("""
        WITH last_qty AS (
          SELECT c.code AS ticker, CAST(e.amount AS REAL) AS qty,
                 ROW_NUMBER() OVER (PARTITION BY c.code ORDER BY e.created_at DESC) rn
          FROM entries e
          JOIN currencies c   ON c.id  = e.currency_id
          JOIN categories cat ON cat.id = e.category_id
          WHERE e.mode='asset' AND cat.name=? AND e.user_id=?
        )
        SELECT ticker, qty FROM last_qty WHERE rn=1 AND qty>0 ORDER BY ticker
    """, (STOCK_CATEGORY, user_id))
    holdings = {r["ticker"]: r["qty"] for r in cur.fetchall()}

    # История снапшотов кол-ва
    cur.execute("""
        SELECT c.code AS ticker, date(e.created_at) AS d, CAST(e.amount AS REAL) AS qty
        FROM entries e
        JOIN currencies c   ON c.id  = e.currency_id
        JOIN categories cat ON cat.id = e.category_id
        WHERE e.mode='asset' AND cat.name=? AND e.user_id=?
        ORDER BY c.code, e.created_at
    """, (STOCK_CATEGORY, user_id))
    holdings_history: dict[str, list] = {}
    for r in cur.fetchall():
        holdings_history.setdefault(r["ticker"], []).append((r["d"], r["qty"]))

    # Последний курс по всем валютам
    cur.execute("""
        WITH last_rate AS (
          SELECT currency_code AS code, rate_to_usd, rate_date,
                 ROW_NUMBER() OVER (PARTITION BY currency_code ORDER BY rate_date DESC) rn
          FROM currency_rates
        )
        SELECT code, rate_to_usd, rate_date FROM last_rate WHERE rn=1
    """)
    rate_to_usd = {r["code"]: (r["rate_to_usd"], r["rate_date"]) for r in cur.fetchall()}
    rub_usd = rate_to_usd.get("RUB", (None, None))[0]
    if not rub_usd:
        con.close()
        return {"error": "no RUB rate"}

    prices_rub, price_date = {}, None
    for code, (r2usd, rdate) in rate_to_usd.items():
        if code == "RUB":
            continue
        prices_rub[code] = round(r2usd / rub_usd, 4)
        price_date = rdate

    # Средние месячные расходы — ЕДИНЫЙ централизованный расчёт (metrics): по завершённым
    # месяцам в RUB, скользящее окно ≤12 мес, исторический курс месяца. Та же цифра, что
    # на странице аналитики и в финзапасе на активах.
    code_to_usd = {c: v[0] for c, v in rate_to_usd.items()}
    _exp = metrics.avg_monthly_expense(con, user_id, "RUB", code_to_usd)
    avg_expenses = _exp["avg"]
    exp_by_month = _exp["by_month"]

    # Все исторические курсы
    cur.execute("SELECT currency_code AS code, rate_date AS d, rate_to_usd AS r FROM currency_rates")
    rates_by_code: dict[str, dict] = {}
    for r in cur.fetchall():
        rates_by_code.setdefault(r["code"], {})[r["d"]] = r["r"]
    con.close()

    div_cache = load_dividend_cache(cache_path)

    # ── Позиции ──
    positions = []
    for ticker, qty in holdings.items():
        price = prices_rub.get(ticker)
        value = round(qty * price) if price else 0
        d = div_cache.get(ticker, {})
        pos = {
            "ticker": ticker, "qty": qty, "price": price, "value": value,
            "is_etf": ticker in ETF_TICKERS, "is_cash": ticker in SKIP_AS_CASH,
            "forward_div": d.get("forward_div", 0.0) if ticker not in ETF_TICKERS and ticker not in SKIP_AS_CASH else 0.0,
            "current_yield": d.get("current_yield", 0.0),
            "payout": d.get("payout"), "dsi": d.get("dsi"),
            "div_cagr": d.get("div_cagr"),
            "history": d.get("history", {}),
            "calendar": d.get("calendar", []),
            "source": d.get("source"),
        }
        pos["annual_div_gross"] = round(qty * pos["forward_div"]) if pos["forward_div"] else 0
        positions.append(pos)

    total_value = sum(p["value"] for p in positions)
    total_div_gross = sum(p["annual_div_gross"] for p in positions)
    total_div_net = round(total_div_gross * (1 - TAX))
    for p in positions:
        p["weight"] = round(p["value"] / total_value, 4) if total_value else 0

    # ── Дивиденды текущего года ──
    div_year = []
    for p in positions:
        for ev in p.get("calendar", []):
            if not ev["record_date"].startswith(str(year)):
                continue
            past = ev["record_date"] <= today
            shares = _shares_at(holdings_history.get(p["ticker"]), ev["record_date"]) if past else p["qty"]
            gross = round(shares * ev["per_share"], 2)
            div_year.append({
                "ticker": p["ticker"], "record_date": ev["record_date"],
                "buy_by": _prev_business_day(ev["record_date"]),
                "per_share": ev["per_share"], "declared": ev["declared"], "past": past,
                "shares_at_date": round(shares, 4), "current_qty": p["qty"], "price": p["price"],
                "gross": gross, "net": round(gross * (1 - TAX), 2),
            })
    div_year.sort(key=lambda e: e["record_date"])

    # Реальные события следующего года (метка «прогноз» в проекции)
    div_next = []
    for p in positions:
        for ev in p.get("calendar", []):
            if ev["record_date"].startswith(str(year + 1)):
                div_next.append({"ticker": p["ticker"], "record_date": ev["record_date"],
                                 "per_share": ev["per_share"], "declared": ev["declared"]})
    div_next.sort(key=lambda e: e["record_date"])

    # ── Факт стоимости портфеля по завершённым месяцам ──
    def rate_on(code, iso):
        ds = [d for d in rates_by_code.get(code, {}) if d <= iso]
        return rates_by_code[code][max(ds)] if ds else None

    def last_day(y, m):
        nm = f"{y+(m//12):04d}-{(m%12)+1:02d}-01"
        return (datetime.strptime(nm, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

    def pf_value(qty_date, price_date_):
        rub = rate_on("RUB", price_date_)
        if not rub:
            return None
        v = 0.0
        for ticker, hist in holdings_history.items():
            q = _shares_at(hist, qty_date)
            if q <= 0:
                continue
            if ticker == "RUB":
                v += q
                continue
            pr = rate_on(ticker, price_date_)
            if pr:
                v += q * pr / rub
        return v

    actual_pf, breakdown = {}, {}
    cur_month = int(today[5:7])
    for m in range(1, cur_month):
        first_m = f"{year:04d}-{m:02d}-01"
        close = f"{year:04d}-{m+1:02d}-01" if m < 12 else f"{year+1:04d}-01-01"
        eom = last_day(year, m)
        eom_prev = last_day(year, m - 1) if m > 1 else last_day(year - 1, 12)
        end = pf_value(eom, close)
        if end is None:
            continue
        start = pf_value(eom_prev, first_m) or 0.0
        actual_pf[f"{year:04d}-{m:02d}"] = round(end)
        rub_s, rub_e = rate_on("RUB", first_m), rate_on("RUB", close)
        market = 0.0
        for ticker, hist in holdings_history.items():
            q0 = _shares_at(hist, eom_prev)
            if q0 <= 0 or ticker == "RUB":
                continue
            pr_s, pr_e = rate_on(ticker, first_m), rate_on(ticker, close)
            if pr_s and pr_e and rub_s and rub_e:
                market += q0 * (pr_e / rub_e - pr_s / rub_s)
        breakdown[f"{year:04d}-{m:02d}"] = {
            "value": round(end), "start": round(start),
            "market": round(market), "contributed": round(end - start - market)}

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dividends_updated_at": _cache_ts(cache_path),
        "today": today, "year": year, "price_date": price_date,
        "avg_monthly_expenses": avg_expenses, "expenses_by_month": exp_by_month,
        "total_value": total_value, "annual_div_gross": total_div_gross,
        "annual_div_net": total_div_net, "monthly_div_net": round(total_div_net / 12),
        "current_yield_pct": round(total_div_gross / total_value * 100, 2) if total_value else 0,
        "tax_rate": TAX, "positions": positions,
        "dividends_year": div_year, "dividends_next": div_next,
        "actual_portfolio_by_month": actual_pf, "portfolio_breakdown_by_month": breakdown,
    }


def _cache_ts(cache_path: Path) -> str | None:
    if cache_path.exists():
        try:
            return json.loads(cache_path.read_text(encoding="utf-8")).get("generated_at")
        except Exception:
            return None
    return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    p = refresh_dividend_cache()
    print(f"Закэшировано тикеров: {len(p['tickers'])} → {CACHE_PATH}")