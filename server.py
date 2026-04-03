"""
FastAPI сервер:
  GET  /                              → web/index.html
  GET  /analytics                     → web/analytics.html
  GET  /api/user-data?user_id         → валюты и категории из БД
  GET  /api/analytics?user_id&period&currency → данные для графиков (с конвертацией)

Курсы валют читаются ТОЛЬКО из БД (currency_rates).
Обновление курсов — фоновые шедулеры (update_daily_rates, update_monthly_rates).
"""
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import update_daily_rates
import update_monthly_rates
import db_repo

WEB_DIR = Path(__file__).parent / "web"
DB_PATH = Path(__file__).parent / "data" / "app.db"

RU_MONTHS = ["Янв","Фев","Мар","Апр","Май","Июн","Июл","Авг","Сен","Окт","Ноя","Дек"]

# Последний запасной вариант — только если БД пуста
FALLBACK_TO_USD = {
    "USD": 1.0, "USDT": 1.0,
    "RUB": 0.011, "EUR": 1.08, "VND": 0.0000394,
    "THB": 0.028, "MYR": 0.022,
    "BTC": 65000.0, "ETH": 3000.0,
}

_rates_cache: dict = {"rates": {}, "ts": 0}
_CACHE_TTL = 3600  # 1 час — БД обновляется шедулером раз в сутки


def get_rates_to_usd() -> dict:
    """
    Читает актуальные курсы из БД (последняя запись на валюту).
    Кешируется на 1 час. Нет HTTP-запросов в request path.
    """
    now = time.time()
    if now - _rates_cache["ts"] < _CACHE_TTL and _rates_cache["rates"]:
        return _rates_cache["rates"]

    rates = dict(FALLBACK_TO_USD)
    if DB_PATH.exists():
        con = sqlite3.connect(str(DB_PATH))
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("""
            SELECT currency_code, CAST(rate_to_usd AS REAL) as r
            FROM currency_rates
            WHERE (currency_code, rate_date) IN (
                SELECT currency_code, MAX(rate_date) FROM currency_rates GROUP BY currency_code
            )
        """)
        for row in cur.fetchall():
            rates[row["currency_code"]] = row["r"]
        con.close()

    _rates_cache["rates"] = rates
    _rates_cache["ts"] = now
    return rates


def _run_daily():
    """Обёртка для APScheduler — запуск daily обновления и сброс кеша."""
    try:
        update_daily_rates.run()
        _rates_cache["ts"] = 0  # сбрасываем кеш — свежие данные в БД
    except Exception as e:
        import logging
        logging.getLogger(__name__).error("daily_rates failed: %s", e)


def _run_monthly():
    """Обёртка для APScheduler — запуск monthly обновления и сброс кеша."""
    try:
        update_monthly_rates.run()
        _rates_cache["ts"] = 0
    except Exception as e:
        import logging
        logging.getLogger(__name__).error("monthly_rates failed: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = AsyncIOScheduler(timezone="UTC")
    # Ежедневно в 02:00 UTC — обновляем текущий курс
    scheduler.add_job(_run_daily, "cron", hour=2, minute=0, id="daily_rates")
    # 1-го числа в 03:00 UTC — фиксируем среднемесячный курс за прошлый месяц
    scheduler.add_job(_run_monthly, "cron", day=1, hour=3, minute=0, id="monthly_rates")
    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")



def load_historical_rates(con) -> dict:
    """
    Загружает все курсы из currency_rates в словарь {(code, 'YYYY-MM'): rate_to_usd}.
    Если в одном месяце несколько записей — берём среднее.
    """
    cur = con.cursor()
    cur.execute("""
        SELECT currency_code, strftime('%Y-%m', rate_date) as month,
               AVG(CAST(rate_to_usd AS REAL)) as avg_rate
        FROM currency_rates
        GROUP BY currency_code, month
    """)
    return {(r["currency_code"], r["month"]): r["avg_rate"] for r in cur.fetchall()}


def convert_h(
    amount: float, src: str, dst: str, month: str,
    hist_rates: dict, curr_rates: dict,
) -> float:
    """
    Конвертация с учётом исторического курса месяца транзакции.
    - hist_rates: {(code, 'YYYY-MM'): rate_to_usd} — из currency_rates в БД
    - curr_rates: {code: rate_to_usd} — актуальные (fallback)
    Оба курса (src и dst) берутся за один месяц → суммы «в ценах того времени».
    """
    if src == dst:
        return amount
    src_r = hist_rates.get((src, month)) or curr_rates.get(src, 0)
    dst_r = hist_rates.get((dst, month)) or curr_rates.get(dst, 0)
    if not src_r or not dst_r:
        return 0.0
    return amount * src_r / dst_r


class EntryCreate(BaseModel):
    user_id: int
    mode: str
    amount: float
    currency: str
    category: str | None = None
    note: str | None = None


class EntryUpdate(BaseModel):
    user_id: int
    amount: float | None = None
    currency: str | None = None
    category: str | None = None
    note: str | None = None


def get_db():
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    return con


@app.get("/")
async def root():
    return FileResponse(WEB_DIR / "index.html")


@app.get("/analytics")
async def analytics_page():
    return FileResponse(WEB_DIR / "analytics.html")


@app.get("/history")
async def history_page():
    return FileResponse(WEB_DIR / "history.html")


@app.post("/api/entry")
async def create_entry(body: EntryCreate):
    import asyncio
    entry_id = await asyncio.to_thread(
        db_repo.save_entry,
        user_id=body.user_id,
        mode=body.mode,
        amount=body.amount,
        currency_code=body.currency,
        category_name=body.category or None,
        note=body.note or None,
        db_path=DB_PATH,
    )
    return JSONResponse({"ok": True, "id": entry_id})


@app.get("/api/history")
async def history_data(
    user_id: int = Query(...),
    limit: int = Query(30),
    offset: int = Query(0),
):
    if not DB_PATH.exists():
        return JSONResponse({"entries": [], "total": 0})
    entries, total = db_repo.get_entries(user_id, limit, offset, db_path=DB_PATH)
    return JSONResponse({"entries": entries, "total": total})


@app.delete("/api/entry/{entry_id}")
async def delete_entry(entry_id: int, user_id: int = Query(...)):
    if not DB_PATH.exists():
        return JSONResponse({"ok": False}, status_code=404)
    deleted = db_repo.delete_entry(user_id, entry_id, db_path=DB_PATH)
    return JSONResponse({"ok": deleted}, status_code=200 if deleted else 404)


@app.patch("/api/entry/{entry_id}")
async def update_entry(entry_id: int, body: EntryUpdate):
    if not DB_PATH.exists():
        return JSONResponse({"ok": False}, status_code=404)
    updated = db_repo.update_entry(
        user_id=body.user_id,
        entry_id=entry_id,
        amount=body.amount,
        currency_code=body.currency,
        category_name=body.category,
        note=body.note,
        db_path=DB_PATH,
    )
    return JSONResponse({"ok": updated}, status_code=200 if updated else 404)


@app.get("/api/user-data")
async def user_data(user_id: int = Query(...)):
    if not DB_PATH.exists():
        return JSONResponse({"currencies": [], "categories": {"expense": [], "income": [], "asset": []}})

    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT code FROM currencies WHERE user_id=?
        ORDER BY CASE WHEN last_used_at IS NULL THEN 1 ELSE 0 END, last_used_at DESC
    """, (user_id,))
    currencies = [r["code"] for r in cur.fetchall()]

    categories = {"expense": [], "income": [], "asset": []}
    for mode in categories:
        cur.execute("""
            SELECT name FROM categories WHERE user_id=? AND mode=?
            ORDER BY CASE WHEN last_used_at IS NULL THEN 1 ELSE 0 END, last_used_at DESC
        """, (user_id, mode))
        categories[mode] = [r["name"] for r in cur.fetchall()]

    con.close()
    return JSONResponse({"currencies": currencies, "categories": categories})


@app.get("/assets")
async def assets_page():
    return FileResponse(WEB_DIR / "assets.html")


@app.get("/api/asset-history")
async def asset_history(
    user_id: int = Query(...),
    ticker: str = Query(...),
    currency: str = Query("USD"),
):
    """Все снапшоты одного тикера с конвертированной стоимостью."""
    if not DB_PATH.exists():
        return JSONResponse({"points": []})

    curr_rates = get_rates_to_usd()
    con = get_db()
    hist_rates = load_historical_rates(con)
    cur = con.cursor()

    cur.execute("""
        SELECT CAST(e.amount AS REAL) as qty,
               date(e.created_at) as day,
               strftime('%Y-%m', e.created_at) as month
        FROM entries e
        JOIN currencies c ON c.id = e.currency_id
        WHERE e.mode = 'asset' AND e.user_id = ? AND c.code = ?
        ORDER BY e.created_at
    """, (user_id, ticker))

    src_r = curr_rates.get(ticker, 0)
    dst_r = curr_rates.get(currency, 0)

    # Все снапшоты пользователя по тикеру (для маркеров изменений)
    snapshots = cur.fetchall()  # уже отсортированы по created_at

    # Первый и последний месяц со снапшотом
    if not snapshots:
        con.close()
        return JSONResponse({"ticker": ticker, "currency": currency, "points": []})

    first_month = snapshots[0]["month"]

    # Все месяцы с курсом тикера начиная с first_month
    cur.execute("""
        SELECT strftime('%Y-%m', rate_date) as month,
               AVG(CAST(rate_to_usd AS REAL)) as rate
        FROM currency_rates
        WHERE currency_code = ?
          AND strftime('%Y-%m', rate_date) >= ?
        GROUP BY month
        ORDER BY month
    """, (ticker, first_month))
    rate_rows = cur.fetchall()

    # Курс валюты назначения по месяцам
    cur.execute("""
        SELECT strftime('%Y-%m', rate_date) as month,
               AVG(CAST(rate_to_usd AS REAL)) as rate
        FROM currency_rates
        WHERE currency_code = ?
        GROUP BY month
        ORDER BY month
    """, (currency,))
    dst_rate_rows = {r["month"]: r["rate"] for r in cur.fetchall()}

    # Маркеры изменений: {month → список qty}
    # Берём для каждого снапшота его месяц
    snapshot_changes: dict[str, float] = {}
    for s in snapshots:
        snapshot_changes[s["month"]] = s["qty"]  # последнее изменение в месяце

    # Строим qty-ряд: для каждого месяца — последний снапшот ≤ этого месяца
    def qty_at_month(month: str) -> float:
        result = 0.0
        for s in snapshots:
            if s["month"] <= month:
                result = s["qty"]
            else:
                break
        return result

    today = datetime.now(timezone.utc)
    today_month = today.strftime("%Y-%m")
    today_str = today.strftime("%Y-%m-%d")

    # Собираем точки по месячным курсам
    points = []
    for rr in rate_rows:
        m = rr["month"]
        qty = qty_at_month(m)
        if qty <= 0:
            continue  # позиция закрыта
        rate_src = rr["rate"]
        rate_dst = dst_rate_rows.get(m) or curr_rates.get(currency, 0)
        if not rate_src or not rate_dst:
            continue
        val = qty * rate_src / rate_dst
        mo_idx = int(m[5:]) - 1
        is_snapshot = m in snapshot_changes
        points.append({
            "month": m,
            "label": RU_MONTHS[mo_idx] + " " + m[2:4],
            "qty": qty,
            "value": round(val, 2),
            "is_snapshot": is_snapshot,
            "is_current": False,
        })

    # Добавляем точку «сейчас» если текущий месяц ещё без курса в БД или просто хотим свежий курс
    if src_r and dst_r:
        last_qty = qty_at_month(today_month)
        if last_qty > 0:
            curr_val = last_qty * src_r / dst_r
            # Обновляем или добавляем текущий месяц
            if points and points[-1]["month"] == today_month:
                points[-1].update({"value": round(curr_val, 2), "is_current": True})
            else:
                mo_idx = today.month - 1
                points.append({
                    "month": today_month,
                    "label": RU_MONTHS[mo_idx] + " " + today_month[2:4],
                    "qty": last_qty,
                    "value": round(curr_val, 2),
                    "is_snapshot": today_month in snapshot_changes,
                    "is_current": True,
                })

    con.close()
    return JSONResponse({"ticker": ticker, "currency": currency, "points": points})


@app.get("/api/assets")
async def assets_data(
    user_id: int = Query(...),
    currency: str = Query("USD"),
):
    if not DB_PATH.exists():
        return JSONResponse({})

    curr_rates = get_rates_to_usd()
    con = get_db()
    hist_rates = load_historical_rates(con)
    cur = con.cursor()
    now = datetime.now(timezone.utc)

    def _conv(amount: float, src: str, dst: str, month: str) -> float:
        return convert_h(amount, src, dst, month, hist_rates, curr_rates)

    # ── Последний снапшот по каждому тикеру ──
    cur.execute("""
        SELECT c.code, cat.name as category,
               CAST(e.amount AS REAL) as amt,
               strftime('%Y-%m', e.created_at) as month,
               date(e.created_at) as day
        FROM entries e
        JOIN currencies c ON c.id = e.currency_id
        LEFT JOIN categories cat ON cat.id = e.category_id
        WHERE e.mode = 'asset' AND e.user_id = ?
          AND e.created_at = (
              SELECT MAX(e2.created_at) FROM entries e2
              WHERE e2.mode = 'asset' AND e2.user_id = e.user_id
                AND e2.currency_id = e.currency_id
          )
          AND CAST(e.amount AS REAL) > 0
    """, (user_id,))
    positions = []
    for r in cur.fetchall():
        # Текущая стоимость — только актуальные курсы (не исторические)
        src_r = curr_rates.get(r["code"], 0)
        dst_r = curr_rates.get(currency, 0)
        val = r["amt"] * src_r / dst_r if src_r and dst_r else 0.0
        positions.append({
            "ticker": r["code"],
            "category": r["category"] or "Без категории",
            "qty": r["amt"],
            "price_usd": curr_rates.get(r["code"], 0),
            "value": round(val, 2),
            "month": r["month"],
            "day": r["day"],
        })
    positions.sort(key=lambda x: x["value"], reverse=True)

    total_value = sum(p["value"] for p in positions)

    # Добавляем % к позициям
    for p in positions:
        p["pct"] = round(p["value"] / total_value * 100, 1) if total_value else 0

    # ── По категориям ──
    cat_totals: dict[str, float] = {}
    for p in positions:
        cat_totals[p["category"]] = cat_totals.get(p["category"], 0) + p["value"]
    by_category = sorted(
        [{"name": k, "value": round(v, 2), "pct": round(v / total_value * 100, 1) if total_value else 0}
         for k, v in cat_totals.items()],
        key=lambda x: x["value"], reverse=True
    )

    # ── Динамика: один срез на каждый месяц ──
    cur.execute("""
        SELECT DISTINCT strftime('%Y-%m', created_at) as month
        FROM entries WHERE mode='asset' AND user_id=?
        ORDER BY month
    """, (user_id,))
    months_with_data = [r["month"] for r in cur.fetchall()]

    timeline = []
    for m in months_with_data:
        # Последний снапшот каждого тикера ДО конца месяца
        cur.execute("""
            SELECT c.code, CAST(e.amount AS REAL) as amt,
                   strftime('%Y-%m', e.created_at) as entry_month
            FROM entries e
            JOIN currencies c ON c.id = e.currency_id
            WHERE e.mode='asset' AND e.user_id=?
              AND strftime('%Y-%m', e.created_at) <= ?
              AND e.created_at = (
                  SELECT MAX(e2.created_at) FROM entries e2
                  WHERE e2.mode='asset' AND e2.user_id=e.user_id
                    AND e2.currency_id=e.currency_id
                    AND strftime('%Y-%m', e2.created_at) <= ?
              )
        """, (user_id, m, m))
        snap_total = 0.0
        for r in cur.fetchall():
            snap_total += _conv(r["amt"], r["code"], currency, m)
        mo_idx = int(m[5:]) - 1
        timeline.append({
            "month": m,
            "label": RU_MONTHS[mo_idx] + " " + m[2:4],
            "value": round(snap_total, 2),
        })

    # ── Runway: сколько месяцев можно прожить ──
    since_1y = (now - timedelta(days=365)).strftime("%Y-%m-%d")
    cur.execute("""
        SELECT strftime('%Y-%m', e.created_at) as month,
               CAST(e.amount AS REAL) as amt, c.code
        FROM entries e JOIN currencies c ON c.id=e.currency_id
        WHERE e.user_id=? AND e.mode='expense'
          AND date(e.created_at) >= ?
    """, (user_id, since_1y))
    exp_by_month: dict[str, float] = {}
    for r in cur.fetchall():
        m = r["month"]
        exp_by_month[m] = exp_by_month.get(m, 0) + _conv(r["amt"], r["code"], currency, m)

    n_months = len(exp_by_month)
    avg_monthly_expense = sum(exp_by_month.values()) / n_months if n_months else 0
    runway_months = round(total_value / avg_monthly_expense, 1) if avg_monthly_expense else None

    # ── Пассивный доход ──
    PASSIVE_KEYWORDS = ("процент", "дивиденд", "купон", "рент", "аренд", "пассив", "роялт", "кэшбэк")

    cur.execute("""
        SELECT cat.name, strftime('%Y-%m', e.created_at) as month,
               CAST(e.amount AS REAL) as amt, c.code
        FROM entries e
        LEFT JOIN categories cat ON cat.id=e.category_id
        JOIN currencies c ON c.id=e.currency_id
        WHERE e.user_id=? AND e.mode='income'
          AND date(e.created_at) >= ?
    """, (user_id, since_1y))
    passive_by_month: dict[str, float] = {}
    passive_cats: set[str] = set()
    this_month = now.strftime("%Y-%m")
    for r in cur.fetchall():
        cat_name = (r["name"] or "").lower()
        if not any(kw in cat_name for kw in PASSIVE_KEYWORDS):
            continue
        passive_cats.add(r["name"] or "")
        m = r["month"]
        passive_by_month[m] = passive_by_month.get(m, 0) + _conv(r["amt"], r["code"], currency, m)

    n_passive = len(passive_by_month)
    avg_passive = sum(passive_by_month.values()) / n_passive if n_passive else 0
    passive_this_month = passive_by_month.get(this_month, 0)

    con.close()

    return JSONResponse({
        "currency": currency,
        "total": round(total_value, 2),
        "positions": positions,
        "by_category": by_category,
        "timeline": timeline,
        "runway": {
            "months": runway_months,
            "avg_monthly_expense": round(avg_monthly_expense, 2),
            "expense_months_count": n_months,
        },
        "passive_income": {
            "this_month": round(passive_this_month, 2),
            "avg_monthly": round(avg_passive, 2),
            "categories": sorted(passive_cats),
        },
    })


@app.get("/api/analytics")
async def analytics_data(
    user_id: int = Query(...),
    period: str = Query("6m"),
    currency: str = Query("VND"),
):
    if not DB_PATH.exists():
        return JSONResponse({})

    curr_rates = get_rates_to_usd()

    con = get_db()
    hist_rates = load_historical_rates(con)

    def _conv(amount: float, src: str, dst: str, month: str) -> float:
        return convert_h(amount, src, dst, month, hist_rates, curr_rates)

    cur = con.cursor()

    # Период
    now = datetime.now(timezone.utc)
    months_map = {"1m": 1, "3m": 3, "6m": 6, "1y": 12, "all": 999}
    months_back = months_map.get(period, 6)
    since = "2000-01-01" if months_back == 999 else (now - timedelta(days=30 * months_back)).strftime("%Y-%m-%d")

    # ── Месячные данные ──
    cur.execute("""
        SELECT strftime('%Y-%m', e.created_at) as month, e.mode,
               CAST(e.amount AS REAL) as amt, c.code
        FROM entries e JOIN currencies c ON c.id=e.currency_id
        WHERE e.user_id=? AND e.mode IN ('expense','income')
          AND date(e.created_at)>=?
    """, (user_id, since))
    monthly_exp: dict[str, float] = {}
    monthly_inc: dict[str, float] = {}
    for r in cur.fetchall():
        val = _conv(r["amt"], r["code"], currency, r["month"])
        m = r["month"]
        if r["mode"] == "expense":
            monthly_exp[m] = monthly_exp.get(m, 0) + val
        else:
            monthly_inc[m] = monthly_inc.get(m, 0) + val

    all_months = sorted(set(list(monthly_exp.keys()) + list(monthly_inc.keys())))
    monthly_chart = []
    for m in all_months:
        y, mo = int(m[:4]), int(m[5:])
        monthly_chart.append({
            "month": m,
            "label": RU_MONTHS[mo - 1] + " " + str(y)[2:],
            "expense": round(monthly_exp.get(m, 0)),
            "income":  round(monthly_inc.get(m, 0)),
        })

    # ── Топ категорий расходов ──
    cur.execute("""
        SELECT cat.name, strftime('%Y-%m', e.created_at) as month,
               CAST(e.amount AS REAL) as amt, c.code
        FROM entries e
        LEFT JOIN categories cat ON cat.id=e.category_id
        JOIN currencies c ON c.id=e.currency_id
        WHERE e.user_id=? AND e.mode='expense' AND date(e.created_at)>=?
    """, (user_id, since))
    cat_totals: dict[str, float] = {}
    for r in cur.fetchall():
        name = r["name"] or "Без категории"
        val = _conv(r["amt"], r["code"], currency, r["month"])
        cat_totals[name] = cat_totals.get(name, 0) + val

    sorted_cats = sorted(cat_totals.items(), key=lambda x: x[1], reverse=True)[:8]
    total_cats = sum(v for _, v in sorted_cats) or 1
    top_categories = [
        {"name": name, "amount": round(val), "pct": round(val / total_cats * 100)}
        for name, val in sorted_cats
    ]

    # ── Дневной тренд (30 дней) ──
    cur.execute("""
        SELECT date(e.created_at) as day,
               strftime('%Y-%m', e.created_at) as month,
               CAST(e.amount AS REAL) as amt, c.code
        FROM entries e JOIN currencies c ON c.id=e.currency_id
        WHERE e.user_id=? AND e.mode='expense'
          AND date(e.created_at) >= date('now','-30 days')
    """, (user_id,))
    daily_raw: dict[str, float] = {}
    for r in cur.fetchall():
        val = _conv(r["amt"], r["code"], currency, r["month"])
        daily_raw[r["day"]] = daily_raw.get(r["day"], 0) + val

    daily_trend = []
    for i in range(30):
        d = (now - timedelta(days=29 - i)).strftime("%Y-%m-%d")
        mo = int(d[5:7]); day = int(d[8:10])
        daily_trend.append({
            "date": d,
            "label": f"{day} {RU_MONTHS[mo-1]}",
            "amount": round(daily_raw.get(d, 0)),
        })

    # ── Summary текущего месяца ──
    this_month = now.strftime("%Y-%m")
    cur.execute("""
        SELECT COUNT(*) as cnt FROM entries WHERE user_id=? AND date(created_at)>=?
    """, (user_id, since))
    total_entries = cur.fetchone()["cnt"]

    con.close()

    return JSONResponse({
        "currency": currency,
        "monthly": monthly_chart,
        "top_categories": top_categories,
        "daily_trend": daily_trend,
        "summary": {
            "exp_month": round(monthly_exp.get(this_month, 0)),
            "inc_month": round(monthly_inc.get(this_month, 0)),
            "currency": currency,
            "total_entries": total_entries,
        }
    })
