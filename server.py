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


@app.get("/api/realty-history")
async def realty_history(
    user_id: int = Query(...),
    realty_id: int = Query(...),
    currency: str = Query("USD"),
):
    """История стоимости объекта недвижимости по realty_rates."""
    if not DB_PATH.exists():
        return JSONResponse({"points": []})

    curr_rates = get_rates_to_usd()
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT r.area_m2, r.currency as src_currency,
               rr.price_per_m2, rr.rate_date,
               strftime('%Y-%m', rr.rate_date) as month
        FROM realty r
        JOIN realty_rates rr ON rr.realty_id = r.id
        WHERE r.id = ? AND r.user_id = ?
        ORDER BY rr.rate_date
    """, (realty_id, user_id))
    rows = cur.fetchall()
    con.close()

    if not rows:
        return JSONResponse({"points": []})

    hist_rates = load_historical_rates(con) if False else {}  # realty in RUB — use curr_rates monthly
    dst_r = curr_rates.get(currency, 0)
    src_currency = rows[0]["src_currency"]

    points = []
    today = datetime.now(timezone.utc)
    today_month = today.strftime("%Y-%m")

    for rr in rows:
        value_src = rr["area_m2"] * rr["price_per_m2"]
        src_r = curr_rates.get(src_currency, 0)
        val = value_src * src_r / dst_r if src_r and dst_r else 0.0
        mo_idx = int(rr["month"][5:]) - 1
        is_current = rr["month"] == today_month
        points.append({
            "month": rr["month"],
            "label": RU_MONTHS[mo_idx] + " " + rr["month"][2:4],
            "qty": rr["area_m2"],
            "value": round(val, 2),
            "is_snapshot": True,
            "is_current": is_current,
        })

    return JSONResponse({"points": points})


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
    # ── Недвижимость ──
    cur.execute("""
        SELECT r.id, r.address, r.area_m2, r.currency,
               rr.price_per_m2, rr.rate_date
        FROM realty r
        JOIN realty_rates rr ON rr.realty_id = r.id
        WHERE r.user_id = ?
          AND rr.rate_date = (
              SELECT MAX(rr2.rate_date) FROM realty_rates rr2 WHERE rr2.realty_id = r.id
          )
    """, (user_id,))
    for r in cur.fetchall():
        value_src = r["area_m2"] * r["price_per_m2"]
        src_r = curr_rates.get(r["currency"], 0)
        dst_r = curr_rates.get(currency, 0)
        val = value_src * src_r / dst_r if src_r and dst_r else 0.0
        positions.append({
            "ticker": r["address"],
            "category": "Недвижимость",
            "qty": r["area_m2"],
            "value": round(val, 2),
            "month": r["rate_date"][:7],
            "day": r["rate_date"],
            "realty_id": r["id"],
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

    # ── Недвижимость: все ставки для timeline ──
    cur.execute("""
        SELECT r.id, r.area_m2, r.currency,
               rr.price_per_m2, strftime('%Y-%m', rr.rate_date) as rate_month
        FROM realty r
        JOIN realty_rates rr ON rr.realty_id = r.id
        WHERE r.user_id = ?
        ORDER BY r.id, rr.rate_date
    """, (user_id,))
    realty_all_rates = cur.fetchall()

    def _realty_value_at(month: str) -> float:
        """Стоимость всей недвижимости по последней ставке <= month."""
        latest: dict[int, sqlite3.Row] = {}
        for rr in realty_all_rates:
            if rr["rate_month"] <= month:
                latest[rr["id"]] = rr
        total = 0.0
        for rr in latest.values():
            value_src = rr["area_m2"] * rr["price_per_m2"]
            total += _conv(value_src, rr["currency"], currency, month)
        return total

    # ── Динамика: один срез на каждый месяц ──
    cur.execute("""
        SELECT DISTINCT strftime('%Y-%m', created_at) as month
        FROM entries WHERE mode='asset' AND user_id=?
        ORDER BY month
    """, (user_id,))
    entry_months = {r["month"] for r in cur.fetchall()}

    realty_months = {rr["rate_month"] for rr in realty_all_rates}
    all_months = sorted(entry_months | realty_months)

    timeline = []
    for m in all_months:
        # Последний снапшот каждого тикера ДО конца месяца
        cur.execute("""
            SELECT c.code, CAST(e.amount AS REAL) as amt
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
        snap_total = sum(_conv(r["amt"], r["code"], currency, m) for r in cur.fetchall())
        snap_total += _realty_value_at(m)
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


@app.get("/model")
async def model_page():
    return FileResponse(WEB_DIR / "model.html")


@app.get("/fin-model")
async def fin_model_page():
    return FileResponse(WEB_DIR / "fin_model.html")


@app.get("/api/model")
async def model_data(
    user_id: int = Query(...),
    monthly_saving: float = Query(580200),
    target: float = Query(10000000),
):
    if not DB_PATH.exists():
        return JSONResponse({"error": "no db"}, status_code=500)

    con = get_db()
    cur = con.cursor()

    # Все снапшоты вклада (mode='asset', категория 'Вклад'), сортировка по created_at
    cur.execute("""
        SELECT CAST(e.amount AS REAL) as amount,
               strftime('%Y-%m', e.created_at) as month,
               date(e.created_at) as day
        FROM entries e
        JOIN categories cat ON cat.id = e.category_id
        WHERE e.user_id = ?
          AND e.mode = 'asset'
          AND cat.name = 'Вклад'
        ORDER BY e.created_at
    """, (user_id,))
    snapshots = cur.fetchall()
    con.close()

    # Фактические данные (все снапшоты)
    actual = []
    for s in snapshots:
        mo_idx = int(s["month"][5:]) - 1
        actual.append({
            "month": s["month"],
            "label": RU_MONTHS[mo_idx] + " " + s["month"][2:4],
            "amount": round(s["amount"], 2),
        })

    # Текущий баланс — последняя запись с amount > 0
    latest_actual = 0.0
    last_month = None
    for s in reversed(snapshots):
        if s["amount"] > 0:
            latest_actual = s["amount"]
            last_month = s["month"]
            break

    # Если нет данных — берём нулевой старт
    if last_month is None:
        now = datetime.now(timezone.utc)
        last_month = now.strftime("%Y-%m")

    # Прогресс
    progress_pct = round(latest_actual / target * 100, 1) if target else 0

    # Ежемесячный процент по базовой ставке (12% годовых)
    monthly_interest_current = round(latest_actual * 0.12 / 12)

    # Строим прогноз по 3 сценариям
    scenarios = {
        "optimistic": 0.20,
        "base": 0.12,
        "conservative": 0.08,
    }

    def next_month(ym: str) -> str:
        y, m = int(ym[:4]), int(ym[5:])
        m += 1
        if m > 12:
            m = 1
            y += 1
        return f"{y:04d}-{m:02d}"

    def month_label(ym: str) -> str:
        mo_idx = int(ym[5:]) - 1
        return RU_MONTHS[mo_idx] + " " + ym[2:4]

    # Подсчитываем месяцы между двумя YYYY-MM строками
    def months_between(from_ym: str, to_ym: str) -> int:
        fy, fm = int(from_ym[:4]), int(from_ym[5:])
        ty, tm = int(to_ym[:4]), int(to_ym[5:])
        return (ty - fy) * 12 + (tm - fm)

    now = datetime.now(timezone.utc)
    current_month = now.strftime("%Y-%m")

    forecast = {}
    targets_info = {}

    for name, annual_rate in scenarios.items():
        monthly_rate = annual_rate / 12
        series = []
        balance = latest_actual
        m = next_month(last_month)
        months_count = 0

        while months_count < 36:
            interest = round(balance * monthly_rate)
            balance = balance * (1 + monthly_rate) + monthly_saving
            mo_idx = int(m[5:]) - 1
            series.append({
                "month": m,
                "label": month_label(m),
                "amount": round(balance, 2),
                "interest": interest,
            })
            months_count += 1
            if balance >= target:
                break
            m = next_month(m)

        forecast[name] = series

        # Когда достигнем цели
        if series and series[-1]["amount"] >= target:
            target_month = series[-1]["month"]
            months_left = months_between(current_month, target_month)
            targets_info[name] = {
                "month": target_month,
                "label": month_label(target_month),
                "months_left": months_left,
            }
        else:
            targets_info[name] = None

    return JSONResponse({
        "target": target,
        "monthly_saving": monthly_saving,
        "current": latest_actual,
        "actual": actual,
        "forecast": forecast,
        "targets": targets_info,
        "progress_pct": progress_pct,
        "monthly_interest_current": monthly_interest_current,
    })


@app.get("/api/fin-model")
async def fin_model_data(
    user_id: int = Query(...),
    monthly_saving: float = Query(580200),
    apartment_cost: float = Query(10000000),
    rent_gross: float = Query(65000),
    vacancy_rate: float = Query(0.15),
    mgmt_fee: float = Query(0.10),
    maintenance: float = Query(0.05),
    tax_rate: float = Query(0.04),
    stock_monthly: float = Query(22500),
    deposit_rate_conservative: float = Query(0.08),
):
    if not DB_PATH.exists():
        return JSONResponse({"error": "no db"}, status_code=500)

    curr_rates = get_rates_to_usd()
    con = get_db()
    hist_rates = load_historical_rates(con)
    cur = con.cursor()
    now = datetime.now(timezone.utc)
    since_1y = (now - timedelta(days=365)).strftime("%Y-%m-%d")

    def _conv_rub(amount: float, src: str, month: str) -> float:
        """Конвертация в RUB через USD."""
        if src == "RUB":
            return amount
        src_r = hist_rates.get((src, month)) or curr_rates.get(src, 0)
        rub_r = hist_rates.get(("RUB", month)) or curr_rates.get("RUB", 0)
        if not src_r or not rub_r:
            return 0.0
        return amount * src_r / rub_r

    # ── A. Текущее состояние ──

    # Среднемесячные расходы за последние 12 мес
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
        exp_by_month[m] = exp_by_month.get(m, 0) + _conv_rub(r["amt"], r["code"], m)
    n_exp = len(exp_by_month)
    avg_expenses = sum(exp_by_month.values()) / n_exp if n_exp else 1.0

    # Пассивный доход (по ключевым словам категорий) за последние 12 мес
    PASSIVE_KEYWORDS = ("процент", "дивиденд", "купон", "рент", "аренд")
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
    for r in cur.fetchall():
        cat_name = (r["name"] or "").lower()
        if not any(kw in cat_name for kw in PASSIVE_KEYWORDS):
            continue
        m = r["month"]
        passive_by_month[m] = passive_by_month.get(m, 0) + _conv_rub(r["amt"], r["code"], m)
    n_passive = len(passive_by_month)
    passive_income_monthly = sum(passive_by_month.values()) / n_passive if n_passive else 0.0

    # Баланс вклада — последняя запись mode='asset', категория 'Вклад', amount > 0
    cur.execute("""
        SELECT CAST(e.amount AS REAL) as amount,
               strftime('%Y-%m', e.created_at) as month,
               c.code
        FROM entries e
        JOIN categories cat ON cat.id = e.category_id
        JOIN currencies c ON c.id = e.currency_id
        WHERE e.user_id = ? AND e.mode = 'asset' AND cat.name = 'Вклад'
          AND CAST(e.amount AS REAL) > 0
        ORDER BY e.created_at DESC
        LIMIT 1
    """, (user_id,))
    dep_row = cur.fetchone()
    deposit_balance_rub = 0.0
    last_deposit_month = None
    if dep_row:
        deposit_balance_rub = _conv_rub(dep_row["amount"], dep_row["code"], dep_row["month"])
        last_deposit_month = dep_row["month"]

    # Ликвидные активы (вклад + USD-кэш)
    cur.execute("""
        SELECT CAST(e.amount AS REAL) as amt, c.code,
               strftime('%Y-%m', e.created_at) as month
        FROM entries e
        JOIN currencies c ON c.id = e.currency_id
        WHERE e.user_id = ? AND e.mode = 'asset' AND c.code = 'USD'
          AND e.created_at = (
              SELECT MAX(e2.created_at) FROM entries e2
              WHERE e2.mode='asset' AND e2.user_id=e.user_id
                AND e2.currency_id=e.currency_id
          )
          AND CAST(e.amount AS REAL) > 0
    """, (user_id,))
    usd_row = cur.fetchone()
    usd_rub = 0.0
    if usd_row:
        usd_rub = _conv_rub(usd_row["amt"], "USD", usd_row["month"])
    liquid_assets = deposit_balance_rub + usd_rub
    liquid_months = round(liquid_assets / avg_expenses, 1) if avg_expenses else 0.0

    deposit_passive_conservative = round(deposit_balance_rub * deposit_rate_conservative / 12)
    gcr_current = round(passive_income_monthly / avg_expenses, 3) if avg_expenses else 0.0

    # ── B. Чистая аренда ──
    net_rent = rent_gross * (1 - vacancy_rate) * (1 - mgmt_fee) * (1 - maintenance) * (1 - tax_rate)
    net_rent = round(net_rent)
    gcr_post_purchase = round(net_rent / avg_expenses, 3) if avg_expenses else 0.0

    # ── C. Прогноз фазы 1 ──
    def next_month(ym: str) -> str:
        y, mo = int(ym[:4]), int(ym[5:])
        mo += 1
        if mo > 12:
            mo = 1
            y += 1
        return f"{y:04d}-{mo:02d}"

    def month_label(ym: str) -> str:
        mo_idx = int(ym[5:]) - 1
        return RU_MONTHS[mo_idx] + " " + ym[2:4]

    def months_between(from_ym: str, to_ym: str) -> int:
        fy, fm = int(from_ym[:4]), int(from_ym[5:])
        ty, tm = int(to_ym[:4]), int(to_ym[5:])
        return (ty - fy) * 12 + (tm - fm)

    current_month = now.strftime("%Y-%m")
    start_month = last_deposit_month if last_deposit_month else current_month

    scenarios_rates = {"optimistic": 0.20, "base": 0.14, "conservative": 0.10}
    phase1_forecast: dict[str, list] = {}
    reach_target: dict[str, dict | None] = {}

    for name, annual_rate in scenarios_rates.items():
        monthly_rate = annual_rate / 12
        series = []
        balance = deposit_balance_rub
        m = next_month(start_month)
        months_count = 0
        while months_count < 48:
            interest = round(balance * monthly_rate)
            balance = balance * (1 + monthly_rate) + monthly_saving
            mo_idx = int(m[5:]) - 1
            series.append({
                "month": m,
                "label": month_label(m),
                "amount": round(balance, 2),
                "interest": interest,
            })
            months_count += 1
            if balance >= apartment_cost:
                break
            m = next_month(m)
        phase1_forecast[name] = series
        if series and series[-1]["amount"] >= apartment_cost:
            target_month = series[-1]["month"]
            months_left = months_between(current_month, target_month)
            reach_target[name] = {
                "month": target_month,
                "label": month_label(target_month),
                "months_left": months_left,
            }
        else:
            reach_target[name] = None

    # ── D. GCR прогноз по годам ──
    # Определяем год покупки квартиры (базовый сценарий)
    purchase_year = None
    if reach_target["base"]:
        purchase_year = int(reach_target["base"]["month"][:4])
    elif reach_target["optimistic"]:
        purchase_year = int(reach_target["optimistic"]["month"][:4])
    else:
        purchase_year = 2030  # fallback

    dividend_yield = 0.07
    haircut = 0.6
    gcr_100_year = None
    gcr_forecast = []

    # Баланс вклада на начало расчёта
    sim_balance = deposit_balance_rub
    base_rate = 0.14 / 12
    # Симуляция по месяцам базового сценария для точного баланса по годам
    balance_by_month: dict[str, float] = {start_month: deposit_balance_rub}
    m = next_month(start_month)
    for _ in range(72):
        sim_balance = sim_balance * (1 + base_rate) + monthly_saving
        balance_by_month[m] = sim_balance
        if sim_balance >= apartment_cost:
            break
        m = next_month(m)

    # Накопленные инвестиции в акции к году
    stock_accumulated_by_year: dict[int, float] = {}
    for yr in range(2026, 2032):
        months_from_now = (yr - int(current_month[:4])) * 12
        stock_accumulated_by_year[yr] = stock_monthly * max(0, months_from_now)

    for year in range(2026, 2032):
        year_month = f"{year}-06"
        if year < purchase_year:
            # Фаза 1: пассивный доход = проценты по вкладу (cons ставка)
            dep_bal = balance_by_month.get(year_month) or deposit_balance_rub
            dep_passive = dep_bal * deposit_rate_conservative / 12
            passive = passive_income_monthly + dep_passive
            phase = 1
        else:
            # Фаза 2/3: аренда + дивиденды
            years_since_purchase = year - purchase_year
            stock_total = stock_accumulated_by_year.get(year, 0)
            dividends = stock_total * dividend_yield * haircut / 12
            passive = net_rent + dividends
            phase = 2 if years_since_purchase < 3 else 3
        gcr = round(passive / avg_expenses, 3) if avg_expenses else 0.0
        gcr_forecast.append({
            "year": year,
            "gcr": gcr,
            "gcr_pct": round(gcr * 100, 1),
            "passive": round(passive),
            "phase": phase,
        })
        if gcr >= 1.0 and gcr_100_year is None:
            gcr_100_year = year

    # ── E. Стресс-тесты ──
    def stress_status(gcr_val: float) -> str:
        if gcr_val >= 0.5:
            return "ok"
        elif gcr_val >= 0.25:
            return "warning"
        else:
            return "danger"

    stress_tests = []
    # Базовый
    g = gcr_post_purchase
    stress_tests.append({"name": "Базовый", "gcr": round(g, 3), "gcr_pct": round(g * 100, 1),
                          "passive": round(net_rent), "status": stress_status(g)})
    # Аренда -30%
    passive_s = net_rent * 0.7
    g = round(passive_s / avg_expenses, 3) if avg_expenses else 0.0
    stress_tests.append({"name": "Аренда −30%", "gcr": g, "gcr_pct": round(g * 100, 1),
                          "passive": round(passive_s), "status": stress_status(g)})
    # Простой 6 мес.
    passive_s = net_rent * (6 / 12)
    g = round(passive_s / avg_expenses, 3) if avg_expenses else 0.0
    stress_tests.append({"name": "Простой 6 мес.", "gcr": g, "gcr_pct": round(g * 100, 1),
                          "passive": round(passive_s), "status": stress_status(g)})
    # Расходы +30%
    passive_s = net_rent
    g = round(passive_s / (avg_expenses * 1.3), 3) if avg_expenses else 0.0
    stress_tests.append({"name": "Расходы +30%", "gcr": g, "gcr_pct": round(g * 100, 1),
                          "passive": round(passive_s), "status": stress_status(g)})
    # Комбо-кризис
    passive_s = net_rent * 0.7 * (9 / 12)
    g = round(passive_s / (avg_expenses * 1.2), 3) if avg_expenses else 0.0
    stress_tests.append({"name": "Комбо-кризис", "gcr": g, "gcr_pct": round(g * 100, 1),
                          "passive": round(passive_s), "status": stress_status(g)})

    con.close()

    progress_pct = round(deposit_balance_rub / apartment_cost * 100, 1) if apartment_cost else 0.0

    return JSONResponse({
        "current": {
            "gcr": gcr_current,
            "gcr_pct": round(gcr_current * 100, 1),
            "passive_income": round(passive_income_monthly),
            "avg_expenses": round(avg_expenses),
            "deposit_balance": round(deposit_balance_rub),
            "liquid_months": liquid_months,
            "deposit_passive_conservative": deposit_passive_conservative,
        },
        "rental": {
            "gross": round(rent_gross),
            "net": net_rent,
            "vacancy_rate": vacancy_rate,
            "mgmt_fee": mgmt_fee,
            "maintenance": maintenance,
            "tax_rate": tax_rate,
            "gcr_post_purchase": gcr_post_purchase,
            "gcr_post_purchase_pct": round(gcr_post_purchase * 100, 1),
        },
        "phase1": {
            "target": apartment_cost,
            "progress_pct": progress_pct,
            "optimistic": phase1_forecast["optimistic"],
            "base": phase1_forecast["base"],
            "conservative": phase1_forecast["conservative"],
            "reach_target": reach_target,
        },
        "gcr_forecast": gcr_forecast,
        "stress_tests": stress_tests,
        "monthly_saving": monthly_saving,
        "stock_monthly": stock_monthly,
        "gcr_100_year": gcr_100_year,
        "purchase_year": purchase_year,
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
