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
from fastapi import FastAPI, Query, Depends
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import update_daily_rates
import update_monthly_rates
import db_repo
import fin_model_data
import metrics
from metrics import load_historical_rates, convert_h, avg_monthly_expense
from tg_auth import require_user_id

# Символы валют для форматирования сумм в событиях
CUR_SYM = {"RUB": "₽", "USD": "$", "USDT": "$", "VND": "₫", "EUR": "€", "THB": "฿", "MYR": "RM"}


def _sym(code: str) -> str:
    return CUR_SYM.get(code, code)

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


def _run_dividends():
    """1-го числа — пере-парсим дивиденды dohod (факт/объявлено/прогноз/корреляция)."""
    try:
        fin_model_data.refresh_dividend_cache()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error("dividends refresh failed: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = AsyncIOScheduler(timezone="UTC")
    # Ежедневно в 02:00 UTC — обновляем текущий курс
    scheduler.add_job(_run_daily, "cron", hour=2, minute=0, id="daily_rates")
    # 1-го числа в 03:00 UTC — фиксируем среднемесячный курс за прошлый месяц
    scheduler.add_job(_run_monthly, "cron", day=1, hour=3, minute=0, id="monthly_rates")
    # 1-го числа в 04:00 UTC — обновляем дивиденды (после фиксации курсов)
    scheduler.add_job(_run_dividends, "cron", day=1, hour=4, minute=0, id="dividends")
    scheduler.start()
    # Первичное наполнение кэша дивидендов, если его ещё нет (фоном, чтобы не тормозить старт)
    if not fin_model_data.CACHE_PATH.exists():
        import asyncio
        asyncio.get_running_loop().run_in_executor(None, _run_dividends)
    yield
    scheduler.shutdown()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")



class EntryCreate(BaseModel):
    mode: str
    amount: float
    currency: str
    category: str | None = None
    note: str | None = None


class EntryUpdate(BaseModel):
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
async def create_entry(body: EntryCreate, user_id: int = Depends(require_user_id)):
    import asyncio
    entry_id = await asyncio.to_thread(
        db_repo.save_entry,
        user_id=user_id,
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
    user_id: int = Depends(require_user_id),
    limit: int = Query(30),
    offset: int = Query(0),
):
    if not DB_PATH.exists():
        return JSONResponse({"entries": [], "total": 0})
    entries, total = db_repo.get_entries(user_id, limit, offset, db_path=DB_PATH)
    return JSONResponse({"entries": entries, "total": total})


@app.delete("/api/entry/{entry_id}")
async def delete_entry(entry_id: int, user_id: int = Depends(require_user_id)):
    if not DB_PATH.exists():
        return JSONResponse({"ok": False}, status_code=404)
    deleted = db_repo.delete_entry(user_id, entry_id, db_path=DB_PATH)
    return JSONResponse({"ok": deleted}, status_code=200 if deleted else 404)


@app.patch("/api/entry/{entry_id}")
async def update_entry(
    entry_id: int, body: EntryUpdate, user_id: int = Depends(require_user_id)
):
    if not DB_PATH.exists():
        return JSONResponse({"ok": False}, status_code=404)
    updated = db_repo.update_entry(
        user_id=user_id,
        entry_id=entry_id,
        amount=body.amount,
        currency_code=body.currency,
        category_name=body.category,
        note=body.note,
        db_path=DB_PATH,
    )
    return JSONResponse({"ok": updated}, status_code=200 if updated else 404)


@app.get("/api/user-data")
async def user_data(user_id: int = Depends(require_user_id)):
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
    user_id: int = Depends(require_user_id),
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
    user_id: int = Depends(require_user_id),
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
    user_id: int = Depends(require_user_id),
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

    # ── Финансовый запас: сколько месяцев проживём на ЛИКВИДНОСТИ ──
    # Считаем только по категории «Ликвидность» (не по всему капиталу — акции/крипта
    # волатильны и не предназначены для проедания). Среднемесячные расходы — единый
    # централизованный расчёт (metrics.avg_monthly_expense), как на аналитике/в фин-модели.
    since_1y = (now - timedelta(days=365)).strftime("%Y-%m-%d")
    LIQUID_CATEGORIES = {"Ликвидность"}
    liquid_value = sum(p["value"] for p in positions if p["category"] in LIQUID_CATEGORIES)
    exp_metric = avg_monthly_expense(con, user_id, currency, curr_rates, hist_rates)
    avg_exp = exp_metric["avg"]
    runway_months = round(liquid_value / avg_exp, 1) if avg_exp else None
    # Целевой уровень подушки — 1 год расходов
    TARGET_MONTHS = 12
    target_value = round(avg_exp * TARGET_MONTHS, 2) if avg_exp else 0
    coverage_pct = round(liquid_value / target_value * 100) if target_value else None
    target_gap = round(target_value - liquid_value, 2) if target_value else 0

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
            "avg_monthly_expense": round(avg_exp, 2),
            "expense_months_count": exp_metric["months_count"],
            "liquid_value": round(liquid_value, 2),
            "target_months": TARGET_MONTHS,
            "target_value": target_value,
            "coverage_pct": coverage_pct,
            "target_gap": target_gap,
        },
        "passive_income": {
            "this_month": round(passive_this_month, 2),
            "avg_monthly": round(avg_passive, 2),
            "categories": sorted(passive_cats),
        },
    })


@app.get("/api/asset-events")
async def asset_events(
    user_id: int = Depends(require_user_id),
    currency: str = Query("RUB"),
):
    """Авто-сводка «что повлияло на портфель за ~месяц» для страницы Активы.
    Все суммы — в валюте `currency` (по умолчанию RUB)."""
    if not DB_PATH.exists():
        return JSONResponse({"events": []})
    SYM = _sym(currency)
    con = get_db()
    cur = con.cursor()

    # Курсы: серия по каждому коду + последняя дата
    cur.execute("SELECT currency_code AS code, rate_date AS d, CAST(rate_to_usd AS REAL) AS r "
                "FROM currency_rates ORDER BY currency_code, rate_date")
    series: dict[str, list] = {}
    has_rates = False
    for row in cur.fetchall():
        series.setdefault(row["code"], []).append((row["d"], row["r"]))
        has_rates = True
    if not has_rates:
        con.close()
        return JSONResponse({"events": []})

    # Окно: завершённый прошлый месяц по архивным срезам 1-го числа.
    # now_d = начало текущего месяца (= конец прошлого), ago = начало прошлого месяца.
    today_dt = datetime.now(timezone.utc)
    ly, lm = today_dt.year, today_dt.month
    now_d = f"{ly:04d}-{lm:02d}-01"
    py, pm = (ly, lm - 1) if lm > 1 else (ly - 1, 12)
    ago = f"{py:04d}-{pm:02d}-01"

    def rate_at(code, iso):
        v = None
        for d, r in series.get(code, []):
            if d <= iso:
                v = r
            else:
                break
        return v

    def now_then(code):
        return rate_at(code, now_d), rate_at(code, ago)

    rub_now, rub_then = now_then("RUB")
    cur_now, cur_then = now_then(currency)
    if not rub_now or not rub_then or not cur_now or not cur_then:
        con.close()
        return JSONResponse({"events": []})

    def pct(a, b):
        return (a / b - 1) * 100 if a and b else 0

    # Какие валюты реально лежат в НЕСКОЛЬКИХ категориях одновременно (по дедуп-таблице
    # asset_latest_values). RUB — в Вклад/Инвесткопилка/Акции (3 пула) → считаем по (код+кат)
    # и суммируем. USDT/USD/акции/крипта — в одной категории → по коду (перекладка между
    # категориями = смена ярлыка, мержим историю, новыми деньгами не считаем).
    cur.execute("SELECT currency_code AS code, COALESCE(category_name,'') AS cat "
                "FROM asset_latest_values WHERE user_id=?", (user_id,))
    code_cats: dict = {}
    for r in cur.fetchall():
        code_cats.setdefault(r["code"], set()).add(r["cat"])
    multi = {code for code, cats in code_cats.items() if len(cats) > 1}

    cur.execute("""
        SELECT c.code AS code, COALESCE(cat.name,'') AS category,
               date(e.created_at) AS d, CAST(e.amount AS REAL) AS qty
        FROM entries e
        JOIN currencies c ON c.id=e.currency_id
        LEFT JOIN categories cat ON cat.id=e.category_id
        WHERE e.mode='asset' AND e.user_id=?
        ORDER BY c.code, e.created_at
    """, (user_id,))
    by_code: dict = {}      # ключ: code (single) или (code, category) для мультипула
    stock_codes = set()
    for r in cur.fetchall():
        key = (r["code"], r["category"]) if r["code"] in multi else r["code"]
        by_code.setdefault(key, []).append((r["d"], r["qty"]))
        if r["category"] == "Акции":
            stock_codes.add(r["code"])

    def key_code(k):
        return k[0] if isinstance(k, tuple) else k

    def shares_at(series, iso):
        q = 0.0
        for d, v in series:
            if d <= iso:
                q = v
            else:
                break
        return q

    # Цена 1 единицы `code` в выбранной валюте на момент now/then.
    def price_rub(code, when):
        if code == currency:
            return 1.0
        n, t = now_then(code)
        r, base = (n, cur_now) if when == "now" else (t, cur_then)
        return r / base if (r and base) else None

    def money(v):
        return f"{round(v):,}".replace(",", " ")

    # Сводный расчёт по всему портфелю: стоимость сейчас/месяц назад, рынок, пополнения
    value_now = value_then = market = 0.0
    held = set()
    for key, hist in by_code.items():
        code = key_code(key)
        qn = shares_at(hist, now_d)
        qt = shares_at(hist, ago)
        if qn > 0:
            held.add(code)
        pn, pt = price_rub(code, "now"), price_rub(code, "then")
        if pn is not None:
            value_now += qn * pn
        if pt is not None:
            value_then += qt * pt
        if pn is not None and pt is not None and code != currency:
            market += qt * (pn - pt)
    net_change = value_now - value_then
    contributed = net_change - market

    events = []

    # A. Чистый прирост за месяц (с учётом пополнений, просадок и переоценки)
    if value_then:
        nch = net_change / value_then * 100
        events.append({"icon": "📊", "tone": "good" if net_change >= 0 else "bad", "value": nch, "priority": 100,
                       "title": f"Чистый прирост портфеля: {'+' if net_change >= 0 else '−'}{money(abs(net_change))} {SYM}",
                       "detail": f"За месяц {nch:+.1f}% — с учётом пополнений, просадок и переоценки."})

    # A2. Вложено новых денег (чистые пополнения; внутренние переводы взаимозачитываются)
    if abs(contributed) >= 1000:
        pos = contributed >= 0
        events.append({"icon": "➕" if pos else "➖", "tone": "good" if pos else "neutral",
                       "value": 0, "priority": 95,
                       "title": f"{'Вложено' if pos else 'Выведено'} за месяц: {'+' if pos else '−'}{money(abs(contributed))} {SYM}",
                       "detail": "Чистые пополнения портфеля за месяц."})

    # A3. Рынок: переоценка активов за месяц (без операций)
    if value_then and abs(market) >= 1000:
        mch = market / value_then * 100
        events.append({"icon": "💹", "tone": "good" if market >= 0 else "bad", "value": mch, "priority": 80,
                       "title": f"Рынок {'добавил' if market >= 0 else 'забрал'} {'+' if market >= 0 else '−'}{money(abs(market))} {SYM}",
                       "detail": f"Переоценка активов за месяц ({mch:+.1f}%), без учёта пополнений."})

    # B. Рубль к доллару
    rub_ch = pct(rub_now, rub_then)
    if abs(rub_ch) >= 0.5:
        events.append({"icon": "🇷🇺", "tone": "neutral", "value": rub_ch, "priority": 50,
                       "title": f"Рубль {'укрепился' if rub_ch >= 0 else 'ослаб'} на {abs(rub_ch):.1f}% к доллару",
                       "detail": "Меняет рублёвую стоимость валютных и крипто-активов."})

    # C. Крипта — одной карточкой: общий % в $ + разбивка по монетам
    CRYPTO = {"BTC": "BTC", "ETH": "ETH", "SOL": "SOL", "TRX": "TRX"}
    cval_now = cval_then = 0.0
    coin_moves = []
    for code in CRYPTO:
        if code not in held:
            continue
        qn = shares_at(by_code[code], now_d)   # qty на 1-е число (без докупок текущего месяца)
        n, t = now_then(code)
        if n:
            cval_now += qn * n
        if t:
            cval_then += qn * t
        if n and t:
            coin_moves.append((CRYPTO[code], pct(n, t)))
    if cval_then and coin_moves:
        cch = pct(cval_now, cval_then)
        if abs(cch) >= 5:                                   # порог ±5%
            coin_moves.sort(key=lambda x: x[1])
            parts = ", ".join(f"{c} {m:+.0f}%" for c, m in coin_moves)
            events.append({"icon": "🪙", "tone": "good" if cch >= 0 else "bad", "value": cch, "priority": 20,
                           "title": f"Крипта {'выросла' if cch >= 0 else 'просела'} на {abs(cch):.0f}% за месяц",
                           "detail": f"В долларах: {parts}."})

    # D0. Акции — общая плашка движения (порог ±5%)
    sval_now = sval_then = 0.0
    for code in stock_codes:
        if code not in held or code in ("RUB", "TRUR"):
            continue
        qn = shares_at(by_code[code], now_d)
        pn, pt = price_rub(code, "now"), price_rub(code, "then")
        if pn:
            sval_now += qn * pn
        if pt:
            sval_then += qn * pt
    if sval_then:
        sch = pct(sval_now, sval_then)
        if abs(sch) >= 5:
            events.append({"icon": "🏛", "tone": "good" if sch >= 0 else "bad", "value": sch, "priority": 25,
                           "title": f"Акции {'выросли' if sch >= 0 else 'просели'} на {abs(sch):.0f}% за месяц",
                           "detail": "Совокупная переоценка акций портфеля за месяц."})

    # D. Топ-гейнер / топ-лузер среди акций
    moves = []
    for code in stock_codes:
        if code not in held or code in ("RUB", "TRUR"):
            continue
        pn, pt = price_rub(code, "now"), price_rub(code, "then")
        if pn and pt:
            moves.append((code, pct(pn, pt)))
    if moves:
        moves.sort(key=lambda x: x[1])
        worst, best = moves[0], moves[-1]
        if best[1] >= 3:
            events.append({"icon": "📈", "tone": "good", "value": best[1], "priority": 5,
                           "title": f"Лучшая бумага: {best[0]} {best[1]:+.0f}%", "detail": "Рост за месяц."})
        if worst[1] <= -3 and worst[0] != best[0]:
            events.append({"icon": "📉", "tone": "bad", "value": worst[1], "priority": 5,
                           "title": f"Слабее всех: {worst[0]} {worst[1]:+.0f}%", "detail": "Снижение за месяц."})

    # E. Дивиденды/проценты за прошлый месяц
    prev_m = (datetime(ly, lm, 1) - timedelta(days=1)).strftime("%Y-%m")
    cur.execute("""
        SELECT cat.name AS name, CAST(e.amount AS REAL) AS amt, c.code AS code
        FROM entries e LEFT JOIN categories cat ON cat.id=e.category_id
        JOIN currencies c ON c.id=e.currency_id
        WHERE e.user_id=? AND e.mode='income' AND strftime('%Y-%m', e.created_at)=?
    """, (user_id, prev_m))
    KW = ("дивиденд", "купон", "процент", "рент")
    div_sum = 0.0
    for r in cur.fetchall():
        if any(k in (r["name"] or "").lower() for k in KW):
            p = price_rub(r["code"], "now")
            if p is not None:
                div_sum += r["amt"] * p
    if div_sum >= 1:
        events.append({"icon": "💰", "tone": "good", "value": 0, "priority": 30,
                       "title": f"Дивиденды и проценты: {money(div_sum)} {SYM}",
                       "detail": f"Поступило за {prev_m}."})

    # ── Помесячная ретроспектива: рынок (±) и пополнения, до последнего завершённого месяца ──
    def price_at(code, iso):
        if code == currency:
            return 1.0
        r, base = rate_at(code, iso), rate_at(currency, iso)
        return r / base if (r and base) else None

    def period_stats(then_iso, now_iso):
        vn = vt = mk = 0.0
        drivers = []   # (code, market_i) для пояснений
        for key, h in by_code.items():
            code = key_code(key)
            qn, qt = shares_at(h, now_iso), shares_at(h, then_iso)
            pn, pt = price_at(code, now_iso), price_at(code, then_iso)
            if pn is not None:
                vn += qn * pn
            if pt is not None:
                vt += qt * pt
            if pn is not None and pt is not None and code != currency:
                mi = qt * (pn - pt)
                mk += mi
                if abs(mi) >= 1:
                    drivers.append((code, mi))
        drivers.sort(key=lambda x: abs(x[1]), reverse=True)
        return vn, vt, mk, drivers

    def short(v):
        a = abs(round(v))
        if a >= 1_000_000:
            return f"{a/1_000_000:.1f}".rstrip("0").rstrip(".") + " млн"
        if a >= 1000:
            return f"{round(a/1000)}к"
        return str(a)

    # Доходы по месяцам (зарплата/премия/… — поясняют пополнения)
    cur.execute("""
        SELECT strftime('%Y-%m', e.created_at) AS m, COALESCE(cat.name, 'Доход') AS name,
               c.code AS code, SUM(CAST(e.amount AS REAL)) AS amt
        FROM entries e LEFT JOIN categories cat ON cat.id=e.category_id
        JOIN currencies c ON c.id=e.currency_id
        WHERE e.mode='income' AND e.user_id=?
        GROUP BY m, name, c.code
    """, (user_id,))
    income_by_month: dict = {}
    for r in cur.fetchall():
        base = rate_at(currency, r["m"] + "-28")
        rr = rate_at(r["code"], r["m"] + "-28")
        val = r["amt"] if r["code"] == currency else (r["amt"] * rr / base if rr and base else 0)
        income_by_month.setdefault(r["m"], []).append((r["name"], val))

    all_dates = [d for s in by_code.values() for d, _ in s]
    monthly = []
    if all_dates:
        yy, mm = int(min(all_dates)[:4]), int(min(all_dates)[5:7])
        while (yy, mm) <= (py, pm):
            month = f"{yy:04d}-{mm:02d}"
            m_then = f"{month}-01"
            ny, nm = (yy, mm + 1) if mm < 12 else (yy + 1, 1)
            vn, vt, mk, drivers = period_stats(m_then, f"{ny:04d}-{nm:02d}-01")
            if vt > 0:
                # пояснение рынка: топ-активы + курс рубля
                rt, rn = rate_at("RUB", m_then), rate_at("RUB", f"{ny:04d}-{nm:02d}-01")
                rubmv = (rn / rt - 1) * 100 if rt and rn else 0
                mp = [f"{c} {'+' if v >= 0 else '−'}{short(v)}" for c, v in drivers[:2]]
                if abs(rubmv) >= 2:
                    mp.append(f"рубль {rubmv:+.0f}%")
                # пояснение пополнений: топ доходов месяца
                inc = sorted(income_by_month.get(month, []), key=lambda x: -x[1])
                ip = [f"{n} {short(v)}" for n, v in inc[:3] if v >= 10000]
                monthly.append({"month": month, "market": round(mk),
                                "contributed": round((vn - vt) - mk),
                                "market_note": ", ".join(mp), "income_note": ", ".join(ip)})
            yy, mm = ny, nm

    con.close()
    events.sort(key=lambda e: (e["priority"], abs(e["value"])), reverse=True)
    MONTHS_FULL = ["", "январь", "февраль", "март", "апрель", "май", "июнь",
                   "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"]
    return JSONResponse({"period_from": ago, "period_to": now_d,
                         "period_label": f"{MONTHS_FULL[pm]} {py}", "currency": currency,
                         "events": events, "monthly": monthly})


@app.get("/model")
async def model_page():
    return FileResponse(WEB_DIR / "model.html")


@app.get("/fin-model")
async def fin_model_page():
    return FileResponse(WEB_DIR / "fin_model.html")


@app.get("/api/model")
async def model_data(
    user_id: int = Depends(require_user_id),
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
async def fin_model_endpoint(user_id: int = Depends(require_user_id)):
    """Дивидендная фин-модель: портфель из БД + дивиденды из месячного кэша dohod."""
    if not DB_PATH.exists():
        return JSONResponse({"error": "no db"}, status_code=500)
    try:
        data = fin_model_data.build_fin_model(user_id)
    except Exception as e:
        import logging
        logging.getLogger(__name__).error("fin-model build failed: %s", e)
        return JSONResponse({"error": "build failed"}, status_code=500)
    return JSONResponse(data)


@app.get("/api/analytics")
async def analytics_data(
    user_id: int = Depends(require_user_id),
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


@app.get("/api/expense-trends")
async def expense_trends(
    user_id: int = Depends(require_user_id),
    currency: str = Query("RUB"),
):
    """Тренды для аналитики (в валюте `currency`, по завершённым месяцам, окно ≤12 мес):
    1) среднемесячные расходы и их MoM-динамика; 2) доля расходов от доходов."""
    if not DB_PATH.exists():
        return JSONResponse({"months": []})
    SYM = _sym(currency)
    curr_rates = get_rates_to_usd()
    con = get_db()
    hist_rates = load_historical_rates(con)
    cur = con.cursor()

    cur.execute("""
        SELECT strftime('%Y-%m', e.created_at) AS m, e.mode AS mode, COALESCE(cat.name,'Прочее') AS cat,
               CAST(e.amount AS REAL) AS amt, c.code AS code
        FROM entries e JOIN currencies c ON c.id=e.currency_id
        LEFT JOIN categories cat ON cat.id=e.category_id
        WHERE e.user_id=? AND e.mode IN ('expense','income')
    """, (user_id,))
    exp: dict[str, float] = {}
    inc: dict[str, float] = {}
    exp_cat: dict[str, dict] = {}   # month -> {категория: rub}
    inc_cat: dict[str, dict] = {}
    for r in cur.fetchall():
        v = convert_h(r["amt"], r["code"], currency, r["m"], hist_rates, curr_rates)
        if r["mode"] == "expense":
            exp[r["m"]] = exp.get(r["m"], 0) + v
            exp_cat.setdefault(r["m"], {})[r["cat"]] = exp_cat.setdefault(r["m"], {}).get(r["cat"], 0) + v
        else:
            inc[r["m"]] = inc.get(r["m"], 0) + v
            inc_cat.setdefault(r["m"], {})[r["cat"]] = inc_cat.setdefault(r["m"], {}).get(r["cat"], 0) + v
    con.close()

    this_month = datetime.now(timezone.utc).strftime("%Y-%m")
    months = sorted(set(exp) | set(inc))
    completed = [m for m in months if m < this_month]    # текущий неполный месяц не берём
    completed = completed[1:]                            # первый месяц (онбординг, неполный) тоже

    out = []
    prev_avg = prev_ratio = None
    for i, m in enumerate(completed):
        win = completed[max(0, i - 11):i + 1]            # скользящее окно ≤12 мес
        avg_e = sum(exp.get(w, 0) for w in win) / len(win)
        sum_e = sum(exp.get(w, 0) for w in win)
        sum_i = sum(inc.get(w, 0) for w in win)
        ratio = (sum_e / sum_i * 100) if sum_i else 0
        avg_chg = ((avg_e / prev_avg - 1) * 100) if prev_avg else None
        ratio_chg = (ratio - prev_ratio) if prev_ratio is not None else None
        y, mo = int(m[:4]), int(m[5:7])
        out.append({
            "month": m, "label": f"{RU_MONTHS[mo-1]} {str(y)[2:]}",
            "avg_expense": round(avg_e), "avg_change_pct": round(avg_chg, 1) if avg_chg is not None else None,
            "expense_ratio": round(ratio, 1), "ratio_change_pp": round(ratio_chg, 1) if ratio_chg is not None else None,
        })
        prev_avg, prev_ratio = avg_e, ratio

    # ── События за последний завершённый месяц ──
    def money(v):
        return f"{round(v):,}".replace(",", " ")

    MONTHS_FULL = ["", "январь", "февраль", "март", "апрель", "май", "июнь",
                   "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"]
    events = []
    period_label = None
    if completed:
        M = completed[-1]
        prevM = completed[-2] if len(completed) >= 2 else None
        ml = MONTHS_FULL[int(M[5:7])]
        period_label = f"{ml} {M[:4]}"
        eM, iM = exp.get(M, 0), inc.get(M, 0)
        avg = out[-1]["avg_expense"] if out else 0
        ratio = out[-1]["expense_ratio"] if out else 0

        if avg:
            dpct = (eM / avg - 1) * 100
            below = eM <= avg
            events.append({"icon": "💸", "tone": "good" if below else "bad",
                           "title": f"Расходы за {ml}: {money(eM)} {SYM}",
                           "detail": f"На {abs(dpct):.0f}% {'ниже' if below else 'выше'} среднего за год ({money(avg)} {SYM})."})
        if iM:
            top_inc = sorted(inc_cat.get(M, {}).items(), key=lambda x: -x[1])[:2]
            events.append({"icon": "💰", "tone": "good",
                           "title": f"Доход за {ml}: {money(iM)} {SYM}",
                           "detail": ", ".join(f"{n} {money(v)}" for n, v in top_inc if v >= 1000) or "—"})
        if iM:
            ratioM = eM / iM * 100
            events.append({"icon": "📊", "tone": "good" if ratioM < 50 else "neutral",
                           "title": f"Потрачено {ratioM:.0f}% дохода за {ml}",
                           "detail": f"Отложено {max(0, 100 - ratioM):.0f}% дохода месяца."})
        cats = sorted(exp_cat.get(M, {}).items(), key=lambda x: -x[1])
        if cats and eM:
            c, a = cats[0]
            events.append({"icon": "🛒", "tone": "neutral",
                           "title": f"Крупнейшая статья: {c} — {money(a)} {SYM}",
                           "detail": f"{a / eM * 100:.0f}% расходов месяца."})
        if prevM:
            allc = set(exp_cat.get(M, {})) | set(exp_cat.get(prevM, {}))
            diffs = sorted(((c, exp_cat.get(M, {}).get(c, 0) - exp_cat.get(prevM, {}).get(c, 0)) for c in allc),
                           key=lambda x: -x[1])
            if diffs and diffs[0][1] >= 15000:
                c, d = diffs[0]
                events.append({"icon": "📈", "tone": "bad",
                               "title": f"Выросли траты: {c} +{money(d)} {SYM}",
                               "detail": f"Было {money(exp_cat.get(prevM, {}).get(c, 0))}, стало {money(exp_cat.get(M, {}).get(c, 0))}."})

    return JSONResponse({"months": out[-12:], "events": events,
                         "period_label": period_label, "currency": currency})
