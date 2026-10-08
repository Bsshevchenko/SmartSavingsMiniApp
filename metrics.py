#!/usr/bin/env python3
"""
Централизованные финансовые метрики, общие для всех страниц приложения.

Здесь живёт ЕДИНЫЙ источник правды для:
  - конвертации валют по историческому курсу месяца (convert_h);
  - среднемесячных расходов (avg_monthly_expense) — по ЗАВЕРШЁННЫМ месяцам,
    скользящее окно ≤12 мес, в любой валюте.

Используется и server.py (аналитика, активы), и fin_model_data.py (фин-модель),
чтобы цифра «средние ежемесячные расходы» совпадала на всех страницах.
"""
from __future__ import annotations

from datetime import datetime, timezone


def load_historical_rates(con) -> dict:
    """{(code, 'YYYY-MM'): rate_to_usd} — средний курс месяца из currency_rates."""
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


def price_lookup(hist_rates: dict) -> dict:
    """{code: [(месяц, курс), ...]} отсортировано — для поиска цены на любой месяц."""
    by_code: dict[str, list] = {}
    for (code, month), rate in hist_rates.items():
        by_code.setdefault(code, []).append((month, rate))
    for hist in by_code.values():
        hist.sort()
    return by_code


def price_at(lookup: dict, code: str, month: str, currency: str):
    """Цена единицы `code` в `currency` на месяц: последний слот ≤ month.

    До появления первой цены берём первую известную — иначе позиция «появлялась
    из воздуха» в момент первого слота, и это выглядело бы как рыночный рост.
    """
    def rate(c):
        if c == "USD":
            return 1.0
        hist = lookup.get(c)
        if not hist:
            return None
        found = None
        for m, r in hist:
            if m <= month:
                found = r
            else:
                break
        return found if found is not None else hist[0][1]

    src, dst = rate(code), rate(currency)
    return src / dst if src and dst else None


# Валюта в «Акциях» — свободный остаток на брокерском счёте, который копится до
# следующей покупки. Это отдельная позиция: снапшот «RUB в Акциях» не должен
# перетирать «RUB во Вкладе». Всё остальное по-прежнему определяется одним кодом —
# перенос USD из «Кэша» в «Ликвидность» остаётся сменой ярлыка, а не второй позицией.
BROKER_CASH_CATEGORY = "Акции"
CASH_CODES = {"RUB", "USD", "EUR", "CNY", "HKD", "VND", "THB", "MYR"}


def position_key(code: str, category: str | None) -> str:
    """Чем определяется позиция в снапшотной модели: обычно кодом, брокерский кэш —
    кодом и категорией. Цена позиции всегда ищется по коду (см. key_code)."""
    if code in CASH_CODES and category == BROKER_CASH_CATEGORY:
        return f"{code}@{category}"
    return code


def key_code(key: str) -> str:
    return key.split("@", 1)[0]


# Доход, который приносит сам актив, а не рынок: приходит отдельной записью income
# и в снапшотной модели неотличим от пополнения, поэтому учитывается явно.
INCOME_TO_CATEGORY = {
    "процент": "Вклад",
    "дивиденд": "Акции",
    "купон": "Акции",
    "рент": "Недвижимость",
}


def asset_income_by_category(
    con, user_id: int, currency: str, curr_rates: dict, hist_rates: dict,
) -> dict[str, dict]:
    """{категория актива: {amount, sources}} — доход за всё время в `currency`.

    `sources` — названия категорий доходов, из которых он сложился («Проценты»,
    «Дивиденды»): подписывать строку в интерфейсе нужно тем, что реально было.
    Конвертация по курсу месяца получения: деньги пришли тогда, в тех ценах.
    """
    cur = con.cursor()
    cur.execute("""
        SELECT COALESCE(cat.name, '') AS name, c.code AS code,
               CAST(e.amount AS REAL) AS amt, strftime('%Y-%m', e.created_at) AS month
        FROM entries e
        LEFT JOIN categories cat ON cat.id = e.category_id
        JOIN currencies c ON c.id = e.currency_id
        WHERE e.mode = 'income' AND e.user_id = ?
    """, (user_id,))
    out: dict[str, dict] = {}
    for r in cur.fetchall():
        name = (r["name"] or "").lower()
        target = next((cat for kw, cat in INCOME_TO_CATEGORY.items() if kw in name), None)
        if target:
            entry = out.setdefault(target, {"amount": 0.0, "sources": []})
            entry["amount"] += convert_h(
                r["amt"], r["code"], currency, r["month"], hist_rates, curr_rates)
            if r["name"] and r["name"] not in entry["sources"]:
                entry["sources"].append(r["name"])
    return out


def portfolio_pnl(
    asset_rows, realty_rows, months: list[str], currency: str,
    hist_rates: dict, income_by_category: dict[str, float],
) -> dict:
    """Прибыль/убыток портфеля за всё время, в `currency`.

    В базе нет сделок — только снапшоты количества, поэтому результат восстанавливается
    помесячно: сколько лежало на конец прошлого месяца × изменение цены. Цена берётся
    в валюте отображения, поэтому переоценка самого актива и валютная переоценка
    учитываются одной формулой (для USD-актива в рублях это движение и курса, и цены).

    Доход, который платит сам актив (проценты по вкладу, дивиденды), рынком не создаётся
    и в снапшоты попадает как рост количества — он приходит отдельно, по категориям.

    asset_rows: строки (code, category, month, amt), отсортированные по created_at.
    Результат по позициям (`by_ticker`, `category_of`) — по ключу position_key.
    realty_rows: строки (id, area_m2, currency, price_per_m2, rate_month) по возрастанию даты.
    """
    lookup = price_lookup(hist_rates)
    by_ticker: dict[str, float] = {}
    by_category: dict[str, float] = {}
    realty_pnl = 0.0

    qty: dict[str, float] = {}
    category_of: dict[str, str] = {}
    row_i = 0
    prev_qty: dict[str, float] = {}
    prev_realty: dict = {}
    prev_month = None

    for month in months:
        while row_i < len(asset_rows) and asset_rows[row_i]["month"] <= month:
            r = asset_rows[row_i]
            key = position_key(r["code"], r["category"])
            qty[key] = r["amt"]
            category_of[key] = r["category"] or "Без категории"
            row_i += 1
        realty_now = {r["id"]: r for r in realty_rows if r["rate_month"] <= month}

        if prev_month is not None:
            for key, q in prev_qty.items():
                if q <= 0:
                    continue
                now = price_at(lookup, key_code(key), month, currency)
                then = price_at(lookup, key_code(key), prev_month, currency)
                if now is None or then is None:
                    continue
                by_ticker[key] = by_ticker.get(key, 0) + q * (now - then)
            for rid, prev_rate in prev_realty.items():
                cur_rate = realty_now.get(rid, prev_rate)
                k_now = price_at(lookup, prev_rate["currency"], month, currency)
                k_then = price_at(lookup, prev_rate["currency"], prev_month, currency)
                if k_now is None or k_then is None:
                    continue
                area = prev_rate["area_m2"]
                delta = area * (cur_rate["price_per_m2"] * k_now
                                - prev_rate["price_per_m2"] * k_then)
                realty_pnl += delta

        prev_qty, prev_realty, prev_month = dict(qty), realty_now, month

    # Результат тикера целиком относим к его ТЕКУЩЕЙ категории — так же, как страница
    # показывает его одной позицией. Тикеры переезжают между категориями (USD: Кэш →
    # Ликвидность), и если разносить результат по категории «на тот момент», сумма
    # позиций в карточке категории перестанет сходиться с её итогом.
    for code, value in by_ticker.items():
        cat = category_of.get(code, "Без категории")
        by_category[cat] = by_category.get(cat, 0) + value

    if realty_rows:
        by_category["Недвижимость"] = by_category.get("Недвижимость", 0) + realty_pnl

    # Доход активов добавляем к их категориям (по бумагам разнести нельзя: дивиденд
    # приходит одной записью и тикер в ней не указан)
    for cat, amount in income_by_category.items():
        by_category[cat] = by_category.get(cat, 0) + amount

    return {
        "by_ticker": by_ticker,
        "by_category": by_category,
        "category_of": category_of,
        "realty": realty_pnl,
        "income": dict(income_by_category),
        "total": sum(by_category.values()),
    }


def expense_by_month(
    con, user_id: int, currency: str, curr_rates: dict, hist_rates: dict | None = None,
) -> dict[str, float]:
    """Все расходы пользователя по месяцам {'YYYY-MM': сумма в `currency`}."""
    if hist_rates is None:
        hist_rates = load_historical_rates(con)
    cur = con.cursor()
    cur.execute("""
        SELECT strftime('%Y-%m', e.created_at) AS m,
               CAST(e.amount AS REAL) AS amt, c.code AS code
        FROM entries e JOIN currencies c ON c.id=e.currency_id
        WHERE e.user_id=? AND e.mode='expense'
    """, (user_id,))
    out: dict[str, float] = {}
    for r in cur.fetchall():
        out[r["m"]] = out.get(r["m"], 0) + convert_h(
            r["amt"], r["code"], currency, r["m"], hist_rates, curr_rates)
    return out


def completed_months(by_month: dict[str, float]) -> list[str]:
    """Завершённые месяцы: без текущего (неполного) и без самого раннего
    (онбординг, обычно неполный). Иначе среднее занижено."""
    this_m = datetime.now(timezone.utc).strftime("%Y-%m")
    months = sorted(m for m in by_month if m < this_m)
    if len(months) > 1:
        months = months[1:]
    return months


def avg_monthly_expense(
    con, user_id: int, currency: str, curr_rates: dict,
    hist_rates: dict | None = None, window: int = 12,
) -> dict:
    """
    Среднемесячные расходы в `currency` по завершённым месяцам, скользящее окно ≤`window`.
    Возвращает {avg, months_count, by_month, completed} — единый расчёт для всех страниц.
    """
    by_month = expense_by_month(con, user_id, currency, curr_rates, hist_rates)
    completed = completed_months(by_month)
    win = completed[-window:]
    avg = round(sum(by_month[m] for m in win) / len(win)) if win else 0
    return {"avg": avg, "months_count": len(win), "by_month": by_month, "completed": completed}


PASSIVE_KEYWORDS = ("процент", "дивиденд", "купон", "рент", "аренд", "пассив", "роялт", "кэшбэк")


def _shift_month(ym: str, delta: int) -> str:
    y, m = int(ym[:4]), int(ym[5:7]) - 1 + delta
    return f"{y + m // 12:04d}-{m % 12 + 1:02d}"


def passive_income(
    con, user_id: int, currency: str, curr_rates: dict,
    hist_rates: dict | None = None, window: int = 12,
) -> dict:
    """Среднемесячный пассивный доход в `currency` за скользящий год.

    Окно — `window` завершённых месяцев перед текущим, делим на число месяцев окна,
    а не на число месяцев с поступлениями: дивиденды приходят 1–2 раза в год, и
    «среднее по месяцам с выплатами» завышало доход в разы. Если история короче
    окна, делим на месяцы с начала учёта (первая запись пользователя).
    """
    if hist_rates is None:
        hist_rates = load_historical_rates(con)
    this_m = datetime.now(timezone.utc).strftime("%Y-%m")
    win_from = _shift_month(this_m, -window)
    cur = con.cursor()
    cur.execute("SELECT MIN(strftime('%Y-%m', created_at)) AS m FROM entries WHERE user_id=?",
                (user_id,))
    first_m = cur.fetchone()["m"] or this_m
    cur.execute("""
        SELECT cat.name AS name, strftime('%Y-%m', e.created_at) AS month,
               CAST(e.amount AS REAL) AS amt, c.code AS code
        FROM entries e
        LEFT JOIN categories cat ON cat.id = e.category_id
        JOIN currencies c ON c.id = e.currency_id
        WHERE e.user_id = ? AND e.mode = 'income'
          AND e.created_at >= ?
    """, (user_id, win_from + "-01"))
    by_month: dict[str, float] = {}
    cats: set[str] = set()
    for r in cur.fetchall():
        if not any(kw in (r["name"] or "").lower() for kw in PASSIVE_KEYWORDS):
            continue
        cats.add(r["name"] or "")
        by_month[r["month"]] = by_month.get(r["month"], 0) + convert_h(
            r["amt"], r["code"], currency, r["month"], hist_rates, curr_rates)

    months = [m for m in (_shift_month(this_m, -i) for i in range(window, 0, -1))
              if m >= first_m]
    total = sum(by_month.get(m, 0) for m in months)
    return {
        "avg": total / len(months) if months else 0.0,
        "total": total,
        "months_count": len(months),
        "this_month": by_month.get(this_m, 0.0),
        "categories": sorted(cats),
    }
