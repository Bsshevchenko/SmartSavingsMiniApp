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
