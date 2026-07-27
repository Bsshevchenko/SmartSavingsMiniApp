"""
Окно сводки «что произошло за прошлый месяц» на странице Активы.

Слот currency_rates за текущий месяц — живой: update_daily_rates перезаписывает его
каждый день. Пока он участвовал в расчёте как «цена на конец прошлого месяца», сводка
за июнь ежедневно менялась и подмешивала июльские движения рынка. Тест держит границу:
цены берутся только из закрытых месячных слотов.
"""
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import server
from tg_auth import require_user_id

SCHEMA_SQL = Path(__file__).parent.parent / "tools" / "schema.sql"
USER_ID = 777


def month_slot(shift: int) -> str:
    """Слот YYYY-MM-01 со сдвигом на `shift` месяцев назад от текущего."""
    d = datetime.now(timezone.utc).replace(day=1)
    for _ in range(shift):
        d = (d - timedelta(days=1)).replace(day=1)
    return d.strftime("%Y-%m-01")


@pytest.fixture
def client(tmp_path):
    db_file = tmp_path / "app.db"
    con = sqlite3.connect(str(db_file))
    con.executescript(SCHEMA_SQL.read_text())

    now = datetime.now(timezone.utc).isoformat()
    con.execute("INSERT INTO users (id, username, first_seen, last_seen) VALUES (?,?,?,?)",
                (USER_ID, "t", now, now))
    con.execute("INSERT INTO currencies (id, user_id, code, created_at) VALUES (1,?,'SBER',?)",
                (USER_ID, now))
    con.execute("INSERT INTO categories (id, user_id, mode, name, created_at) "
                "VALUES (1,?,'asset','Акции',?)", (USER_ID, now))

    # Снапшоты количества: 100 бумаг два месяца назад, 150 — в прошлом месяце
    for slot, qty in ((month_slot(2), 100), (month_slot(1), 150)):
        con.execute("INSERT INTO entries (user_id, mode, amount, currency_id, category_id, "
                    "created_at, updated_at) VALUES (?,'asset',?,1,1,?,?)",
                    (USER_ID, qty, f"{slot}T10:00:00", f"{slot}T10:00:00"))

    # Курсы: закрытые слоты + живой слот текущего месяца
    rates = [("RUB", month_slot(2), 0.0125), ("RUB", month_slot(1), 0.0130),
             ("RUB", month_slot(0), 0.0128),
             ("SBER", month_slot(2), 3.0), ("SBER", month_slot(1), 3.3),
             ("SBER", month_slot(0), 2.0)]          # текущий месяц: обвал −40%
    for code, d, r in rates:
        con.execute("INSERT INTO currency_rates (currency_code, rate_date, rate_to_usd, "
                    "source, created_at) VALUES (?,?,?,'test',?)", (code, d, r, now))
    con.execute("INSERT INTO asset_latest_values (user_id, currency_code, category_name, "
                "amount, last_updated) VALUES (?,'SBER','Акции',150,?)", (USER_ID, now))
    con.commit()

    server.DB_PATH = db_file
    server.app.dependency_overrides[require_user_id] = lambda: USER_ID
    yield TestClient(server.app), con
    server.app.dependency_overrides.clear()
    con.close()


def test_summary_ignores_live_slot_of_current_month(client):
    """Обвал цены в текущем месяце не должен менять сводку за прошлый месяц."""
    api, con = client
    before = api.get("/api/asset-events?currency=RUB").json()

    con.execute("UPDATE currency_rates SET rate_to_usd = rate_to_usd * 0.5 "
                "WHERE rate_date = ?", (month_slot(0),))
    con.commit()
    after = api.get("/api/asset-events?currency=RUB").json()

    assert before["events"] == after["events"]
    assert before["monthly"] == after["monthly"]


def test_window_covers_previous_month(client):
    """Период — прошлый месяц целиком: от его 1-го числа до 1-го числа текущего."""
    api, _ = client
    data = api.get("/api/asset-events?currency=RUB").json()
    assert data["period_from"] == month_slot(1)
    assert data["period_to"] == month_slot(0)


def test_market_move_taken_from_closed_slots(client):
    """Переоценка считается по закрытым слотам: SBER 3.0 → 3.3 (+10%), а не по живому."""
    api, _ = client
    data = api.get("/api/asset-events?currency=RUB").json()
    stocks = [e for e in data["events"] if "Акции" in e["title"]]
    assert stocks, "ожидалась плашка движения акций"
    assert "выросли" in stocks[0]["title"], stocks[0]["title"]