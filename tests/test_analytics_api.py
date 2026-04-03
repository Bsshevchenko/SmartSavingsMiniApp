"""
Интеграционные тесты аналитического API (/api/analytics).

Сценарии с реальным FastAPI TestClient + временная БД.
Проверяем что конвертация в аналитике использует исторические курсы.
"""
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import server
from tests.conftest import make_db, seed_rates, seed_entry, USER_ID


@pytest.fixture
def test_db(tmp_path):
    """
    Временная БД с курсами и транзакциями.

    Курсы:
      Sep 2025: RUB=0.010, VND=0.000040  → 100 RUB = 25 000 VND
      Mar 2026: RUB=0.0125, VND=0.000038 → 100 RUB ≈ 32 894 VND

    Транзакции:
      Sep 2025: расход 100 RUB (категория «Кафе»)
      Mar 2026: расход 100 RUB (категория «Кафе»)
      Mar 2026: доход 1000 VND
    """
    db_path = tmp_path / "test.db"
    con = make_db(str(db_path))

    seed_rates(con, [
        ("USD", "2025-09-01", 1.0,       "monthly_avg"),
        ("RUB", "2025-09-01", 0.0100,    "monthly_avg"),
        ("VND", "2025-09-01", 0.0000400, "monthly_avg"),
        ("USD", "2026-03-01", 1.0,       "monthly_avg"),
        ("RUB", "2026-03-01", 0.0125,    "monthly_avg"),
        ("VND", "2026-03-01", 0.0000380, "monthly_avg"),
        ("USD", "2026-04-01", 1.0,       "daily"),
        ("RUB", "2026-04-01", 0.0124,    "daily"),
        ("VND", "2026-04-01", 0.0000382, "daily"),
    ])

    seed_entry(con, 100.0, "RUB", "expense", "2025-09-15 10:00:00", "Кафе")
    seed_entry(con, 100.0, "RUB", "expense", "2026-03-15 10:00:00", "Кафе")
    seed_entry(con, 1000.0, "VND", "income", "2026-03-20 10:00:00")

    con.close()
    return db_path


@pytest.fixture
def client(test_db, monkeypatch):
    """TestClient с переопределённым DB_PATH на временную БД."""
    monkeypatch.setattr(server, "DB_PATH", test_db)
    server._rates_cache["ts"] = 0  # сбрасываем кеш курсов
    with TestClient(server.app) as c:
        yield c


# ── Структура ответа ──────────────────────────────────────────────────────────

def test_analytics_response_structure(client):
    r = client.get(f"/api/analytics?user_id={USER_ID}&period=all&currency=VND")
    assert r.status_code == 200
    data = r.json()

    assert "monthly" in data
    assert "top_categories" in data
    assert "daily_trend" in data
    assert "summary" in data
    assert data["currency"] == "VND"


def test_monthly_chart_has_correct_months(client):
    r = client.get(f"/api/analytics?user_id={USER_ID}&period=all&currency=VND")
    data = r.json()

    months = [m["month"] for m in data["monthly"]]
    assert "2025-09" in months
    assert "2026-03" in months


# ── Ключевой сценарий: исторические курсы применяются корректно ───────────────

def test_historical_rates_affect_monthly_totals(client):
    """
    100 RUB в сентябре и 100 RUB в марте должны конвертироваться
    в РАЗНОЕ количество VND, потому что курсы за эти месяцы разные.

    Sep 2025: 100 RUB * 0.010 / 0.000040  = 25 000 VND
    Mar 2026: 100 RUB * 0.0125 / 0.000038 ≈ 32 894 VND
    """
    r = client.get(f"/api/analytics?user_id={USER_ID}&period=all&currency=VND")
    data = r.json()

    monthly = {m["month"]: m for m in data["monthly"]}

    sep_expense = monthly["2025-09"]["expense"]
    mar_expense = monthly["2026-03"]["expense"]

    assert abs(sep_expense - 25_000) < 100, f"Sep expense: ожидали ~25000, получили {sep_expense}"
    assert abs(mar_expense - 32_894) < 100, f"Mar expense: ожидали ~32894, получили {mar_expense}"
    assert sep_expense != mar_expense, "Суммы должны быть разными из-за разных курсов"


def test_income_converted_correctly(client):
    """1000 VND дохода в марте 2026 в VND = 1000 VND (та же валюта)."""
    r = client.get(f"/api/analytics?user_id={USER_ID}&period=all&currency=VND")
    data = r.json()

    monthly = {m["month"]: m for m in data["monthly"]}
    mar_income = monthly["2026-03"]["income"]

    assert mar_income == 1000


def test_currency_switch_changes_values(client):
    """Переключение отображаемой валюты меняет числовые значения."""
    r_vnd = client.get(f"/api/analytics?user_id={USER_ID}&period=all&currency=VND")
    r_usd = client.get(f"/api/analytics?user_id={USER_ID}&period=all&currency=USD")

    monthly_vnd = {m["month"]: m for m in r_vnd.json()["monthly"]}
    monthly_usd = {m["month"]: m for m in r_usd.json()["monthly"]}

    # В VND сумма должна быть значительно больше, чем в USD
    assert monthly_vnd["2025-09"]["expense"] > monthly_usd["2025-09"]["expense"]


def test_top_categories_present(client):
    r = client.get(f"/api/analytics?user_id={USER_ID}&period=all&currency=VND")
    data = r.json()

    assert len(data["top_categories"]) > 0
    cat = data["top_categories"][0]
    assert cat["name"] == "Кафе"
    assert cat["pct"] == 100  # единственная категория расходов


def test_summary_reflects_current_month(client):
    """Summary показывает расходы/доходы текущего месяца."""
    r = client.get(f"/api/analytics?user_id={USER_ID}&period=all&currency=VND")
    data = r.json()

    summary = data["summary"]
    assert "exp_month" in summary
    assert "inc_month" in summary
    assert summary["total_entries"] >= 3  # 3 записи добавлено


# ── Граничные случаи ──────────────────────────────────────────────────────────

def test_period_filter_limits_months(client):
    """period=1m возвращает только последний месяц, не сентябрь 2025."""
    r = client.get(f"/api/analytics?user_id={USER_ID}&period=1m&currency=VND")
    data = r.json()

    months = [m["month"] for m in data["monthly"]]
    assert "2025-09" not in months


def test_nonexistent_user_returns_empty(client):
    """Несуществующий user_id → пустые данные, не ошибка."""
    r = client.get("/api/analytics?user_id=99999&period=all&currency=VND")
    assert r.status_code == 200
    data = r.json()
    assert data["monthly"] == []
    assert data["top_categories"] == []
    assert data["summary"]["total_entries"] == 0