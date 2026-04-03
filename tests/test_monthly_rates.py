"""
Тесты update_monthly_rates.py

Проверяем:
- prev_month() корректно вычисляет прошлый месяц (включая смену года)
- run() запрашивает данные и сохраняет среднемесячный курс в слот YYYY-MM-01
- run() перезаписывает daily-запись за тот же месяц (source меняется на monthly_avg)
- при ошибке API частичные данные всё равно сохраняются
"""
import sqlite3
from datetime import date
from unittest.mock import patch

import pytest

import update_monthly_rates as umr
from tests.conftest import make_db, seed_rates


@pytest.fixture
def tmp_db(tmp_path):
    db_path = tmp_path / "test.db"
    con = make_db(str(db_path))
    con.close()
    return db_path


# ── prev_month() ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("today, expected", [
    (date(2026, 4, 1),  (2026, 3)),
    (date(2026, 1, 1),  (2025, 12)),  # смена года
    (date(2026, 12, 15), (2026, 11)),
    (date(2025, 2, 28), (2025, 1)),
])
def test_prev_month(today, expected):
    assert umr.prev_month(today) == expected


# ── run() с моками ────────────────────────────────────────────────────────────

MOCK_FIAT_SEP = {"RUB": 0.0100, "VND": 0.0000400, "THB": 0.031, "MYR": 0.024, "EUR": 1.17}
MOCK_CRYPTO_PRICE = 65000.0  # BTC на любую дату


def _mock_fiat(d):
    return MOCK_FIAT_SEP


def _mock_crypto(coin_id, d):
    prices = {"bitcoin": 65000.0, "ethereum": 4000.0, "solana": 150.0, "tron": 0.30, "tether": 1.0}
    return prices.get(coin_id)


def _mock_moex(ticker, year, month):
    return [300.0, 302.0, 298.0]  # 3 торговых дня, среднее = 300


def test_run_saves_monthly_avg_slot(tmp_db):
    """run() сохраняет данные с датой YYYY-MM-01 прошлого месяца."""
    ref = date(2026, 4, 1)  # «сегодня» 1 апреля → считаем март
    expected_slot = "2026-03-01"

    with patch.object(umr, "fetch_fiat_on_date", side_effect=_mock_fiat), \
         patch.object(umr, "fetch_crypto_on_date", side_effect=_mock_crypto), \
         patch.object(umr, "fetch_moex_month_avg", side_effect=_mock_moex):
        saved = umr.run(db_path=tmp_db, reference_date=ref)

    assert saved > 0

    con = sqlite3.connect(str(tmp_db))
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT DISTINCT rate_date FROM currency_rates")
    dates = [r["rate_date"] for r in cur.fetchall()]
    con.close()

    assert expected_slot in dates


def test_run_source_is_monthly_avg(tmp_db):
    """Записи получают source='monthly_avg'."""
    ref = date(2026, 4, 1)

    with patch.object(umr, "fetch_fiat_on_date", side_effect=_mock_fiat), \
         patch.object(umr, "fetch_crypto_on_date", side_effect=_mock_crypto), \
         patch.object(umr, "fetch_moex_month_avg", side_effect=_mock_moex):
        umr.run(db_path=tmp_db, reference_date=ref)

    con = sqlite3.connect(str(tmp_db))
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT DISTINCT source FROM currency_rates")
    sources = {r["source"] for r in cur.fetchall()}
    con.close()

    assert sources == {"monthly_avg"}


def test_run_overwrites_daily_slot(tmp_db):
    """
    Если за этот месяц уже была daily-запись — monthly_avg её перезаписывает.
    После: одна строка, source='monthly_avg'.
    """
    ref = date(2026, 4, 1)

    # Предзаполняем daily-запись за март (схема уже создана фикстурой tmp_db)
    con = sqlite3.connect(str(tmp_db))
    con.row_factory = sqlite3.Row
    seed_rates(con, [("RUB", "2026-03-01", 0.0120, "daily")])
    con.close()

    with patch.object(umr, "fetch_fiat_on_date", side_effect=_mock_fiat), \
         patch.object(umr, "fetch_crypto_on_date", side_effect=_mock_crypto), \
         patch.object(umr, "fetch_moex_month_avg", side_effect=_mock_moex):
        umr.run(db_path=tmp_db, reference_date=ref)

    con = sqlite3.connect(str(tmp_db))
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(
        "SELECT count(*) as cnt, source, rate_to_usd FROM currency_rates WHERE currency_code='RUB'"
    )
    row = cur.fetchone()
    con.close()

    assert row["cnt"] == 1
    assert row["source"] == "monthly_avg"
    # Среднее MOCK_FIAT_SEP['RUB'] = 0.010 (4 точки, все одинаковые)
    assert abs(row["rate_to_usd"] - 0.0100) < 0.0001


def test_run_averages_4_sample_points(tmp_db):
    """
    fetch_fiat_on_date вызывается для 4 дат (1, 8, 15, 22).
    Среднее по 4 точкам с разными значениями считается корректно.
    """
    ref = date(2026, 4, 1)
    call_count = {"n": 0}

    def fiat_with_variation(d):
        # Возвращаем разные значения для разных дат
        vals = {1: 0.010, 8: 0.011, 15: 0.012, 22: 0.013}
        call_count["n"] += 1
        return {"RUB": vals.get(d.day, 0.010)}

    with patch.object(umr, "fetch_fiat_on_date", side_effect=fiat_with_variation), \
         patch.object(umr, "fetch_crypto_on_date", return_value=None), \
         patch.object(umr, "fetch_moex_month_avg", return_value=[]):
        umr.run(db_path=tmp_db, reference_date=ref)

    # 4 вызова (1, 8, 15, 22 марта)
    assert call_count["n"] == 4

    con = sqlite3.connect(str(tmp_db))
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT rate_to_usd FROM currency_rates WHERE currency_code='RUB'")
    row = cur.fetchone()
    con.close()

    # Среднее: (0.010 + 0.011 + 0.012 + 0.013) / 4 = 0.0115
    assert abs(row["rate_to_usd"] - 0.0115) < 0.0001


def test_run_partial_failure_saves_available(tmp_db):
    """
    Если crypto API падает — fiat и MOEX всё равно сохраняются.
    """
    ref = date(2026, 4, 1)

    with patch.object(umr, "fetch_fiat_on_date", side_effect=_mock_fiat), \
         patch.object(umr, "fetch_crypto_on_date", side_effect=Exception("CoinGecko down")), \
         patch.object(umr, "fetch_moex_month_avg", side_effect=_mock_moex):
        saved = umr.run(db_path=tmp_db, reference_date=ref)

    con = sqlite3.connect(str(tmp_db))
    cur = con.cursor()
    cur.execute("SELECT currency_code FROM currency_rates")
    codes = {r[0] for r in cur.fetchall()}
    con.close()

    assert "RUB" in codes   # fiat сохранён
    assert "SBER" in codes  # MOEX сохранён
    assert "BTC" not in codes  # crypto не получили