"""
Тесты update_daily_rates.py

Проверяем:
- upsert создаёт новую запись
- повторный запуск перезаписывает ту же строку (идемпотентность)
- дата сохраняется как YYYY-MM-01 (слот текущего месяца, не сегодняшний день)
- все источники (fiat, crypto, moex) записываются при успешном fetch
- сбой одного источника не ломает остальные
"""
import sqlite3
from datetime import date
from unittest.mock import patch

import pytest

import update_daily_rates as udr
from tests.conftest import make_db


@pytest.fixture
def tmp_db(tmp_path):
    db_path = tmp_path / "test.db"
    con = make_db(str(db_path))
    con.close()
    return db_path


# ── upsert ────────────────────────────────────────────────────────────────────

def test_upsert_creates_new_record(tmp_db):
    con = sqlite3.connect(str(tmp_db))
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    udr.upsert_rate(cur, "RUB", "2026-04-01", 0.0124, "daily")
    con.commit()

    cur.execute("SELECT * FROM currency_rates WHERE currency_code='RUB'")
    rows = cur.fetchall()
    con.close()

    assert len(rows) == 1
    assert abs(rows[0]["rate_to_usd"] - 0.0124) < 1e-6
    assert rows[0]["source"] == "daily"


def test_upsert_overwrites_same_slot(tmp_db):
    """Два upsert на одну (code, date) → одна строка с последним значением."""
    con = sqlite3.connect(str(tmp_db))
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    udr.upsert_rate(cur, "RUB", "2026-04-01", 0.0120, "daily")
    udr.upsert_rate(cur, "RUB", "2026-04-01", 0.0124, "daily")  # обновление
    con.commit()

    cur.execute("SELECT COUNT(*) as cnt, rate_to_usd FROM currency_rates WHERE currency_code='RUB'")
    row = cur.fetchone()
    con.close()

    assert row["cnt"] == 1
    assert abs(row["rate_to_usd"] - 0.0124) < 1e-6


def test_upsert_different_months_different_rows(tmp_db):
    """Разные месяцы — разные строки."""
    con = sqlite3.connect(str(tmp_db))
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    udr.upsert_rate(cur, "RUB", "2026-03-01", 0.0125, "monthly_avg")
    udr.upsert_rate(cur, "RUB", "2026-04-01", 0.0124, "daily")
    con.commit()

    cur.execute("SELECT COUNT(*) as cnt FROM currency_rates WHERE currency_code='RUB'")
    row = cur.fetchone()
    con.close()

    assert row["cnt"] == 2


# ── run() с моками ────────────────────────────────────────────────────────────

MOCK_FIAT = {"RUB": 0.0124, "VND": 0.0000382, "THB": 0.0305, "MYR": 0.0248, "EUR": 1.155}
MOCK_CRYPTO = {"BTC": 66000.0, "ETH": 2060.0, "SOL": 80.0, "TRX": 0.31, "USDT": 1.0}
MOCK_MOEX = {"SBER": 3.92, "OZON": 54.0, "YDEX": 52.0}


def test_run_stores_with_current_month_slot(tmp_db):
    """run() сохраняет курсы с датой YYYY-MM-01 текущего месяца (не сегодняшней датой)."""
    expected_slot = date.today().strftime("%Y-%m-01")

    with patch.object(udr, "fetch_fiat_rates", return_value=MOCK_FIAT), \
         patch.object(udr, "fetch_crypto_rates", return_value=MOCK_CRYPTO), \
         patch.object(udr, "fetch_moex_rates", return_value=MOCK_MOEX):
        udr.run(db_path=tmp_db)

    con = sqlite3.connect(str(tmp_db))
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT DISTINCT rate_date FROM currency_rates")
    dates = [r["rate_date"] for r in cur.fetchall()]
    con.close()

    assert dates == [expected_slot], f"Ожидали {expected_slot}, получили {dates}"


def test_run_is_idempotent(tmp_db):
    """Два запуска подряд → одна строка на валюту (перезапись, не дублирование)."""
    with patch.object(udr, "fetch_fiat_rates", return_value=MOCK_FIAT), \
         patch.object(udr, "fetch_crypto_rates", return_value=MOCK_CRYPTO), \
         patch.object(udr, "fetch_moex_rates", return_value=MOCK_MOEX):
        udr.run(db_path=tmp_db)
        udr.run(db_path=tmp_db)

    con = sqlite3.connect(str(tmp_db))
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM currency_rates")
    count = cur.fetchone()[0]
    con.close()

    # Fiat(5) + Crypto(5) + MOEX(3) + USD(1) = 14
    assert count == 14


def test_run_returns_rates_dict(tmp_db):
    """run() возвращает словарь с актуальными курсами для немедленного использования."""
    with patch.object(udr, "fetch_fiat_rates", return_value=MOCK_FIAT), \
         patch.object(udr, "fetch_crypto_rates", return_value=MOCK_CRYPTO), \
         patch.object(udr, "fetch_moex_rates", return_value=MOCK_MOEX):
        result = udr.run(db_path=tmp_db)

    assert "RUB" in result
    assert "BTC" in result
    assert "USD" in result
    assert result["USD"] == 1.0
    assert abs(result["RUB"] - 0.0124) < 1e-6


def test_run_fiat_failure_does_not_stop_crypto(tmp_db):
    """Если fiat упал — crypto и MOEX всё равно сохраняются."""
    with patch.object(udr, "fetch_fiat_rates", side_effect=Exception("API down")), \
         patch.object(udr, "fetch_crypto_rates", return_value=MOCK_CRYPTO), \
         patch.object(udr, "fetch_moex_rates", return_value={}):
        result = udr.run(db_path=tmp_db)

    assert "BTC" in result
    assert "RUB" not in result  # fiat не получили


def test_run_moex_failure_does_not_stop_fiat(tmp_db):
    """Если MOEX упал — fiat и crypto всё равно сохраняются."""
    with patch.object(udr, "fetch_fiat_rates", return_value=MOCK_FIAT), \
         patch.object(udr, "fetch_crypto_rates", return_value=MOCK_CRYPTO), \
         patch.object(udr, "fetch_moex_rates", side_effect=Exception("MOEX down")):
        result = udr.run(db_path=tmp_db)

    assert "RUB" in result
    assert "BTC" in result
    assert "SBER" not in result  # MOEX не получили