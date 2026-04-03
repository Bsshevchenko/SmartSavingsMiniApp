"""
Тесты эндпоинта /api/assets.

Проверяемые сценарии:
  1. Пустой портфель → total=0, positions=[]
  2. Одна позиция — текущая стоимость считается по curr_rates, не hist_rates
  3. Несколько записей одного тикера — учитывается только последняя
  4. Несколько тикеров → positions отсортированы по убыванию стоимости
  5. Группировка по категориям (by_category)
  6. Доля позиции (pct) суммируется в 100%
  7. Конвертация в RUB (не USD)
  8. Тикер без курса → value=0, не ломает запрос
  9. Динамика (timeline): одна точка на каждый уникальный месяц
 10. Timeline использует исторические курсы для прошлых месяцев
 11. В timeline учитывается только снапшот, существовавший на тот месяц
 12. Позиции income/expense не попадают в портфель
"""
import sqlite3
import sys
import pytest
from pathlib import Path
from fastapi.testclient import TestClient

# Добавляем корень проекта в sys.path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import server
from tests.conftest import make_db, seed_entry, seed_rates, USER_ID

# ── Хелперы ──────────────────────────────────────────────────────────────────

CURR_RATES = {
    "USD":  1.0,
    "RUB":  0.0110,   # 1 RUB = 0.011 USD (текущий курс)
    "ETH":  2000.0,
    "BTC":  60000.0,
    "SBER": 4.0,
}


def make_client(tmp_path, rates_override=None):
    """
    Создаёт тестового клиента: поднимает БД во временном файле,
    подменяет get_rates_to_usd() на фиктивные курсы.
    """
    db_file = tmp_path / "app.db"
    con = sqlite3.connect(str(db_file))
    con.row_factory = sqlite3.Row
    from tests.conftest import SCHEMA
    con.executescript(SCHEMA)

    rates = rates_override or CURR_RATES
    server.DB_PATH = db_file
    server._rates_cache["rates"] = rates
    server._rates_cache["ts"] = float("inf")  # кеш «вечный» в рамках теста

    client = TestClient(server.app, raise_server_exceptions=True)
    return client, con


# ── Тесты ────────────────────────────────────────────────────────────────────

class TestEmptyPortfolio:
    def test_empty_returns_zero_total(self, tmp_path):
        client, _ = make_client(tmp_path)
        r = client.get(f"/api/assets?user_id={USER_ID}&currency=USD")
        assert r.status_code == 200
        data = r.json()
        assert data["total"] == 0
        assert data["positions"] == []
        assert data["by_category"] == []
        assert data["timeline"] == []


class TestCurrentRatesUsed:
    def test_current_rate_not_historical(self, tmp_path):
        """
        Запись создана в Jan 2025, когда ETH стоил $5000.
        Текущий курс ETH = $2000.
        Ожидаем value = qty * curr_rate = 1 * 2000, а не 5000.
        """
        client, con = make_client(tmp_path)
        # Исторический курс ETH за Jan 2025
        seed_rates(con, [("ETH", "2025-01-01", 5000.0, "monthly_avg")])
        seed_entry(con, amount=1.0, currency_code="ETH", mode="asset",
                   created_at="2025-01-15 12:00:00", category_name="Крипта")

        r = client.get(f"/api/assets?user_id={USER_ID}&currency=USD")
        data = r.json()
        pos = data["positions"][0]

        assert pos["ticker"] == "ETH"
        assert pos["value"] == pytest.approx(2000.0, rel=1e-3)  # curr_rate

    def test_historical_rate_not_used_for_current_value(self, tmp_path):
        """
        Убеждаемся что при разных hist и curr курсах выбирается curr.
        BTC: hist=100000, curr=60000. qty=0.1 → value должен быть 6000, не 10000.
        """
        client, con = make_client(tmp_path)
        seed_rates(con, [("BTC", "2024-06-01", 100000.0, "monthly_avg")])
        seed_entry(con, amount=0.1, currency_code="BTC", mode="asset",
                   created_at="2024-06-20 10:00:00", category_name="Крипта")

        r = client.get(f"/api/assets?user_id={USER_ID}&currency=USD")
        pos = r.json()["positions"][0]
        assert pos["value"] == pytest.approx(6000.0, rel=1e-3)


class TestLatestSnapshotOnly:
    def test_only_latest_entry_per_ticker(self, tmp_path):
        """
        Три записи SBER: 100, 200, 1210 шт.
        В портфеле должна быть только последняя — 1210.
        """
        client, con = make_client(tmp_path)
        seed_entry(con, 100, "SBER", "asset", "2025-10-01 10:00:00", "Акции")
        seed_entry(con, 200, "SBER", "asset", "2025-11-01 10:00:00", "Акции")
        seed_entry(con, 1210, "SBER", "asset", "2026-03-01 10:00:00", "Акции")

        r = client.get(f"/api/assets?user_id={USER_ID}&currency=USD")
        data = r.json()
        sber_positions = [p for p in data["positions"] if p["ticker"] == "SBER"]
        assert len(sber_positions) == 1
        assert sber_positions[0]["qty"] == 1210

    def test_total_reflects_only_latest(self, tmp_path):
        """Суммарная стоимость не накапливает несколько снапшотов одного тикера."""
        client, con = make_client(tmp_path)
        seed_entry(con, 1, "ETH", "asset", "2025-01-01 00:00:00", "Крипта")
        seed_entry(con, 3, "ETH", "asset", "2026-01-01 00:00:00", "Крипта")

        r = client.get(f"/api/assets?user_id={USER_ID}&currency=USD")
        data = r.json()
        # ETH curr_rate=2000, qty=3 → 6000 (не 1*2000 + 3*2000)
        assert data["total"] == pytest.approx(6000.0, rel=1e-3)


class TestSorting:
    def test_positions_sorted_by_value_desc(self, tmp_path):
        """Позиции идут от самой дорогой к дешёвой."""
        client, con = make_client(tmp_path)
        seed_entry(con, 1, "BTC", "asset", "2026-01-01 10:00:00", "Крипта")   # 60000
        seed_entry(con, 1, "ETH", "asset", "2026-01-02 10:00:00", "Крипта")   # 2000
        seed_entry(con, 1, "USD", "asset", "2026-01-03 10:00:00", "Кэш")      # 1

        r = client.get(f"/api/assets?user_id={USER_ID}&currency=USD")
        tickers = [p["ticker"] for p in r.json()["positions"]]
        assert tickers == ["BTC", "ETH", "USD"]


class TestByCategory:
    def test_grouping_by_category(self, tmp_path):
        """Активы корректно группируются по категориям."""
        client, con = make_client(tmp_path)
        seed_entry(con, 1, "BTC", "asset", "2026-01-01 10:00:00", "Крипта")   # 60000
        seed_entry(con, 1, "ETH", "asset", "2026-01-02 10:00:00", "Крипта")   # 2000
        seed_entry(con, 1000, "SBER", "asset", "2026-01-03 10:00:00", "Акции") # 4000

        r = client.get(f"/api/assets?user_id={USER_ID}&currency=USD")
        data = r.json()
        cats = {c["name"]: c["value"] for c in data["by_category"]}

        assert cats["Крипта"] == pytest.approx(62000.0, rel=1e-3)
        assert cats["Акции"] == pytest.approx(4000.0, rel=1e-3)

    def test_categories_sorted_by_value_desc(self, tmp_path):
        client, con = make_client(tmp_path)
        seed_entry(con, 1, "BTC", "asset", "2026-01-01 10:00:00", "Крипта")   # 60000
        seed_entry(con, 1000, "SBER", "asset", "2026-01-02 10:00:00", "Акции") # 4000

        r = client.get(f"/api/assets?user_id={USER_ID}&currency=USD")
        cats = [c["name"] for c in r.json()["by_category"]]
        assert cats[0] == "Крипта"
        assert cats[1] == "Акции"


class TestPercentages:
    def test_pct_sums_to_100(self, tmp_path):
        client, con = make_client(tmp_path)
        seed_entry(con, 1, "BTC", "asset", "2026-01-01 10:00:00", "Крипта")
        seed_entry(con, 1, "ETH", "asset", "2026-01-02 10:00:00", "Крипта")
        seed_entry(con, 1000, "SBER", "asset", "2026-01-03 10:00:00", "Акции")

        r = client.get(f"/api/assets?user_id={USER_ID}&currency=USD")
        total_pct = sum(p["pct"] for p in r.json()["positions"])
        # С округлением до 0.1% допускаем погрешность ±1%
        assert abs(total_pct - 100.0) <= 1.0

    def test_single_position_is_100pct(self, tmp_path):
        client, con = make_client(tmp_path)
        seed_entry(con, 5, "ETH", "asset", "2026-01-01 10:00:00", "Крипта")

        r = client.get(f"/api/assets?user_id={USER_ID}&currency=USD")
        assert r.json()["positions"][0]["pct"] == 100.0


class TestCurrencyConversion:
    def test_convert_to_rub(self, tmp_path):
        """
        USD qty=100. curr_rates: USD=1.0, RUB=0.011.
        В рублях: 100 * 1.0 / 0.011 ≈ 9090.9
        """
        client, con = make_client(tmp_path)
        seed_entry(con, 100, "USD", "asset", "2026-01-01 10:00:00", "Кэш")

        r = client.get(f"/api/assets?user_id={USER_ID}&currency=RUB")
        data = r.json()
        assert data["currency"] == "RUB"
        assert data["total"] == pytest.approx(100 / 0.011, rel=1e-2)

    def test_same_currency_value_equals_qty(self, tmp_path):
        """ETH qty=2.5, конвертируем в ETH → value=2.5."""
        rates = {**CURR_RATES, "ETH": 2000.0}
        client, con = make_client(tmp_path, rates_override=rates)
        seed_entry(con, 2.5, "ETH", "asset", "2026-01-01 10:00:00", "Крипта")

        r = client.get(f"/api/assets?user_id={USER_ID}&currency=ETH")
        assert r.json()["positions"][0]["value"] == pytest.approx(2.5, rel=1e-3)


class TestMissingRate:
    def test_unknown_ticker_value_is_zero(self, tmp_path):
        """Тикер без курса не ломает запрос, value=0."""
        client, con = make_client(tmp_path)
        seed_entry(con, 50, "UNKWN", "asset", "2026-01-01 10:00:00", "Акции")

        r = client.get(f"/api/assets?user_id={USER_ID}&currency=USD")
        data = r.json()
        assert r.status_code == 200
        pos = data["positions"][0]
        assert pos["ticker"] == "UNKWN"
        assert pos["value"] == 0.0


class TestOnlyAssetMode:
    def test_income_expense_excluded(self, tmp_path):
        """Расходы и доходы не попадают в портфель."""
        client, con = make_client(tmp_path)
        seed_entry(con, 1000, "RUB", "expense", "2026-01-01 10:00:00", "Продукты")
        seed_entry(con, 5000, "RUB", "income",  "2026-01-02 10:00:00", "Зарплата")
        seed_entry(con, 100,  "USD", "asset",   "2026-01-03 10:00:00", "Кэш")

        r = client.get(f"/api/assets?user_id={USER_ID}&currency=USD")
        data = r.json()
        assert len(data["positions"]) == 1
        assert data["positions"][0]["ticker"] == "USD"


class TestTimeline:
    def test_one_point_per_month(self, tmp_path):
        """По одной точке на каждый уникальный месяц в записях."""
        client, con = make_client(tmp_path)
        seed_entry(con, 1, "ETH", "asset", "2025-11-10 10:00:00", "Крипта")
        seed_entry(con, 1, "ETH", "asset", "2025-11-20 10:00:00", "Крипта")  # тот же месяц
        seed_entry(con, 2, "ETH", "asset", "2025-12-05 10:00:00", "Крипта")
        seed_entry(con, 3, "ETH", "asset", "2026-01-15 10:00:00", "Крипта")

        r = client.get(f"/api/assets?user_id={USER_ID}&currency=USD")
        months = [t["month"] for t in r.json()["timeline"]]
        assert months == ["2025-11", "2025-12", "2026-01"]

    def test_timeline_uses_historical_rates(self, tmp_path):
        """
        В timeline используются исторические курсы.
        Нояб-25: ETH=3000 (hist), 1 ETH → timeline[0].value ≈ 3000.
        Дек-25:  ETH=2500 (hist), 2 ETH → timeline[1].value ≈ 5000.
        (curr_rate ETH=2000 намеренно отличается от обоих)
        """
        rates_hist = [
            ("ETH", "2025-11-01", 3000.0, "monthly_avg"),
            ("ETH", "2025-12-01", 2500.0, "monthly_avg"),
            ("USD", "2025-11-01", 1.0, "monthly_avg"),
            ("USD", "2025-12-01", 1.0, "monthly_avg"),
        ]
        client, con = make_client(tmp_path)
        seed_rates(con, rates_hist)
        seed_entry(con, 1, "ETH", "asset", "2025-11-10 10:00:00", "Крипта")
        seed_entry(con, 2, "ETH", "asset", "2025-12-05 10:00:00", "Крипта")

        r = client.get(f"/api/assets?user_id={USER_ID}&currency=USD")
        tl = r.json()["timeline"]
        # Ноябрь: снапшот = 1 ETH × 3000 = 3000
        assert tl[0]["month"] == "2025-11"
        assert tl[0]["value"] == pytest.approx(3000.0, rel=1e-2)
        # Декабрь: снапшот = 2 ETH × 2500 = 5000
        assert tl[1]["month"] == "2025-12"
        assert tl[1]["value"] == pytest.approx(5000.0, rel=1e-2)

    def test_timeline_snapshot_excludes_future_entries(self, tmp_path):
        """
        В точке Nov-25 портфель содержит только записи ≤ Nov-25.
        Запись Dec-25 не должна влиять на Nov-25 срез.
        """
        rates_hist = [
            ("ETH", "2025-11-01", 3000.0, "monthly_avg"),
            ("ETH", "2025-12-01", 2500.0, "monthly_avg"),
            ("BTC", "2025-12-01", 50000.0, "monthly_avg"),
            ("USD", "2025-11-01", 1.0, "monthly_avg"),
            ("USD", "2025-12-01", 1.0, "monthly_avg"),
        ]
        client, con = make_client(tmp_path)
        seed_rates(con, rates_hist)
        seed_entry(con, 1, "ETH", "asset", "2025-11-10 10:00:00", "Крипта")
        seed_entry(con, 1, "BTC", "asset", "2025-12-01 10:00:00", "Крипта")  # появляется в дек

        r = client.get(f"/api/assets?user_id={USER_ID}&currency=USD")
        tl = r.json()["timeline"]
        nov = next(t for t in tl if t["month"] == "2025-11")
        # В ноябре BTC ещё нет → только 1 ETH × 3000
        assert nov["value"] == pytest.approx(3000.0, rel=1e-2)

    def test_timeline_labels_are_russian(self, tmp_path):
        """Метки месяцев на русском языке: 'Янв 26', 'Фев 26', ..."""
        client, con = make_client(tmp_path)
        seed_entry(con, 1, "USD", "asset", "2026-01-10 10:00:00", "Кэш")
        seed_entry(con, 1, "USD", "asset", "2026-02-10 10:00:00", "Кэш")

        r = client.get(f"/api/assets?user_id={USER_ID}&currency=USD")
        labels = [t["label"] for t in r.json()["timeline"]]
        assert labels == ["Янв 26", "Фев 26"]
