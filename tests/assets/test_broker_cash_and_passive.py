"""
Брокерский кэш как отдельная позиция и пассивный доход за скользящий год.

- RUB в «Акциях» (остаток до следующей покупки) не перетирает RUB во «Вкладе»;
  перенос валюты между другими категориями по-прежнему смена ярлыка, а не вторая позиция.
- Пассивный доход: сумма за 12 завершённых месяцев / 12, а не / число месяцев с выплатами.
"""
from datetime import datetime, timezone

from tests.assets.test_assets_api import make_client
from tests.conftest import seed_entry, USER_ID


def _month(delta: int) -> str:
    """'YYYY-MM' со сдвигом от текущего месяца."""
    now = datetime.now(timezone.utc)
    m = now.month - 1 + delta
    return f"{now.year + m // 12:04d}-{m % 12 + 1:02d}"


def _assets(client, currency="RUB"):
    r = client.get(f"/api/assets?user_id={USER_ID}&currency={currency}")
    assert r.status_code == 200
    return r.json()


class TestBrokerCash:
    def test_rub_in_stocks_does_not_replace_deposit(self, tmp_path):
        client, con = make_client(tmp_path)
        seed_entry(con, 1_000_000, "RUB", "asset", "2026-01-10 10:00:00", "Вклад")
        seed_entry(con, 15_000, "RUB", "asset", "2026-01-11 10:00:00", "Акции")

        data = _assets(client)
        by_cat = {(p["ticker"], p["category"]): p["qty"] for p in data["positions"]}
        assert by_cat == {("RUB", "Вклад"): 1_000_000, ("RUB", "Акции"): 15_000}
        assert data["total"] == 1_015_000
        assert data["timeline"][-1]["value"] == 1_015_000

    def test_each_rub_position_has_own_snapshots(self, tmp_path):
        client, con = make_client(tmp_path)
        seed_entry(con, 1_000_000, "RUB", "asset", "2026-01-10 10:00:00", "Вклад")
        seed_entry(con, 15_000, "RUB", "asset", "2026-01-11 10:00:00", "Акции")
        seed_entry(con, 900_000, "RUB", "asset", "2026-02-10 10:00:00", "Вклад")
        seed_entry(con, 0, "RUB", "asset", "2026-02-11 10:00:00", "Акции")  # всё потратил на бумаги

        data = _assets(client)
        assert [(p["ticker"], p["category"], p["qty"]) for p in data["positions"]] == [
            ("RUB", "Вклад", 900_000)]

    def test_moving_other_currency_between_categories_is_relabel(self, tmp_path):
        """USD из «Кэша» в «Ликвидность» — одна позиция, старая не висит дублем."""
        client, con = make_client(tmp_path)
        seed_entry(con, 4400, "USD", "asset", "2026-01-10 10:00:00", "Кэш")
        seed_entry(con, 4100, "USD", "asset", "2026-02-10 10:00:00", "Ликвидность")

        data = _assets(client, "USD")
        assert [(p["ticker"], p["category"], p["qty"]) for p in data["positions"]] == [
            ("USD", "Ликвидность", 4100)]

    def test_asset_history_filters_by_category(self, tmp_path):
        client, con = make_client(tmp_path)
        seed_entry(con, 1_000_000, "RUB", "asset", "2026-01-10 10:00:00", "Вклад")
        seed_entry(con, 15_000, "RUB", "asset", "2026-01-11 10:00:00", "Акции")

        def qtys(category):
            r = client.get(f"/api/asset-history?user_id={USER_ID}&ticker=RUB"
                           f"&category={category}&currency=RUB")
            return {p["qty"] for p in r.json()["points"]}

        assert qtys("Акции") == {15_000}
        assert qtys("Вклад") == {1_000_000}


class TestPassiveIncome:
    def test_average_over_rolling_year_not_payout_months(self, tmp_path):
        client, con = make_client(tmp_path)
        seed_entry(con, 1, "RUB", "expense", f"{_month(-14)}-05 10:00:00", "Еда")  # начало учёта
        seed_entry(con, 12_000, "RUB", "income", f"{_month(-3)}-15 10:00:00", "Дивиденды")
        seed_entry(con, 6_000, "RUB", "income", f"{_month(-8)}-15 10:00:00", "Дивиденды")

        p = _assets(client)["passive_income"]
        assert p["months_count"] == 12
        assert p["avg_monthly"] == 1_500          # 18 000 / 12, а не / 2

    def test_window_excludes_current_and_older_than_year(self, tmp_path):
        client, con = make_client(tmp_path)
        seed_entry(con, 1, "RUB", "expense", f"{_month(-20)}-05 10:00:00", "Еда")
        seed_entry(con, 99_000, "RUB", "income", f"{_month(-13)}-15 10:00:00", "Дивиденды")
        seed_entry(con, 12_000, "RUB", "income", f"{_month(-12)}-15 10:00:00", "Проценты")
        seed_entry(con, 50_000, "RUB", "income", f"{_month(0)}-01 10:00:00", "Дивиденды")
        seed_entry(con, 70_000, "RUB", "income", f"{_month(-1)}-15 10:00:00", "Зарплата")

        p = _assets(client)["passive_income"]
        assert p["avg_monthly"] == 1_000          # только 12 000 за -12-й месяц
        assert p["this_month"] == 50_000

    def test_short_history_divides_by_months_since_start(self, tmp_path):
        client, con = make_client(tmp_path)
        seed_entry(con, 3_000, "RUB", "income", f"{_month(-3)}-15 10:00:00", "Проценты")

        p = _assets(client)["passive_income"]
        assert p["months_count"] == 3             # -3, -2, -1
        assert p["avg_monthly"] == 1_000
