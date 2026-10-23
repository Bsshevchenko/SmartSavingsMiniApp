"""
Новый тикер без цены: сразу после сохранения записи подтягиваются текущие цены,
а не ждём ночного апдейта (иначе позиция сутки показывает 0).
"""
import pytest

import server
from tests.assets.test_assets_api import make_client
from tests.conftest import seed_rates


@pytest.fixture
def calls(monkeypatch):
    calls = []
    monkeypatch.setattr(server, "_run_daily", lambda: calls.append(1))
    return calls


def _post(client, mode, currency, category="Акции"):
    r = client.post("/api/entry", json={"mode": mode, "amount": 10, "currency": currency,
                                        "category": category})
    assert r.status_code == 200


def test_new_asset_ticker_triggers_rates_update(tmp_path, calls):
    client, _ = make_client(tmp_path)
    _post(client, "asset", "LQDT")
    assert calls == [1]


def test_ticker_with_known_price_does_not(tmp_path, calls):
    client, con = make_client(tmp_path)
    seed_rates(con, [("SBER", "2026-10-01", 3.4, "daily")])
    _post(client, "asset", "SBER")
    assert calls == []


def test_expense_never_triggers(tmp_path, calls):
    client, _ = make_client(tmp_path)
    _post(client, "expense", "THB", "Еда")
    assert calls == []
