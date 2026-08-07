"""
P&L портфеля за всё время (metrics.portfolio_pnl).

Сделок в базе нет — только снапшоты количества, поэтому результат восстанавливается
помесячно: сколько лежало × изменение цены. Цена берётся сразу в валюте отображения,
поэтому переоценка актива и валютная переоценка считаются одной формулой.
"""
import pytest

from metrics import portfolio_pnl, price_at, price_lookup


def row(**kw):
    return kw


def rates(pairs):
    """{(код, месяц): курс к USD}"""
    return {(code, month): rate for code, month, rate in pairs}


def test_price_change_is_the_profit():
    """Держали 10 бумаг, цена в USD выросла с 2 до 3 → +10 USD."""
    hist = rates([("AAA", "2026-01", 2.0), ("AAA", "2026-02", 3.0)])
    assets = [row(code="AAA", category="Акции", month="2026-01", amt=10)]
    res = portfolio_pnl(assets, [], ["2026-01", "2026-02"], "USD", hist, {})
    assert res["by_ticker"]["AAA"] == pytest.approx(10.0)
    assert res["total"] == pytest.approx(10.0)


def test_purchase_is_not_profit():
    """Докупка в феврале сама по себе прибыли не создаёт — цена не менялась."""
    hist = rates([("AAA", "2026-01", 2.0), ("AAA", "2026-02", 2.0)])
    assets = [row(code="AAA", category="Акции", month="2026-01", amt=10),
              row(code="AAA", category="Акции", month="2026-02", amt=50)]
    res = portfolio_pnl(assets, [], ["2026-01", "2026-02"], "USD", hist, {})
    assert res["total"] == pytest.approx(0.0)


def test_profit_counted_on_quantity_held_before_the_move():
    """Прибыль считается по количеству ДО роста: докупка в феврале в ней не участвует."""
    hist = rates([("AAA", "2026-01", 2.0), ("AAA", "2026-02", 3.0)])
    assets = [row(code="AAA", category="Акции", month="2026-01", amt=10),
              row(code="AAA", category="Акции", month="2026-02", amt=100)]
    res = portfolio_pnl(assets, [], ["2026-01", "2026-02"], "USD", hist, {})
    assert res["total"] == pytest.approx(10.0)   # 10 бумаг × +1, а не 100


def test_currency_revaluation_shows_up_in_rubles():
    """Долларовый кэш в рублях: доллар подорожал с 80 до 100 ₽ → +2000 ₽ на 100 $."""
    hist = rates([("RUB", "2026-01", 1 / 80), ("RUB", "2026-02", 1 / 100)])
    assets = [row(code="USD", category="Ликвидность", month="2026-01", amt=100)]
    res = portfolio_pnl(assets, [], ["2026-01", "2026-02"], "RUB", hist, {})
    assert res["by_ticker"]["USD"] == pytest.approx(2000.0)


def test_same_currency_asset_has_no_revaluation():
    """Рубли в рублях не переоцениваются — весь результат вклада это проценты."""
    hist = rates([("RUB", "2026-01", 1 / 80), ("RUB", "2026-02", 1 / 100)])
    assets = [row(code="RUB", category="Вклад", month="2026-01", amt=100_000)]
    res = portfolio_pnl(assets, [], ["2026-01", "2026-02"], "RUB", hist, {"Вклад": 5000})
    assert res["by_ticker"].get("RUB", 0) == pytest.approx(0.0)
    assert res["by_category"]["Вклад"] == pytest.approx(5000.0)


def test_income_goes_to_its_category_and_total():
    """Дивиденды рынок не создаёт: они приходят отдельно и попадают в свою категорию."""
    hist = rates([("AAA", "2026-01", 2.0), ("AAA", "2026-02", 1.0)])
    assets = [row(code="AAA", category="Акции", month="2026-01", amt=10)]
    res = portfolio_pnl(assets, [], ["2026-01", "2026-02"], "USD", hist, {"Акции": 4.0})
    assert res["by_ticker"]["AAA"] == pytest.approx(-10.0)   # переоценка отдельно
    assert res["by_category"]["Акции"] == pytest.approx(-6.0)  # −10 + 4 дивидендов
    assert res["total"] == pytest.approx(-6.0)


def test_realty_revalues_only_through_currency_when_price_is_frozen():
    """Цена м² не менялась: в рублях 0, а в долларах — движение курса."""
    hist = rates([("RUB", "2026-01", 1 / 80), ("RUB", "2026-02", 1 / 100)])
    realty = [row(id=1, area_m2=30, currency="RUB", price_per_m2=100_000, rate_month="2026-01")]
    months = ["2026-01", "2026-02"]
    in_rub = portfolio_pnl([], realty, months, "RUB", hist, {})
    in_usd = portfolio_pnl([], realty, months, "USD", hist, {})
    assert in_rub["realty"] == pytest.approx(0.0)
    assert in_usd["realty"] == pytest.approx(30 * 100_000 * (1 / 100 - 1 / 80))


def test_ticker_that_changed_category_counts_once_in_the_current_one():
    """USD переехал из «Кэш» в «Ликвидность»: весь его результат идёт в текущую
    категорию — там же, где страница показывает саму позицию. Иначе итог категории
    не сходится с суммой позиций внутри неё."""
    hist = rates([("RUB", "2026-01", 1 / 80), ("RUB", "2026-02", 1 / 90),
                  ("RUB", "2026-03", 1 / 100)])
    assets = [row(code="USD", category="Кэш", month="2026-01", amt=100),
              row(code="USD", category="Ликвидность", month="2026-02", amt=100)]
    res = portfolio_pnl(assets, [], ["2026-01", "2026-02", "2026-03"], "RUB", hist, {})
    assert res["by_ticker"]["USD"] == pytest.approx(2000.0)      # 100 $ × (100 − 80) ₽
    assert res["by_category"]["Ликвидность"] == pytest.approx(2000.0)
    assert "Кэш" not in res["by_category"]


def test_categories_sum_to_total():
    """Сумма по категориям всегда равна общему результату — иначе цифры на странице разъедутся."""
    hist = rates([("AAA", "2026-01", 2.0), ("AAA", "2026-02", 3.0),
                  ("RUB", "2026-01", 1 / 80), ("RUB", "2026-02", 1 / 100)])
    assets = [row(code="AAA", category="Акции", month="2026-01", amt=10),
              row(code="RUB", category="Вклад", month="2026-01", amt=100_000)]
    realty = [row(id=1, area_m2=30, currency="RUB", price_per_m2=100_000, rate_month="2026-01")]
    res = portfolio_pnl(assets, realty, ["2026-01", "2026-02"], "USD", hist, {"Вклад": 12.0})
    assert sum(res["by_category"].values()) == pytest.approx(res["total"])


def test_price_before_first_quote_falls_back_to_first_known():
    """До появления первой цены берём первую известную: иначе позиция «появлялась
    из воздуха» в месяц первой котировки и это выглядело бы как рыночный рост."""
    lookup = price_lookup(rates([("AAA", "2026-05", 7.0)]))
    assert price_at(lookup, "AAA", "2026-01", "USD") == pytest.approx(7.0)


def test_empty_portfolio_is_zero():
    assert portfolio_pnl([], [], [], "RUB", {}, {})["total"] == 0
