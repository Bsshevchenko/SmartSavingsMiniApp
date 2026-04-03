"""
Тесты конвертации валют с учётом исторических курсов.

Ключевой сценарий: одна и та же сумма в RUB за разные месяцы
должна конвертироваться в VND по РАЗНЫМ курсам.
"""
import pytest
from server import convert_h, load_historical_rates
from tests.conftest import seed_rates


# ── Базовые случаи ────────────────────────────────────────────────────────────

def test_same_currency_no_conversion():
    """Конвертация одной валюты в себя всегда возвращает исходную сумму."""
    result = convert_h(1000.0, "VND", "VND", "2025-09", {}, {})
    assert result == 1000.0


def test_same_currency_zero_amount():
    result = convert_h(0.0, "RUB", "RUB", "2025-09", {}, {})
    assert result == 0.0


def test_missing_both_rates_returns_zero():
    """Нет курса ни в истории, ни в текущих — возвращает 0, не падает."""
    result = convert_h(100.0, "XYZ", "ABC", "2025-09", {}, {})
    assert result == 0.0


def test_missing_src_rate_returns_zero():
    curr = {"VND": 0.00004}
    result = convert_h(100.0, "XYZ", "VND", "2025-09", {}, curr)
    assert result == 0.0


def test_missing_dst_rate_returns_zero():
    curr = {"RUB": 0.011}
    result = convert_h(100.0, "RUB", "XYZ", "2025-09", {}, curr)
    assert result == 0.0


# ── Исторические курсы ────────────────────────────────────────────────────────

def test_historical_rate_used_over_current():
    """
    Если есть исторический курс за месяц — он должен использоваться,
    а не текущий (curr_rates).
    """
    hist = {("RUB", "2025-09"): 0.0100}   # исторический: 100 RUB = 1 USD
    curr = {"RUB": 0.0200}                  # текущий в 2 раза лучше — НЕ должен использоваться
    hist[("USD", "2025-09")] = 1.0
    curr["USD"] = 1.0

    result = convert_h(100.0, "RUB", "USD", "2025-09", hist, curr)

    # Должен использоваться исторический: 100 * 0.010 / 1.0 = 1.0
    assert abs(result - 1.0) < 0.0001


def test_fallback_to_current_when_no_history():
    """Нет исторического курса — должен использоваться текущий."""
    hist = {}
    curr = {"RUB": 0.0110, "USD": 1.0}

    result = convert_h(100.0, "RUB", "USD", "2025-09", hist, curr)

    assert abs(result - 1.1) < 0.0001  # 100 * 0.011 / 1.0


# ── Ключевой сценарий: исторические курсы реально влияют на результат ────────

def test_same_rub_amount_different_months_gives_different_vnd():
    """
    100 RUB потраченных в сентябре и в марте должны конвертироваться в разное
    количество VND — потому что курсы за эти месяцы разные.
    Это и есть цель всей фичи.
    """
    hist = {
        # Sep 2025: 100 RUB = 1 USD, 1 USD = 25 000 VND
        ("RUB", "2025-09"): 0.0100,
        ("VND", "2025-09"): 0.0000400,
        # Mar 2026: 100 RUB = 1.25 USD, 1 USD ≈ 26 315 VND
        ("RUB", "2026-03"): 0.0125,
        ("VND", "2026-03"): 0.0000380,
    }
    curr = {}

    vnd_sep = convert_h(100.0, "RUB", "VND", "2025-09", hist, curr)
    vnd_mar = convert_h(100.0, "RUB", "VND", "2026-03", hist, curr)

    # Sep: 100 * 0.010 / 0.000040 = 25 000 VND
    assert abs(vnd_sep - 25_000) < 1

    # Mar: 100 * 0.0125 / 0.000038 ≈ 32 894 VND
    assert abs(vnd_mar - 32_894) < 1

    # Результаты должны быть разными
    assert vnd_sep != vnd_mar


def test_usdt_to_vnd_historical():
    """USDT ≈ USD, но курс VND меняется — проверяем конвертацию стейблкоина."""
    hist = {
        ("USDT", "2025-09"): 1.001,
        ("VND",  "2025-09"): 0.0000400,  # 1 USD = 25 000 VND
        ("USDT", "2026-03"): 0.9998,
        ("VND",  "2026-03"): 0.0000380,  # 1 USD ≈ 26 315 VND
    }
    curr = {}

    vnd_sep = convert_h(100.0, "USDT", "VND", "2025-09", hist, curr)
    vnd_mar = convert_h(100.0, "USDT", "VND", "2026-03", hist, curr)

    # Sep: 100 * 1.001 / 0.000040 = 2 502 500
    assert abs(vnd_sep - 2_502_500) < 100

    assert vnd_sep != vnd_mar


# ── Работа с реальной БД ──────────────────────────────────────────────────────

def test_load_historical_rates_from_db(db_with_rates):
    """load_historical_rates корректно читает курсы из БД."""
    hist = load_historical_rates(db_with_rates)

    assert ("RUB", "2025-09") in hist
    assert ("VND", "2025-09") in hist
    assert ("RUB", "2026-03") in hist

    # Проверяем конкретные значения
    assert abs(hist[("RUB", "2025-09")] - 0.0100) < 0.0001
    assert abs(hist[("RUB", "2026-03")] - 0.0125) < 0.0001


def test_convert_with_db_rates(db_with_rates):
    """Сквозной тест: load_historical_rates + convert_h дают корректный результат."""
    hist = load_historical_rates(db_with_rates)
    curr = {}

    # Sep 2025: 100 RUB → VND
    vnd_sep = convert_h(100.0, "RUB", "VND", "2025-09", hist, curr)
    assert abs(vnd_sep - 25_000) < 1  # 100 * 0.010 / 0.000040

    # Mar 2026: те же 100 RUB → VND по мартовскому курсу
    vnd_mar = convert_h(100.0, "RUB", "VND", "2026-03", hist, curr)
    # 100 * 0.0125 / 0.000038 ≈ 32 894
    assert abs(vnd_mar - 32_894) < 1

    assert vnd_sep < vnd_mar  # в марте рубль стал стоить больше VND


def test_monthly_avg_for_month_with_multiple_entries(db):
    """
    Если в одном месяце несколько записей (разные даты) —
    load_historical_rates должен вернуть среднее.
    """
    seed_rates(db, [
        ("RUB", "2025-10-01", 0.0110, "daily"),
        ("RUB", "2025-10-15", 0.0120, "daily"),
        ("RUB", "2025-10-22", 0.0130, "daily"),
    ])
    hist = load_historical_rates(db)

    # Среднее: (0.011 + 0.012 + 0.013) / 3 = 0.012
    assert abs(hist[("RUB", "2025-10")] - 0.0120) < 0.0001