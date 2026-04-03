"""
Тесты db_repo.save_entry — сохранение транзакций из Mini App в БД.

Пользовательские сценарии:
  1. Новый пользователь — первая транзакция создаёт user + currency + category + entry
  2. Повторная транзакция той же валютой — обновляет last_used_at, не дублирует
  3. Повторная транзакция той же категорией — обновляет last_used_at, не дублирует
  4. Расход без категории (например, asset) — category_id = NULL
  5. Транзакция без заметки — note = NULL
  6. Транзакция с заметкой — note сохраняется
  7. Разные режимы (expense / income / asset) — сохраняются корректно
  8. Несколько пользователей — данные изолированы (у каждого свои currency_id)
  9. Дробные суммы — сохраняются точно (NUMERIC)
 10. Невалидный mode — база отклоняет (CHECK constraint)
"""
import sqlite3
import time

import pytest

import db_repo
from tests.conftest import make_db

USER_A = 100001
USER_B = 100002


@pytest.fixture
def tmp_db(tmp_path):
    db_path = tmp_path / "test.db"
    make_db(str(db_path)).close()
    return db_path


def q(db_path, sql, params=()):
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    con.close()
    return rows


# ── 1. Новый пользователь ─────────────────────────────────────────────────────

def test_first_entry_creates_user(tmp_db):
    db_repo.save_entry(USER_A, "expense", 100.0, "VND", "Кафе", db_path=tmp_db)
    rows = q(tmp_db, "SELECT * FROM users WHERE id=?", (USER_A,))
    assert len(rows) == 1
    assert rows[0]["first_seen"] is not None
    assert rows[0]["last_seen"] is not None


def test_first_entry_creates_currency(tmp_db):
    db_repo.save_entry(USER_A, "expense", 100.0, "VND", "Кафе", db_path=tmp_db)
    rows = q(tmp_db, "SELECT * FROM currencies WHERE user_id=? AND code='VND'", (USER_A,))
    assert len(rows) == 1
    assert rows[0]["last_used_at"] is not None


def test_first_entry_creates_category(tmp_db):
    db_repo.save_entry(USER_A, "expense", 100.0, "VND", "Кафе", db_path=tmp_db)
    rows = q(tmp_db, "SELECT * FROM categories WHERE user_id=? AND name='Кафе'", (USER_A,))
    assert len(rows) == 1


def test_first_entry_creates_entry_record(tmp_db):
    entry_id = db_repo.save_entry(USER_A, "expense", 100.0, "VND", "Кафе", db_path=tmp_db)
    rows = q(tmp_db, "SELECT * FROM entries WHERE id=?", (entry_id,))
    assert len(rows) == 1
    assert rows[0]["user_id"] == USER_A
    assert rows[0]["mode"] == "expense"
    assert float(rows[0]["amount"]) == 100.0


# ── 2. Повторная транзакция — дубликат валюты не создаётся ────────────────────

def test_same_currency_twice_no_duplicate(tmp_db):
    db_repo.save_entry(USER_A, "expense", 100.0, "VND", "Кафе", db_path=tmp_db)
    db_repo.save_entry(USER_A, "expense", 200.0, "VND", "Продукты", db_path=tmp_db)

    rows = q(tmp_db, "SELECT * FROM currencies WHERE user_id=? AND code='VND'", (USER_A,))
    assert len(rows) == 1, "Должна быть одна запись валюты VND, а не две"


def test_same_currency_updates_last_used_at(tmp_db):
    db_repo.save_entry(USER_A, "expense", 100.0, "VND", "Кафе", db_path=tmp_db)
    first_ts = q(tmp_db, "SELECT last_used_at FROM currencies WHERE user_id=?", (USER_A,))[0]["last_used_at"]

    time.sleep(0.01)
    db_repo.save_entry(USER_A, "expense", 200.0, "VND", "Продукты", db_path=tmp_db)
    second_ts = q(tmp_db, "SELECT last_used_at FROM currencies WHERE user_id=?", (USER_A,))[0]["last_used_at"]

    assert second_ts > first_ts, "last_used_at должен обновляться при каждом использовании"


# ── 3. Повторная транзакция — дубликат категории не создаётся ─────────────────

def test_same_category_twice_no_duplicate(tmp_db):
    db_repo.save_entry(USER_A, "expense", 100.0, "VND", "Кафе", db_path=tmp_db)
    db_repo.save_entry(USER_A, "expense", 200.0, "RUB", "Кафе", db_path=tmp_db)

    rows = q(tmp_db, "SELECT * FROM categories WHERE user_id=? AND name='Кафе'", (USER_A,))
    assert len(rows) == 1, "Должна быть одна запись категории 'Кафе'"


def test_same_category_updates_last_used_at(tmp_db):
    db_repo.save_entry(USER_A, "expense", 100.0, "VND", "Кафе", db_path=tmp_db)
    ts1 = q(tmp_db, "SELECT last_used_at FROM categories WHERE user_id=?", (USER_A,))[0]["last_used_at"]

    time.sleep(0.01)
    db_repo.save_entry(USER_A, "expense", 200.0, "VND", "Кафе", db_path=tmp_db)
    ts2 = q(tmp_db, "SELECT last_used_at FROM categories WHERE user_id=?", (USER_A,))[0]["last_used_at"]

    assert ts2 > ts1


# ── 4. Транзакция без категории ───────────────────────────────────────────────

def test_entry_without_category_has_null_category_id(tmp_db):
    entry_id = db_repo.save_entry(USER_A, "asset", 50000.0, "RUB", category_name=None, db_path=tmp_db)
    rows = q(tmp_db, "SELECT category_id FROM entries WHERE id=?", (entry_id,))
    assert rows[0]["category_id"] is None


def test_entry_with_empty_string_category_treated_as_null(tmp_db):
    """Пустая строка из UI интерпретируется как отсутствие категории."""
    entry_id = db_repo.save_entry(USER_A, "expense", 100.0, "VND", category_name="", db_path=tmp_db)
    rows = q(tmp_db, "SELECT category_id FROM entries WHERE id=?", (entry_id,))
    # "" → None в bot.py (category or None), тест проверяет db_repo напрямую
    # db_repo получает None если bot.py передаёт None, здесь передаём "" → должен быть None
    assert rows[0]["category_id"] is None, "Пустая категория должна сохраняться как NULL"


# ── 5-6. Заметка ──────────────────────────────────────────────────────────────

def test_entry_without_note_is_null(tmp_db):
    entry_id = db_repo.save_entry(USER_A, "expense", 100.0, "VND", "Кафе", note=None, db_path=tmp_db)
    rows = q(tmp_db, "SELECT note FROM entries WHERE id=?", (entry_id,))
    assert rows[0]["note"] is None


def test_entry_with_note_saved_correctly(tmp_db):
    entry_id = db_repo.save_entry(USER_A, "expense", 100.0, "VND", "Кафе", note="Бизнес-ланч", db_path=tmp_db)
    rows = q(tmp_db, "SELECT note FROM entries WHERE id=?", (entry_id,))
    assert rows[0]["note"] == "Бизнес-ланч"


# ── 7. Режимы ─────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("mode", ["expense", "income", "asset"])
def test_all_modes_saved_correctly(tmp_db, mode):
    entry_id = db_repo.save_entry(USER_A, mode, 100.0, "VND", db_path=tmp_db)
    rows = q(tmp_db, "SELECT mode FROM entries WHERE id=?", (entry_id,))
    assert rows[0]["mode"] == mode


# ── 8. Изоляция пользователей ─────────────────────────────────────────────────

def test_two_users_have_separate_currency_records(tmp_db):
    """У разных пользователей разные currency_id, даже для одной и той же валюты."""
    db_repo.save_entry(USER_A, "expense", 100.0, "VND", "Кафе", db_path=tmp_db)
    db_repo.save_entry(USER_B, "expense", 200.0, "VND", "Кафе", db_path=tmp_db)

    cur_a = q(tmp_db, "SELECT id FROM currencies WHERE user_id=?", (USER_A,))[0]["id"]
    cur_b = q(tmp_db, "SELECT id FROM currencies WHERE user_id=?", (USER_B,))[0]["id"]
    assert cur_a != cur_b, "Каждый пользователь должен иметь свою запись валюты"


def test_two_users_entries_do_not_interfere(tmp_db):
    """Транзакции одного пользователя не видны в запросе другого."""
    db_repo.save_entry(USER_A, "expense", 100.0, "VND", db_path=tmp_db)
    db_repo.save_entry(USER_B, "income", 9999.0, "USD", db_path=tmp_db)

    entries_a = q(tmp_db, "SELECT * FROM entries WHERE user_id=?", (USER_A,))
    entries_b = q(tmp_db, "SELECT * FROM entries WHERE user_id=?", (USER_B,))

    assert len(entries_a) == 1
    assert float(entries_a[0]["amount"]) == 100.0
    assert len(entries_b) == 1
    assert float(entries_b[0]["amount"]) == 9999.0


# ── 9. Дробные суммы ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("amount", [0.01, 99.99, 1_000_000.5, 0.001])
def test_fractional_amounts_saved_precisely(tmp_db, amount):
    entry_id = db_repo.save_entry(USER_A, "expense", amount, "USD", db_path=tmp_db)
    rows = q(tmp_db, "SELECT amount FROM entries WHERE id=?", (entry_id,))
    assert abs(float(rows[0]["amount"]) - amount) < 1e-6


# ── 10. Невалидный mode ───────────────────────────────────────────────────────

def test_invalid_mode_rejected_by_db(tmp_db):
    """БД отклоняет записи с невалидным режимом через CHECK constraint."""
    with pytest.raises(Exception):
        db_repo.save_entry(USER_A, "transfer", 100.0, "VND", db_path=tmp_db)


# ── 11. username сохраняется ──────────────────────────────────────────────────

def test_username_saved_on_first_entry(tmp_db):
    db_repo.save_entry(USER_A, "expense", 100.0, "VND", username="alice", db_path=tmp_db)
    rows = q(tmp_db, "SELECT username FROM users WHERE id=?", (USER_A,))
    assert rows[0]["username"] == "alice"


def test_username_updated_on_subsequent_entry(tmp_db):
    db_repo.save_entry(USER_A, "expense", 100.0, "VND", username="alice", db_path=tmp_db)
    db_repo.save_entry(USER_A, "expense", 200.0, "VND", username="alice_new", db_path=tmp_db)
    rows = q(tmp_db, "SELECT username FROM users WHERE id=?", (USER_A,))
    assert rows[0]["username"] == "alice_new"


# ── 12. Целостность данных ────────────────────────────────────────────────────

def test_entry_currency_id_matches_currencies_table(tmp_db):
    """currency_id в entries указывает на реальную запись в currencies."""
    entry_id = db_repo.save_entry(USER_A, "expense", 100.0, "VND", db_path=tmp_db)
    entry = q(tmp_db, "SELECT currency_id FROM entries WHERE id=?", (entry_id,))[0]
    currency = q(tmp_db, "SELECT id, code FROM currencies WHERE id=?", (entry["currency_id"],))[0]
    assert currency["code"] == "VND"


def test_entry_category_id_matches_categories_table(tmp_db):
    """category_id в entries указывает на реальную запись в categories."""
    entry_id = db_repo.save_entry(USER_A, "expense", 100.0, "VND", "Транспорт", db_path=tmp_db)
    entry = q(tmp_db, "SELECT category_id FROM entries WHERE id=?", (entry_id,))[0]
    category = q(tmp_db, "SELECT id, name FROM categories WHERE id=?", (entry["category_id"],))[0]
    assert category["name"] == "Транспорт"


def test_multiple_entries_accumulate_correctly(tmp_db):
    """5 транзакций → 5 записей в entries, 1 запись в currencies."""
    for i in range(5):
        db_repo.save_entry(USER_A, "expense", float(i + 1) * 100, "VND", "Кафе", db_path=tmp_db)

    entries = q(tmp_db, "SELECT COUNT(*) as cnt FROM entries WHERE user_id=?", (USER_A,))
    currencies = q(tmp_db, "SELECT COUNT(*) as cnt FROM currencies WHERE user_id=?", (USER_A,))
    categories = q(tmp_db, "SELECT COUNT(*) as cnt FROM categories WHERE user_id=?", (USER_A,))

    assert entries[0]["cnt"] == 5
    assert currencies[0]["cnt"] == 1   # VND создана только один раз
    assert categories[0]["cnt"] == 1   # "Кафе" создана только один раз