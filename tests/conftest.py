"""
Общие фикстуры для тестов.
Создаёт временную SQLite БД с минимальной схемой и тестовыми данными.
"""
import sqlite3
import pytest
from datetime import datetime, timezone
from pathlib import Path


SCHEMA = """
CREATE TABLE currency_rates (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    currency_code VARCHAR(10) NOT NULL,
    rate_date   DATE NOT NULL,
    rate_to_usd NUMERIC(20, 10) NOT NULL,
    source      VARCHAR(50) NOT NULL,
    created_at  DATETIME NOT NULL,
    CONSTRAINT uq_currency_rate_code_date UNIQUE (currency_code, rate_date)
);

CREATE TABLE users (
    id         BIGINT PRIMARY KEY,
    username   VARCHAR(64),
    first_seen DATETIME NOT NULL,
    last_seen  DATETIME NOT NULL
);

CREATE TABLE currencies (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      BIGINT NOT NULL,
    code         VARCHAR(32) NOT NULL,
    created_at   DATETIME NOT NULL,
    last_used_at DATETIME,
    CONSTRAINT uq_currency_user_code UNIQUE (user_id, code)
);

CREATE TABLE categories (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      BIGINT NOT NULL,
    mode         VARCHAR(16) NOT NULL CHECK(mode IN ('income','expense','asset')),
    name         VARCHAR(64) NOT NULL,
    created_at   DATETIME NOT NULL,
    last_used_at DATETIME,
    CONSTRAINT uq_category_user_mode_name UNIQUE (user_id, mode, name)
);

CREATE TABLE entries (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    mode        VARCHAR(16) NOT NULL CHECK(mode IN ('income','expense','asset')),
    amount      NUMERIC(28,10) NOT NULL,
    currency_id INTEGER,
    category_id INTEGER,
    note        VARCHAR(512),
    created_at  DATETIME NOT NULL,
    updated_at  DATETIME NOT NULL,
    FOREIGN KEY(currency_id) REFERENCES currencies(id) ON DELETE SET NULL,
    FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE SET NULL
);
"""

USER_ID = 1
NOW = datetime.now(timezone.utc).isoformat()


def make_db(path: str = ":memory:") -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    return con


def seed_rates(con: sqlite3.Connection, rates: list[tuple]):
    """
    rates: [(currency_code, rate_date, rate_to_usd, source), ...]
    """
    con.executemany("""
        INSERT OR REPLACE INTO currency_rates
            (currency_code, rate_date, rate_to_usd, source, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, [(c, d, r, s, NOW) for c, d, r, s in rates])
    con.commit()


def seed_entry(con: sqlite3.Connection, amount: float, currency_code: str,
               mode: str, created_at: str, category_name: str | None = None):
    """Добавляет валюту (если не существует), опционально категорию, и запись."""
    cur = con.cursor()

    cur.execute("SELECT id FROM currencies WHERE user_id=? AND code=?", (USER_ID, currency_code))
    row = cur.fetchone()
    if row:
        currency_id = row["id"]
    else:
        cur.execute(
            "INSERT INTO currencies (user_id, code, created_at) VALUES (?, ?, ?)",
            (USER_ID, currency_code, NOW),
        )
        currency_id = cur.lastrowid

    category_id = None
    if category_name:
        cur.execute("SELECT id FROM categories WHERE user_id=? AND name=?", (USER_ID, category_name))
        row = cur.fetchone()
        if row:
            category_id = row["id"]
        else:
            cur.execute(
                "INSERT INTO categories (user_id, mode, name, created_at) VALUES (?, ?, ?, ?)",
                (USER_ID, mode, category_name, NOW),
            )
            category_id = cur.lastrowid

    cur.execute("""
        INSERT INTO entries (user_id, mode, amount, currency_id, category_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (USER_ID, mode, amount, currency_id, category_id, created_at, created_at))
    con.commit()


@pytest.fixture
def db():
    """Пустая in-memory БД с правильной схемой."""
    con = make_db()
    yield con
    con.close()


@pytest.fixture
def db_with_rates():
    """
    БД с историческими курсами за сентябрь и март.
    Курсы намеренно разные — чтобы тест мог отличить «сентябрьский» от «мартовского».
    """
    con = make_db()
    seed_rates(con, [
        # Sep 2025
        ("RUB",  "2025-09-01", 0.0100, "monthly_avg"),  # 100 RUB = 1.00 USD
        ("VND",  "2025-09-01", 0.0000400, "monthly_avg"),  # 1 USD = 25 000 VND
        ("USD",  "2025-09-01", 1.0, "monthly_avg"),
        # Mar 2026
        ("RUB",  "2026-03-01", 0.0125, "monthly_avg"),  # 100 RUB = 1.25 USD
        ("VND",  "2026-03-01", 0.0000380, "monthly_avg"),  # 1 USD ≈ 26 315 VND
        ("USD",  "2026-03-01", 1.0, "monthly_avg"),
        # Apr 2026 (текущий месяц, source=daily)
        ("RUB",  "2026-04-01", 0.0124, "daily"),
        ("VND",  "2026-04-01", 0.0000382, "daily"),
        ("USD",  "2026-04-01", 1.0, "daily"),
    ])
    yield con
    con.close()