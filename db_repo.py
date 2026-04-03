"""
Слой работы с БД для сохранения транзакций из Mini App.

Все операции синхронные (sqlite3). В bot.py вызываются через asyncio.to_thread.
"""
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "app.db"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_or_create_user(cur: sqlite3.Cursor, user_id: int, username: str | None) -> None:
    """Создаёт пользователя при первом визите, обновляет last_seen при каждом."""
    now = _now()
    cur.execute("""
        INSERT INTO users (id, username, first_seen, last_seen)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            username  = excluded.username,
            last_seen = excluded.last_seen
    """, (user_id, username, now, now))


def _get_or_create_currency(cur: sqlite3.Cursor, user_id: int, code: str) -> int:
    """Возвращает id валюты, создаёт если не существует. Обновляет last_used_at."""
    now = _now()
    cur.execute("""
        INSERT INTO currencies (user_id, code, created_at, last_used_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id, code) DO UPDATE SET last_used_at = excluded.last_used_at
    """, (user_id, code, now, now))
    cur.execute("SELECT id FROM currencies WHERE user_id=? AND code=?", (user_id, code))
    return cur.fetchone()["id"]


def _get_or_create_category(cur: sqlite3.Cursor, user_id: int, mode: str, name: str) -> int:
    """Возвращает id категории, создаёт если не существует. Обновляет last_used_at."""
    now = _now()
    cur.execute("""
        INSERT INTO categories (user_id, mode, name, created_at, last_used_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id, mode, name) DO UPDATE SET last_used_at = excluded.last_used_at
    """, (user_id, mode, name, now, now))
    cur.execute(
        "SELECT id FROM categories WHERE user_id=? AND mode=? AND name=?",
        (user_id, mode, name),
    )
    return cur.fetchone()["id"]


def save_entry(
    user_id: int,
    mode: str,
    amount: float,
    currency_code: str,
    category_name: str | None = None,
    note: str | None = None,
    username: str | None = None,
    db_path: Path = DB_PATH,
) -> int:
    """
    Сохраняет транзакцию в БД.

    Порядок операций:
      1. upsert users (создать/обновить last_seen)
      2. upsert currencies (создать/обновить last_used_at) → currency_id
      3. upsert categories если category_name задано → category_id (иначе None)
      4. INSERT INTO entries

    Возвращает id созданной записи.
    """
    now = _now()

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    try:
        _get_or_create_user(cur, user_id, username)
        currency_id = _get_or_create_currency(cur, user_id, currency_code)
        category_id = (
            _get_or_create_category(cur, user_id, mode, category_name)
            if category_name
            else None
        )

        cur.execute("""
            INSERT INTO entries (user_id, mode, amount, currency_id, category_id, note, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (user_id, mode, amount, currency_id, category_id, note, now, now))

        entry_id = cur.lastrowid
        con.commit()
        return entry_id

    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def get_entries(
    user_id: int,
    limit: int = 30,
    offset: int = 0,
    db_path: Path = DB_PATH,
) -> list[dict]:
    """Возвращает записи пользователя, отсортированные по дате (новые первые)."""
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("""
        SELECT e.id, e.mode, CAST(e.amount AS REAL) as amount,
               c.code as currency, cat.name as category,
               e.note, e.created_at
        FROM entries e
        JOIN currencies c ON c.id = e.currency_id
        LEFT JOIN categories cat ON cat.id = e.category_id
        WHERE e.user_id = ?
        ORDER BY e.created_at DESC
        LIMIT ? OFFSET ?
    """, (user_id, limit, offset))
    rows = [dict(r) for r in cur.fetchall()]
    cur.execute("SELECT COUNT(*) as cnt FROM entries WHERE user_id=?", (user_id,))
    total = cur.fetchone()["cnt"]
    con.close()
    return rows, total


def delete_entry(user_id: int, entry_id: int, db_path: Path = DB_PATH) -> bool:
    """Удаляет запись. Проверяет принадлежность пользователю. Возвращает True если удалено."""
    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.execute("DELETE FROM entries WHERE id=? AND user_id=?", (entry_id, user_id))
    deleted = cur.rowcount > 0
    con.commit()
    con.close()
    return deleted


def update_entry(
    user_id: int,
    entry_id: int,
    amount: float | None = None,
    currency_code: str | None = None,
    category_name: str | None = None,
    note: str | None = None,
    db_path: Path = DB_PATH,
) -> bool:
    """
    Обновляет поля записи. Передавать только те поля, которые изменились.
    Возвращает True если запись найдена и обновлена.
    """
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # Проверяем что запись принадлежит пользователю
    cur.execute("SELECT id, mode FROM entries WHERE id=? AND user_id=?", (entry_id, user_id))
    entry = cur.fetchone()
    if not entry:
        con.close()
        return False

    mode = entry["mode"]
    now = _now()
    updates = []
    params = []

    if amount is not None:
        updates.append("amount=?")
        params.append(amount)

    if currency_code is not None:
        currency_id = _get_or_create_currency(cur, user_id, currency_code)
        updates.append("currency_id=?")
        params.append(currency_id)

    if category_name is not None:
        if category_name == "":
            updates.append("category_id=NULL")
        else:
            category_id = _get_or_create_category(cur, user_id, mode, category_name)
            updates.append("category_id=?")
            params.append(category_id)

    # note может быть явно передан как "" (очистить) или None (не менять)
    # используем sentinel: update_entry(..., note=...) — если передан любой str, обновляем
    if note is not None:
        updates.append("note=?")
        params.append(note if note != "" else None)

    if not updates:
        con.close()
        return True

    updates.append("updated_at=?")
    params.append(now)
    params.append(entry_id)

    cur.execute(f"UPDATE entries SET {', '.join(updates)} WHERE id=?", params)
    con.commit()
    con.close()
    return True
