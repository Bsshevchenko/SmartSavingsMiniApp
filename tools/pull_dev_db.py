#!/usr/bin/env python3
"""Собрать локальную data/app.db из продового зеркала в Postgres (схема smartsavings).

Боевые данные каждую ночь (03:30) переливаются с 185.x в Postgres на 45.x —
это самый свежий источник, доступный с ноутбука. Скрипт тянет оттуда все таблицы
и пересобирает локальный SQLite-файл, чтобы отлаживать приложение на реальных данных.

    python tools/pull_dev_db.py             # пересобрать data/app.db (прежняя → *.local-backup-*)
    python tools/pull_dev_db.py --dry-run   # только показать, что лежит в Postgres
    python tools/pull_dev_db.py --max-age 12  # пропустить, если тянули меньше 12 часов назад
    python tools/pull_dev_db.py -o data/app.prod.db --no-backup

Postgres наружу не смотрит (127.0.0.1:5432), поэтому скрипт сам поднимает SSH-туннель.
Доступы берутся из KnowledgeBase/.env (переопределяется переменной KB_ENV) — в репозитории
секретов нет. Требуются `psql` и `sshpass` (оба ставятся через brew).

Сервер при DEV_MODE зовёт `refresh_local_db()` на старте — см. `_maybe_refresh_dev_db`
в `server.py`; отметка о последней выкачке лежит в `data/.dev_pull_stamp`.
"""

from __future__ import annotations

import argparse
import csv
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parent.parent
SCHEMA_SQL = Path(__file__).resolve().parent / "schema.sql"
DEFAULT_DB = ROOT / "data" / "app.db"
DEFAULT_STAMP = ROOT / "data" / ".dev_pull_stamp"
DEFAULT_KB_ENV = Path.home() / "PycharmProjects" / "KnowledgeBase" / ".env"

PG_SCHEMA = "smartsavings"
# Порядок важен: таблицы грузятся по зависимостям FK.
TABLES = [
    "users",
    "currencies",
    "categories",
    "entries",
    "currency_rates",
    "capital_snapshots",
    "asset_latest_values",
    "realty",
    "realty_rates",
]
NULL_TOKEN = "\\N"  # чтобы отличить NULL от пустой строки в CSV
PSQL_TIMEOUT = 120  # с; чтобы недоступная сеть не вешала старт сервера


class PullError(RuntimeError):
    """Не удалось собрать локальную БД (нет доступов, сети, разошлась схема)."""


# ── доступы ───────────────────────────────────────────────────────────────────

def load_env(path: Path) -> dict[str, str]:
    if not path.exists():
        raise PullError(f"нет файла с доступами: {path} (путь задаётся переменной KB_ENV)")
    env: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    required = ["DATA_PLATFORM_HOST", "DATA_PLATFORM_SSH_USER", "DATA_PLATFORM_SSH_PASSWORD",
                "PG_DATABASE", "PG_APP_USER", "PG_APP_PASSWORD"]
    missing = [k for k in required if not env.get(k)]
    if missing:
        raise PullError(f"в {path} не хватает ключей: {', '.join(missing)}")
    return env


def require_binaries() -> None:
    for binary, hint in (("psql", "brew install libpq && brew link --force libpq"),
                         ("sshpass", "brew install sshpass")):
        if shutil.which(binary) is None:
            raise PullError(f"не найден {binary} (установить: {hint})")


# ── SSH-туннель ───────────────────────────────────────────────────────────────

def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Tunnel:
    """SSH-туннель к Postgres на data-платформе; закрывается вместе со скриптом."""

    def __init__(self, env: dict[str, str]):
        self.env = env
        self.port = free_port()
        self.proc: subprocess.Popen | None = None

    def __enter__(self) -> "Tunnel":
        remote_port = self.env.get("PG_PORT", "5432")
        cmd = [
            "sshpass", "-e", "ssh", "-N",
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ExitOnForwardFailure=yes",
            "-o", "ConnectTimeout=10",
            "-L", f"{self.port}:127.0.0.1:{remote_port}",
            f"{self.env['DATA_PLATFORM_SSH_USER']}@{self.env['DATA_PLATFORM_HOST']}",
        ]
        proc_env = {**os.environ, "SSHPASS": self.env["DATA_PLATFORM_SSH_PASSWORD"]}
        self.proc = subprocess.Popen(cmd, env=proc_env,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        deadline = time.time() + 20
        while time.time() < deadline:
            if self.proc.poll() is not None:
                err = (self.proc.stderr.read() or b"").decode(errors="replace").strip()
                raise PullError(f"SSH-туннель не поднялся: {err}")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=1):
                    return self
            except OSError:
                time.sleep(0.3)
        self.__exit__(None, None, None)
        raise PullError("SSH-туннель не поднялся за 20 секунд")

    def __exit__(self, *_exc) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()


# ── выгрузка из Postgres ──────────────────────────────────────────────────────

def psql(env: dict[str, str], port: int, sql: str | None = None,
         script: Path | None = None) -> str:
    """Выполнить запрос/скрипт и вернуть stdout (пароль идёт через PGPASSWORD, не через argv)."""
    dsn = f"postgresql://{env['PG_APP_USER']}@127.0.0.1:{port}/{env['PG_DATABASE']}"
    source = ["-c", sql] if sql is not None else ["-f", str(script)]
    try:
        proc = subprocess.run(
            ["psql", dsn, "--no-psqlrc", "--quiet", "--no-align", "--tuples-only",
             "--field-separator", "\t", "-v", "ON_ERROR_STOP=1", *source],
            env={**os.environ, "PGPASSWORD": env["PG_APP_PASSWORD"]},
            capture_output=True, text=True, timeout=PSQL_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        raise PullError(f"psql не ответил за {PSQL_TIMEOUT} с")
    if proc.returncode != 0:
        raise PullError(f"psql: {proc.stderr.strip()}")
    return proc.stdout


def pg_columns(env: dict[str, str], port: int) -> dict[str, list[tuple[str, str]]]:
    """{таблица: [(колонка, тип), ...]} в порядке ordinal_position."""
    out = psql(env, port, f"""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{PG_SCHEMA}'
        ORDER BY table_name, ordinal_position
    """)
    cols: dict[str, list[tuple[str, str]]] = {}
    for line in out.splitlines():
        if not line.strip():
            continue
        table, column, dtype = line.split("\t")
        cols.setdefault(table, []).append((column, dtype))
    return cols


def select_expr(column: str, dtype: str) -> str:
    """Приводим значение к тексту так, как его хранит SQLite приложения."""
    if dtype == "timestamp with time zone":
        return f"to_char({column} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US')"
    if dtype == "timestamp without time zone":
        return f"to_char({column}, 'YYYY-MM-DD HH24:MI:SS.US')"
    if dtype == "date":
        return f"to_char({column}, 'YYYY-MM-DD')"
    return f"{column}::text"


def fetch_all(env: dict[str, str], port: int,
              pg_cols: dict[str, list[tuple[str, str]]]) -> dict[str, list[list[str | None]]]:
    """Выгрузить все таблицы одним сеансом psql: соединение через туннель — дорогое."""
    with tempfile.TemporaryDirectory() as tmpdir:
        dumps = {t: Path(tmpdir) / f"{t}.csv" for t in TABLES}
        script = Path(tmpdir) / "dump.sql"
        script.write_text("\n".join(
            "\\copy (SELECT {cols} FROM {schema}.{table}) TO '{out}' "
            "WITH (FORMAT csv, NULL '{null}')".format(
                cols=", ".join(select_expr(c, t) for c, t in pg_cols[table]),
                schema=PG_SCHEMA, table=table, out=dumps[table], null=NULL_TOKEN)
            for table in TABLES) + "\n")
        psql(env, port, script=script)

        data: dict[str, list[list[str | None]]] = {}
        for table, path in dumps.items():
            with path.open(newline="") as fh:
                data[table] = [[None if v == NULL_TOKEN else v for v in row]
                               for row in csv.reader(fh) if row]
    return data


# ── запись в SQLite ───────────────────────────────────────────────────────────

def sqlite_columns(con: sqlite3.Connection, table: str) -> list[tuple[str, str]]:
    return [(r[1], (r[2] or "").upper()) for r in con.execute(f"PRAGMA table_info({table})")]


def to_sqlite_value(value: str | None, decl_type: str):
    """Строку из CSV — в тот же тип хранения, что был в исходной БД."""
    if value is None:
        return None
    if decl_type.startswith(("INT", "BIGINT")):
        return int(value)
    if decl_type.startswith(("REAL", "NUMERIC", "FLOAT", "DOUBLE", "DECIMAL")):
        number = float(value)
        return int(number) if number.is_integer() else number
    return value


def build_db(target: Path, data: dict[str, list[list[str | None]]],
             pg_cols: dict[str, list[tuple[str, str]]]) -> Path:
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.unlink(missing_ok=True)
    con = sqlite3.connect(str(tmp))
    try:
        con.executescript(SCHEMA_SQL.read_text())
        for table in TABLES:
            cols = sqlite_columns(con, table)
            if not cols:
                raise PullError(f"в {SCHEMA_SQL.name} нет таблицы {table} — обнови схему")
            names = [c for c, _ in cols]
            pg_names = [c for c, _ in pg_cols[table]]
            if names != pg_names:
                raise PullError(
                    f"схемы разошлись по таблице {table}:\n"
                    f"  SQLite:   {names}\n  Postgres: {pg_names}\n"
                    f"Обнови {SCHEMA_SQL.name} под текущую боевую схему.")
            types = [t for _, t in cols]
            rows = [tuple(to_sqlite_value(v, t) for v, t in zip(row, types))
                    for row in data[table]]
            placeholders = ",".join("?" * len(names))
            con.executemany(
                f"INSERT INTO {table} ({','.join(names)}) VALUES ({placeholders})", rows)
        con.commit()
        broken = con.execute("PRAGMA foreign_key_check").fetchall()
        if broken:
            raise PullError(f"битые внешние ключи после загрузки: {broken[:5]}")
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
    finally:
        con.close()
    return tmp


# ── публичная точка входа ─────────────────────────────────────────────────────

def is_fresh(stamp: Path, max_age_hours: float | None) -> bool:
    """Тянули ли данные недавно. В Postgres они всё равно обновляются раз в сутки."""
    if not max_age_hours or not stamp.exists():
        return False
    return (time.time() - stamp.stat().st_mtime) < max_age_hours * 3600


def refresh_local_db(target: Path = DEFAULT_DB, *, backup: bool = True,
                     max_age_hours: float | None = None,
                     stamp: Path = DEFAULT_STAMP,
                     env_path: Path | None = None,
                     log: Callable[[str], None] = lambda _msg: None) -> str:
    """Пересобрать `target` из Postgres-зеркала. Вернёт 'updated' или 'skipped'.

    Бросает PullError, если собрать не удалось — вызывающий решает, критично это
    или можно работать на той БД, что уже лежит локально.
    """
    if is_fresh(stamp, max_age_hours):
        age_h = (time.time() - stamp.stat().st_mtime) / 3600
        log(f"локальная БД обновлялась {age_h:.1f} ч назад — пропускаю выкачку")
        return "skipped"

    require_binaries()
    env = load_env(env_path or Path(os.environ.get("KB_ENV", DEFAULT_KB_ENV)))
    log(f"тяну боевые данные из Postgres ({env['DATA_PLATFORM_HOST']}, схема {PG_SCHEMA})")

    with Tunnel(env) as tunnel:
        pg_cols = pg_columns(env, tunnel.port)
        missing = [t for t in TABLES if t not in pg_cols]
        if missing:
            raise PullError(f"в Postgres нет таблиц: {', '.join(missing)}")
        data = fetch_all(env, tunnel.port, pg_cols)

    tmp = build_db(target, data, pg_cols)
    created_at = [c for c, _ in pg_cols["entries"]].index("created_at")
    last_entry = max((row[created_at] for row in data["entries"]), default="—")
    if target.exists() and backup:
        copy = target.with_name(f"{target.name}.local-backup-{datetime.now():%Y%m%d-%H%M%S}")
        shutil.copy2(target, copy)
        log(f"прежняя версия: {copy.name}")
    os.replace(tmp, target)
    stamp.write_text(datetime.now().isoformat(timespec="seconds"))
    log(f"{target.name}: {len(data['entries'])} записей, последняя от {last_entry}")
    return "updated"


def main() -> None:
    ap = argparse.ArgumentParser(description="Локальная БД из продового зеркала в Postgres")
    ap.add_argument("-o", "--output", type=Path, default=DEFAULT_DB,
                    help=f"куда положить SQLite (по умолчанию {DEFAULT_DB.relative_to(ROOT)})")
    ap.add_argument("--no-backup", action="store_true",
                    help="не сохранять предыдущую версию файла")
    ap.add_argument("--max-age", type=float, metavar="ЧАСЫ",
                    help="пропустить выкачку, если данные тянули меньше ЧАСОВ назад")
    ap.add_argument("--dry-run", action="store_true",
                    help="только показать содержимое Postgres, ничего не писать")
    args = ap.parse_args()

    try:
        if args.dry_run:
            require_binaries()
            env = load_env(Path(os.environ.get("KB_ENV", DEFAULT_KB_ENV)))
            print(f"→ туннель к {env['DATA_PLATFORM_HOST']}, схема {PG_SCHEMA}")
            with Tunnel(env) as tunnel:
                pg_cols = pg_columns(env, tunnel.port)
                data = fetch_all(env, tunnel.port, pg_cols)
                last = psql(env, tunnel.port,
                            f"SELECT max(created_at) FROM {PG_SCHEMA}.entries").strip()
            for table in TABLES:
                print(f"   {table:<20} {len(data[table]):>6}")
            print(f"   последняя запись:    {last}")
            print("dry-run: локальная БД не тронута")
            return

        target = args.output if args.output.is_absolute() else ROOT / args.output
        status = refresh_local_db(target, backup=not args.no_backup,
                                  max_age_hours=args.max_age, log=lambda m: print(f"→ {m}"))
        print("✓ готово" if status == "updated" else "✓ данные и так свежие")
    except PullError as e:
        sys.exit(f"✗ {e}")


if __name__ == "__main__":
    main()