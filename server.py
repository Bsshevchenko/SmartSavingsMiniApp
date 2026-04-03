"""
FastAPI сервер:
  GET  /                      → web/index.html
  GET  /api/user-data?user_id → валюты и категории пользователя из БД
"""
import sqlite3
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

WEB_DIR = Path(__file__).parent / "web"
DB_PATH = Path(__file__).parent / "data" / "app.db"

app = FastAPI()
app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")


@app.get("/")
async def root():
    return FileResponse(WEB_DIR / "index.html")


@app.get("/api/user-data")
async def user_data(user_id: int = Query(...)):
    """Возвращает валюты и категории пользователя, отсортированные по last_used_at DESC."""
    if not DB_PATH.exists():
        return JSONResponse({"currencies": [], "categories": {"expense": [], "income": [], "asset": []}})

    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # Валюты: сначала недавно использованные, потом остальные
    cur.execute("""
        SELECT code FROM currencies
        WHERE user_id = ?
        ORDER BY
            CASE WHEN last_used_at IS NULL THEN 1 ELSE 0 END,
            last_used_at DESC
    """, (user_id,))
    currencies = [r["code"] for r in cur.fetchall()]

    # Категории по режимам
    categories = {"expense": [], "income": [], "asset": []}
    for mode in categories:
        cur.execute("""
            SELECT name FROM categories
            WHERE user_id = ? AND mode = ?
            ORDER BY
                CASE WHEN last_used_at IS NULL THEN 1 ELSE 0 END,
                last_used_at DESC
        """, (user_id, mode))
        categories[mode] = [r["name"] for r in cur.fetchall()]

    con.close()
    return JSONResponse({"currencies": currencies, "categories": categories})
