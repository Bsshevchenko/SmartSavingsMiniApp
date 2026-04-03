"""
FastAPI сервер:
  GET  /         → отдаёт web/index.html
  POST /webhook  → принимает Telegram webhook (опционально)

Запуск:
  uvicorn server:app --reload --port 8000

Для локальной разработки через ngrok:
  ngrok http 8000
  → WEBAPP_URL = https://<id>.ngrok-free.app
"""
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

WEB_DIR = Path(__file__).parent / "web"

app = FastAPI()
app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")


@app.get("/")
async def root():
    return FileResponse(WEB_DIR / "index.html")
