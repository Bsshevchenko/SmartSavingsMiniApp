from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    BOT_TOKEN: str
    WEBAPP_URL: str = "http://localhost:8000"
    NGROK_AUTHTOKEN: str = ""

    # Локальная разработка вне Telegram: при DEV_MODE=true запросы без валидной
    # initData выполняются от имени DEV_USER_ID. В проде должно быть выключено.
    DEV_MODE: bool = False
    DEV_USER_ID: int = 0

    # При DEV_MODE сервер перед стартом пересобирает data/app.db из продового
    # зеркала в Postgres (tools/pull_dev_db.py). Выкачка пропускается, если данные
    # тянули меньше DEV_DB_MAX_AGE_HOURS назад — в Postgres они и так суточные.
    DEV_DB_AUTO_PULL: bool = True
    DEV_DB_MAX_AGE_HOURS: float = 12

    class Config:
        env_file = ".env"


settings = Settings()
