from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    BOT_TOKEN: str
    WEBAPP_URL: str = "http://localhost:8000"
    NGROK_AUTHTOKEN: str = ""

    # Локальная разработка вне Telegram: при DEV_MODE=true запросы без валидной
    # initData выполняются от имени DEV_USER_ID. В проде должно быть выключено.
    DEV_MODE: bool = False
    DEV_USER_ID: int = 0

    class Config:
        env_file = ".env"


settings = Settings()
