from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    BOT_TOKEN: str
    WEBAPP_URL: str = "http://localhost:8000"  # заменить на ngrok/prod URL

    class Config:
        env_file = ".env"


settings = Settings()
