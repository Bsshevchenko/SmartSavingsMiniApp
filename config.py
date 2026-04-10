from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    BOT_TOKEN: str
    WEBAPP_URL: str = "http://localhost:8000"
    NGROK_AUTHTOKEN: str = ""

    class Config:
        env_file = ".env"


settings = Settings()
