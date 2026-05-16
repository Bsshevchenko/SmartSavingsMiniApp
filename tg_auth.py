"""
Валидация Telegram WebApp initData.

Mini App получает от Telegram подписанную строку initData. Сервер ОБЯЗАН
проверять HMAC-подпись этой строки секретом, производным от BOT_TOKEN, и брать
user_id ТОЛЬКО оттуда. Никогда нельзя доверять user_id из query-параметра или
тела запроса — иначе любой человек сможет подставить чужой id и получить чужие
данные.

Алгоритм проверки — стандартный для Telegram Mini Apps:
  data_check_string = "\n".join(f"{k}={v}" for k,v in sorted(fields без hash))
  secret_key        = HMAC_SHA256(key="WebAppData", msg=BOT_TOKEN)
  ожидаемый hash    = HMAC_SHA256(key=secret_key, msg=data_check_string)
"""
import hashlib
import hmac
import json
import logging
import time
from urllib.parse import parse_qsl

from fastapi import Header, HTTPException

from config import settings

log = logging.getLogger(__name__)

# initData считается просроченной через сутки после выдачи Telegram
_MAX_AGE_SECONDS = 24 * 60 * 60


def _validate_init_data(init_data: str) -> dict | None:
    """Проверяет подпись initData. Возвращает dict пользователя или None."""
    if not init_data:
        return None
    try:
        pairs = dict(parse_qsl(init_data, strict_parsing=True))
    except ValueError:
        return None

    received_hash = pairs.pop("hash", None)
    if not received_hash:
        return None

    data_check_string = "\n".join(f"{k}={pairs[k]}" for k in sorted(pairs))
    secret_key = hmac.new(b"WebAppData", settings.BOT_TOKEN.encode(), hashlib.sha256).digest()
    computed_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(computed_hash, received_hash):
        return None

    # initData не должна быть слишком старой (защита от воспроизведения)
    auth_date = pairs.get("auth_date")
    if auth_date:
        try:
            if time.time() - int(auth_date) > _MAX_AGE_SECONDS:
                return None
        except ValueError:
            return None

    user_raw = pairs.get("user")
    if not user_raw:
        return None
    try:
        user = json.loads(user_raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(user, dict) or "id" not in user:
        return None
    return user


def resolve_user_id(init_data: str | None) -> int | None:
    """
    Возвращает user_id из проверенной initData.
    В DEV_MODE (локальная разработка вне Telegram) — фолбэк на DEV_USER_ID.
    В проде DEV_MODE выключен → без валидной подписи доступа нет.
    """
    user = _validate_init_data(init_data or "")
    if user is not None:
        return int(user["id"])
    if settings.DEV_MODE and settings.DEV_USER_ID:
        log.warning("DEV_MODE: запрос без валидной initData → DEV_USER_ID=%s", settings.DEV_USER_ID)
        return settings.DEV_USER_ID
    return None


async def require_user_id(
    x_telegram_init_data: str | None = Header(default=None),
) -> int:
    """FastAPI-зависимость: аутентифицированный user_id либо 401."""
    user_id = resolve_user_id(x_telegram_init_data)
    if user_id is None:
        raise HTTPException(
            status_code=401,
            detail="Откройте приложение из Telegram — требуется авторизация",
        )
    return user_id