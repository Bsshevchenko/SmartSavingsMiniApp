"""
SmartSavings Mini App Bot
Принимает данные из Telegram Mini App и подтверждает сохранение транзакции.
"""
import json
import logging
import asyncio

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import CommandStart
from aiogram.types import Message, WebAppInfo, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode

from config import settings
import db_repo

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

r = Router()

MODE_META = {
    "income":  {"icon": "💰", "title": "Доход"},
    "expense": {"icon": "💸", "title": "Расход"},
    "asset":   {"icon": "📦", "title": "Актив"},
}


@r.message(CommandStart())
async def cmd_start(m: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="💳 Добавить транзакцию",
            web_app=WebAppInfo(url=settings.WEBAPP_URL),
        )
    ]])
    await m.answer(
        "👋 <b>SmartSavings Mini App</b>\n\n"
        "Нажми кнопку ниже, чтобы открыть форму ввода — "
        "быстрый numpad прямо в Telegram без задержек.",
        reply_markup=kb,
        parse_mode=ParseMode.HTML,
    )


@r.message(F.web_app_data)
async def on_webapp_data(m: Message):
    raw = m.web_app_data.data
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        await m.answer("❌ Ошибка: неверный формат данных")
        return

    mode = data.get("mode", "expense")
    amount = data.get("amount", "0")
    currency = data.get("currency", "")
    category = data.get("category", "")
    note = data.get("note")

    meta = MODE_META.get(mode, MODE_META["expense"])

    user = m.from_user
    try:
        entry_id = await asyncio.to_thread(
            db_repo.save_entry,
            user_id=user.id,
            mode=mode,
            amount=float(amount),
            currency_code=currency,
            category_name=category or None,
            note=note or None,
            username=user.username,
        )
        log.info("Saved entry id=%d for user=%d: %s", entry_id, user.id, data)
    except Exception as e:
        log.error("Failed to save entry for user=%d: %s", user.id, e)
        await m.answer("❌ Ошибка при сохранении. Попробуйте ещё раз.")
        return

    lines = [
        f"{meta['icon']} <b>{meta['title']} сохранён</b>",
        "",
        f"Сумма: <b>{amount} {currency}</b>",
    ]
    if category:
        lines.append(f"Категория: <b>{category}</b>")
    if note:
        lines.append(f"Заметка: <i>{note}</i>")

    await m.answer("\n".join(lines), parse_mode=ParseMode.HTML)


async def main():
    bot = Bot(token=settings.BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(r)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
