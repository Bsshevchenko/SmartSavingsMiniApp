# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Telegram Mini App для ввода финансовых транзакций и просмотра аналитики. Является фронтендом для бота SmartSavings (`/Users/bsshevchenko/PycharmProjects/Pet_projects/SmartSavings`) — использует его БД (`data/app.db`, SQLite) напрямую.

## Running the App

Нужно три процесса одновременно:

```bash
# 1. Веб-сервер (раздаёт HTML + API)
uvicorn server:app --port 8000 --reload

# 2. HTTPS туннель (Telegram требует HTTPS для Mini App)
ngrok http 8000
# Скопировать https://xxx.ngrok-free.app → WEBAPP_URL в .env

# 3. Telegram бот
python bot.py
```

## Environment

```
BOT_TOKEN=...        # токен от @BotFather
WEBAPP_URL=...       # публичный HTTPS URL (ngrok или деплой)
DEV_MODE=false       # true → запросы без валидной initData идут от DEV_USER_ID (только локально!)
DEV_USER_ID=0        # Telegram user_id для DEV_MODE
```

## Авторизация API

Все `/api/*` эндпоинты требуют заголовок `X-Telegram-Init-Data` с подписанной
строкой `Telegram.WebApp.initData`. Сервер (`tg_auth.py`) проверяет HMAC-подпись
секретом из `BOT_TOKEN` и берёт `user_id` ТОЛЬКО из проверенной initData —
`user_id` из query/тела игнорируется. Без валидной подписи — `401`.
Веб-клиент монки-патчит `fetch`/`XMLHttpRequest` и подставляет заголовок сам.
`DEV_MODE=true` разрешает фолбэк на `DEV_USER_ID` для отладки в браузере.

## Architecture

```
Telegram user
  ↓ открывает Mini App (кнопка в боте)
web/index.html       ← форма ввода транзакции (SPA, ванильный JS)
web/analytics.html   ← страница аналитики (Chart.js 4)
  ↓ sendData() / fetch API
bot.py               ← aiogram 3, принимает web_app_data, сохраняет запись
server.py            ← FastAPI, раздаёт web/, отвечает на /api/*
  ↓ sqlite3 (sync)
data/app.db          ← БД SmartSavings (схема: users, entries, currencies, categories, ...)
```

### server.py — ключевые эндпоинты

| Метод | Путь | Описание |
|---|---|---|
| GET | `/` | `web/index.html` |
| GET | `/analytics` | `web/analytics.html` |
| GET | `/api/user-data?user_id` | Валюты и категории пользователя, отсортированные по `last_used_at DESC` |
| GET | `/api/analytics?user_id&period&currency` | Данные для графиков с конвертацией валют |

**Конвертация валют** (`/api/analytics`): курсы запрашиваются с `open.er-api.com/v6/latest/USD` (TTL 10 мин), fallback — последние курсы из таблицы `currency_rates` в БД, затем хардкод. Все суммы конвертируются через USD как промежуточную валюту.

### web/index.html — форма ввода

- Режимы: `expense` / `income` / `asset`
- Вся логика numpad и выбора категорий/валют — чистый JS без фреймворков
- При старте загружает реальные валюты и категории пользователя через `GET /api/user-data`
- При сабмите вызывает `Telegram.WebApp.sendData(JSON)` → бот получает `web_app_data`
- Dev-режим (без Telegram): доступ к API только при `DEV_MODE=true` в `.env`

**Особенности рендеринга** (важно для совместимости с Telegram Desktop WebView):
- Чипы валют — `<button>` с `display: inline-block`, контейнер `display: block; white-space: nowrap; overflow-x: auto` (не flex!)
- Карточка суммы — без `overflow: hidden`; `::before` glow через `z-index: -1` + `#amount-card > * { z-index: 1 }`
- Высота приложения — `height: 100vh` + `height: 100dvh` (dvh как override)
- Клавиатура на мобиле: `focus` прячет `#bottom-area`, `blur` возвращает; `viewportChanged` ресайзит `#app`

### web/analytics.html

- Переключатели: период (1М/6М/1Г/Всё) + валюта (RUB/VND/USD)
- Каждое изменение → новый `GET /api/analytics` запрос
- Chart.js 4.4 подключён с CDN

### bot.py

Принимает `web_app_data` от Mini App и выводит подтверждение. Подключение к БД для сохранения записей — TODO (подключить repo из SmartSavings).

## DB Schema (ключевые таблицы)

- `users` — `id` = Telegram user_id
- `currencies` — `(user_id, code)`, поле `last_used_at` для сортировки
- `categories` — `(user_id, mode, name)`, поле `last_used_at` для сортировки
- `entries` — транзакции; `mode IN ('income','expense','asset')`, `currency_id` и `category_id` через FK с `ondelete="SET NULL"`
- `currency_rates` — исторические курсы `rate_to_usd`, используются как fallback при недоступности API
