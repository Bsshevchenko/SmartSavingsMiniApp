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

## Свежие данные для локальной отладки

Локальная `data/app.db` быстро устаревает (боевая живёт на 185.x). Продовые данные
каждую ночь переливаются в Postgres на data-платформе (схема `smartsavings`), оттуда
их и берём — приложение при этом остаётся на своём SQLite.

**При `DEV_MODE=true` это происходит само:** сервер перед стартом пересобирает
`data/app.db` (`_maybe_refresh_dev_db` в `server.py`) и только потом начинает отвечать —
~9 секунд. Выкачка пропускается, если данные тянули меньше `DEV_DB_MAX_AGE_HOURS` назад
(отметка — `data/.dev_pull_stamp`), поэтому `--reload` не дёргает сеть на каждой
перезагрузке. Не достучались до Postgres — в лог уходит `dev-db: не обновлена…`,
и сервер поднимается на той БД, что уже лежит локально. Прод (`DEV_MODE=false`) и pytest
хук не трогают.

Вручную — тот же код:

```bash
python tools/pull_dev_db.py              # пересобрать (прежняя → *.local-backup-*)
python tools/pull_dev_db.py --dry-run    # только показать, что лежит в Postgres
python tools/pull_dev_db.py --max-age 12 # пропустить, если тянули недавно
```

Скрипт сам поднимает SSH-туннель (Postgres слушает только `127.0.0.1:5432`), выгружает все
таблицы одним сеансом `psql` (соединение через туннель дорогое) и собирает SQLite по
`tools/schema.sql`, приводя типы к тому же виду, что пишет приложение (`timestamptz` →
`YYYY-MM-DD HH:MM:SS.ffffff` в UTC, `numeric` → int/real). Доступы читаются из
`KnowledgeBase/.env` (путь переопределяется `KB_ENV`) — в репозитории секретов нет.
Нужны `psql` и `sshpass`. Данные в Postgres на сутки старше боевых (переливка в 03:30);
если разошлись колонки — падает с diff'ом и просит обновить `tools/schema.sql`.

⚠️ Автовыкачка затирает локальную БД целиком: записи, введённые локально через Mini App,
переживут перезапуск только до следующей выкачки. Мешает — `DEV_DB_AUTO_PULL=false`.

## Environment

```
BOT_TOKEN=...        # токен от @BotFather
WEBAPP_URL=...       # публичный HTTPS URL (ngrok или деплой)
DEV_MODE=false       # true → запросы без валидной initData идут от DEV_USER_ID (только локально!)
DEV_USER_ID=0        # Telegram user_id для DEV_MODE
DEV_DB_AUTO_PULL=true    # при DEV_MODE тянуть свежую БД из Postgres перед стартом
DEV_DB_MAX_AGE_HOURS=12  # ...но не чаще, чем раз в N часов
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
| GET | `/fin-model` · `/api/fin-model` | Дивидендная фин-модель (страница + данные) |

**Конвертация валют** (`/api/analytics`): курсы запрашиваются с `open.er-api.com/v6/latest/USD` (TTL 10 мин), fallback — последние курсы из таблицы `currency_rates` в БД, затем хардкод. Все суммы конвертируются через USD как промежуточную валюту.

**Дивидендная фин-модель** (`fin_model_data.py` + `web/fin_model.html`): цель — дивидендный доход ≥ средних расходов.
- `build_fin_model(user_id)` собирает payload из БД на лету: портфель (категория «Акции» + ETF, кол-во из снапшотов `entries`), цены MOEX, средние расходы за 12 мес, факт стоимости портфеля по завершённым месяцам (кол-во × месячные цены `currency_rates`).
- Дивиденды (форвард, доходность, история, **календарь выплат** = факт/объявлено/прогноз) парсятся с **dohod.ru** и кэшируются в `data/dividends_cache.json`. Кэш обновляется **1-го числа каждого месяца** джобом APScheduler `dividends` (см. `_run_dividends` в `server.py`); при отсутствии кэша — наполняется фоном на старте. Ручной запуск: `python fin_model_data.py`.
- Будущие годы — экстраполяция («корреляция») от форвардного прогноза × надёжность; считается на клиенте.

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
