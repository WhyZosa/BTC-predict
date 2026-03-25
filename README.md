# BTC Forecast Mini App

Telegram Mini App для прогноза цены Bitcoin на горизонтах `6 часов`, `1 день` и `1 неделя`.

## Что внутри

- `Flask` backend для котировок, истории и предикта.
- `Telegram Mini App` с современным интерфейсом и графиком.
- ML-пайплайн на чистом Python с временными рядами.
- Готовность к высокой нагрузке через кэширование, stateless API и Redis.

## Архитектура

- `app/main.py` - точка входа API и выдача фронтенда Mini App.
- `app/services/market.py` - получение котировок BTC/USD с fallback по провайдерам.
- `app/services/predictor.py` - загрузка модели и построение прогноза.
- `app/ml/` - фичи, метрики и утилиты обучения.
- `scripts/train_models.py` - обучение моделей на исторических OHLCV.
- `bot/bot.py` - Telegram-бот, который открывает Mini App.
- `app/static/` - фронтенд.

## Быстрый старт

### Вариант для обычного `cmd`

0. Для настоящего Mini App в Telegram установи туннель:

```cmd
install_cloudflared.cmd
```

1. Запусти API:

```cmd
run_api.cmd
```

2. В новом окне `cmd` подними публичный HTTPS URL с твоего ПК:

```cmd
run_tunnel.cmd
```

3. В новом окне `cmd` запусти бота:

```cmd
run_bot.cmd
```

4. Если хочешь переобучить модель:

```cmd
train_models.cmd
```

Порядок для Telegram Mini App:

1. `run_api.cmd`
2. `run_tunnel.cmd`
3. `run_bot.cmd`

`run_tunnel.cmd` поднимает Cloudflare Quick Tunnel, сохраняет публичный URL в `.env` и обновляет menu button у бота.

### Вариант для `PowerShell`

1. Создай виртуальное окружение:

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
py -m pip install -r requirements.txt
```

2. Скопируй окружение:

```powershell
Copy-Item .env.example .env
```

3. Запусти API:

```powershell
python -m waitress --listen=0.0.0.0:8000 app.main:app
```

4. В отдельном терминале запусти бота:

```powershell
python -m bot.bot
```

## Обучение модели

```powershell
python scripts/train_models.py --provider binance --symbol BTCUSDT --interval 1h --limit 2000
```

Артефакты модели сохраняются в `artifacts/` в формате JSON.

## Telegram Mini App

В `.env` укажи:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_MINI_APP_URL`

`TELEGRAM_MINI_APP_URL` должен указывать на публичный HTTPS URL backend-приложения, например через VPS или reverse proxy.

## Масштабирование

- Выносить кэш в Redis.
- Запускать API в нескольких воркерах.
- Собирать график по кэшированным данным, а не делать запрос к бирже на каждого пользователя.
- Использовать CDN и reverse proxy для статики.

## Совместимость

Проект адаптирован под `Python 3.14`, чтобы запускаться без сборки тяжёлых бинарных зависимостей.
