from __future__ import annotations

import asyncio
import atexit
import os
import sys
from pathlib import Path

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    MenuButtonCommands,
    MenuButtonWebApp,
    ReplyKeyboardMarkup,
    Update,
    WebAppInfo,
)
from telegram.error import BadRequest, Conflict, Forbidden
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from app.config import settings
from app.schemas import UserContext
from app.services.market import MarketDataError, market_data_service
from app.services.predictor import PredictorUnavailableError, predictor_service
from app.services.user_data import user_data_service


HORIZON_LABELS = {
    "6h": "6 часов",
    "1d": "1 день",
    "1w": "1 неделя",
}

TEXT_TO_HORIZON = {
    "6 часов": "6h",
    "1 день": "1d",
    "1 неделя": "1w",
}

CURRENT_PRICE_TEXT = "Текущая цена"
OPEN_CHART_TEXT = "Открыть график"

PID_FILE = Path(__file__).resolve().parents[1] / ".bot.pid"
CONFLICT_NOTIFIED = False


def _format_usd(value: float) -> str:
    return f"${value:,.0f}"


def _is_public_url(url: str) -> bool:
    lowered = url.lower()
    return lowered.startswith("https://") and "127.0.0.1" not in lowered and "localhost" not in lowered


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _register_pid() -> None:
    if PID_FILE.exists():
        try:
            existing_pid = int(PID_FILE.read_text(encoding="utf-8").strip())
        except ValueError:
            existing_pid = 0
        if _pid_is_alive(existing_pid):
            raise RuntimeError(
                f"Бот уже запущен в другом окне. Закрой предыдущий экземпляр или заверши PID {existing_pid}."
            )
        PID_FILE.unlink(missing_ok=True)

    PID_FILE.write_text(str(os.getpid()), encoding="utf-8")
    atexit.register(lambda: PID_FILE.unlink(missing_ok=True))


def _menu_keyboard() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="6 часов"), KeyboardButton(text="1 день"), KeyboardButton(text="1 неделя")],
        [KeyboardButton(text=CURRENT_PRICE_TEXT)],
    ]
    url = settings.telegram_mini_app_url.strip()
    if url and _is_public_url(url):
        rows.append([KeyboardButton(text=OPEN_CHART_TEXT, web_app=WebAppInfo(url=url))])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _current_public_webapp_url() -> str:
    url = settings.telegram_mini_app_url.strip()
    return url if url and _is_public_url(url) else ""


def _mini_app_inline_markup() -> InlineKeyboardMarkup | None:
    url = _current_public_webapp_url()
    if not url:
        return None
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(text="Открыть Mini App", web_app=WebAppInfo(url=url))]]
    )


async def _sync_chat_menu_button(
    application: Application,
    chat_id: int | None = None,
    force_refresh: bool = False,
) -> None:
    url = _current_public_webapp_url()
    if url:
        menu_button = MenuButtonWebApp(
            text=settings.telegram_menu_button_text,
            web_app=WebAppInfo(url=url),
        )
    else:
        menu_button = MenuButtonCommands()

    await application.bot.set_chat_menu_button(menu_button=menu_button)
    if chat_id is not None:
        if force_refresh:
            await application.bot.set_chat_menu_button(chat_id=chat_id, menu_button=MenuButtonCommands())
        await application.bot.set_chat_menu_button(chat_id=chat_id, menu_button=menu_button)


def _user_context_from_update(update: Update) -> UserContext | None:
    user = update.effective_user
    chat = update.effective_chat
    if user is None:
        return None
    return UserContext(
        user_key=f"tg:{user.id}",
        telegram_user_id=user.id,
        chat_id=chat.id if chat is not None else user.id,
        username=user.username,
        first_name=user.first_name,
    )


async def _send_prediction(update: Update, horizon: str) -> None:
    message = update.message
    if message is None:
        return

    context = _user_context_from_update(update)
    if context is not None:
        user_data_service.upsert_user(context)

    try:
        required_limit = max(settings.history_limit, predictor_service.required_history_rows(horizon) + 48)
        snapshot = market_data_service.get_snapshot(limit=required_limit)
        prediction = predictor_service.predict(horizon, snapshot.candles)
    except PredictorUnavailableError as exc:
        await message.reply_text(f"Модель прогноза сейчас недоступна: {exc}", reply_markup=_menu_keyboard())
        return
    except (MarketDataError, ValueError) as exc:
        await message.reply_text(f"Не удалось получить прогноз: {exc}", reply_markup=_menu_keyboard())
        return

    if context is not None:
        user_data_service.save_prediction(context, prediction)

    target_time = prediction.target_at.astimezone().strftime("%d.%m.%Y %H:%M")
    text = (
        f"Прогноз через {HORIZON_LABELS[horizon]}\n"
        f"Время: {target_time}\n"
        f"Ожидаемая цена: {_format_usd(prediction.predicted_price)}\n"
        f"Текущая цена: {_format_usd(prediction.current_price)}\n"
        f"Диапазон: {_format_usd(prediction.confidence_low)} - {_format_usd(prediction.confidence_high)}\n"
        f"Изменение: {prediction.delta_pct:+.2f}%"
    )
    await message.reply_text(text, reply_markup=_menu_keyboard())


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None:
        return

    user_context = _user_context_from_update(update)
    if user_context is not None:
        user_data_service.upsert_user(user_context)
        await _sync_chat_menu_button(context.application, chat_id=user_context.chat_id, force_refresh=True)

    url = _current_public_webapp_url()
    if url:
        text = (
            "Выбери горизонт прогноза или открой Mini App.\n"
            "В Mini App доступны история прогнозов, алерты и интерактивный график."
        )
    else:
        text = (
            "Выбери горизонт прогноза. Для Mini App внутри Telegram нужен публичный HTTPS URL. "
            "Локальный 127.0.0.1 работает только в браузере на этом ПК."
        )
    await message.reply_text(text, reply_markup=_menu_keyboard())
    inline_markup = _mini_app_inline_markup()
    if inline_markup is not None:
        await message.reply_text("Быстрый запуск Mini App:", reply_markup=inline_markup)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None:
        return

    user_context = _user_context_from_update(update)
    if user_context is not None:
        user_data_service.upsert_user(user_context)
        await _sync_chat_menu_button(context.application, chat_id=user_context.chat_id)

    text = (message.text or "").strip()
    if text in TEXT_TO_HORIZON:
        await _send_prediction(update, TEXT_TO_HORIZON[text])
        return

    if text == CURRENT_PRICE_TEXT:
        try:
            snapshot = market_data_service.get_snapshot(limit=48)
        except MarketDataError as exc:
            await message.reply_text(f"Не удалось получить цену: {exc}", reply_markup=_menu_keyboard())
            return
        change_text = "--" if snapshot.change_24h_pct is None else f"{snapshot.change_24h_pct:+.2f}%"
        await message.reply_text(
            f"BTC/USD: {_format_usd(snapshot.latest.price)}\n24h: {change_text}\nИсточник: {snapshot.latest.source}",
            reply_markup=_menu_keyboard(),
        )
        return

    await message.reply_text("Нажми одну из кнопок ниже.", reply_markup=_menu_keyboard())


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    global CONFLICT_NOTIFIED
    if isinstance(context.error, Conflict):
        if not CONFLICT_NOTIFIED:
            print(
                "Ошибка Telegram: другой экземпляр бота уже использует getUpdates. "
                "Закрой все остальные запуски этого бота и запусти только один.",
                file=sys.stderr,
            )
            CONFLICT_NOTIFIED = True
        return
    print(f"Ошибка бота: {context.error}", file=sys.stderr)


async def alert_monitor_loop(application: Application) -> None:
    while True:
        try:
            snapshot = market_data_service.get_snapshot(limit=48)
            due_alerts = user_data_service.get_triggerable_alerts(snapshot.latest.price)
            for alert, chat_id in due_alerts:
                if alert.kind == "above_price":
                    trigger_text = f"BTC поднялся выше {_format_usd(alert.threshold_value)}."
                elif alert.kind == "below_price":
                    trigger_text = f"BTC опустился ниже {_format_usd(alert.threshold_value)}."
                else:
                    trigger_text = f"BTC упал на {alert.threshold_value:.2f}% от цены, которая была при создании алерта."

                target_price = _format_usd(alert.target_price) if alert.target_price is not None else "--"
                text = (
                    "Сработал алерт\n"
                    f"{trigger_text}\n"
                    f"Текущая цена: {_format_usd(snapshot.latest.price)}\n"
                    f"Порог: {target_price}"
                )
                try:
                    await application.bot.send_message(chat_id=chat_id, text=text, reply_markup=_menu_keyboard())
                    user_data_service.mark_alert_triggered(alert.id)
                except (BadRequest, Forbidden) as exc:
                    user_data_service.deactivate_alert(alert.id)
                    print(
                        f"Алерт {alert.id} отключен: Telegram не принимает сообщения в chat_id={chat_id} ({exc})",
                        file=sys.stderr,
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"Ошибка мониторинга алертов: {exc}", file=sys.stderr)

        await asyncio.sleep(max(10, settings.alert_check_seconds))


async def post_init(application: Application) -> None:
    await application.bot.delete_webhook(drop_pending_updates=True)
    await _sync_chat_menu_button(application)
    application.bot_data["alert_task"] = asyncio.create_task(alert_monitor_loop(application))


async def post_shutdown(application: Application) -> None:
    task = application.bot_data.get("alert_task")
    if task is not None:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def main() -> None:
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set in .env")

    _register_pid()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    application = (
        Application.builder()
        .token(settings.telegram_bot_token)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    application.add_error_handler(error_handler)
    application.run_polling(drop_pending_updates=True, bootstrap_retries=0)


if __name__ == "__main__":
    main()
