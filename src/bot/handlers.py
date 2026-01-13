from __future__ import annotations

from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, FSInputFile

from src.bot.keyboards import main_menu
from src.bot.services.market_data import get_last_candle, load_df_last_n
from src.bot.services.charts import make_candles_chart
from src.bot.services.indicators import calc_indicators

router = Router()


@router.message(CommandStart())
async def start(m: Message):
    await m.answer(
        "👋 Привет! Я бот по BTC (данные + индикаторы + графики).\n\nВыбирай действие:",
        reply_markup=main_menu(),
    )


@router.callback_query(F.data == "help")
async def help_cb(cb: CallbackQuery):
    txt = (
        "ℹ️ Справка\n"
        "— 📈 Цена сейчас: последняя свеча из файла данных\n"
        "— 🕯 График: свечи + объём (последние 300 часов)\n"
        "— 📊 Индикаторы: RSI и MACD с короткой интерпретацией\n"
        "— 🔮 Прогноз: пока заглушка, позже подключим модель напарника\n"
    )
    await cb.message.answer(txt, reply_markup=main_menu())
    await cb.answer()


@router.callback_query(F.data == "price_now")
async def price_now(cb: CallbackQuery):
    c = get_last_candle()
    txt = (
        "📈 Последняя свеча BTC/USDT (1h)\n"
        f"🕒 {c['timestamp_utc']}\n"
        f"Open: {c['open']:.2f}\n"
        f"High: {c['high']:.2f}\n"
        f"Low:  {c['low']:.2f}\n"
        f"Close:{c['close']:.2f}\n"
        f"Volume: {c['volume']:.4f}\n"
    )
    await cb.message.answer(txt, reply_markup=main_menu())
    await cb.answer()


@router.callback_query(F.data == "chart")
async def chart(cb: CallbackQuery):
    df = load_df_last_n(300)
    out_path = "data/processed/chart_last_300.png"
    make_candles_chart(df, out_path)
    photo = FSInputFile(out_path)
    await cb.message.answer_photo(photo, caption="🕯 Свечной график (последние 300 часов)", reply_markup=main_menu())
    await cb.answer()


@router.callback_query(F.data == "indicators")
async def indicators(cb: CallbackQuery):
    df = load_df_last_n(400)
    ind = calc_indicators(df)
    txt = (
        "📊 Индикаторы (по последним данным)\n"
        f"RSI(14): {ind['rsi']:.2f}\n"
        f"MACD: {ind['macd']:.4f}\n"
        f"Signal: {ind['signal']:.4f}\n"
        f"Hist: {ind['hist']:.4f}\n\n"
        f"Комментарий RSI: {ind['rsi_text']}\n"
        f"Комментарий MACD: {ind['macd_text']}\n"
    )
    await cb.message.answer(txt, reply_markup=main_menu())
    await cb.answer()


@router.callback_query(F.data == "forecast")
async def forecast(cb: CallbackQuery):
    c = get_last_candle()
    txt = (
        "🔮 Прогноз (пока заглушка)\n"
        f"Текущая цена (close): {c['close']:.2f}\n\n"
        "Дальше подключим модель напарника через API /predict и будем выдавать:\n"
        "— прогноз на 1h и 1d\n"
        "— интервал (квантили)\n"
        "— метрики модели\n"
    )
    await cb.message.answer(txt, reply_markup=main_menu())
    await cb.answer()
