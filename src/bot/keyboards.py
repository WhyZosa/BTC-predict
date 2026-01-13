from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📈 Цена сейчас", callback_data="price_now")],
            [InlineKeyboardButton(text="🕯 График (свечи)", callback_data="chart")],
            [InlineKeyboardButton(text="📊 Индикаторы (RSI/MACD)", callback_data="indicators")],
            [InlineKeyboardButton(text="🔮 Прогноз (заглушка)", callback_data="forecast")],
            [InlineKeyboardButton(text="ℹ️ Справка", callback_data="help")],
        ]
    )
