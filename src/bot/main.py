import asyncio
from aiogram import Bot, Dispatcher

from src.common.config import require_telegram_token
from src.common.logging import setup_logger
from src.bot.handlers import router


async def main():
    setup_logger()
    token = require_telegram_token()

    bot = Bot(token=token)
    dp = Dispatcher()
    dp.include_router(router)

    print("✅ Бот запущен. Нажми Ctrl+C для остановки.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
