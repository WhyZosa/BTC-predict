import os
from pathlib import Path
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv(Path(__file__).with_name(".env"), override=True)

TOKEN = os.getenv("BOT_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL", "http://localhost:5173")

if not TOKEN:
    raise RuntimeError("BOT_TOKEN is missing. Open bot\\.env and set BOT_TOKEN=...")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = [[InlineKeyboardButton("Окрыть трейдер-миниапп", web_app=WebAppInfo(url=WEBAPP_URL))]]
    await update.message.reply_text("Окрывай мини-приложение 👇", reply_markup=InlineKeyboardMarkup(kb))

def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.run_polling()

if __name__ == "__main__":
    main()

