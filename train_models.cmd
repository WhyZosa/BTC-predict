@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] Virtual environment not found: .venv\Scripts\python.exe
  echo Run: py -m venv .venv
  exit /b 1
)

echo Training models on BTCUSDT hourly candles
".venv\Scripts\python.exe" scripts\train_models.py --provider binance --symbol BTCUSDT --interval 1h --limit 400
