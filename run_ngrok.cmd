@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] Virtual environment not found: .venv\Scripts\python.exe
  exit /b 1
)

if not exist ".\tools\ngrok.exe" (
  echo ngrok not found. Run install_ngrok.cmd first.
  exit /b 1
)

echo Starting ngrok tunnel for http://127.0.0.1:8000
".venv\Scripts\python.exe" scripts\start_ngrok_tunnel.py
