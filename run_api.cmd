@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] Virtual environment not found: .venv\Scripts\python.exe
  echo Run: py -m venv .venv
  exit /b 1
)

if not exist ".env" (
  copy /Y ".env.example" ".env" >nul
)

echo Starting API on http://127.0.0.1:8000
".venv\Scripts\python.exe" -m waitress --listen=0.0.0.0:8000 app.main:app
