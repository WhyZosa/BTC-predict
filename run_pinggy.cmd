@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] Virtual environment not found: .venv\Scripts\python.exe
  exit /b 1
)

where ssh >nul 2>nul
if errorlevel 1 (
  echo [ERROR] OpenSSH client was not found in PATH.
  echo Install OpenSSH Client in Windows and run again.
  exit /b 1
)

echo Starting Pinggy tunnel for http://127.0.0.1:8000
echo If SSH asks for a password, just press Enter once.
".venv\Scripts\python.exe" scripts\start_pinggy_tunnel.py
