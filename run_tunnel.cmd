@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] Virtual environment not found: .venv\Scripts\python.exe
  exit /b 1
)

if not exist ".\tools\cloudflared.exe" (
  echo cloudflared not found. Run install_cloudflared.cmd first.
  exit /b 1
)

echo Starting Cloudflare Tunnel for http://127.0.0.1:8000 using HTTP/2
".venv\Scripts\python.exe" scripts\start_cloudflare_tunnel.py
