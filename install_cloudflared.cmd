@echo off
setlocal
cd /d "%~dp0"

if not exist "tools" mkdir "tools"

echo Downloading cloudflared for Windows x64 ...
powershell -NoProfile -Command "Invoke-WebRequest -Uri 'https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe' -OutFile '.\tools\cloudflared.exe'"

if not exist ".\tools\cloudflared.exe" (
  echo [ERROR] cloudflared download failed
  exit /b 1
)

echo cloudflared installed at .\tools\cloudflared.exe
