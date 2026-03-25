@echo off
setlocal
cd /d "%~dp0"

if not exist "tools" mkdir "tools"

echo Downloading ngrok for Windows...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$zip = Join-Path (Get-Location) 'tools\\ngrok.zip';" ^
  "Invoke-WebRequest -UseBasicParsing 'https://bin.equinox.io/c/bNyj1mQVY4c/ngrok-v3-stable-windows-amd64.zip' -OutFile $zip;" ^
  "Expand-Archive -Path $zip -DestinationPath 'tools' -Force;" ^
  "Remove-Item $zip -Force;"

if exist "tools\ngrok.exe" (
  echo ngrok installed: tools\ngrok.exe
  exit /b 0
)

echo [ERROR] Failed to install ngrok.
exit /b 1
