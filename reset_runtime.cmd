@echo off
setlocal
cd /d "%~dp0"

echo Stopping local project python processes from .venv ...
powershell -NoProfile -Command "Get-Process python,pythonw -ErrorAction SilentlyContinue | Where-Object { $_.Path -eq (Resolve-Path '.\.venv\Scripts\python.exe').Path } | Stop-Process -Force"
powershell -NoProfile -Command "$target = Resolve-Path '.\tools\ngrok.exe' -ErrorAction SilentlyContinue; if ($target) { Get-Process ngrok -ErrorAction SilentlyContinue | Where-Object { $_.Path -eq $target.Path } | Stop-Process -Force }"
powershell -NoProfile -Command "$target = Resolve-Path '.\tools\cloudflared.exe' -ErrorAction SilentlyContinue; if ($target) { Get-Process cloudflared -ErrorAction SilentlyContinue | Where-Object { $_.Path -eq $target.Path } | Stop-Process -Force }"
powershell -NoProfile -Command "if (Test-Path '.\.pinggy-ssh.pid') { $pid = Get-Content '.\.pinggy-ssh.pid' | Select-Object -First 1; if ($pid) { Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue }; Remove-Item '.\.pinggy-ssh.pid' -Force -ErrorAction SilentlyContinue }"

if exist ".bot.pid" del /q ".bot.pid" >nul 2>nul
if exist ".pinggy-ssh.pid" del /q ".pinggy-ssh.pid" >nul 2>nul

echo Done. Now start API and bot again:
echo   run_api.cmd
echo   run_bot.cmd
