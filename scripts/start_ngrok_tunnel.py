from __future__ import annotations

import asyncio
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path

from dotenv import dotenv_values
import httpx
from telegram import Bot, MenuButtonCommands, MenuButtonWebApp, WebAppInfo


BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
NGROK_PATH = BASE_DIR / "tools" / "ngrok.exe"
LOCAL_API_URL = "http://127.0.0.1:8000"
LOCAL_NGROK_API = "http://127.0.0.1:4040/api/tunnels"


def update_env(key: str, value: str) -> None:
    if ENV_PATH.exists():
        lines = ENV_PATH.read_text(encoding="utf-8").splitlines()
    else:
        lines = []

    updated = False
    output = []
    for line in lines:
        if line.startswith(f"{key}="):
            output.append(f"{key}={value}")
            updated = True
        else:
            output.append(line)

    if not updated:
        output.append(f"{key}={value}")

    ENV_PATH.write_text("\n".join(output) + "\n", encoding="utf-8")


async def close_bot(bot: Bot) -> None:
    shutdown = getattr(bot, "shutdown", None)
    if callable(shutdown):
        await shutdown()
        return

    request = getattr(bot, "request", None)
    if request is not None:
        shutdown_request = getattr(request, "shutdown", None)
        if callable(shutdown_request):
            await shutdown_request()


async def configure_menu_button(url: str) -> None:
    env = dotenv_values(ENV_PATH)
    token = str(env.get("TELEGRAM_BOT_TOKEN", "")).strip()
    button_text = str(env.get("TELEGRAM_MENU_BUTTON_TEXT", "Open BTC Forecast")).strip() or "Open BTC Forecast"
    if not token:
        return

    bot = Bot(token=token)
    try:
        await bot.set_chat_menu_button(
            menu_button=MenuButtonWebApp(text=button_text, web_app=WebAppInfo(url=url))
        )
        me = await bot.get_me()
        print(f"Telegram menu button updated for @{me.username}")
    finally:
        await close_bot(bot)


async def reset_menu_button() -> None:
    env = dotenv_values(ENV_PATH)
    token = str(env.get("TELEGRAM_BOT_TOKEN", "")).strip()
    if not token:
        return

    bot = Bot(token=token)
    try:
        await bot.set_chat_menu_button(menu_button=MenuButtonCommands())
    finally:
        await close_bot(bot)


def wait_until_public_url_is_ready(url: str, timeout_seconds: int = 45) -> bool:
    deadline = time.time() + timeout_seconds
    last_status = None
    while time.time() < deadline:
        try:
            response = httpx.get(url, timeout=10, follow_redirects=True)
            last_status = response.status_code
            if 200 <= response.status_code < 500 and response.status_code != 530:
                return True
        except Exception:
            last_status = None
        time.sleep(2)

    if last_status is not None:
        print(f"Public URL did not become ready in time. Last status: {last_status}")
    else:
        print("Public URL did not become ready in time.")
    return False


def wait_for_ngrok_https_url(timeout_seconds: int = 30) -> str | None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            response = httpx.get(LOCAL_NGROK_API, timeout=5)
            response.raise_for_status()
            tunnels = response.json().get("tunnels", [])
            for tunnel in tunnels:
                public_url = str(tunnel.get("public_url", "")).strip()
                if public_url.startswith("https://"):
                    return public_url
        except Exception:
            pass
        time.sleep(1)
    return None


def pump_stdout(process: subprocess.Popen[str], line_queue: queue.Queue[str]) -> None:
    assert process.stdout is not None
    for line in process.stdout:
        line_queue.put(line)


def detect_known_ngrok_error(line: str) -> str | None:
    if "ERR_NGROK_9040" in line:
        return (
            "ngrok blocked connections from your current IP address (ERR_NGROK_9040). "
            "Try another network, such as mobile hotspot or a different VPN exit."
        )
    if "ERR_NGROK_108" in line:
        return "Too many simultaneous ngrok agent sessions for this account. Stop other ngrok sessions and try again."
    return None


def main() -> None:
    if not NGROK_PATH.exists():
        raise RuntimeError("ngrok.exe not found. Run install_ngrok.cmd first.")

    env = dotenv_values(ENV_PATH)
    token = str(env.get("NGROK_AUTHTOKEN", "")).strip()
    if not token:
        raise RuntimeError("NGROK_AUTHTOKEN is not set. Run configure_ngrok.cmd first.")

    process = subprocess.Popen(
        [
            str(NGROK_PATH),
            "http",
            LOCAL_API_URL,
            "--authtoken",
            token,
            "--log",
            "stdout",
        ],
        cwd=str(BASE_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    line_queue: queue.Queue[str] = queue.Queue()
    reader = threading.Thread(target=pump_stdout, args=(process, line_queue), daemon=True)
    reader.start()

    public_url = None
    try:
        print("Waiting for ngrok to publish an HTTPS URL ...")
        deadline = time.time() + 30
        startup_error = None
        while time.time() < deadline and public_url is None and startup_error is None:
            while True:
                try:
                    line = line_queue.get_nowait()
                except queue.Empty:
                    break
                sys.stdout.write(line)
                sys.stdout.flush()
                startup_error = detect_known_ngrok_error(line)
                if startup_error:
                    break

            if startup_error or process.poll() is not None:
                break

            public_url = wait_for_ngrok_https_url(timeout_seconds=1)

        if startup_error:
            print(startup_error)
        elif not public_url:
            print("ngrok did not expose an HTTPS URL in time. Stop this window and run run_ngrok.cmd again.")
        else:
            print(f"Public Mini App URL: {public_url}")
            print("Waiting until the public URL becomes reachable ...")
            if wait_until_public_url_is_ready(public_url):
                update_env("TELEGRAM_MINI_APP_URL", public_url)
                print("Saved to .env as TELEGRAM_MINI_APP_URL")
                asyncio.run(configure_menu_button(public_url))
                print("Now restart the bot with run_bot.cmd if it is already running.\n")
            else:
                print("ngrok URL is not ready yet. Stop this window and run run_ngrok.cmd again.\n")

        while process.poll() is None:
            try:
                line = line_queue.get(timeout=0.5)
                sys.stdout.write(line)
                sys.stdout.flush()
            except queue.Empty:
                continue

        while True:
            try:
                line = line_queue.get_nowait()
            except queue.Empty:
                break
            sys.stdout.write(line)
            sys.stdout.flush()

        process.wait()
    except KeyboardInterrupt:
        print("\nStopping ngrok tunnel...")
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()

        if public_url:
            try:
                print("Tunnel stopped. Restoring local TELEGRAM_MINI_APP_URL ...")
                update_env("TELEGRAM_MINI_APP_URL", LOCAL_API_URL)
                asyncio.run(reset_menu_button())
            except Exception:
                pass


if __name__ == "__main__":
    main()
