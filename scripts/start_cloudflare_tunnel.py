from __future__ import annotations

import asyncio
import re
import subprocess
import sys
import time
from pathlib import Path

from dotenv import dotenv_values
import httpx
from telegram import Bot, MenuButtonCommands, MenuButtonWebApp, WebAppInfo


BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
CLOUDFLARED_PATH = BASE_DIR / "tools" / "cloudflared.exe"
PUBLIC_URL_PATTERN = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com", re.IGNORECASE)


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


def main() -> None:
    if not CLOUDFLARED_PATH.exists():
        raise RuntimeError("cloudflared.exe not found. Run install_cloudflared.cmd first.")

    process = subprocess.Popen(
        [
            str(CLOUDFLARED_PATH),
            "tunnel",
            "--url",
            "http://127.0.0.1:8000",
            "--protocol",
            "http2",
            "--no-autoupdate",
        ],
        cwd=str(BASE_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    public_url = None
    try:
        assert process.stdout is not None
        for line in process.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()

            match = PUBLIC_URL_PATTERN.search(line)
            if match and public_url is None:
                public_url = match.group(0)
                print(f"\nPublic Mini App URL: {public_url}")
                print("Waiting until the public URL becomes reachable ...")

                if wait_until_public_url_is_ready(public_url):
                    update_env("TELEGRAM_MINI_APP_URL", public_url)
                    print("Saved to .env as TELEGRAM_MINI_APP_URL")
                    asyncio.run(configure_menu_button(public_url))
                    print("Now restart the bot with run_bot.cmd if it is already running.\n")
                else:
                    print("Tunnel URL is not ready yet. Stop this window and run run_tunnel.cmd again.\n")

        process.wait()
    except KeyboardInterrupt:
        print("\nStopping Cloudflare Tunnel...")
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
                update_env("TELEGRAM_MINI_APP_URL", "http://127.0.0.1:8000")
                asyncio.run(reset_menu_button())
            except Exception:
                pass


if __name__ == "__main__":
    main()
