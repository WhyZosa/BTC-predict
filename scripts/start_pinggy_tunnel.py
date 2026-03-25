from __future__ import annotations

import asyncio
import atexit
import queue
import re
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
PINGGY_PID_PATH = BASE_DIR / ".pinggy-ssh.pid"
KNOWN_HOSTS_PATH = BASE_DIR / "tools" / "pinggy_known_hosts"
LOCAL_API_URL = "http://127.0.0.1:8000"
PINGGY_DEBUG_URL = "http://127.0.0.1:4300/urls"
PINGGY_HOST = "a.pinggy.io"
PINGGY_URL_PATTERN = re.compile(r"https://[^\s]+\.pinggy\.link\b", re.IGNORECASE)


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


def get_pinggy_https_url() -> str | None:
    try:
        response = httpx.get(PINGGY_DEBUG_URL, timeout=5)
        response.raise_for_status()
        payload = response.json()
        urls = payload.get("urls", [])
        for url in urls:
            if isinstance(url, str) and url.startswith("https://"):
                return url
    except Exception:
        return None
    return None


def extract_pinggy_https_url(line: str) -> str | None:
    match = PINGGY_URL_PATTERN.search(line)
    if not match:
        return None
    return match.group(0).rstrip("/")


def pump_stdout(process: subprocess.Popen[str], line_queue: queue.Queue[str]) -> None:
    assert process.stdout is not None
    for line in process.stdout:
        line_queue.put(line)


def detect_pinggy_error(line: str) -> str | None:
    lowered = line.lower()
    if "could not resolve hostname" in lowered:
        return "Pinggy hostname could not be resolved. Check your internet connection and DNS settings."
    if "permission denied" in lowered:
        return "Pinggy SSH login was denied. If you see a password prompt, press Enter once without typing a password."
    if "connection timed out" in lowered or "operation timed out" in lowered:
        return "Pinggy connection timed out. Try another network or VPN."
    return None


def cleanup_pid_file() -> None:
    PINGGY_PID_PATH.unlink(missing_ok=True)


def main() -> None:
    KNOWN_HOSTS_PATH.parent.mkdir(parents=True, exist_ok=True)

    process = subprocess.Popen(
        [
            "ssh",
            "-p",
            "443",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            f"UserKnownHostsFile={KNOWN_HOSTS_PATH}",
            "-o",
            "ServerAliveInterval=30",
            "-o",
            "TCPKeepAlive=yes",
            "-o",
            "ExitOnForwardFailure=yes",
            "-T",
            "-R",
            "0:127.0.0.1:8000",
            "-L",
            "4300:127.0.0.1:4300",
            PINGGY_HOST,
        ],
        cwd=str(BASE_DIR),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    PINGGY_PID_PATH.write_text(str(process.pid), encoding="utf-8")
    atexit.register(cleanup_pid_file)

    line_queue: queue.Queue[str] = queue.Queue()
    reader = threading.Thread(target=pump_stdout, args=(process, line_queue), daemon=True)
    reader.start()

    public_url = None
    try:
        if process.stdin is not None:
            try:
                process.stdin.write("\n")
                process.stdin.flush()
            except OSError:
                pass

        print("Waiting for Pinggy to publish an HTTPS URL ...")
        deadline = time.time() + 35
        startup_error = None
        while time.time() < deadline and public_url is None and startup_error is None:
            while True:
                try:
                    line = line_queue.get_nowait()
                except queue.Empty:
                    break
                sys.stdout.write(line)
                sys.stdout.flush()
                public_url = extract_pinggy_https_url(line) or public_url
                startup_error = detect_pinggy_error(line)
                if startup_error or public_url:
                    break

            if startup_error or process.poll() is not None:
                break

            public_url = public_url or get_pinggy_https_url()
            if public_url:
                break

            time.sleep(1)

        if startup_error:
            print(startup_error)
        elif not public_url:
            print("Pinggy did not expose an HTTPS URL in time. If there was an SSH password prompt, press Enter once and run run_pinggy.cmd again.")
        else:
            print(f"Public Mini App URL: {public_url}")
            print("Waiting until the public URL becomes reachable ...")
            if wait_until_public_url_is_ready(public_url):
                update_env("TELEGRAM_MINI_APP_URL", public_url)
                print("Saved to .env as TELEGRAM_MINI_APP_URL")
                asyncio.run(configure_menu_button(public_url))
                print("Now restart the bot with run_bot.cmd if it is already running.\n")
            else:
                print("Pinggy URL is not ready yet. Stop this window and run run_pinggy.cmd again.\n")

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
        print("\nStopping Pinggy tunnel...")
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()

        cleanup_pid_file()

        if public_url:
            try:
                print("Tunnel stopped. Restoring local TELEGRAM_MINI_APP_URL ...")
                update_env("TELEGRAM_MINI_APP_URL", LOCAL_API_URL)
                asyncio.run(reset_menu_button())
            except Exception:
                pass


if __name__ == "__main__":
    main()
