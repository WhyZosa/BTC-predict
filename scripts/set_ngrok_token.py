from __future__ import annotations

from pathlib import Path

from dotenv import dotenv_values


BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"


def update_env(key: str, value: str) -> None:
    if ENV_PATH.exists():
        lines = ENV_PATH.read_text(encoding="utf-8").splitlines()
    else:
        lines = []

    updated = False
    output: list[str] = []
    for line in lines:
        if line.startswith(f"{key}="):
            output.append(f"{key}={value}")
            updated = True
        else:
            output.append(line)

    if not updated:
        output.append(f"{key}={value}")

    ENV_PATH.write_text("\n".join(output) + "\n", encoding="utf-8")


def main() -> None:
    env = dotenv_values(ENV_PATH)
    current = str(env.get("NGROK_AUTHTOKEN", "")).strip()
    if current:
        print("Current NGROK_AUTHTOKEN is already set.")
        answer = input("Replace it? [y/N]: ").strip().lower()
        if answer not in {"y", "yes"}:
            print("Token was not changed.")
            return

    token = input("Paste ngrok authtoken: ").strip()
    if not token:
        raise SystemExit("NGROK_AUTHTOKEN was not provided.")

    update_env("NGROK_AUTHTOKEN", token)
    print("Saved NGROK_AUTHTOKEN to .env")


if __name__ == "__main__":
    main()
