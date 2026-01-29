from __future__ import annotations

import sys
import subprocess
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler


def run_module(module: str) -> None:
    print(f"\n🛠 Запуск: {module} ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    p = subprocess.run(
        [sys.executable, "-m", module],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="ignore",
    )
    if p.stdout:
        print(p.stdout)
    if p.stderr:
        print("⚠️ STDERR:\n" + p.stderr)
    if p.returncode != 0:
        raise RuntimeError(f"Команда {module} завершилась с кодом {p.returncode}")


def job():
    run_module("src.data_pipeline.download_ohlcv")
    run_module("src.data_pipeline.fix_gaps")
    run_module("src.data_pipeline.export_csv")
    print("✅ Цикл обновления завершён.")


def main():
    print("✅ Обновлятор запущен. Интервал: 5 минут. Остановка: Ctrl+C")
    scheduler = BlockingScheduler()
    scheduler.add_job(job, "interval", minutes=5, next_run_time=datetime.now())
    scheduler.start()


if __name__ == "__main__":
    main()
