from __future__ import annotations

import csv
import io
import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

from app.config import settings
from app.schemas import AlertItem, Candle, PredictionHistoryItem, PredictionResponse, UserContext


def _to_iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _direction(value: float, epsilon: float = 1e-9) -> int:
    if value > epsilon:
        return 1
    if value < -epsilon:
        return -1
    return 0


class UserDataService:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        settings.data_dir.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(settings.database_path, timeout=30, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_db(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_key TEXT PRIMARY KEY,
                    telegram_user_id INTEGER,
                    chat_id INTEGER,
                    username TEXT,
                    first_name TEXT,
                    last_seen_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS prediction_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_key TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    horizon TEXT NOT NULL,
                    current_price REAL NOT NULL,
                    predicted_price REAL NOT NULL,
                    target_at TEXT NOT NULL,
                    delta_abs REAL NOT NULL,
                    delta_pct REAL NOT NULL,
                    confidence_low REAL NOT NULL,
                    confidence_high REAL NOT NULL,
                    model_name TEXT NOT NULL,
                    generated_at TEXT NOT NULL,
                    actual_price REAL,
                    actual_at TEXT,
                    abs_error REAL,
                    pct_error REAL,
                    direction_hit INTEGER,
                    status TEXT NOT NULL DEFAULT 'pending'
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_key TEXT NOT NULL,
                    telegram_user_id INTEGER,
                    chat_id INTEGER,
                    kind TEXT NOT NULL,
                    threshold_value REAL NOT NULL,
                    baseline_price REAL,
                    created_price REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    triggered_at TEXT,
                    is_active INTEGER NOT NULL DEFAULT 1
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_prediction_user_generated ON prediction_history(user_key, generated_at DESC)"
            )
            connection.execute("CREATE INDEX IF NOT EXISTS idx_prediction_pending ON prediction_history(status, target_at)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_alerts_active ON alerts(is_active, kind)")
            connection.commit()

    def upsert_user(self, context: UserContext) -> None:
        if not context.user_key:
            return
        now_iso = _to_iso(datetime.now(UTC))
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO users (user_key, telegram_user_id, chat_id, username, first_name, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_key) DO UPDATE SET
                    telegram_user_id=COALESCE(excluded.telegram_user_id, users.telegram_user_id),
                    chat_id=COALESCE(excluded.chat_id, users.chat_id),
                    username=COALESCE(excluded.username, users.username),
                    first_name=COALESCE(excluded.first_name, users.first_name),
                    last_seen_at=excluded.last_seen_at
                """,
                (
                    context.user_key,
                    context.telegram_user_id,
                    context.chat_id,
                    context.username,
                    context.first_name,
                    now_iso,
                ),
            )
            connection.commit()

    def save_prediction(self, context: UserContext, prediction: PredictionResponse) -> PredictionHistoryItem:
        self.upsert_user(context)
        with self._lock, self._connect() as connection:
            duplicate_row = connection.execute(
                """
                SELECT * FROM prediction_history
                WHERE user_key = ? AND horizon = ? AND target_at = ? AND generated_at >= ?
                ORDER BY generated_at DESC
                LIMIT 1
                """,
                (
                    context.user_key,
                    prediction.horizon,
                    _to_iso(prediction.target_at),
                    _to_iso(datetime.now(UTC) - timedelta(seconds=20)),
                ),
            ).fetchone()
            if duplicate_row is not None:
                return PredictionHistoryItem.from_row(dict(duplicate_row))

            cursor = connection.execute(
                """
                INSERT INTO prediction_history (
                    user_key, symbol, horizon, current_price, predicted_price,
                    target_at, delta_abs, delta_pct, confidence_low, confidence_high,
                    model_name, generated_at, status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (
                    context.user_key,
                    prediction.symbol,
                    prediction.horizon,
                    prediction.current_price,
                    prediction.predicted_price,
                    _to_iso(prediction.target_at),
                    prediction.delta_abs,
                    prediction.delta_pct,
                    prediction.confidence_low,
                    prediction.confidence_high,
                    prediction.model_name,
                    _to_iso(prediction.generated_at),
                ),
            )
            row_id = int(cursor.lastrowid)
            connection.commit()

        return self.get_prediction(row_id)

    def get_prediction(self, prediction_id: int) -> PredictionHistoryItem:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM prediction_history WHERE id = ?", (prediction_id,)).fetchone()
        if row is None:
            raise KeyError(f"Prediction {prediction_id} not found")
        return PredictionHistoryItem.from_row(dict(row))

    def sync_due_predictions(self, candles: list[Candle]) -> None:
        if not candles:
            return

        latest_timestamp = candles[-1].timestamp
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM prediction_history
                WHERE status = 'pending' AND target_at <= ?
                ORDER BY target_at ASC
                """,
                (_to_iso(latest_timestamp),),
            ).fetchall()

            for row in rows:
                item = PredictionHistoryItem.from_row(dict(row))
                actual_candle = self._nearest_candle(candles, item.target_at)
                if actual_candle is None:
                    continue

                actual_price = actual_candle.close
                abs_error = abs(actual_price - item.predicted_price)
                pct_error = (abs_error / actual_price) * 100 if actual_price else None
                predicted_direction = _direction(item.predicted_price - item.current_price)
                actual_direction = _direction(actual_price - item.current_price)
                direction_hit = 1 if predicted_direction == actual_direction else 0
                connection.execute(
                    """
                    UPDATE prediction_history
                    SET actual_price = ?, actual_at = ?, abs_error = ?, pct_error = ?, direction_hit = ?, status = 'resolved'
                    WHERE id = ?
                    """,
                    (
                        actual_price,
                        _to_iso(actual_candle.timestamp),
                        abs_error,
                        pct_error,
                        direction_hit,
                        item.id,
                    ),
                )

            connection.commit()

    def list_predictions(self, user_key: str, limit: int = 20) -> list[PredictionHistoryItem]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM prediction_history
                WHERE user_key = ?
                ORDER BY generated_at DESC
                LIMIT ?
                """,
                (user_key, limit),
            ).fetchall()
        return [PredictionHistoryItem.from_row(dict(row)) for row in rows]

    def list_all_predictions(self, user_key: str) -> list[PredictionHistoryItem]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM prediction_history
                WHERE user_key = ?
                ORDER BY generated_at DESC
                """,
                (user_key,),
            ).fetchall()
        return [PredictionHistoryItem.from_row(dict(row)) for row in rows]

    def export_predictions_csv(self, user_key: str) -> str:
        items = self.list_all_predictions(user_key)
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "id",
                "horizon",
                "generated_at",
                "target_at",
                "current_price",
                "predicted_price",
                "confidence_low",
                "confidence_high",
                "actual_price",
                "actual_at",
                "abs_error",
                "pct_error",
                "direction_hit",
                "interval_hit",
                "success_tolerance_pct",
                "tolerance_low",
                "tolerance_high",
                "tolerance_hit",
                "outcome_status",
                "model_name",
            ]
        )
        for item in items:
            writer.writerow(
                [
                    item.id,
                    item.horizon,
                    _to_iso(item.generated_at),
                    _to_iso(item.target_at),
                    round(item.current_price, 2),
                    round(item.predicted_price, 2),
                    round(item.confidence_low, 2),
                    round(item.confidence_high, 2),
                    "" if item.actual_price is None else round(item.actual_price, 2),
                    "" if item.actual_at is None else _to_iso(item.actual_at),
                    "" if item.abs_error is None else round(item.abs_error, 2),
                    "" if item.pct_error is None else round(item.pct_error, 4),
                    "" if item.direction_hit is None else int(item.direction_hit),
                    "" if item.interval_hit is None else int(item.interval_hit),
                    round(item.success_tolerance_pct, 4),
                    round(item.tolerance_low, 2),
                    round(item.tolerance_high, 2),
                    "" if item.tolerance_hit is None else int(item.tolerance_hit),
                    item.outcome_status,
                    item.model_name,
                ]
            )
        return output.getvalue()

    def get_live_quality_summary(self, user_key: str) -> dict[str, object]:
        items = self.list_all_predictions(user_key)
        resolved = [item for item in items if item.actual_price is not None]

        overall = self._build_quality_bucket(resolved)
        by_horizon = {horizon: self._build_quality_bucket([item for item in resolved if item.horizon == horizon]) for horizon in ("6h", "1d", "1w")}

        return {
            "overall": overall,
            "by_horizon": by_horizon,
            "total_predictions": len(items),
            "resolved_predictions": len(resolved),
        }

    def create_alert(self, context: UserContext, kind: str, threshold_value: float, current_price: float) -> AlertItem:
        if kind not in {"above_price", "below_price", "drop_percent"}:
            raise ValueError("Unsupported alert kind")
        self.upsert_user(context)
        baseline_price = current_price if kind == "drop_percent" else None
        now = datetime.now(UTC)
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO alerts (
                    user_key, telegram_user_id, chat_id, kind, threshold_value,
                    baseline_price, created_price, created_at, is_active
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    context.user_key,
                    context.telegram_user_id,
                    context.chat_id,
                    kind,
                    threshold_value,
                    baseline_price,
                    current_price,
                    _to_iso(now),
                ),
            )
            alert_id = int(cursor.lastrowid)
            connection.commit()
        return self.get_alert(alert_id)

    def get_alert(self, alert_id: int) -> AlertItem:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,)).fetchone()
        if row is None:
            raise KeyError(f"Alert {alert_id} not found")
        return self._row_to_alert(dict(row))

    def list_alerts(self, user_key: str, limit: int = 20) -> list[AlertItem]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM alerts
                WHERE user_key = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (user_key, limit),
            ).fetchall()
        return [self._row_to_alert(dict(row)) for row in rows]

    def delete_alert(self, user_key: str, alert_id: int) -> None:
        with self._lock, self._connect() as connection:
            connection.execute("DELETE FROM alerts WHERE id = ? AND user_key = ?", (alert_id, user_key))
            connection.commit()

    def get_triggerable_alerts(self, current_price: float) -> list[tuple[AlertItem, int]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM alerts WHERE is_active = 1 ORDER BY created_at ASC").fetchall()

        due_alerts: list[tuple[AlertItem, int]] = []
        for row in rows:
            item = self._row_to_alert(dict(row))
            if item.kind == "above_price":
                should_trigger = current_price >= item.threshold_value
            elif item.kind == "below_price":
                should_trigger = current_price <= item.threshold_value
            else:
                if not item.baseline_price:
                    continue
                should_trigger = current_price <= item.baseline_price * (1 - item.threshold_value / 100)
            if should_trigger:
                chat_id = int(row["chat_id"]) if row["chat_id"] is not None else 0
                if chat_id:
                    due_alerts.append((item, chat_id))
        return due_alerts

    def mark_alert_triggered(self, alert_id: int) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                "UPDATE alerts SET is_active = 0, triggered_at = ? WHERE id = ?",
                (_to_iso(datetime.now(UTC)), alert_id),
            )
            connection.commit()

    def deactivate_alert(self, alert_id: int) -> None:
        with self._lock, self._connect() as connection:
            connection.execute("UPDATE alerts SET is_active = 0 WHERE id = ?", (alert_id,))
            connection.commit()

    def _build_quality_bucket(self, items: list[PredictionHistoryItem]) -> dict[str, object]:
        if not items:
            return {
                "available": False,
                "count": 0,
                "mae": None,
                "smape": None,
                "directional_accuracy": None,
                "interval_hit_rate": None,
            }

        mae = sum(item.abs_error or 0.0 for item in items) / len(items)
        directional_hits = [item.direction_hit for item in items if item.direction_hit is not None]
        interval_hits = [item.interval_hit for item in items if item.interval_hit is not None]
        smape_values = []
        for item in items:
            if item.actual_price is None:
                continue
            denom = (abs(item.actual_price) + abs(item.predicted_price)) / 2
            if denom:
                smape_values.append(abs(item.actual_price - item.predicted_price) / denom * 100)

        return {
            "available": True,
            "count": len(items),
            "mae": round(mae, 2),
            "smape": round(sum(smape_values) / len(smape_values), 4) if smape_values else None,
            "directional_accuracy": round(sum(1 for value in directional_hits if value) / len(directional_hits) * 100, 4) if directional_hits else None,
            "interval_hit_rate": round(sum(1 for value in interval_hits if value) / len(interval_hits) * 100, 4) if interval_hits else None,
        }

    def _nearest_candle(self, candles: list[Candle], target_at: datetime) -> Candle | None:
        if not candles:
            return None
        candidate = min(candles, key=lambda candle: abs((candle.timestamp - target_at).total_seconds()))
        if abs((candidate.timestamp - target_at).total_seconds()) > 7200:
            return None
        return candidate

    def _row_to_alert(self, row: dict[str, object]) -> AlertItem:
        target_price = None
        if row["kind"] == "above_price":
            target_price = float(row["threshold_value"])
        elif row["kind"] == "below_price":
            target_price = float(row["threshold_value"])
        elif row["baseline_price"] is not None:
            target_price = float(row["baseline_price"]) * (1 - float(row["threshold_value"]) / 100)

        payload = dict(row)
        payload["target_price"] = target_price
        return AlertItem.from_row(payload)


user_data_service = UserDataService()
