from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal


Horizon = Literal["6h", "1d", "1w"]
HORIZON_SUCCESS_TOLERANCE_PCT: dict[Horizon, float] = {
    "6h": 0.75,
    "1d": 1.5,
    "1w": 4.5,
}


def _to_iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _from_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@dataclass(slots=True)
class PricePoint:
    timestamp: datetime
    price: float
    source: str

    def to_dict(self) -> dict[str, str | float]:
        return {
            "timestamp": _to_iso(self.timestamp),
            "price": round(self.price, 2),
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, str | float]) -> "PricePoint":
        return cls(
            timestamp=datetime.fromisoformat(str(payload["timestamp"]).replace("Z", "+00:00")),
            price=float(payload["price"]),
            source=str(payload["source"]),
        )


@dataclass(slots=True)
class Candle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0

    def to_dict(self) -> dict[str, str | float]:
        return {
            "timestamp": _to_iso(self.timestamp),
            "open": round(self.open, 2),
            "high": round(self.high, 2),
            "low": round(self.low, 2),
            "close": round(self.close, 2),
            "volume": round(self.volume, 4),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, str | float]) -> "Candle":
        return cls(
            timestamp=datetime.fromisoformat(str(payload["timestamp"]).replace("Z", "+00:00")),
            open=float(payload["open"]),
            high=float(payload["high"]),
            low=float(payload["low"]),
            close=float(payload["close"]),
            volume=float(payload.get("volume", 0.0)),
        )


@dataclass(slots=True)
class MarketSnapshot:
    symbol: str
    latest: PricePoint
    change_24h_pct: float | None = None
    candles: list[Candle] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "latest": self.latest.to_dict(),
            "change_24h_pct": None if self.change_24h_pct is None else round(self.change_24h_pct, 4),
            "candles": [candle.to_dict() for candle in self.candles],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "MarketSnapshot":
        candles = [Candle.from_dict(item) for item in list(payload.get("candles", []))]
        return cls(
            symbol=str(payload["symbol"]),
            latest=PricePoint.from_dict(dict(payload["latest"])),
            change_24h_pct=None if payload.get("change_24h_pct") is None else float(payload["change_24h_pct"]),
            candles=candles,
        )


@dataclass(slots=True)
class PredictionResponse:
    symbol: str
    horizon: Horizon
    current_price: float
    predicted_price: float
    target_at: datetime
    delta_abs: float
    delta_pct: float
    confidence_low: float
    confidence_high: float
    model_name: str
    generated_at: datetime
    features_used: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "horizon": self.horizon,
            "current_price": round(self.current_price, 2),
            "predicted_price": round(self.predicted_price, 2),
            "target_at": _to_iso(self.target_at),
            "delta_abs": round(self.delta_abs, 2),
            "delta_pct": round(self.delta_pct, 4),
            "confidence_low": round(self.confidence_low, 2),
            "confidence_high": round(self.confidence_high, 2),
            "model_name": self.model_name,
            "generated_at": _to_iso(self.generated_at),
            "features_used": self.features_used,
        }


@dataclass(slots=True)
class UserContext:
    user_key: str
    telegram_user_id: int | None = None
    chat_id: int | None = None
    username: str | None = None
    first_name: str | None = None


@dataclass(slots=True)
class PredictionHistoryItem:
    id: int
    user_key: str
    symbol: str
    horizon: Horizon
    current_price: float
    predicted_price: float
    target_at: datetime
    delta_abs: float
    delta_pct: float
    confidence_low: float
    confidence_high: float
    model_name: str
    generated_at: datetime
    status: str
    actual_price: float | None = None
    actual_at: datetime | None = None
    abs_error: float | None = None
    pct_error: float | None = None
    direction_hit: bool | None = None

    @property
    def interval_hit(self) -> bool | None:
        if self.actual_price is None:
            return None
        return self.confidence_low <= self.actual_price <= self.confidence_high

    @property
    def success_tolerance_pct(self) -> float:
        return HORIZON_SUCCESS_TOLERANCE_PCT.get(self.horizon, 1.5)

    @property
    def tolerance_low(self) -> float:
        return self.predicted_price * (1 - self.success_tolerance_pct / 100)

    @property
    def tolerance_high(self) -> float:
        return self.predicted_price * (1 + self.success_tolerance_pct / 100)

    @property
    def tolerance_hit(self) -> bool | None:
        if self.actual_price is None:
            return None
        return self.tolerance_low <= self.actual_price <= self.tolerance_high

    @property
    def outcome_status(self) -> str:
        if self.actual_price is None:
            return "pending"
        return "hit" if self.tolerance_hit else "miss"

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "user_key": self.user_key,
            "symbol": self.symbol,
            "horizon": self.horizon,
            "current_price": round(self.current_price, 2),
            "predicted_price": round(self.predicted_price, 2),
            "target_at": _to_iso(self.target_at),
            "delta_abs": round(self.delta_abs, 2),
            "delta_pct": round(self.delta_pct, 4),
            "confidence_low": round(self.confidence_low, 2),
            "confidence_high": round(self.confidence_high, 2),
            "model_name": self.model_name,
            "generated_at": _to_iso(self.generated_at),
            "status": self.status,
            "actual_price": None if self.actual_price is None else round(self.actual_price, 2),
            "actual_at": None if self.actual_at is None else _to_iso(self.actual_at),
            "abs_error": None if self.abs_error is None else round(self.abs_error, 2),
            "pct_error": None if self.pct_error is None else round(self.pct_error, 4),
            "direction_hit": self.direction_hit,
            "interval_hit": self.interval_hit,
            "success_tolerance_pct": round(self.success_tolerance_pct, 4),
            "tolerance_low": round(self.tolerance_low, 2),
            "tolerance_high": round(self.tolerance_high, 2),
            "tolerance_hit": self.tolerance_hit,
            "outcome_status": self.outcome_status,
        }

    @classmethod
    def from_row(cls, row: dict[str, object]) -> "PredictionHistoryItem":
        return cls(
            id=int(row["id"]),
            user_key=str(row["user_key"]),
            symbol=str(row["symbol"]),
            horizon=str(row["horizon"]),
            current_price=float(row["current_price"]),
            predicted_price=float(row["predicted_price"]),
            target_at=_from_iso(str(row["target_at"])),
            delta_abs=float(row["delta_abs"]),
            delta_pct=float(row["delta_pct"]),
            confidence_low=float(row["confidence_low"]),
            confidence_high=float(row["confidence_high"]),
            model_name=str(row["model_name"]),
            generated_at=_from_iso(str(row["generated_at"])),
            status=str(row["status"]),
            actual_price=None if row["actual_price"] is None else float(row["actual_price"]),
            actual_at=None if row["actual_at"] is None else _from_iso(str(row["actual_at"])),
            abs_error=None if row["abs_error"] is None else float(row["abs_error"]),
            pct_error=None if row["pct_error"] is None else float(row["pct_error"]),
            direction_hit=None if row["direction_hit"] is None else bool(int(row["direction_hit"])),
        )


@dataclass(slots=True)
class AlertItem:
    id: int
    user_key: str
    kind: str
    threshold_value: float
    baseline_price: float | None
    created_price: float
    created_at: datetime
    is_active: bool
    triggered_at: datetime | None = None
    target_price: float | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "user_key": self.user_key,
            "kind": self.kind,
            "threshold_value": round(self.threshold_value, 4),
            "baseline_price": None if self.baseline_price is None else round(self.baseline_price, 2),
            "created_price": round(self.created_price, 2),
            "created_at": _to_iso(self.created_at),
            "is_active": self.is_active,
            "triggered_at": None if self.triggered_at is None else _to_iso(self.triggered_at),
            "target_price": None if self.target_price is None else round(self.target_price, 2),
        }

    @classmethod
    def from_row(cls, row: dict[str, object]) -> "AlertItem":
        return cls(
            id=int(row["id"]),
            user_key=str(row["user_key"]),
            kind=str(row["kind"]),
            threshold_value=float(row["threshold_value"]),
            baseline_price=None if row["baseline_price"] is None else float(row["baseline_price"]),
            created_price=float(row["created_price"]),
            created_at=_from_iso(str(row["created_at"])),
            is_active=bool(int(row["is_active"])),
            triggered_at=None if row["triggered_at"] is None else _from_iso(str(row["triggered_at"])),
            target_price=None if row.get("target_price") is None else float(row["target_price"]),
        )
