from __future__ import annotations

from flask import Flask, Response, jsonify, request, send_from_directory

from app.config import settings
from app.schemas import UserContext
from app.services.market import MarketDataError, market_data_service
from app.services.predictor import PredictorUnavailableError, predictor_service
from app.services.user_data import user_data_service


app = Flask(__name__, static_folder=str(settings.static_dir), static_url_path="/static")


def _request_payload() -> dict[str, object]:
    if request.method == "POST":
        payload = request.get_json(silent=True)
        if isinstance(payload, dict):
            return payload
    return dict(request.args)


def _user_context_from_payload(payload: dict[str, object]) -> UserContext | None:
    user_key = str(payload.get("user_key", "")).strip()
    if not user_key:
        return None

    telegram_user_id = payload.get("telegram_user_id")
    chat_id = payload.get("chat_id")
    return UserContext(
        user_key=user_key,
        telegram_user_id=int(telegram_user_id) if telegram_user_id not in (None, "", "null") else None,
        chat_id=int(chat_id) if chat_id not in (None, "", "null") else None,
        username=str(payload.get("username", "")).strip() or None,
        first_name=str(payload.get("first_name", "")).strip() or None,
    )


@app.get("/")
def index():
    return send_from_directory(settings.static_dir, "index.html")


@app.get("/api/health")
def health():
    return jsonify({"status": "ok", "env": settings.app_env})


@app.get("/api/market")
def market():
    limit = request.args.get("limit", default=settings.history_limit, type=int)
    limit = max(48, min(limit, 1000))
    try:
        snapshot = market_data_service.get_snapshot(limit=limit)
    except MarketDataError as exc:
        return jsonify({"detail": f"Market data provider failed: {exc}"}), 502
    return jsonify(snapshot.to_dict())


@app.route("/api/predict", methods=["GET", "POST"])
def predict():
    payload = _request_payload()
    horizon = str(payload.get("horizon", "")).strip()
    if horizon not in {"6h", "1d", "1w"}:
        return jsonify({"detail": "Unsupported horizon"}), 400

    try:
        required_limit = max(settings.history_limit, predictor_service.required_history_rows(horizon) + 48)
        snapshot = market_data_service.get_snapshot(limit=required_limit)
        prediction = predictor_service.predict(horizon, snapshot.candles)
    except PredictorUnavailableError as exc:
        return jsonify({"detail": f"Prediction model is unavailable: {exc}"}), 503
    except (MarketDataError, ValueError) as exc:
        return jsonify({"detail": f"Prediction failed: {exc}"}), 502

    response = prediction.to_dict()
    context = _user_context_from_payload(payload)
    if context is not None:
        if context.chat_id is None and context.telegram_user_id is not None:
            context.chat_id = context.telegram_user_id
        history_item = user_data_service.save_prediction(context, prediction)
        response["history_id"] = history_item.id
    return jsonify(response)


@app.get("/api/history")
def history():
    payload = _request_payload()
    context = _user_context_from_payload(payload)
    if context is None:
        return jsonify({"detail": "user_key is required"}), 400

    try:
        snapshot = market_data_service.get_snapshot(limit=1000)
        user_data_service.sync_due_predictions(snapshot.candles)
    except MarketDataError:
        pass

    items = user_data_service.list_predictions(context.user_key, limit=20)
    return jsonify({"items": [item.to_dict() for item in items]})


@app.get("/api/history/export")
def export_history():
    payload = _request_payload()
    context = _user_context_from_payload(payload)
    if context is None:
        return jsonify({"detail": "user_key is required"}), 400

    csv_content = "\ufeff" + user_data_service.export_predictions_csv(context.user_key)
    return Response(
        csv_content,
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="btc_forecast_history.csv"'},
    )


@app.get("/api/model-quality")
def model_quality():
    payload = _request_payload()
    context = _user_context_from_payload(payload)
    if context is None:
        return jsonify({"detail": "user_key is required"}), 400

    try:
        snapshot = market_data_service.get_snapshot(limit=1000)
        user_data_service.sync_due_predictions(snapshot.candles)
    except MarketDataError:
        pass

    return jsonify(
        {
            "backtest": predictor_service.get_quality_summary(),
            "live": user_data_service.get_live_quality_summary(context.user_key),
        }
    )


@app.route("/api/alerts", methods=["GET", "POST"])
def alerts():
    payload = _request_payload()
    context = _user_context_from_payload(payload)
    if context is None:
        return jsonify({"detail": "user_key is required"}), 400

    if request.method == "GET":
        items = user_data_service.list_alerts(context.user_key, limit=20)
        return jsonify({"items": [item.to_dict() for item in items]})

    kind = str(payload.get("kind", "")).strip()
    threshold_value = payload.get("threshold_value")
    try:
        threshold = float(threshold_value)
    except (TypeError, ValueError):
        return jsonify({"detail": "threshold_value must be numeric"}), 400

    if threshold <= 0:
        return jsonify({"detail": "threshold_value must be positive"}), 400

    if context.telegram_user_id is None and context.chat_id is None:
        return jsonify({"detail": "Telegram user context is required for alerts"}), 400
    if context.chat_id is None and context.telegram_user_id is not None:
        context.chat_id = context.telegram_user_id

    try:
        snapshot = market_data_service.get_snapshot(limit=48)
        item = user_data_service.create_alert(context, kind=kind, threshold_value=threshold, current_price=snapshot.latest.price)
    except (ValueError, MarketDataError) as exc:
        return jsonify({"detail": f"Alert creation failed: {exc}"}), 400

    return jsonify(item.to_dict()), 201


@app.delete("/api/alerts/<int:alert_id>")
def delete_alert(alert_id: int):
    payload = _request_payload()
    context = _user_context_from_payload(payload)
    if context is None:
        return jsonify({"detail": "user_key is required"}), 400

    user_data_service.delete_alert(context.user_key, alert_id)
    return jsonify({"status": "deleted", "id": alert_id})


if __name__ == "__main__":
    from waitress import serve

    serve(app, host=settings.host, port=settings.port, threads=8)
