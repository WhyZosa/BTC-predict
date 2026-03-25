from __future__ import annotations

import json
import threading
import time
from typing import Any


class MemoryCache:
    def __init__(self) -> None:
        self._store: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Any | None:
        with self._lock:
            value = self._store.get(key)
            if value is None:
                return None
            expires_at, payload = value
            if expires_at < time.time():
                self._store.pop(key, None)
                return None
            return payload

    def set(self, key: str, payload: Any, ttl_seconds: int) -> None:
        with self._lock:
            self._store[key] = (time.time() + ttl_seconds, payload)


class RedisCache:
    def __init__(self, redis_client: Any) -> None:
        self._redis = redis_client

    def get(self, key: str) -> Any | None:
        raw = self._redis.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    def set(self, key: str, payload: Any, ttl_seconds: int) -> None:
        self._redis.set(key, json.dumps(payload), ex=ttl_seconds)
