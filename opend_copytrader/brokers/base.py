"""券商适配层共享的错误类型、标签与 HTTP 工具。"""

from __future__ import annotations

import json
import math
import ssl
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from ..api_policy import ApiPacer


class BrokerError(RuntimeError):
    pass


BROKER_LABELS = {
    "moomoo": "moomoo OpenD",
    "ibkr": "IBKR（盈透证券）",
    "webull": "Webull",
    "schwab": "Charles Schwab（嘉信）",
    "robinhood": "Robinhood Agentic",
}


def json_request(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    context: ssl.SSLContext | None = None,
    timeout: float = 8.0,
    pacer: ApiPacer | None = None,
    route: str = "",
) -> Any:
    if pacer is not None:
        pacer.acquire(route or urllib.parse.urlparse(url).path)
    request = urllib.request.Request(url, data=body, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
            raw = response.read(4_000_000)
        if pacer is not None:
            pacer.record_success()
    except urllib.error.HTTPError as exc:
        raw = exc.read(256_000)
        if exc.code == 429 and pacer is not None:
            raw_retry = str(exc.headers.get("Retry-After") or "").strip()
            try:
                retry_after = float(raw_retry)
            except (TypeError, ValueError):
                retry_after = None
            cooldown = pacer.record_429(retry_after)
        else:
            cooldown = 0.0
        try:
            detail = json.loads(raw.decode("utf-8"))
        except Exception:
            detail = raw.decode("utf-8", errors="replace")
        suffix = f"；已进入 {cooldown:.0f} 秒本地冷却" if cooldown else ""
        raise BrokerError(f"HTTP {exc.code}: {detail}{suffix}") from exc
    except Exception as exc:
        raise BrokerError(str(exc) or type(exc).__name__) from exc
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise BrokerError("券商 API 返回了无法解析的数据") from exc


def to_number(value: Any) -> float | None:
    try:
        parsed = float(str(value).replace(",", ""))
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def as_items(payload: Any, *keys: str) -> list[dict[str, Any]]:
    value = payload
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    if isinstance(value, dict):
        return [value]
    return []
