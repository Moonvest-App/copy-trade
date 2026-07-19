from __future__ import annotations

import hashlib
import json
import math
import os
import re
import threading
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Callable, Iterable, Iterator

from .broker_adapters import KeychainStore
from .config import SettingsStore
from .engine import CopyEngine
from .instruments import OptionContract
from .store import LocalStore


MOONVEST_ENDPOINT = "https://stream.moonvest.app/v1/active_trades/subscribe"
CURSOR_META_KEY = "moonvest.last_event_id"
ACTIONS = {"opened", "added_to", "partially_closed", "closed", "edited", "expired"}
STATUSES = {"open", "closed", "expired"}
SYMBOL_RE = re.compile(r"^[A-Z0-9][A-Z0-9.-]{0,15}$")


class MoonvestError(RuntimeError):
    pass


class MoonvestProtocolError(MoonvestError):
    pass


@dataclass(frozen=True, slots=True)
class SSEFrame:
    event: str
    event_id: str
    data: str


def iter_sse_frames(lines: Iterable[bytes | str]) -> Iterator[SSEFrame]:
    """Parse SSE without treating comment keepalives as events."""
    event_type = "message"
    event_id = ""
    data_lines: list[str] = []

    def dispatch() -> SSEFrame | None:
        nonlocal event_type, event_id, data_lines
        if not data_lines:
            event_type, event_id, data_lines = "message", "", []
            return None
        frame = SSEFrame(event_type or "message", event_id, "\n".join(data_lines))
        event_type, event_id, data_lines = "message", "", []
        return frame

    for raw in lines:
        if isinstance(raw, bytes):
            try:
                line = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise MoonvestProtocolError("SSE 数据不是 UTF-8") from exc
        else:
            line = str(raw)
        line = line.rstrip("\r\n")
        if not line:
            frame = dispatch()
            if frame is not None:
                yield frame
            continue
        if line.startswith(":"):
            continue
        field, separator, value = line.partition(":")
        if separator and value.startswith(" "):
            value = value[1:]
        if field == "event":
            event_type = value
        elif field == "id" and "\x00" not in value:
            event_id = value
        elif field == "data":
            data_lines.append(value)
    frame = dispatch()
    if frame is not None:
        yield frame


class MoonvestCredentials:
    """Reads the API key from an environment override or the login Keychain."""

    def __init__(self, keychain: KeychainStore | None = None):
        self.keychain = keychain or KeychainStore()

    def api_key(self) -> str | None:
        environment = os.environ.get("MOONVEST_API_KEY", "").strip()
        if environment:
            return environment
        return self.keychain.get("moonvest", "api_key")

    def status(self) -> dict[str, Any]:
        environment = bool(os.environ.get("MOONVEST_API_KEY", "").strip())
        configured = bool(self.api_key())
        return {
            "api_key_configured": configured,
            "credential_source": "environment" if environment else "keychain" if configured else "none",
        }

    def save(self, api_key: str) -> dict[str, Any]:
        value = str(api_key or "").strip()
        if not value or len(value) > 512 or any(char in value for char in "\r\n\x00"):
            raise ValueError("Moonvest API key 格式无效")
        self.keychain.set("moonvest", "api_key", value)
        return self.status()

    def clear(self) -> dict[str, Any]:
        self.keychain.delete("moonvest", "api_key")
        return self.status()


def _number(value: Any, field: str, *, required: bool = False) -> float | None:
    if value in {None, ""}:
        if required:
            raise MoonvestProtocolError(f"Moonvest {field} 缺失")
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise MoonvestProtocolError(f"Moonvest {field} 不是数字") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise MoonvestProtocolError(f"Moonvest {field} 无效")
    return parsed


def normalize_trade_payload(payload: Any, frame_id: str = "") -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise MoonvestProtocolError("Moonvest trade data 必须是 JSON 对象")
    result = dict(payload)
    payload_id = str(result.get("id") or "").strip()
    cursor = str(frame_id or payload_id).strip()
    if not cursor:
        raise MoonvestProtocolError("Moonvest trade 事件缺少 id cursor")
    if payload_id and frame_id and payload_id != frame_id:
        raise MoonvestProtocolError("Moonvest SSE id 与 data.id 不一致")
    result["id"] = cursor

    actor = str(result.get("actor") or "").strip()
    if not actor:
        raise MoonvestProtocolError("Moonvest actor 缺失")
    result["actor"] = actor
    action = str(result.get("action") or "").strip().lower()
    if action not in ACTIONS:
        raise MoonvestProtocolError(f"Moonvest action 不受支持：{action or 'empty'}")
    result["action"] = action
    symbol = str(result.get("symbol") or "").strip().upper().removeprefix("US.")
    if not SYMBOL_RE.fullmatch(symbol):
        raise MoonvestProtocolError("Moonvest symbol 无效")
    result["symbol"] = symbol
    asset_type = str(result.get("asset_type") or "").strip().lower()
    if asset_type not in {"stock", "option"}:
        raise MoonvestProtocolError("Moonvest asset_type 无效")
    result["asset_type"] = asset_type
    side = str(result.get("side") or "").strip().lower()
    if side not in {"buy", "sell"}:
        raise MoonvestProtocolError("Moonvest side 无效")
    result["side"] = side
    kind = str(result.get("kind") or "").strip().lower()
    if kind not in {"single", "vertical"}:
        raise MoonvestProtocolError("Moonvest kind 无效")
    result["kind"] = kind
    status = str(result.get("status") or "").strip().lower()
    if status not in STATUSES:
        raise MoonvestProtocolError("Moonvest status 无效")
    result["status"] = status
    result["qty"] = _number(result.get("qty"), "qty", required=True)
    result["entry_price"] = _number(result.get("entry_price"), "entry_price")
    result["qty_added"] = _number(result.get("qty_added"), "qty_added")
    result["qty_closed"] = _number(result.get("qty_closed"), "qty_closed")
    result["exit_price"] = _number(result.get("exit_price"), "exit_price")
    realized = result.get("realized_pnl")
    if realized in {None, ""}:
        result["realized_pnl"] = None
    else:
        try:
            parsed_realized = float(realized)
        except (TypeError, ValueError) as exc:
            raise MoonvestProtocolError("Moonvest realized_pnl 不是数字") from exc
        if not math.isfinite(parsed_realized):
            raise MoonvestProtocolError("Moonvest realized_pnl 无效")
        result["realized_pnl"] = parsed_realized
    subscriber_only = result.get("subscriber_only", False)
    if not isinstance(subscriber_only, bool):
        raise MoonvestProtocolError("Moonvest subscriber_only 必须是布尔值")
    result["subscriber_only"] = subscriber_only
    result["note"] = None if result.get("note") is None else str(result.get("note"))[:2000]
    changes = result.get("changes") or []
    if not isinstance(changes, list):
        raise MoonvestProtocolError("Moonvest changes 必须是数组")
    result["changes"] = [str(change)[:500] for change in changes]

    legs = result.get("legs") or []
    if not isinstance(legs, list):
        raise MoonvestProtocolError("Moonvest legs 必须是数组")
    if asset_type == "stock" and legs:
        raise MoonvestProtocolError("股票事件不能包含期权 legs")
    if asset_type == "option":
        expected_legs = 1 if kind == "single" else 2
        if len(legs) != expected_legs:
            raise MoonvestProtocolError(f"{kind} 期权事件必须包含 {expected_legs} 个 leg")
        expiry_text = str(result.get("expiry") or "").strip()
        try:
            date.fromisoformat(expiry_text)
        except ValueError as exc:
            raise MoonvestProtocolError("期权 expiry 必须是 YYYY-MM-DD") from exc
        result["expiry"] = expiry_text
    else:
        result["expiry"] = None
    result["legs"] = [_normalize_leg(leg) for leg in legs]

    if action == "added_to" and not result["qty_added"]:
        raise MoonvestProtocolError("added_to 事件缺少 qty_added")
    if action == "partially_closed" and not result["qty_closed"]:
        raise MoonvestProtocolError("partially_closed 事件缺少 qty_closed")
    return result


def _normalize_leg(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise MoonvestProtocolError("Moonvest option leg 必须是对象")
    strike = _number(value.get("strike"), "legs.strike", required=True)
    if not strike:
        raise MoonvestProtocolError("Moonvest option strike 必须大于 0")
    right_raw = str(value.get("right") or "").strip().lower()
    right = {"c": "call", "call": "call", "p": "put", "put": "put"}.get(right_raw)
    side = str(value.get("side") or "").strip().lower()
    if right is None or side not in {"buy", "sell"}:
        raise MoonvestProtocolError("Moonvest option leg right/side 无效")
    return {"strike": strike, "right": right, "side": side}


def moonvest_position_key(payload: dict[str, Any]) -> str:
    identity = {
        "actor": payload["actor"],
        "symbol": payload["symbol"],
        "asset_type": payload["asset_type"],
        "kind": payload["kind"],
        "side": payload["side"],
        "expiry": payload.get("expiry"),
        "legs": payload.get("legs") or [],
    }
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def trade_to_signal(payload: dict[str, Any]) -> dict[str, Any]:
    action = {
        "opened": "OPEN",
        "added_to": "ADD",
        "partially_closed": "TRIM",
        "closed": "CLOSE",
        "edited": "EDIT",
        "expired": "EXPIRE",
    }[payload["action"]]
    position_side = payload["side"].upper()
    execution_side = position_side
    if action in {"TRIM", "CLOSE"}:
        execution_side = "SELL" if position_side == "BUY" else "BUY"
    quantity = {
        "OPEN": payload["qty"],
        "ADD": payload.get("qty_added") or 0,
        "TRIM": payload.get("qty_closed") or 0,
        "CLOSE": 0,
        "EDIT": payload["qty"],
        "EXPIRE": payload["qty"],
    }[action]
    price = (
        payload.get("entry_price")
        if action == "OPEN"
        else None
        if action == "ADD"
        else payload.get("exit_price")
        if action in {"TRIM", "CLOSE"}
        else None
    )

    instrument: dict[str, Any]
    non_executable_reason = ""
    if payload["asset_type"] == "stock":
        code = f"US.{payload['symbol']}"
        instrument = {"kind": "stock", "ticker": payload["symbol"], "multiplier": "1"}
    elif payload["kind"] == "vertical":
        code = f"US.{payload['symbol']}"
        instrument = {
            "kind": "vertical",
            "underlying": payload["symbol"],
            "expiry": payload["expiry"],
            "legs": payload["legs"],
            "multiplier": "100",
        }
        non_executable_reason = "Moonvest vertical 组合事件已完整记录；当前券商执行层仅支持单腿订单"
    else:
        leg = payload["legs"][0]
        strike_millis = int(
            (Decimal(str(leg["strike"])) * Decimal("1000")).to_integral_value(
                rounding=ROUND_HALF_UP
            )
        )
        contract = OptionContract(
            root=payload["symbol"],
            expiry=date.fromisoformat(payload["expiry"]),
            right="C" if leg["right"] == "call" else "P",
            strike_millis=strike_millis,
        )
        code = contract.to_moomoo()
        instrument = {
            "kind": "option",
            "underlying": payload["symbol"],
            "occ": contract.to_occ(),
            "expiry": payload["expiry"],
            "strike": str(leg["strike"]),
            "right": leg["right"],
            "leg_side": leg["side"],
            "multiplier": "100",
        }

    return {
        "external_id": payload["id"],
        "actor": payload["actor"],
        "code": code,
        "side": execution_side,
        "quantity": quantity,
        "action": action,
        "signal_price": price,
        "order_type": "NORMAL" if price else "MARKET",
        "note": payload.get("note") or "",
        "position_side": position_side,
        "moonvest_action": payload["action"],
        "subscriber_only": payload["subscriber_only"],
        "changes": payload["changes"],
        "status": payload["status"],
        "qty_after": payload["qty"],
        "entry_price": payload.get("entry_price"),
        "exit_price": payload.get("exit_price"),
        "realized_pnl": payload.get("realized_pnl"),
        "instrument": instrument,
        "non_executable_reason": non_executable_reason,
        "moonvest_event": payload,
    }


class MoonvestStream:
    """Durable, reconnecting Moonvest Active Trades SSE consumer."""

    def __init__(
        self,
        settings: SettingsStore,
        store: LocalStore,
        engine: CopyEngine,
        credentials: MoonvestCredentials,
        *,
        opener: Callable[..., Any] | None = None,
        resync_handler: Callable[[], dict[str, Any]] | None = None,
    ):
        self.settings = settings
        self.store = store
        self.engine = engine
        self.credentials = credentials
        self._opener = opener or urllib.request.urlopen
        self._resync_handler = resync_handler
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._response: Any = None
        self._status_lock = threading.RLock()
        self._status: dict[str, Any] = {
            "endpoint": MOONVEST_ENDPOINT,
            "connected": False,
            "last_event_at": None,
            "last_error": "",
            "reconnect_count": 0,
            "resync_count": 0,
            "resync_required": False,
        }

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="moonvest-sse", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        response, self._response = self._response, None
        if response is not None:
            try:
                response.close()
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=4)

    def wake(self) -> None:
        self._wake.set()
        response, self._response = self._response, None
        if response is not None:
            try:
                response.close()
            except Exception:
                pass

    def status(self) -> dict[str, Any]:
        with self._status_lock:
            result = dict(self._status)
        settings = self.settings.get()
        result.update(self.credentials.status())
        result["follow"] = list(settings.moonvest_follow)
        result["cursor"] = self.store.get_meta(CURSOR_META_KEY)
        result["cursor_mode"] = settings.moonvest_cursor_mode
        return result

    def _set_status(self, **fields: Any) -> None:
        with self._status_lock:
            self._status.update(fields)

    def _run(self) -> None:
        delay = 1.0
        while not self._stop.is_set():
            settings = self.settings.get()
            api_key = self.credentials.api_key()
            if not settings.moonvest_follow or not api_key:
                missing = "follow 用户" if not settings.moonvest_follow else "API key"
                self._set_status(connected=False, last_error=f"等待配置 Moonvest {missing}")
                self._wait(3)
                continue
            try:
                self._session(api_key)
                delay = 1.0
                if not self._stop.is_set():
                    raise ConnectionError("Moonvest SSE 连接已结束")
            except urllib.error.HTTPError as exc:
                reason = {
                    400: "follow 用户超过 2 个",
                    401: "Moonvest API key 无效或已失效",
                    422: "未提供 follow 用户",
                }.get(exc.code, f"Moonvest HTTP {exc.code}")
                self._lost(reason)
                self._wait(15 if exc.code == 401 else delay)
                delay = min(delay * 2, 30.0)
            except Exception as exc:
                if self._stop.is_set():
                    break
                self._lost(str(exc) or type(exc).__name__)
                self._wait(delay)
                delay = min(delay * 2, 30.0)

    def _wait(self, seconds: float) -> None:
        self._wake.wait(timeout=seconds)
        self._wake.clear()

    def _lost(self, reason: str) -> None:
        was_connected = bool(self.status().get("connected"))
        reconnect_count = int(self.status().get("reconnect_count") or 0) + 1
        self._set_status(connected=False, last_error=reason[:500], reconnect_count=reconnect_count)
        if was_connected and self.engine.state().get("armed"):
            self.engine.disarm()
            self.store.event(
                "moonvest.stream_lost",
                "Moonvest SSE 已断开，订单执行已自动关闭并等待重连",
                level="warning",
                details={"reason": reason[:500]},
            )

    def _session(self, api_key: str) -> None:
        settings = self.settings.get()
        cursor = self.store.get_meta(CURSOR_META_KEY)
        params: list[tuple[str, str]] = [("follow", username) for username in settings.moonvest_follow]
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "text/event-stream",
            "Cache-Control": "no-cache",
            "User-Agent": "Moonvest/1.1.0",
        }
        if cursor:
            if settings.moonvest_cursor_mode == "since":
                params.append(("since", cursor))
            else:
                headers["Last-Event-ID"] = cursor
        url = f"{MOONVEST_ENDPOINT}?{urllib.parse.urlencode(params)}"
        request = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with self._opener(request, timeout=90) as response:
                self._response = response
                self._set_status(connected=True, last_error="")
                for frame in iter_sse_frames(response):
                    if self._stop.is_set():
                        return
                    if frame.event == "resync":
                        self._handle_resync(frame)
                    elif frame.event == "trade":
                        self._handle_trade(frame)
                    else:
                        self.store.event(
                            "moonvest.sse_ignored",
                            f"已忽略未知 SSE event：{frame.event}",
                            level="warning",
                        )
        finally:
            self._response = None

    def _handle_trade(self, frame: SSEFrame) -> None:
        try:
            decoded = json.loads(frame.data)
            payload = normalize_trade_payload(decoded, frame.event_id)
        except Exception as exc:
            cursor = frame.event_id
            self.store.event(
                "moonvest.invalid_event",
                f"Moonvest 事件校验失败：{exc}",
                level="error",
                details={"event_id": cursor},
            )
            if cursor:
                self.store.set_meta(CURSOR_META_KEY, cursor)
            return

        event_id = payload["id"]
        duplicate = self.store.has_signal("moonvest", event_id)
        if not duplicate:
            try:
                self.engine.submit(trade_to_signal(payload), source="moonvest")
            except Exception as exc:
                self.store.event(
                    "moonvest.event_rejected",
                    f"Moonvest 事件无法形成安全的本地记录：{exc}",
                    level="error",
                    details={"event_id": event_id, "actor": payload.get("actor")},
                )
        self.store.upsert_moonvest_position(moonvest_position_key(payload), payload)
        # Advance only after the durable signal/state work is complete.
        self.store.set_meta(CURSOR_META_KEY, event_id)
        self._set_status(
            connected=True,
            last_event_at=self._timestamp(),
            last_error="",
            resync_required=False,
        )
        if duplicate:
            self.store.event(
                "moonvest.duplicate",
                "Moonvest 重投事件已按 id 去重",
                details={"event_id": event_id},
            )

    def _handle_resync(self, frame: SSEFrame) -> None:
        try:
            payload = json.loads(frame.data)
        except ValueError:
            payload = {"control": "resync", "reason": "invalid_payload"}
        stale = self.store.mark_moonvest_positions_stale()
        self.store.set_meta(CURSOR_META_KEY, "")
        if self.engine.state().get("armed"):
            self.engine.disarm()
        snapshot: dict[str, Any] = {}
        if self._resync_handler is not None:
            try:
                snapshot = self._resync_handler()
            except Exception as exc:
                snapshot = {"broker_snapshot_error": str(exc)}
        count = int(self.status().get("resync_count") or 0) + 1
        self._set_status(resync_count=count, resync_required=True)
        self.store.event(
            "moonvest.resync",
            "Moonvest cursor 已过期：已标记来源状态为待同步、重新读取券商持仓并关闭订单执行；SSE 将继续接收实时事件",
            level="warning",
            details={"control": payload, "stale_positions": stale, "broker_snapshot": snapshot},
        )

    @staticmethod
    def _timestamp() -> str:
        from datetime import datetime, timezone

        return datetime.now(timezone.utc).isoformat(timespec="seconds")
