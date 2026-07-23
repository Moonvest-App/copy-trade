from __future__ import annotations

import base64
import hashlib
import json
import math
import secrets
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import date, datetime
from typing import Any, Callable

from . import __version__ as APP_VERSION
from .api_policy import ApiPacer, EndpointRule, RateLimitError
from .config import AppSettings
from .instruments import OptionContract, parse_option_contract
from .models import OrderResult, Quote
from .tls import trusted_ssl_context


MCP_ENDPOINT = "https://agent.robinhood.com/mcp/trading"
REGISTRATION_ENDPOINT = "https://agent.robinhood.com/oauth/trading/register"
AUTHORIZATION_ENDPOINT = "https://robinhood.com/oauth"
TOKEN_ENDPOINT = "https://api.robinhood.com/oauth2/token/"
PROTOCOL_VERSION = "2025-06-18"


class RobinhoodMCPError(RuntimeError):
    pass


class RobinhoodOrderRejected(RobinhoodMCPError):
    """A definite pre-submit or broker rejection that is safe to isolate."""


class RobinhoodOrderUncertain(RobinhoodMCPError):
    """The place request may have reached Robinhood; execution must fail closed."""


class RobinhoodRequestNotSent(RobinhoodMCPError):
    """A local/HTTP guard proved the write request was not accepted."""


def _number(value: Any) -> float | None:
    try:
        parsed = float(str(value).replace(",", "").replace("$", ""))
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def _json_from_text(text: str) -> Any:
    candidate = str(text or "").strip()
    if candidate.startswith("```"):
        lines = candidate.splitlines()
        candidate = "\n".join(lines[1:-1]).strip() if len(lines) > 2 else candidate
    try:
        return json.loads(candidate)
    except (TypeError, ValueError):
        start = min((index for index in (candidate.find("{"), candidate.find("[")) if index >= 0), default=-1)
        if start >= 0:
            closing = "}" if candidate[start] == "{" else "]"
            end = candidate.rfind(closing)
            if end > start:
                try:
                    return json.loads(candidate[start : end + 1])
                except (TypeError, ValueError):
                    pass
    return {"text": candidate}


def parse_mcp_http_body(raw: bytes, content_type: str = "application/json") -> dict[str, Any]:
    text = raw.decode("utf-8", errors="replace").strip()
    if "text/event-stream" in content_type:
        messages: list[dict[str, Any]] = []
        data_lines: list[str] = []
        for line in text.splitlines() + [""]:
            if line.startswith("data:"):
                data_lines.append(line[5:].lstrip())
            elif not line and data_lines:
                payload = _json_from_text("\n".join(data_lines))
                if isinstance(payload, dict):
                    messages.append(payload)
                data_lines = []
        if not messages:
            raise RobinhoodMCPError("Robinhood MCP 返回了空的事件流")
        return messages[-1]
    payload = _json_from_text(text)
    if not isinstance(payload, dict):
        raise RobinhoodMCPError("Robinhood MCP 返回格式无效")
    return payload


def mcp_result_data(result: dict[str, Any]) -> Any:
    structured = result.get("structuredContent")
    if structured not in (None, {}):
        return structured
    blocks = result.get("content") or []
    parsed: list[Any] = []
    for block in blocks if isinstance(blocks, list) else []:
        if isinstance(block, dict) and block.get("type") == "text":
            parsed.append(_json_from_text(str(block.get("text") or "")))
    if len(parsed) == 1:
        return parsed[0]
    return parsed or result


def _walk_dicts(value: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(value, dict):
        rows.append(value)
        for child in value.values():
            rows.extend(_walk_dicts(child))
    elif isinstance(value, list):
        for child in value:
            rows.extend(_walk_dicts(child))
    return rows


def _rows(value: Any, *container_names: str) -> list[dict[str, Any]]:
    """Return business rows without mistaking nested metadata for extra rows."""
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if not isinstance(value, dict):
        return []
    wanted = {str(name).casefold().replace("-", "_") for name in container_names}
    for key, child in value.items():
        normalized = str(key).casefold().replace("-", "_")
        if normalized not in wanted:
            continue
        if isinstance(child, list):
            return [item for item in child if isinstance(item, dict)]
        if isinstance(child, dict):
            nested = _rows(child, *container_names)
            return nested or [child]
    return [value]


def _pick(row: dict[str, Any], *names: str) -> Any:
    normalized = {str(key).casefold().replace("-", "_"): value for key, value in row.items()}
    for name in names:
        value = normalized.get(name.casefold())
        if value not in (None, ""):
            return value
    return None


def _deep_number(value: Any, *names: str) -> float | None:
    for row in _walk_dicts(value):
        parsed = _number(_pick(row, *names))
        if parsed is not None:
            return parsed
    return None


class RobinhoodMCPAdapter:
    """Official Robinhood Trading MCP client with OAuth 2.1 + PKCE."""

    keychain_fields = (
        "client_id",
        "redirect_uri",
        "access_token",
        "refresh_token",
        "expires_at",
    )

    def __init__(
        self,
        keychain: Any,
        *,
        diagnostic: Callable[..., None] | None = None,
    ) -> None:
        self.keychain = keychain
        self._diagnostic = diagnostic
        self._redirect_uri = ""
        self._oauth_pending: dict[str, Any] | None = None
        self._lock = threading.RLock()
        self._session_id = ""
        self._rpc_id = 0
        self._tools: dict[str, dict[str, Any]] = {}
        self._tools_at = 0.0
        self._account_cache: tuple[float, list[dict[str, Any]]] | None = None
        self._option_instruments: dict[str, dict[str, Any]] = {}
        # Robinhood publishes no fixed MCP quota. Use a conservative local
        # window and adapt immediately when the service returns HTTP 429.
        self.pacer = ApiPacer(
            "Robinhood MCP",
            max_calls=18,
            period_seconds=60,
            endpoint_rules=(EndpointRule("tools/call", 0.35),),
        )

    def _trace(self, event: str, **fields: Any) -> None:
        if self._diagnostic is None:
            return
        try:
            self._diagnostic(event, **fields)
        except Exception:
            pass

    def set_redirect_uri(self, value: str) -> None:
        redirect = str(value or "").strip()
        parsed = urllib.parse.urlparse(redirect)
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost"}:
            raise ValueError("Robinhood OAuth 回调必须使用本机地址")
        self._redirect_uri = redirect

    def _get(self, name: str) -> str:
        return str(self.keychain.get("robinhood", name) or "")

    def _set(self, name: str, value: Any) -> None:
        self.keychain.set("robinhood", name, str(value))

    def credential_status(self) -> dict[str, bool]:
        return {
            "oauth_connected": bool(self._get("access_token") or self._get("refresh_token")),
            "refresh_token": bool(self._get("refresh_token")),
        }

    def rate_status(self) -> dict[str, object]:
        return self.pacer.status()

    def _request_json(
        self,
        url: str,
        *,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
        timeout: float = 10.0,
    ) -> tuple[Any, Any]:
        route = urllib.parse.urlparse(url).path or "oauth"
        started = time.monotonic()
        self._trace("robinhood.oauth.http.started", route=route)
        try:
            self.pacer.acquire(f"oauth{route}")
        except RateLimitError as exc:
            self._trace("robinhood.oauth.http.failed", route=route, error=str(exc))
            raise RobinhoodRequestNotSent(str(exc)) from exc
        request_headers = {"User-Agent": f"Moonvest/{APP_VERSION}", **(headers or {})}
        request = urllib.request.Request(url, data=body, headers=request_headers, method=method)
        try:
            with urllib.request.urlopen(
                request, timeout=timeout, context=trusted_ssl_context()
            ) as response:
                raw = response.read(4_000_000)
                result = _json_from_text(raw.decode("utf-8", errors="replace"))
                self.pacer.record_success()
                self._trace(
                    "robinhood.oauth.http.completed",
                    route=route,
                    elapsed_ms=round((time.monotonic() - started) * 1000),
                )
                return result, response.headers
        except urllib.error.HTTPError as exc:
            raw = exc.read(256_000).decode("utf-8", errors="replace")
            if exc.code == 429:
                retry = str(exc.headers.get("Retry-After") or "").strip()
                try:
                    retry_after = float(retry)
                except ValueError:
                    retry_after = None
                cooldown = self.pacer.record_429(retry_after)
                self._trace(
                    "robinhood.oauth.http.failed",
                    route=route,
                    status=exc.code,
                    cooldown_seconds=round(cooldown, 1),
                )
                raise RobinhoodRequestNotSent(
                    f"Robinhood 已限频，本机冷却 {cooldown:.0f} 秒"
                ) from exc
            self._trace(
                "robinhood.oauth.http.failed",
                route=route,
                status=exc.code,
                elapsed_ms=round((time.monotonic() - started) * 1000),
                error=raw[:500] or str(exc.reason),
            )
            raise RobinhoodMCPError(f"Robinhood HTTP {exc.code}: {raw[:500] or exc.reason}") from exc
        except Exception as exc:
            self._trace(
                "robinhood.oauth.http.failed",
                route=route,
                elapsed_ms=round((time.monotonic() - started) * 1000),
                error=str(exc),
                error_type=type(exc).__name__,
            )
            raise RobinhoodMCPError(f"Robinhood 连接失败：{exc}") from exc

    def _register_client(self) -> str:
        if not self._redirect_uri:
            raise RobinhoodMCPError("本机 OAuth 回调尚未就绪")
        stored_id = self._get("client_id")
        if stored_id and self._get("redirect_uri") == self._redirect_uri:
            return stored_id
        payload = {
            "client_name": "Moonvest",
            "client_uri": "https://github.com/Moonvest-App",
            "redirect_uris": [self._redirect_uri],
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none",
            "application_type": "native",
        }
        data, _ = self._request_json(
            REGISTRATION_ENDPOINT,
            method="POST",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        )
        client_id = str(data.get("client_id") if isinstance(data, dict) else "").strip()
        if not client_id:
            raise RobinhoodMCPError("Robinhood 没有返回 OAuth client_id")
        self._set("client_id", client_id)
        self._set("redirect_uri", self._redirect_uri)
        return client_id

    def authorization_url(self) -> str:
        self._trace("robinhood.oauth.started")
        client_id = self._register_client()
        verifier = secrets.token_urlsafe(64)
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
        state = secrets.token_urlsafe(32)
        with self._lock:
            self._oauth_pending = {
                "state": state,
                "verifier": verifier,
                "client_id": client_id,
                "created_at": time.time(),
            }
        query = urllib.parse.urlencode({
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": self._redirect_uri,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
            "scope": "internal",
            "resource": MCP_ENDPOINT,
            "state": state,
        })
        return f"{AUTHORIZATION_ENDPOINT}?{query}"

    def complete_oauth(self, code: str, state: str) -> None:
        with self._lock:
            pending = self._oauth_pending
            self._oauth_pending = None
        if not pending or not secrets.compare_digest(str(state), str(pending.get("state") or "")):
            raise RobinhoodMCPError("Robinhood OAuth state 无效或已过期")
        if time.time() - float(pending.get("created_at") or 0) > 600:
            raise RobinhoodMCPError("Robinhood OAuth 授权已超时，请重新连接")
        form = urllib.parse.urlencode({
            "grant_type": "authorization_code",
            "code": str(code or ""),
            "client_id": pending["client_id"],
            "redirect_uri": self._redirect_uri,
            "code_verifier": pending["verifier"],
            "resource": MCP_ENDPOINT,
        }).encode("utf-8")
        data, _ = self._request_json(
            TOKEN_ENDPOINT,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
            body=form,
        )
        self._save_tokens(data)
        self._trace("robinhood.oauth.completed")

    def _save_tokens(self, data: Any) -> None:
        if not isinstance(data, dict) or not str(data.get("access_token") or ""):
            raise RobinhoodMCPError("Robinhood OAuth 没有返回 access token")
        self._set("access_token", data["access_token"])
        if data.get("refresh_token"):
            self._set("refresh_token", data["refresh_token"])
        expires_in = max(float(data.get("expires_in") or 3600), 60)
        self._set("expires_at", time.time() + expires_in)
        self._reset_session()

    def _refresh(self) -> str:
        refresh_token = self._get("refresh_token")
        client_id = self._get("client_id")
        if not refresh_token or not client_id:
            raise RobinhoodMCPError("Robinhood 授权已失效，请重新连接")
        form = urllib.parse.urlencode({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "resource": MCP_ENDPOINT,
        }).encode("utf-8")
        data, _ = self._request_json(
            TOKEN_ENDPOINT,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
            body=form,
        )
        self._save_tokens(data)
        return self._get("access_token")

    def _access_token(self) -> str:
        access = self._get("access_token")
        refresh = self._get("refresh_token")
        if not access and not refresh:
            raise RobinhoodMCPError("请先连接 Robinhood 并在官方页面完成授权")
        expires = _number(self._get("expires_at")) or 0
        if access and expires > time.time() + 60:
            return access
        return self._refresh()

    def disconnect(self) -> None:
        for name in self.keychain_fields:
            self.keychain.delete("robinhood", name)
        with self._lock:
            self._oauth_pending = None
        self._reset_session()

    def _reset_session(self) -> None:
        with self._lock:
            self._session_id = ""
            self._tools = {}
            self._tools_at = 0.0
            self._account_cache = None
            self._option_instruments.clear()

    def _post(self, payload: dict[str, Any], *, allow_refresh: bool = True) -> dict[str, Any]:
        method = str(payload.get("method") or "mcp")
        route = "tools/call" if method == "tools/call" else method
        started = time.monotonic()
        self._trace("robinhood.mcp.request.started", method=method)
        try:
            self.pacer.acquire(route)
        except RateLimitError as exc:
            self._trace("robinhood.mcp.request.failed", method=method, error=str(exc))
            raise RobinhoodRequestNotSent(str(exc)) from exc
        token = self._access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
            "MCP-Protocol-Version": PROTOCOL_VERSION,
            "User-Agent": f"Moonvest/{APP_VERSION}",
        }
        if self._session_id:
            headers["Mcp-Session-Id"] = self._session_id
        request = urllib.request.Request(
            MCP_ENDPOINT,
            data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(
                request, timeout=25, context=trusted_ssl_context()
            ) as response:
                raw = response.read(4_000_000)
                session = str(response.headers.get("Mcp-Session-Id") or "")
                if session:
                    self._session_id = session
                if not raw:
                    self.pacer.record_success()
                    self._trace(
                        "robinhood.mcp.request.completed",
                        method=method,
                        elapsed_ms=round((time.monotonic() - started) * 1000),
                    )
                    return {}
                result = parse_mcp_http_body(raw, str(response.headers.get("Content-Type") or ""))
                self.pacer.record_success()
        except urllib.error.HTTPError as exc:
            detail = exc.read(256_000).decode("utf-8", errors="replace")
            if exc.code == 429:
                retry = str(exc.headers.get("Retry-After") or "").strip()
                try:
                    retry_after = float(retry)
                except ValueError:
                    retry_after = None
                cooldown = self.pacer.record_429(retry_after)
                self._trace(
                    "robinhood.mcp.request.failed",
                    method=method,
                    status=exc.code,
                    cooldown_seconds=round(cooldown, 1),
                )
                raise RobinhoodRequestNotSent(
                    f"Robinhood MCP 已限频，本机冷却 {cooldown:.0f} 秒"
                ) from exc
            if exc.code == 401 and allow_refresh and self._get("refresh_token"):
                self._refresh()
                return self._post(payload, allow_refresh=False)
            self._trace(
                "robinhood.mcp.request.failed",
                method=method,
                status=exc.code,
                elapsed_ms=round((time.monotonic() - started) * 1000),
                error=detail[:500] or str(exc.reason),
            )
            raise RobinhoodMCPError(f"Robinhood MCP HTTP {exc.code}: {detail[:500] or exc.reason}") from exc
        except Exception as exc:
            if isinstance(exc, RobinhoodMCPError):
                raise
            self._trace(
                "robinhood.mcp.request.failed",
                method=method,
                elapsed_ms=round((time.monotonic() - started) * 1000),
                error=str(exc),
                error_type=type(exc).__name__,
            )
            raise RobinhoodMCPError(f"Robinhood MCP 请求失败：{exc}") from exc
        if result.get("error"):
            error = result["error"]
            message = error.get("message") if isinstance(error, dict) else error
            self._trace(
                "robinhood.mcp.request.failed",
                method=method,
                elapsed_ms=round((time.monotonic() - started) * 1000),
                error=str(message)[:500],
            )
            raise RobinhoodMCPError(f"Robinhood MCP：{message}")
        self._trace(
            "robinhood.mcp.request.completed",
            method=method,
            elapsed_ms=round((time.monotonic() - started) * 1000),
        )
        return result

    def _next_id(self) -> int:
        with self._lock:
            self._rpc_id += 1
            return self._rpc_id

    def _ensure_session(self) -> None:
        if self._session_id:
            return
        request_id = self._next_id()
        result = self._post({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "Moonvest", "version": APP_VERSION},
            },
        })
        if result.get("id") != request_id or not isinstance(result.get("result"), dict):
            raise RobinhoodMCPError("Robinhood MCP 初始化失败")
        self._post({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}})

    def _rpc(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        self._ensure_session()
        request_id = self._next_id()
        response = self._post({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
        if response.get("id") != request_id:
            raise RobinhoodMCPError("Robinhood MCP 响应编号不匹配")
        result = response.get("result")
        if not isinstance(result, dict):
            raise RobinhoodMCPError("Robinhood MCP 没有返回结果")
        return result

    def tools(self, *, force: bool = False) -> dict[str, dict[str, Any]]:
        if not force and self._tools and time.monotonic() - self._tools_at < 60:
            return dict(self._tools)
        result = self._rpc("tools/list", {})
        tools = result.get("tools") or []
        self._tools = {
            str(tool.get("name")): tool for tool in tools if isinstance(tool, dict) and tool.get("name")
        }
        self._tools_at = time.monotonic()
        return dict(self._tools)

    def _call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> Any:
        if name not in self.tools():
            raise RobinhoodMCPError(f"Robinhood MCP 当前未开放 {name}")
        result = self._rpc("tools/call", {"name": name, "arguments": arguments or {}})
        if result.get("isError"):
            detail = mcp_result_data(result)
            raise RobinhoodOrderRejected(f"Robinhood {name} 拒绝：{detail}")
        return mcp_result_data(result)

    @staticmethod
    def _normalized_name(value: Any) -> str:
        return str(value).replace("_", "").replace("-", "").casefold()

    @classmethod
    def _direct_value(cls, row: dict[str, Any], name: str) -> Any:
        """Read one level only when a nested identifier would be unsafe."""
        wanted = cls._normalized_name(name)
        for key, value in row.items():
            if cls._normalized_name(key) == wanted and value not in (None, ""):
                return value
        return None

    @classmethod
    def _payload_value(cls, payload: Any, name: str) -> Any:
        wanted = cls._normalized_name(name)
        for row in _walk_dicts(payload):
            for key, value in row.items():
                if cls._normalized_name(key) == wanted and value not in (None, ""):
                    return value
        return None

    @staticmethod
    def _bool_value(value: Any) -> bool | None:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and value in {0, 1}:
            return bool(value)
        normalized = str(value or "").strip().casefold()
        if normalized in {"true", "yes", "1", "enabled", "active", "agentic"}:
            return True
        if normalized in {"false", "no", "0", "disabled", "inactive"}:
            return False
        return None

    @classmethod
    def _account_agentic_marker(cls, row: dict[str, Any]) -> tuple[bool, str]:
        explicit_keys = (
            "agentic_allowed",
            "agenticAllowed",
            "is_agentic_allowed",
            "isAgenticAllowed",
            "agentic",
            "is_agentic",
            "isAgentic",
            "agentic_enabled",
            "agenticEnabled",
            "is_agentic_account",
            "isAgenticAccount",
        )
        for key in explicit_keys:
            parsed = cls._bool_value(cls._payload_value(row, key))
            if parsed is not None:
                return parsed, f"{key}={'true' if parsed else 'false'}"
        for key in (
            "account_type",
            "accountType",
            "account_subtype",
            "accountSubtype",
            "product_type",
            "productType",
            "product_name",
            "productName",
        ):
            if "agentic" in str(cls._payload_value(row, key) or "").casefold():
                return True, f"{key}=agentic"
        for key in ("type", "name"):
            if "agentic" in str(row.get(key) or "").casefold():
                return True, f"{key}=agentic"
        return False, "未返回 Agentic 能力标记"

    @classmethod
    def _review_block_reason(cls, payload: Any) -> str:
        rows = _walk_dicts(payload)
        if not rows:
            return "预审没有返回结构化结果"
        for row in rows:
            for key in (
                "accepted",
                "approved",
                "can_place",
                "canPlace",
                "can_submit",
                "canSubmit",
                "valid",
            ):
                if key in row and cls._bool_value(row.get(key)) is False:
                    return f"预审字段 {key} 未通过"
            for key in ("errors", "blocking_errors", "blockingErrors"):
                value = row.get(key)
                if isinstance(value, list) and value:
                    return str(value[0])[:300]
                if isinstance(value, dict) and value:
                    return str(value)[:300]
            for key in ("severity", "level", "status", "state"):
                value = str(row.get(key) or "").casefold()
                if any(
                    word in value
                    for word in ("reject", "block", "fatal", "error", "denied", "invalid", "failed")
                ):
                    detail = row.get("message") or row.get("detail") or row.get("reason") or value
                    return str(detail)[:300]
        return ""

    def _schema_error(self, tool_name: str, detail: str) -> RobinhoodOrderRejected:
        schema = (self._tools.get(tool_name) or {}).get("inputSchema") or {}
        return RobinhoodOrderRejected(
            f"Robinhood 工具 {tool_name} 的参数结构无法安全执行：{detail}；"
            f"必填参数={list(schema.get('required') or [])}"
        )

    @classmethod
    def _enum_choice(cls, spec: dict[str, Any], *wanted: str) -> Any:
        for candidate in wanted:
            normalized = cls._normalized_name(candidate)
            for value in spec.get("enum") or []:
                if cls._normalized_name(value) == normalized:
                    return value
        return None

    @classmethod
    def _schema_value(cls, spec: dict[str, Any], value: Any) -> Any:
        kind = str(spec.get("type") or "")
        if kind == "string" and not isinstance(value, (dict, list)):
            return str(value)
        if kind == "integer":
            return int(float(value))
        if kind == "number":
            return float(value)
        return value

    def _read_arguments(
        self,
        tool_name: str,
        *,
        account: str = "",
        symbols: list[str] | None = None,
        instrument_ids: list[str] | None = None,
        contract: OptionContract | None = None,
    ) -> dict[str, Any]:
        schema = (self._tools.get(tool_name) or {}).get("inputSchema") or {}
        properties = schema.get("properties") or {}
        required = set(schema.get("required") or [])
        if not isinstance(properties, dict) or not properties:
            fallback: dict[str, Any] = {}
            if account:
                fallback["account_number"] = account
            if symbols is not None:
                fallback["symbols"] = symbols
            if instrument_ids is not None:
                fallback["instrument_ids"] = instrument_ids
            return fallback
        result: dict[str, Any] = {}
        for name, raw_spec in properties.items():
            spec = raw_spec if isinstance(raw_spec, dict) else {}
            normalized = self._normalized_name(name)
            value: Any = None
            if account and "account" in normalized:
                value = account
            elif symbols is not None and "symbol" in normalized:
                value = symbols if spec.get("type") == "array" or normalized.endswith("s") else symbols[0]
            elif instrument_ids is not None and (
                "instrument" in normalized or normalized in {"optionids", "ids"}
            ):
                value = (
                    instrument_ids
                    if spec.get("type") == "array" or normalized.endswith("s")
                    else instrument_ids[0]
                )
            elif contract is not None and normalized in {
                "underlying",
                "underlyingsymbol",
                "chainsymbol",
                "symbol",
            }:
                value = contract.root
            elif contract is not None and "expir" in normalized:
                value = contract.expiry.isoformat()
            elif contract is not None and "strike" in normalized:
                value = float(contract.strike)
            elif contract is not None and normalized in {
                "right",
                "optiontype",
                "putcall",
                "type",
            }:
                wanted = "call" if contract.right == "C" else "put"
                value = self._enum_choice(spec, wanted, contract.right)
                if value is None and not spec.get("enum"):
                    value = wanted
            elif contract is not None and normalized in {"occ", "occsymbol", "optionsymbol"}:
                value = contract.to_occ()
            elif name in required:
                raise self._schema_error(tool_name, f"未知必填参数 {name}")
            if value is not None:
                result[name] = self._schema_value(spec, value)
        return result

    def _equity_arguments(
        self,
        tool_name: str,
        *,
        account: str,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        order_type: str,
        remark: str,
        position_effect: str,
        ref_id: str = "",
        review: Any = None,
    ) -> dict[str, Any]:
        schema = (self._tools.get(tool_name) or {}).get("inputSchema") or {}
        properties = schema.get("properties") or {}
        required = set(schema.get("required") or [])
        if not isinstance(properties, dict) or not properties:
            raise self._schema_error(tool_name, "没有声明 properties")
        desired_type = "limit" if order_type == "NORMAL" else "market"
        result: dict[str, Any] = {}
        covered: set[str] = set()
        for name, raw_spec in properties.items():
            spec = raw_spec if isinstance(raw_spec, dict) else {}
            normalized = self._normalized_name(name)
            value: Any = None
            known = True
            from_review = self._payload_value(review, name) if review is not None else None
            if normalized in {"refid", "idempotencykey", "clientorderid"}:
                value = ref_id or None
                covered.add("ref_id")
            elif "account" in normalized:
                value = account
                covered.add("account")
            elif normalized in {"symbol", "ticker", "equitysymbol", "instrumentsymbol"}:
                value = symbol
                covered.add("symbol")
            elif normalized in {"side", "direction", "action", "transactiontype"}:
                value = self._enum_choice(spec, side.casefold())
                if value is None and not spec.get("enum"):
                    value = side.casefold()
                covered.add("side")
            elif normalized in {"positioneffect", "effect", "openclose"}:
                value = self._enum_choice(spec, position_effect)
                if value is None and not spec.get("enum"):
                    value = position_effect
            elif normalized in {"type", "ordertype"}:
                value = self._enum_choice(spec, desired_type)
                if value is None and not spec.get("enum"):
                    value = desired_type
                covered.add("order_type")
            elif normalized in {"markethours", "markettime", "session", "tradingsession"}:
                value = self._enum_choice(spec, "regular_hours", "regular", "rth")
                if value is None and not spec.get("enum"):
                    value = "regular_hours"
            elif normalized in {"timeinforce", "tif", "duration"}:
                value = self._enum_choice(spec, "gfd", "day", "good_for_day", "good_till_close")
                if value is None and not spec.get("enum"):
                    value = "gfd"
                covered.add("time_in_force")
            elif normalized in {"quantity", "qty", "shares", "sharequantity"}:
                value = quantity
                covered.add("quantity")
            elif "price" in normalized and not any(
                word in normalized for word in ("stop", "trigger", "trail")
            ):
                if order_type == "NORMAL":
                    value = price
                    covered.add("price")
            elif normalized in {"remark", "memo"}:
                value = remark
            elif review is not None and any(
                word in normalized for word in ("review", "preview", "confirmation")
            ):
                value = from_review if from_review is not None else self._payload_value(review, "id")
            else:
                known = False
            if value is not None:
                result[name] = self._schema_value(spec, value)
            elif name in required:
                detail = f"未知必填参数 {name}" if not known else f"无法构造必填参数 {name}"
                raise self._schema_error(tool_name, detail)
        expected = {"account", "symbol", "side", "order_type", "quantity"}
        if order_type == "NORMAL":
            expected.add("price")
        missing = expected - covered
        if missing:
            raise self._schema_error(tool_name, f"缺少订单语义字段 {sorted(missing)}")
        if ref_id and "ref_id" not in covered:
            raise self._schema_error(tool_name, "place schema 没有声明 ref_id/幂等键")
        return result

    def _option_arguments(
        self,
        tool_name: str,
        *,
        account: str,
        contracts: int,
        price: float,
        order_type: str,
        leg: dict[str, Any],
        ref_id: str = "",
        review: Any = None,
    ) -> dict[str, Any]:
        schema = (self._tools.get(tool_name) or {}).get("inputSchema") or {}
        properties = schema.get("properties") or {}
        required = set(schema.get("required") or [])
        if not isinstance(properties, dict) or not properties:
            raise self._schema_error(tool_name, "没有声明 properties")
        desired_type = "limit" if order_type == "NORMAL" else "market"
        result: dict[str, Any] = {}
        covered: set[str] = set()
        for name, raw_spec in properties.items():
            spec = raw_spec if isinstance(raw_spec, dict) else {}
            normalized = self._normalized_name(name)
            value: Any = None
            known = True
            if normalized in {"refid", "idempotencykey", "clientorderid"}:
                value = ref_id or None
                covered.add("ref_id")
            elif "account" in normalized:
                value = account
                covered.add("account")
            elif normalized in {"quantity", "qty", "contracts", "contractquantity"}:
                value = contracts
                covered.add("quantity")
            elif normalized in {"type", "ordertype"}:
                value = self._enum_choice(spec, desired_type)
                if value is None and not spec.get("enum"):
                    value = desired_type
                covered.add("order_type")
            elif normalized in {"timeinforce", "tif", "duration"}:
                value = self._enum_choice(spec, "gfd", "day", "good_for_day", "good_till_close")
                if value is None and not spec.get("enum"):
                    value = "gfd"
            elif normalized in {"legs", "orderlegs", "optionlegs"}:
                value = [leg]
                covered.add("legs")
            elif "price" in normalized and not any(
                word in normalized for word in ("stop", "trigger", "trail")
            ):
                if order_type == "NORMAL":
                    value = price
                    covered.add("price")
            elif review is not None and any(
                word in normalized for word in ("review", "preview", "confirmation")
            ):
                value = self._payload_value(review, name) or self._payload_value(review, "id")
            else:
                known = False
            if value is not None:
                result[name] = self._schema_value(spec, value)
            elif name in required:
                detail = f"未知必填参数 {name}" if not known else f"无法构造必填参数 {name}"
                raise self._schema_error(tool_name, detail)
        expected = {"account", "quantity", "order_type", "legs"}
        if order_type == "NORMAL":
            expected.add("price")
        missing = expected - covered
        if missing:
            raise self._schema_error(tool_name, f"缺少期权订单语义字段 {sorted(missing)}")
        if ref_id and "ref_id" not in covered:
            raise self._schema_error(tool_name, "place schema 没有声明 ref_id/幂等键")
        return result

    def _selected_account(self, settings: AppSettings) -> str:
        account = str(settings.robinhood_account_id or "").strip()
        if not account:
            raise RobinhoodOrderRejected("请先从 Robinhood 账户目录选择执行账户")
        return account

    def _selected_agentic_account(
        self,
        settings: AppSettings,
        *,
        cached_only: bool = False,
    ) -> dict[str, Any]:
        selected = self._selected_account(settings)
        if cached_only:
            with self._lock:
                cached = self._account_cache
                rows = [dict(row) for row in cached[1]] if cached else []
            if not rows:
                raise RobinhoodOrderRejected("尚未缓存 Robinhood 账户能力，请在主窗口刷新账户")
        else:
            rows = self.accounts(settings)
        for row in rows:
            if str(row.get("acc_id") or "").strip().casefold() != selected.casefold():
                continue
            if not bool(row.get("agentic_allowed")):
                raise RobinhoodOrderRejected(
                    "当前选择的是 Robinhood 标准账户；官方仅允许 Agentic 专用账户通过 MCP 下单"
                )
            return row
        raise RobinhoodOrderRejected("Robinhood 当前账户目录中找不到已选账户，请重新刷新")

    def execution_capability(
        self,
        settings: AppSettings,
        *,
        cached_only: bool = False,
    ) -> dict[str, Any]:
        prerequisites: dict[str, Any] = {
            "authorized": self.credential_status()["oauth_connected"],
            "account_selected": bool(str(settings.robinhood_account_id or "").strip()),
            "agentic_verified": False,
            "equity_review": False,
            "equity_place": False,
            "option_lookup": False,
            "option_review": False,
            "option_place": False,
        }
        if not prerequisites["authorized"]:
            return {
                "execution": False,
                "agentic": False,
                "assets": [],
                "prerequisites": prerequisites,
                "status": "请先通过 Robinhood 官方页面完成 OAuth 授权",
            }
        try:
            if cached_only:
                with self._lock:
                    if not self._tools:
                        raise RobinhoodOrderRejected("尚未缓存 Robinhood MCP 工具能力")
            else:
                self.tools()
            selected = self._selected_agentic_account(settings, cached_only=cached_only)
            prerequisites["agentic_verified"] = True
            names = set(self._tools)
            prerequisites.update(
                equity_review="review_equity_order" in names,
                equity_place="place_equity_order" in names,
                option_lookup="get_option_instruments" in names,
                option_review="review_option_order" in names,
                option_place="place_option_order" in names,
            )
            assets: list[str] = []
            if {"review_equity_order", "place_equity_order"} <= names:
                assets.append("equity")
            if {
                "get_option_instruments",
                "review_option_order",
                "place_option_order",
            } <= names:
                assets.append("option")
            if not assets:
                status = "Agentic 账户已验证，但当前官方 MCP 授权未提供成对的 review/place 工具"
                self._trace(
                    "robinhood.execution.capability",
                    execution=False,
                    agentic=True,
                    prerequisites=prerequisites,
                )
                return {
                    "execution": False,
                    "agentic": True,
                    "assets": [],
                    "prerequisites": prerequisites,
                    "status": status,
                }
            labels = {"equity": "股票", "option": "期权单腿"}
            result = {
                "execution": True,
                "agentic": True,
                "assets": assets,
                "account": str(selected.get("acc_id") or ""),
                "prerequisites": prerequisites,
                "status": f"Agentic 账户已验证 · {'、'.join(labels[item] for item in assets)} review→place",
            }
            self._trace("robinhood.execution.capability", **result)
            return result
        except Exception as exc:
            status = str(exc)
            self._trace(
                "robinhood.execution.capability",
                execution=False,
                agentic=False,
                reason=status,
            )
            return {
                "execution": False,
                "agentic": False,
                "assets": [],
                "prerequisites": prerequisites,
                "status": status,
            }

    def execution_status(self, settings: AppSettings | None = None) -> tuple[bool, str]:
        if settings is None:
            if not self.credential_status()["oauth_connected"]:
                return False, "请先通过 Robinhood 官方页面完成 OAuth 授权"
            return False, "需要选择并验证 Robinhood Agentic 账户"
        result = self.execution_capability(settings)
        return bool(result["execution"]), str(result["status"])

    def health(self, settings: AppSettings) -> dict[str, Any]:
        status = self.credential_status()
        if not status["oauth_connected"]:
            return {
                "connected": False,
                "broker": "robinhood",
                "broker_label": "Robinhood Agentic",
                "endpoint": MCP_ENDPOINT,
                "oauth": status,
                "error": "等待通过 Robinhood 官方页面授权",
            }
        try:
            tools = self.tools()
            capability = self.execution_capability(settings)
            return {
                "connected": True,
                "broker": "robinhood",
                "broker_label": "Robinhood Agentic",
                "endpoint": MCP_ENDPOINT,
                "oauth": status,
                "tool_count": len(tools),
                "execution_tools": {
                    name: name in tools
                    for name in (
                        "review_equity_order",
                        "place_equity_order",
                        "get_option_instruments",
                        "review_option_order",
                        "place_option_order",
                    )
                },
                "execution_capability": capability,
            }
        except Exception as exc:
            return {
                "connected": False,
                "broker": "robinhood",
                "broker_label": "Robinhood Agentic",
                "endpoint": MCP_ENDPOINT,
                "oauth": status,
                "error": str(exc),
            }

    def accounts(self, _settings: AppSettings) -> list[dict[str, Any]]:
        with self._lock:
            cached = self._account_cache
            if cached and time.monotonic() - cached[0] < 30:
                return [dict(row) for row in cached[1]]
        data = self._call_tool(
            "get_accounts",
            self._read_arguments("get_accounts"),
        )
        result: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in _rows(data, "accounts", "results", "data"):
            account = ""
            for field in ("account_number", "accountNumber", "account_id", "accountId"):
                account = str(self._payload_value(row, field) or "").strip()
                if account:
                    break
            account = account or str(row.get("number") or row.get("id") or "").strip()
            if not account or account in seen:
                continue
            kind = ""
            for field in ("account_type", "accountType", "product_name", "productName"):
                kind = str(self._payload_value(row, field) or "").strip()
                if kind:
                    break
            kind = kind or str(row.get("type") or row.get("name") or "账户")
            agentic, marker = self._account_agentic_marker(row)
            seen.add(account)
            result.append({
                **row,
                "broker": "robinhood",
                "acc_id": account,
                "security_firm": "ROBINHOOD",
                "trd_env": "REAL",
                "display_name": f"{'Agentic 可执行' if agentic else '标准账户只读'} · Robinhood · {kind} · …{account[-6:]}",
                "selectable": agentic,
                "agentic_allowed": agentic,
                "agentic_marker": marker,
            })
        self._trace(
            "robinhood.accounts.classified",
            account_count=len(result),
            agentic_count=sum(1 for row in result if row.get("agentic_allowed")),
            markers=sorted({str(row.get("agentic_marker") or "") for row in result}),
        )
        with self._lock:
            self._account_cache = (time.monotonic(), [dict(row) for row in result])
        return result

    @staticmethod
    def _matches_account(row: dict[str, Any], selected: str) -> bool:
        account = str(
            RobinhoodMCPAdapter._payload_value(row, "account_number")
            or RobinhoodMCPAdapter._payload_value(row, "account_id")
            or RobinhoodMCPAdapter._payload_value(row, "account")
            or ""
        ).strip()
        return not selected or not account or account == selected

    @classmethod
    def _option_contract_from_payload(cls, row: dict[str, Any]) -> OptionContract | None:
        raw_occ = str(
            cls._payload_value(row, "occ_symbol")
            or cls._payload_value(row, "occ")
            or cls._payload_value(row, "option_symbol")
            or ""
        ).strip().upper()
        if raw_occ:
            parsed = parse_option_contract("", {"occ": raw_occ})
            if parsed is not None:
                return parsed
        root = str(
            cls._payload_value(row, "underlying_symbol")
            or cls._payload_value(row, "chain_symbol")
            or cls._payload_value(row, "underlying")
            or ""
        ).strip().upper()
        expiry_raw = str(
            cls._payload_value(row, "expiration_date")
            or cls._payload_value(row, "expiry")
            or cls._payload_value(row, "expiration")
            or ""
        ).strip()
        right_raw = str(
            cls._payload_value(row, "option_type")
            or cls._payload_value(row, "put_call")
            or cls._payload_value(row, "right")
            or ""
        ).strip().upper()
        strike = _number(cls._payload_value(row, "strike_price") or cls._payload_value(row, "strike"))
        if not root or not expiry_raw or right_raw not in {"C", "P", "CALL", "PUT"} or strike is None:
            return None
        parsed_expiry: date | None = None
        for fmt in ("%Y-%m-%d", "%Y%m%d", "%y%m%d"):
            try:
                parsed_expiry = datetime.strptime(expiry_raw[:10], fmt).date()
                break
            except ValueError:
                continue
        if parsed_expiry is None:
            return None
        return OptionContract(
            root=root,
            expiry=parsed_expiry,
            right="C" if right_raw in {"C", "CALL"} else "P",
            strike_millis=int(round(strike * 1000)),
        )

    @classmethod
    def _position_code(cls, row: dict[str, Any], kind: str) -> str:
        if kind == "option":
            contract = cls._option_contract_from_payload(row)
            if contract is not None:
                return contract.to_moomoo()
            instrument_id = str(
                cls._payload_value(row, "option_id")
                or cls._payload_value(row, "instrument_id")
                or ""
            ).strip()
            return f"US.OPTION-{instrument_id[-8:]}" if instrument_id else "US.OPTION"
        symbol = str(cls._payload_value(row, "symbol") or cls._payload_value(row, "ticker") or "").strip().upper()
        return symbol if symbol.startswith("US.") else f"US.{symbol}" if symbol else ""

    def portfolio(self, settings: AppSettings) -> dict[str, Any]:
        selected = self._selected_account(settings)
        portfolio = self._call_tool(
            "get_portfolio",
            self._read_arguments("get_portfolio", account=selected),
        )
        positions: list[dict[str, Any]] = []
        for tool_name, kind in (
            ("get_equity_positions", "equity"),
            ("get_option_positions", "option"),
        ):
            if tool_name not in self.tools():
                continue
            payload = self._call_tool(
                tool_name,
                self._read_arguments(tool_name, account=selected),
            )
            for row in _rows(payload, "positions", "results", "data"):
                qty = _number(
                    self._payload_value(row, "quantity")
                    or self._payload_value(row, "qty")
                    or self._payload_value(row, "shares")
                    or self._payload_value(row, "contracts")
                )
                code = self._position_code(row, kind)
                if not code or qty is None or not self._matches_account(row, selected):
                    continue
                positions.append({
                    **row,
                    "code": code,
                    "asset_type": kind,
                    "stock_name": str(
                        self._payload_value(row, "name")
                        or self._payload_value(row, "description")
                        or code
                    ),
                    "qty": qty,
                    "can_sell_qty": max(qty, 0),
                    "average_cost": self._payload_value(row, "average_cost")
                    or self._payload_value(row, "average_buy_price")
                    or self._payload_value(row, "cost_basis")
                    or self._payload_value(row, "avg_cost"),
                    "nominal_price": self._payload_value(row, "market_price")
                    or self._payload_value(row, "last_price")
                    or self._payload_value(row, "current_price")
                    or self._payload_value(row, "price"),
                    "market_val": self._payload_value(row, "market_value")
                    or self._payload_value(row, "equity")
                    or self._payload_value(row, "value"),
                    "unrealized_pl": self._payload_value(row, "unrealized_pl")
                    or self._payload_value(row, "unrealized_gain_loss")
                    or self._payload_value(row, "unrealized_pnl"),
                })
        funds = [{
            "total_assets": _deep_number(portfolio, "portfolio_value", "total_value", "equity", "net_liquidation"),
            "cash": _deep_number(portfolio, "cash", "cash_available", "buying_power"),
            "market_val": _deep_number(portfolio, "equity_value", "securities_value", "market_value"),
            "unrealized_pl": _deep_number(portfolio, "unrealized_pl", "unrealized_gain_loss", "unrealized_pnl"),
        }]
        return {"funds": funds, "positions": positions}

    def orders(self, settings: AppSettings) -> list[dict[str, Any]]:
        selected = self._selected_account(settings)
        orders: list[dict[str, Any]] = []
        seen: set[str] = set()
        for tool_name, kind in (
            ("get_equity_orders", "equity"),
            ("get_option_orders", "option"),
        ):
            if tool_name not in self.tools():
                continue
            payload = self._call_tool(
                tool_name,
                self._read_arguments(tool_name, account=selected),
            )
            for row in _rows(payload, "orders", "results", "data"):
                order_id = str(
                    self._payload_value(row, "order_id")
                    or self._payload_value(row, "id")
                    or ""
                ).strip()
                code = self._position_code(row, kind)
                if not order_id or not code or order_id in seen or not self._matches_account(row, selected):
                    continue
                seen.add(order_id)
                orders.append({
                    **row,
                    "order_id": order_id,
                    "code": code,
                    "asset_type": kind,
                    "trd_side": str(self._payload_value(row, "side") or "").upper(),
                    "qty": self._payload_value(row, "quantity")
                    or self._payload_value(row, "qty")
                    or self._payload_value(row, "shares")
                    or self._payload_value(row, "contracts")
                    or 0,
                    "price": self._payload_value(row, "limit_price")
                    or self._payload_value(row, "price")
                    or self._payload_value(row, "average_price")
                    or 0,
                    "dealt_qty": self._payload_value(row, "filled_quantity")
                    or self._payload_value(row, "executed_quantity")
                    or 0,
                    "dealt_avg_price": self._payload_value(row, "average_price")
                    or self._payload_value(row, "executed_price")
                    or 0,
                    "order_status": str(
                        self._payload_value(row, "status")
                        or self._payload_value(row, "state")
                        or ""
                    ),
                    "create_time": str(
                        self._payload_value(row, "created_at")
                        or self._payload_value(row, "submitted_at")
                        or self._payload_value(row, "updated_at")
                        or ""
                    ),
                })
        return orders

    @staticmethod
    def _stock_symbol(code: str) -> str:
        symbol = str(code or "").strip().upper().removeprefix("US.")
        if not symbol or " " in symbol or len(symbol) > 10 or not symbol.replace(".", "").replace("-", "").isalnum():
            raise RobinhoodOrderRejected("Robinhood 股票代码格式无效")
        return symbol

    def _resolve_option_instrument(self, contract: OptionContract) -> dict[str, Any]:
        key = contract.canonical_id
        with self._lock:
            cached = self._option_instruments.get(key)
            if cached:
                return dict(cached)
        if "get_option_instruments" not in self.tools():
            raise RobinhoodOrderRejected(
                "Robinhood MCP 未提供 get_option_instruments，无法安全解析期权合约"
            )
        payload = self._call_tool(
            "get_option_instruments",
            self._read_arguments("get_option_instruments", contract=contract),
        )
        matches: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for row in _rows(payload, "instruments", "options", "results", "data"):
            parsed = self._option_contract_from_payload(row)
            if parsed is None or parsed.canonical_id != contract.canonical_id:
                continue
            instrument_id = str(
                self._payload_value(row, "option_id")
                or self._payload_value(row, "instrument_id")
                or self._payload_value(row, "id")
                or self._payload_value(row, "uuid")
                or ""
            ).strip()
            state = str(
                self._payload_value(row, "state")
                or self._payload_value(row, "status")
                or "active"
            ).casefold()
            if not instrument_id or instrument_id in seen_ids or state in {"inactive", "expired", "disabled"}:
                continue
            seen_ids.add(instrument_id)
            matches.append({**row, "id": instrument_id})
        if len(matches) != 1:
            raise RobinhoodOrderRejected(
                f"Robinhood 期权合约 {contract.to_occ()} 匹配到 {len(matches)} 个可用 instrument，已阻止歧义下单"
            )
        with self._lock:
            self._option_instruments[key] = dict(matches[0])
        return dict(matches[0])

    def quote(self, _settings: AppSettings, code: str) -> Quote:
        contract = parse_option_contract(code)
        if contract is not None:
            instrument = self._resolve_option_instrument(contract)
            instrument_id = str(instrument["id"])
            if "get_option_quotes" not in self.tools():
                raise RobinhoodOrderRejected("Robinhood MCP 未提供 get_option_quotes")
            data = self._call_tool(
                "get_option_quotes",
                self._read_arguments("get_option_quotes", instrument_ids=[instrument_id]),
            )
            candidates = _rows(data, "quotes", "results", "data")
            if not candidates:
                raise RobinhoodOrderRejected(f"Robinhood 没有返回 {contract.to_occ()} 期权行情")
            row = candidates[0]
            return Quote(
                code=code,
                last=_number(
                    self._payload_value(row, "last_trade_price")
                    or self._payload_value(row, "mark_price")
                    or self._payload_value(row, "last")
                    or self._payload_value(row, "price")
                ),
                bid=_number(self._payload_value(row, "bid_price") or self._payload_value(row, "bid")),
                ask=_number(self._payload_value(row, "ask_price") or self._payload_value(row, "ask")),
                name=contract.to_occ(),
                currency="USD",
            )
        symbol = self._stock_symbol(code)
        data = self._call_tool(
            "get_equity_quotes",
            self._read_arguments("get_equity_quotes", symbols=[symbol]),
        )
        candidates = [row for row in _walk_dicts(data) if str(_pick(row, "symbol", "ticker") or "").upper() == symbol]
        if not candidates:
            raise RobinhoodOrderRejected(f"Robinhood 没有返回 {symbol} 行情")
        row = candidates[0]
        return Quote(
            code=code,
            last=_number(_pick(row, "last_trade_price", "last_price", "price", "mark_price")),
            bid=_number(_pick(row, "bid_price", "bid")),
            ask=_number(_pick(row, "ask_price", "ask")),
            name=str(_pick(row, "name", "description") or symbol),
            currency="USD",
        )

    def sellable_quantity(self, settings: AppSettings, code: str) -> float:
        contract = parse_option_contract(code)
        normalized = contract.to_moomoo() if contract else f"US.{self._stock_symbol(code)}"
        for row in self.portfolio(settings)["positions"]:
            held = str(row.get("code") or "").strip().upper()
            if held == normalized:
                return max(_number(row.get("can_sell_qty")) or 0.0, 0.0)
        return 0.0

    def place_order(
        self,
        settings: AppSettings,
        *,
        code: str,
        side: str,
        quantity: float,
        price: float,
        order_type: str,
        remark: str,
        action: str = "OPEN",
        **_: Any,
    ) -> OrderResult:
        capability = self.execution_capability(settings)
        if not capability["execution"]:
            raise RobinhoodOrderRejected(str(capability["status"]))
        normalized_code = str(code or "").strip().upper()
        if not normalized_code.startswith("US."):
            raise RobinhoodOrderRejected("Robinhood Agentic 执行当前只支持 US 市场标的")
        side = str(side or "").upper()
        order_type = str(order_type or "").upper()
        if side not in {"BUY", "SELL"}:
            raise RobinhoodOrderRejected("Robinhood 下单方向只能是 BUY 或 SELL")
        if quantity <= 0 or not math.isfinite(float(quantity)):
            raise RobinhoodOrderRejected("Robinhood 下单数量必须是有限正数")
        if order_type not in {"NORMAL", "MARKET"}:
            raise RobinhoodOrderRejected("Robinhood 仅支持限价或市价订单")
        if order_type == "NORMAL" and (price <= 0 or not math.isfinite(float(price))):
            raise RobinhoodOrderRejected("Robinhood 限价订单必须提供有限正价格")
        if not str(remark or "").strip():
            raise RobinhoodOrderRejected("Robinhood 真实订单必须提供稳定幂等键")
        contract = parse_option_contract(normalized_code)
        position_effect = "close" if str(action or "").upper() in {"TRIM", "CLOSE"} else "open"
        if contract is not None:
            return self._place_option_order(
                settings,
                contract=contract,
                code=normalized_code,
                side=side,
                quantity=quantity,
                price=price,
                order_type=order_type,
                remark=remark,
                position_effect=position_effect,
            )
        for name in ("review_equity_order", "place_equity_order"):
            if name not in self.tools():
                raise RobinhoodOrderRejected(f"Robinhood MCP 未提供 {name}，已停止股票下单")
        symbol = self._stock_symbol(code)
        account = self._selected_account(settings)
        ref_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"moonvest:{remark}"))
        intent = {
            "account": account,
            "symbol": symbol,
            "side": side,
            "quantity": float(quantity),
            "price": float(price),
            "order_type": order_type,
            "remark": remark,
            "position_effect": position_effect,
        }
        review_arguments = self._equity_arguments("review_equity_order", **intent)
        try:
            self._selected_agentic_account(settings)
            review = self._call_tool("review_equity_order", review_arguments)
        except RobinhoodOrderRejected:
            raise
        except RobinhoodMCPError as exc:
            raise RobinhoodOrderRejected(f"Robinhood 订单预审未完成，未发送真实订单：{exc}") from exc
        blocked = self._review_block_reason(review)
        if blocked:
            raise RobinhoodOrderRejected(f"Robinhood 订单预审未通过：{blocked}")
        place_arguments = self._equity_arguments(
            "place_equity_order",
            **intent,
            ref_id=ref_id,
            review=review,
        )
        try:
            self._selected_agentic_account(settings)
            placed = self._call_tool("place_equity_order", place_arguments)
        except RobinhoodRequestNotSent as exc:
            raise RobinhoodOrderRejected(
                f"Robinhood 真实订单请求未发送：{exc}"
            ) from exc
        except RobinhoodOrderRejected:
            raise
        except RobinhoodMCPError as exc:
            raise RobinhoodOrderUncertain(
                f"Robinhood 下单请求结果无法确认，已关闭后续执行以避免重复下单：{exc}"
            ) from exc
        return self._order_result(
            placed,
            review=review,
            code=normalized_code,
            side=side,
            quantity=quantity,
            price=price,
            ref_id=ref_id,
        )

    def _place_option_order(
        self,
        settings: AppSettings,
        *,
        contract: OptionContract,
        code: str,
        side: str,
        quantity: float,
        price: float,
        order_type: str,
        remark: str,
        position_effect: str,
    ) -> OrderResult:
        for name in ("review_option_order", "place_option_order"):
            if name not in self.tools():
                raise RobinhoodOrderRejected(f"Robinhood MCP 未提供 {name}，已停止期权下单")
        contracts = int(quantity)
        if contracts <= 0 or float(contracts) != float(quantity):
            raise RobinhoodOrderRejected("Robinhood 期权数量必须是正整数张数")
        instrument = self._resolve_option_instrument(contract)
        leg = {
            "option_id": str(instrument["id"]),
            "side": side.casefold(),
            "position_effect": position_effect,
            "ratio_quantity": 1,
        }
        account = self._selected_account(settings)
        review_arguments = self._option_arguments(
            "review_option_order",
            account=account,
            contracts=contracts,
            price=price,
            order_type=order_type,
            leg=leg,
        )
        try:
            self._selected_agentic_account(settings)
            review = self._call_tool("review_option_order", review_arguments)
        except RobinhoodOrderRejected:
            raise
        except RobinhoodMCPError as exc:
            raise RobinhoodOrderRejected(f"Robinhood 期权预审未完成，未发送真实订单：{exc}") from exc
        blocked = self._review_block_reason(review)
        if blocked:
            raise RobinhoodOrderRejected(f"Robinhood 期权订单预审未通过：{blocked}")
        ref_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"moonvest:{remark}"))
        place_arguments = self._option_arguments(
            "place_option_order",
            account=account,
            contracts=contracts,
            price=price,
            order_type=order_type,
            leg=leg,
            ref_id=ref_id,
            review=review,
        )
        try:
            self._selected_agentic_account(settings)
            placed = self._call_tool("place_option_order", place_arguments)
        except RobinhoodRequestNotSent as exc:
            raise RobinhoodOrderRejected(
                f"Robinhood 期权真实订单请求未发送：{exc}"
            ) from exc
        except RobinhoodOrderRejected:
            raise
        except RobinhoodMCPError as exc:
            raise RobinhoodOrderUncertain(
                f"Robinhood 期权下单结果无法确认，已关闭后续执行：{exc}"
            ) from exc
        return self._order_result(
            placed,
            review=review,
            code=code,
            side=side,
            quantity=quantity,
            price=price,
            ref_id=ref_id,
        )

    @classmethod
    def _order_result(
        cls,
        placed: Any,
        *,
        review: Any,
        code: str,
        side: str,
        quantity: float,
        price: float,
        ref_id: str,
    ) -> OrderResult:
        rows = _walk_dicts(placed)
        row: dict[str, Any] = {}
        order_id = ""
        for item in rows:
            candidate = str(cls._direct_value(item, "order_id") or "").strip()
            if candidate:
                row, order_id = item, candidate
                break
        if not order_id:
            # A generic `id` is accepted only on the same object as an order
            # state. Never mistake a nested instrument/review UUID for proof
            # that the real order was created.
            for item in rows:
                candidate = str(cls._direct_value(item, "id") or "").strip()
                state = cls._direct_value(item, "status") or cls._direct_value(item, "state")
                if candidate and state not in (None, ""):
                    row, order_id = item, candidate
                    break
        if not order_id:
            raise RobinhoodOrderUncertain(
                "Robinhood 已返回下单响应但未包含订单号；已关闭后续执行并禁止自动重试"
            )
        return OrderResult(
            order_id=order_id,
            status=str(
                cls._payload_value(row, "status")
                or cls._payload_value(row, "state")
                or "SUBMITTED"
            ).upper(),
            code=code,
            side=side,
            quantity=float(quantity),
            price=float(price),
            raw={"preview": review, "order": placed, "ref_id": ref_id},
        )
