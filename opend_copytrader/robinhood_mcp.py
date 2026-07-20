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
from typing import Any

from . import __version__ as APP_VERSION
from .models import OrderResult, Quote


MCP_ENDPOINT = "https://agent.robinhood.com/mcp/trading"
REGISTRATION_ENDPOINT = "https://agent.robinhood.com/oauth/trading/register"
AUTHORIZATION_ENDPOINT = "https://robinhood.com/oauth"
TOKEN_ENDPOINT = "https://api.robinhood.com/oauth2/token/"
PROTOCOL_VERSION = "2025-06-18"


class RobinhoodMCPError(RuntimeError):
    pass


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

    def __init__(self, keychain: Any) -> None:
        self.keychain = keychain
        self._redirect_uri = ""
        self._oauth_pending: dict[str, Any] | None = None
        self._lock = threading.RLock()
        self._session_id = ""
        self._rpc_id = 0
        self._tools: dict[str, dict[str, Any]] = {}
        self._tools_at = 0.0

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

    @staticmethod
    def _request_json(
        url: str,
        *,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
        timeout: float = 15.0,
    ) -> tuple[Any, Any]:
        request = urllib.request.Request(url, data=body, headers=headers or {}, method=method)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read(4_000_000)
                return _json_from_text(raw.decode("utf-8", errors="replace")), response.headers
        except urllib.error.HTTPError as exc:
            raw = exc.read(256_000).decode("utf-8", errors="replace")
            raise RobinhoodMCPError(f"Robinhood HTTP {exc.code}: {raw[:500] or exc.reason}") from exc
        except Exception as exc:
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

    def _post(self, payload: dict[str, Any], *, allow_refresh: bool = True) -> dict[str, Any]:
        token = self._access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
            "MCP-Protocol-Version": PROTOCOL_VERSION,
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
            with urllib.request.urlopen(request, timeout=25) as response:
                raw = response.read(4_000_000)
                session = str(response.headers.get("Mcp-Session-Id") or "")
                if session:
                    self._session_id = session
                if not raw:
                    return {}
                result = parse_mcp_http_body(raw, str(response.headers.get("Content-Type") or ""))
        except urllib.error.HTTPError as exc:
            detail = exc.read(256_000).decode("utf-8", errors="replace")
            if exc.code == 401 and allow_refresh and self._get("refresh_token"):
                self._refresh()
                return self._post(payload, allow_refresh=False)
            raise RobinhoodMCPError(f"Robinhood MCP HTTP {exc.code}: {detail[:500] or exc.reason}") from exc
        except Exception as exc:
            if isinstance(exc, RobinhoodMCPError):
                raise
            raise RobinhoodMCPError(f"Robinhood MCP 请求失败：{exc}") from exc
        if result.get("error"):
            error = result["error"]
            message = error.get("message") if isinstance(error, dict) else error
            raise RobinhoodMCPError(f"Robinhood MCP：{message}")
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
            raise RobinhoodMCPError(f"Robinhood {name} 失败：{detail}")
        return mcp_result_data(result)

    def execution_status(self) -> tuple[bool, str]:
        if not self.credential_status()["oauth_connected"]:
            return False, "请先通过 Robinhood 官方页面完成 OAuth 授权"
        try:
            names = self.tools()
        except Exception as exc:
            return False, str(exc)
        required = {"review_equity_order", "place_equity_order"}
        if not required.issubset(names):
            return False, "当前 Robinhood MCP 会话未开放股票预检和下单工具"
        return True, "官方 MCP 已连接 · Agentic 账户可执行股票订单"

    def health(self, _settings: Any) -> dict[str, Any]:
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
            return {
                "connected": True,
                "broker": "robinhood",
                "broker_label": "Robinhood Agentic",
                "endpoint": MCP_ENDPOINT,
                "oauth": status,
                "tool_count": len(tools),
                "execution_tools": {name: name in tools for name in ("review_equity_order", "place_equity_order")},
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

    def accounts(self, _settings: Any) -> list[dict[str, Any]]:
        data = self._call_tool("get_accounts")
        result: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in _walk_dicts(data):
            account = str(_pick(row, "account_number", "account_id", "number", "id") or "").strip()
            if not account or account in seen:
                continue
            kind = str(_pick(row, "account_type", "type", "name", "title") or "").strip()
            agentic_raw = _pick(row, "agentic_allowed", "is_agentic", "agentic")
            agentic = agentic_raw is True or str(agentic_raw).lower() == "true" or "agentic" in kind.lower()
            seen.add(account)
            result.append({
                **row,
                "broker": "robinhood",
                "acc_id": account,
                "security_firm": "ROBINHOOD",
                "trd_env": "REAL",
                "display_name": f"{'Agentic 可执行' if agentic else '只读'} · Robinhood · …{account[-6:]}",
                "selectable": agentic,
                "agentic_allowed": agentic,
            })
        return result

    @staticmethod
    def _matches_account(row: dict[str, Any], selected: str) -> bool:
        account = str(_pick(row, "account_number", "account_id", "account") or "").strip()
        return not selected or not account or account == selected

    def portfolio(self, settings: Any) -> dict[str, Any]:
        portfolio = self._call_tool("get_portfolio")
        positions_data = self._call_tool("get_equity_positions")
        positions: list[dict[str, Any]] = []
        selected = str(getattr(settings, "robinhood_account_id", "") or "")
        for row in _walk_dicts(positions_data):
            symbol = str(_pick(row, "symbol", "ticker") or "").strip().upper()
            qty = _number(_pick(row, "quantity", "qty", "shares"))
            if not symbol or qty is None or not self._matches_account(row, selected):
                continue
            market_price = _pick(row, "price", "market_price", "last_price", "current_price")
            market_value = _pick(row, "market_value", "equity", "value")
            positions.append({
                **row,
                "code": symbol,
                "stock_name": str(_pick(row, "name", "description") or symbol),
                "qty": qty,
                "can_sell_qty": max(qty, 0),
                "average_cost": _pick(row, "average_cost", "average_buy_price", "cost_basis", "avg_cost"),
                "nominal_price": market_price,
                "market_val": market_value,
                "unrealized_pl": _pick(row, "unrealized_pl", "unrealized_gain_loss", "unrealized_pnl"),
            })
        funds = [{
            "total_assets": _deep_number(portfolio, "portfolio_value", "total_value", "equity", "net_liquidation"),
            "cash": _deep_number(portfolio, "cash", "cash_available", "buying_power"),
            "market_val": _deep_number(portfolio, "equity_value", "securities_value", "market_value"),
            "unrealized_pl": _deep_number(portfolio, "unrealized_pl", "unrealized_gain_loss", "unrealized_pnl"),
        }]
        return {"funds": funds, "positions": positions}

    def orders(self, settings: Any) -> list[dict[str, Any]]:
        data = self._call_tool("get_equity_orders")
        selected = str(getattr(settings, "robinhood_account_id", "") or "")
        orders: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in _walk_dicts(data):
            order_id = str(_pick(row, "order_id", "id") or "").strip()
            symbol = str(_pick(row, "symbol", "ticker") or "").strip().upper()
            if not order_id or not symbol or order_id in seen or not self._matches_account(row, selected):
                continue
            seen.add(order_id)
            orders.append({
                **row,
                "order_id": order_id,
                "code": symbol,
                "trd_side": str(_pick(row, "side") or "").upper(),
                "qty": _pick(row, "quantity", "qty", "shares") or 0,
                "price": _pick(row, "limit_price", "price", "average_price") or 0,
                "dealt_qty": _pick(row, "filled_quantity", "executed_quantity") or 0,
                "dealt_avg_price": _pick(row, "average_price", "executed_price") or 0,
                "order_status": str(_pick(row, "status", "state") or ""),
                "create_time": str(_pick(row, "created_at", "submitted_at", "updated_at") or ""),
            })
        return orders

    @staticmethod
    def _stock_symbol(code: str) -> str:
        symbol = str(code or "").strip().upper().removeprefix("US.")
        if not symbol or " " in symbol or len(symbol) > 10 or not symbol.replace(".", "").replace("-", "").isalnum():
            raise RobinhoodMCPError("Robinhood 当前仅启用美股/ETF 跟单；期权工具需以实际 MCP 会话能力为准")
        return symbol

    def quote(self, _settings: Any, code: str) -> Quote:
        symbol = self._stock_symbol(code)
        data = self._call_tool("get_equity_quotes", {"symbols": [symbol]})
        candidates = [row for row in _walk_dicts(data) if str(_pick(row, "symbol", "ticker") or "").upper() == symbol]
        if not candidates:
            raise RobinhoodMCPError(f"Robinhood 没有返回 {symbol} 行情")
        row = candidates[0]
        return Quote(
            code=code,
            last=_number(_pick(row, "last_trade_price", "last_price", "price", "mark_price")),
            bid=_number(_pick(row, "bid_price", "bid")),
            ask=_number(_pick(row, "ask_price", "ask")),
            name=str(_pick(row, "name", "description") or symbol),
            currency="USD",
        )

    def sellable_quantity(self, settings: Any, code: str) -> float:
        symbol = self._stock_symbol(code)
        for row in self.portfolio(settings)["positions"]:
            if str(row.get("code") or "").upper() == symbol:
                return max(_number(row.get("can_sell_qty")) or 0.0, 0.0)
        return 0.0

    def place_order(
        self,
        _settings: Any,
        *,
        code: str,
        side: str,
        quantity: float,
        price: float,
        order_type: str,
        **_: Any,
    ) -> OrderResult:
        symbol = self._stock_symbol(code)
        limit_order = str(order_type or "").upper() != "MARKET"
        arguments: dict[str, Any] = {
            "side": str(side or "").lower(),
            "symbol": symbol,
            "quantity": float(quantity),
            "order_type": "limit" if limit_order else "market",
            "time_in_force": "gfd",
        }
        if limit_order:
            arguments["limit_price"] = float(price)
        preview = self._call_tool("review_equity_order", arguments)
        placed = self._call_tool("place_equity_order", arguments)
        rows = _walk_dicts(placed)
        row = next((item for item in rows if _pick(item, "order_id", "id")), rows[0] if rows else {})
        order_id = str(_pick(row, "order_id", "id") or "").strip()
        if not order_id:
            raise RobinhoodMCPError("Robinhood 已接受请求但未返回订单号；已阻止自动重试以避免重复下单")
        return OrderResult(
            order_id=order_id,
            status=str(_pick(row, "status", "state") or "SUBMITTED").upper(),
            code=code,
            side=str(side).upper(),
            quantity=float(quantity),
            price=float(price),
            raw={"preview": preview, "order": placed},
        )
