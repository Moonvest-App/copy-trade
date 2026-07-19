from __future__ import annotations

import base64
import copy
import hashlib
import hmac
import json
import math
import ssl
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from .config import AppSettings
from .api_policy import ApiPacer, EndpointRule, RateLimitError
from .instruments import broker_expiry_policy, broker_symbol
from .models import Quote
from .moomoo_adapter import MoomooAdapter, OpenDError
from .robinhood_mcp import RobinhoodMCPAdapter


class BrokerError(RuntimeError):
    pass


BROKER_LABELS = {
    "moomoo": "moomoo OpenD",
    "ibkr": "IBKR（盈透证券）",
    "webull": "Webull",
    "schwab": "Charles Schwab（嘉信）",
    "robinhood": "Robinhood Agentic",
}


class KeychainStore:
    """Stores API material in the login Keychain, never in settings.json."""

    service_prefix = "app.moonvest.copytrader"

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._cache: dict[tuple[str, str], str | None] = {}

    @classmethod
    def _service(cls, broker: str) -> str:
        return f"{cls.service_prefix}.{broker}"

    def get(self, broker: str, name: str) -> str | None:
        key = (broker, name)
        with self._lock:
            if key in self._cache:
                return self._cache[key]
        try:
            result = subprocess.run(
                ["/usr/bin/security", "find-generic-password", "-a", name, "-s", self._service(broker), "-w"],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
            value = result.stdout.rstrip("\n") if result.returncode == 0 else None
        except Exception:
            value = None
        with self._lock:
            self._cache[key] = value
        return value

    def set(self, broker: str, name: str, value: str) -> None:
        value = str(value or "").strip()
        if not value:
            return
        try:
            result = subprocess.run(
                [
                    "/usr/bin/security",
                    "add-generic-password",
                    "-U",
                    "-a",
                    name,
                    "-s",
                    self._service(broker),
                    "-w",
                    value,
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=8,
            )
        except Exception as exc:
            raise BrokerError(f"无法写入 macOS 钥匙串：{exc}") from exc
        if result.returncode != 0:
            raise BrokerError(f"无法写入 macOS 钥匙串：{result.stderr.strip() or 'security 命令失败'}")
        with self._lock:
            self._cache[(broker, name)] = value

    def delete(self, broker: str, name: str) -> None:
        try:
            subprocess.run(
                ["/usr/bin/security", "delete-generic-password", "-a", name, "-s", self._service(broker)],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
        finally:
            with self._lock:
                self._cache[(broker, name)] = None


def _json_request(
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


def _number(value: Any) -> float | None:
    try:
        parsed = float(str(value).replace(",", ""))
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def _items(payload: Any, *keys: str) -> list[dict[str, Any]]:
    value = payload
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    if isinstance(value, dict):
        return [value]
    return []


class IBKRClientPortalAdapter:
    def __init__(self) -> None:
        self._conids: dict[str, int] = {}
        self.pacer = ApiPacer(
            "IBKR",
            max_calls=8,
            period_seconds=1,
            endpoint_rules=(
                EndpointRule("/portfolio/accounts", 5.1),
                EndpointRule("/iserver/account/orders", 5.1),
                EndpointRule("/iserver/trades", 5.1),
                EndpointRule("/tickle", 1.05),
            ),
        )

    @staticmethod
    def _base(settings: AppSettings) -> str:
        return f"https://{settings.ibkr_host}:{int(settings.ibkr_port)}/v1/api"

    @staticmethod
    def _context() -> ssl.SSLContext:
        # Client Portal Gateway uses a local self-signed certificate. Config
        # validation guarantees that this context is never used for a remote host.
        return ssl._create_unverified_context()  # noqa: SLF001

    def _request(
        self,
        settings: AppSettings,
        path: str,
        *,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
    ) -> Any:
        body = None if payload is None else json.dumps(payload, separators=(",", ":")).encode("utf-8")
        headers = {"Accept": "application/json", "User-Agent": "Moonvest/1.1.0"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        return _json_request(
            f"{self._base(settings)}{path}",
            method=method,
            headers=headers,
            body=body,
            context=self._context(),
            pacer=self.pacer,
            route=path,
        )

    def rate_status(self) -> dict[str, Any]:
        return self.pacer.status()

    def health(self, settings: AppSettings) -> dict[str, Any]:
        try:
            status = self._request(settings, "/iserver/auth/status")
            authenticated = bool(status.get("authenticated")) if isinstance(status, dict) else False
            return {
                "connected": authenticated,
                "broker": "ibkr",
                "broker_label": BROKER_LABELS["ibkr"],
                "endpoint": f"{settings.ibkr_host}:{settings.ibkr_port}",
                "host": settings.ibkr_host,
                "port": settings.ibkr_port,
                "authenticated": authenticated,
                "competing": bool(status.get("competing")) if isinstance(status, dict) else False,
                "error": "Gateway 已启动，但需要在浏览器完成 IBKR 登录" if not authenticated else "",
            }
        except Exception as exc:
            return {
                "connected": False,
                "broker": "ibkr",
                "broker_label": BROKER_LABELS["ibkr"],
                "endpoint": f"{settings.ibkr_host}:{settings.ibkr_port}",
                "host": settings.ibkr_host,
                "port": settings.ibkr_port,
                "error": str(exc),
            }

    def accounts(self, settings: AppSettings) -> list[dict[str, Any]]:
        rows = _items(self._request(settings, "/portfolio/accounts"))
        result = []
        for row in rows:
            account = str(row.get("accountId") or row.get("id") or row.get("accountVan") or "").strip()
            if not account:
                continue
            paper = account.upper().startswith("DU")
            result.append(
                {
                    **row,
                    "broker": "ibkr",
                    "acc_id": account,
                    "security_firm": "IBKR",
                    "trd_env": "SIMULATE" if paper else "REAL",
                    "display_name": f"{'模拟' if paper else '实盘'} · IBKR · {account}",
                    "selectable": True,
                }
            )
        return result

    def portfolio(self, settings: AppSettings) -> dict[str, Any]:
        account = settings.ibkr_account_id
        if not account:
            return {"funds": [], "positions": []}
        encoded = urllib.parse.quote(account, safe="")
        summary = self._request(settings, f"/portfolio/{encoded}/summary")
        positions = _items(self._request(settings, f"/portfolio/{encoded}/positions/0"))
        summary = summary if isinstance(summary, dict) else {}

        def summary_value(*names: str) -> Any:
            for name in names:
                value = summary.get(name)
                if isinstance(value, dict):
                    value = value.get("amount") or value.get("value")
                if value not in {None, ""}:
                    return value
            return None

        funds = [{
            "total_assets": summary_value("netliquidation", "equitywithloanvalue"),
            "cash": summary_value("totalcashvalue", "settledcash"),
            "market_val": summary_value("grosspositionvalue"),
            "unrealized_pl": summary_value("unrealizedpnl"),
        }]
        normalized = []
        for row in positions:
            qty = _number(row.get("position")) or 0.0
            normalized.append({
                **row,
                "code": str(row.get("ticker") or row.get("contractDesc") or row.get("conid") or ""),
                "stock_name": str(row.get("name") or row.get("contractDesc") or ""),
                "qty": qty,
                "can_sell_qty": max(qty, 0),
                "average_cost": row.get("avgCost"),
                "nominal_price": row.get("mktPrice"),
                "market_val": row.get("mktValue"),
                "unrealized_pl": row.get("unrealizedPnl"),
            })
        return {"funds": funds, "positions": normalized}

    def account_equity(self, settings: AppSettings) -> float:
        funds = self.portfolio(settings)["funds"]
        value = _number(funds[0].get("total_assets")) if funds else None
        if value is None or value <= 0:
            raise BrokerError("IBKR 没有返回可用于仓位计算的净值")
        return value

    def orders(self, settings: AppSettings) -> list[dict[str, Any]]:
        payload = self._request(settings, "/iserver/account/orders")
        rows = _items(payload, "orders")
        result = []
        for row in rows:
            result.append({
                **row,
                "order_id": str(row.get("orderId") or row.get("order_id") or ""),
                "code": str(row.get("ticker") or row.get("contractDesc") or row.get("conid") or ""),
                "trd_side": str(row.get("side") or "").upper(),
                "qty": row.get("totalSize") or row.get("remainingQuantity") or 0,
                "price": row.get("price") or row.get("avgPrice") or 0,
                "dealt_qty": row.get("filledQuantity") or row.get("filled") or 0,
                "dealt_avg_price": row.get("avgPrice") or 0,
                "order_status": str(row.get("status") or ""),
                "create_time": str(row.get("lastExecutionTime") or row.get("lastExecutionTime_r") or ""),
            })
        return result

    @staticmethod
    def _symbol(code: str) -> str:
        normalized = str(code or "").strip().upper()
        if normalized.startswith("US."):
            normalized = normalized[3:]
        if not normalized or not normalized.replace(".", "").replace("-", "").isalnum():
            raise BrokerError("IBKR 当前只支持解析美股/ETF ticker；期权合约需后续接入 secdef strike 流程")
        return normalized

    def _conid(self, settings: AppSettings, code: str) -> int:
        symbol = self._symbol(code)
        if symbol in self._conids:
            return self._conids[symbol]
        rows = _items(self._request(settings, "/iserver/secdef/search", method="POST", payload={"symbol": symbol}))
        choices = [row for row in rows if str(row.get("symbol") or "").upper() == symbol]
        stocks = [row for row in choices if str(row.get("secType") or row.get("sectype") or "").upper() in {"STK", "ETF"}]
        row = (stocks or choices or rows)[0] if (stocks or choices or rows) else {}
        try:
            conid = int(row.get("conid"))
        except (TypeError, ValueError) as exc:
            raise BrokerError(f"IBKR 无法解析 {symbol} 的合约编号") from exc
        self._conids[symbol] = conid
        return conid

    def quote(self, settings: AppSettings, code: str) -> Quote:
        conid = self._conid(settings, code)
        query = urllib.parse.urlencode({"conids": str(conid), "fields": "31,55,84,86"})
        rows = _items(self._request(settings, f"/iserver/marketdata/snapshot?{query}"))
        if not rows:
            raise BrokerError("IBKR 没有返回行情快照")
        row = rows[0]
        return Quote(
            code=code,
            last=_number(row.get("31")),
            bid=_number(row.get("84")),
            ask=_number(row.get("86")),
            name=str(row.get("55") or self._symbol(code)),
            currency="USD",
        )

    def sellable_quantity(self, settings: AppSettings, code: str) -> float:
        symbol = self._symbol(code)
        for row in self.portfolio(settings)["positions"]:
            row_symbol = str(row.get("code") or "").upper().split()[0]
            if row_symbol == symbol:
                return max(_number(row.get("can_sell_qty")) or 0.0, 0.0)
        return 0.0

    def place_order(self, settings: AppSettings, **_: Any) -> Any:
        raise BrokerError("IBKR 当前为只读适配器；尚未启用订单警告回复确认链路")


def webull_signature(
    *,
    path: str,
    query: dict[str, str],
    body: str | None,
    app_key: str,
    app_secret: str,
    host: str,
    timestamp: str,
    nonce: str,
) -> str:
    values = dict(query)
    values.update({
        "host": host,
        "x-app-key": app_key,
        "x-signature-algorithm": "HMAC-SHA1",
        "x-signature-nonce": nonce,
        "x-signature-version": "1.0",
        "x-timestamp": timestamp,
    })
    joined = "&".join(f"{key}={values[key]}" for key in sorted(values))
    source = f"{path}&{joined}"
    if body:
        source += "&" + hashlib.md5(body.encode("utf-8")).hexdigest().upper()  # noqa: S324 - mandated by Webull signing spec
    encoded = urllib.parse.quote(source, safe="")
    digest = hmac.new(f"{app_secret}&".encode(), encoded.encode(), hashlib.sha1).digest()  # noqa: S324 - mandated by Webull signing spec
    return base64.b64encode(digest).decode("ascii")


class WebullAdapter:
    def __init__(self, keychain: KeychainStore) -> None:
        self.keychain = keychain
        self.pacer = ApiPacer(
            "Webull",
            max_calls=240,
            period_seconds=60,
            endpoint_rules=(
                EndpointRule("/openapi/account/list", 3.1),
                EndpointRule("/openapi/assets/balance", 1.05),
                EndpointRule("/openapi/assets/positions", 1.05),
                EndpointRule("/openapi/trade/order", 1.05),
            ),
        )

    @staticmethod
    def _base(settings: AppSettings) -> str:
        return "https://api.sandbox.webull.com" if settings.webull_environment == "sandbox" else "https://api.webull.com"

    def _request(self, settings: AppSettings, path: str, query: dict[str, str] | None = None) -> Any:
        app_key = settings.webull_app_key
        secret = self.keychain.get("webull", "app_secret")
        token = self.keychain.get("webull", "access_token")
        if not app_key or not secret:
            raise BrokerError("请先填写 Webull App Key，并把 App Secret 保存到 macOS 钥匙串")
        query = query or {}
        base = self._base(settings)
        parsed = urllib.parse.urlparse(base)
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        nonce = uuid.uuid4().hex
        signature = webull_signature(
            path=path,
            query=query,
            body=None,
            app_key=app_key,
            app_secret=secret,
            host=parsed.netloc,
            timestamp=timestamp,
            nonce=nonce,
        )
        headers = {
            "Accept": "application/json",
            "x-app-key": app_key,
            "x-timestamp": timestamp,
            "x-signature": signature,
            "x-signature-algorithm": "HMAC-SHA1",
            "x-signature-version": "1.0",
            "x-signature-nonce": nonce,
            "x-version": "v2",
        }
        if token:
            headers["x-access-token"] = token
        url = f"{base}{path}"
        if query:
            url += "?" + urllib.parse.urlencode(query)
        return _json_request(url, headers=headers, pacer=self.pacer, route=path)

    def rate_status(self) -> dict[str, Any]:
        return self.pacer.status()

    def health(self, settings: AppSettings) -> dict[str, Any]:
        endpoint = urllib.parse.urlparse(self._base(settings)).netloc + ":443"
        configured = bool(settings.webull_app_key and self.keychain.get("webull", "app_secret"))
        return {
            "connected": configured,
            "configured": configured,
            "broker": "webull",
            "broker_label": BROKER_LABELS["webull"],
            "endpoint": endpoint,
            "host": urllib.parse.urlparse(self._base(settings)).netloc,
            "port": 443,
            "verification": "账户发现时按需验证，不用账户列表充当心跳",
            "error": "请先保存 Webull App Key / App Secret" if not configured else "",
        }

    def accounts(self, settings: AppSettings) -> list[dict[str, Any]]:
        payload = self._request(settings, "/openapi/account/list")
        rows = _items(payload, "data") or _items(payload)
        result = []
        for row in rows:
            account = str(row.get("account_id") or row.get("accountId") or "").strip()
            if not account:
                continue
            paper = settings.webull_environment == "sandbox"
            result.append({
                **row,
                "broker": "webull",
                "acc_id": account,
                "security_firm": "WEBULL",
                "trd_env": "SIMULATE" if paper else "REAL",
                "display_name": f"{'沙盒' if paper else '实盘'} · Webull · {account}",
                "selectable": True,
            })
        return result

    def portfolio(self, settings: AppSettings) -> dict[str, Any]:
        account = settings.webull_account_id
        if not account:
            return {"funds": [], "positions": []}
        query = {"account_id": account}
        balance = self._request(settings, "/openapi/assets/balance", query)
        position_payload = self._request(settings, "/openapi/assets/positions", query)
        positions = _items(position_payload, "data") or _items(position_payload)
        balance_row = (_items(balance, "data") or _items(balance) or [{}])[0]
        funds = [{
            **balance_row,
            "total_assets": balance_row.get("net_liquidation") or balance_row.get("total_asset") or balance_row.get("total_assets"),
            "cash": balance_row.get("cash_balance") or balance_row.get("cash"),
            "market_val": balance_row.get("market_value") or balance_row.get("market_val"),
            "unrealized_pl": balance_row.get("unrealized_profit_loss") or balance_row.get("unrealized_pl"),
        }]
        normalized = []
        for row in positions:
            qty = row.get("quantity") or row.get("qty") or 0
            normalized.append({
                **row,
                "code": str(row.get("symbol") or row.get("ticker") or row.get("instrument_id") or ""),
                "stock_name": str(row.get("name") or row.get("symbol") or ""),
                "qty": qty,
                "can_sell_qty": row.get("available_quantity") or row.get("can_sell_qty") or qty,
                "average_cost": row.get("cost_price") or row.get("average_cost"),
                "nominal_price": row.get("last_price") or row.get("market_price"),
                "market_val": row.get("market_value") or row.get("market_val"),
                "unrealized_pl": row.get("unrealized_profit_loss") or row.get("unrealized_pl"),
            })
        return {"funds": funds, "positions": normalized}

    def account_equity(self, settings: AppSettings) -> float:
        funds = self.portfolio(settings)["funds"]
        value = _number(funds[0].get("total_assets")) if funds else None
        if value is None or value <= 0:
            raise BrokerError("Webull 没有返回可用于仓位计算的账户资产")
        return value

    def orders(self, settings: AppSettings) -> list[dict[str, Any]]:
        if not settings.webull_account_id:
            return []
        payload = self._request(settings, "/openapi/trade/order/open", {"account_id": settings.webull_account_id})
        rows = _items(payload, "data") or _items(payload)
        result = []
        for row in rows:
            result.append({
                **row,
                "order_id": str(row.get("order_id") or row.get("client_order_id") or ""),
                "code": str(row.get("symbol") or row.get("ticker") or row.get("instrument_id") or ""),
                "trd_side": str(row.get("side") or "").upper(),
                "qty": row.get("quantity") or row.get("qty") or 0,
                "price": row.get("limit_price") or row.get("price") or 0,
                "dealt_qty": row.get("filled_quantity") or 0,
                "dealt_avg_price": row.get("average_filled_price") or 0,
                "order_status": str(row.get("status") or ""),
                "create_time": str(row.get("created_at") or row.get("create_time") or ""),
            })
        return result

    def quote(self, settings: AppSettings, code: str) -> Quote:
        raise BrokerError("Webull 实时行情使用独立的 MQTT 行情授权；当前版本尚未把该订阅假装成 REST 快照")

    def sellable_quantity(self, settings: AppSettings, code: str) -> float:
        symbol = code.removeprefix("US.").upper()
        for row in self.portfolio(settings)["positions"]:
            if str(row.get("code") or "").upper() == symbol:
                return max(_number(row.get("can_sell_qty")) or 0.0, 0.0)
        return 0.0

    def place_order(self, settings: AppSettings, **_: Any) -> Any:
        raise BrokerError("Webull 当前为只读适配器；需完成 MQTT 行情与订单预览联调后才会开放执行")


class SchwabAdapter:
    base_url = "https://api.schwabapi.com"

    def __init__(self, keychain: KeychainStore) -> None:
        self.keychain = keychain
        # Schwab's logged-in portal is the source of truth for an app's quota.
        # Until it publishes a stable public number, stay deliberately below a
        # normal interactive workload and honor every 429/Retry-After.
        self.pacer = ApiPacer(
            "Charles Schwab",
            max_calls=60,
            period_seconds=60,
            endpoint_rules=(
                EndpointRule("/trader/v1/accounts/accountNumbers", 2.0),
                EndpointRule("/trader/v1/accounts/", 1.0),
                EndpointRule("/marketdata/v1/quotes", 0.25),
            ),
        )

    def _request(self, path: str, query: dict[str, str] | None = None) -> Any:
        token = self.keychain.get("schwab", "access_token")
        if not token:
            raise BrokerError("请先通过嘉信官方 OAuth 取得访问令牌，并保存到 macOS 钥匙串")
        url = f"{self.base_url}{path}"
        if query:
            url += "?" + urllib.parse.urlencode(query)
        return _json_request(
            url,
            headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
            pacer=self.pacer,
            route=path,
        )

    def rate_status(self) -> dict[str, Any]:
        return self.pacer.status()

    def health(self, settings: AppSettings) -> dict[str, Any]:
        configured = bool(self.keychain.get("schwab", "access_token"))
        return {
            "connected": configured,
            "configured": configured,
            "broker": "schwab",
            "broker_label": BROKER_LABELS["schwab"],
            "endpoint": "api.schwabapi.com:443",
            "host": "api.schwabapi.com",
            "port": 443,
            "verification": "账户发现时按需验证，不用账户列表充当心跳",
            "error": "请先通过嘉信官方 OAuth 保存 Access Token" if not configured else "",
        }

    def accounts(self, settings: AppSettings) -> list[dict[str, Any]]:
        rows = _items(self._request("/trader/v1/accounts/accountNumbers"))
        result = []
        for row in rows:
            account_hash = str(row.get("hashValue") or "").strip()
            masked = str(row.get("accountNumber") or "").strip()
            if not account_hash:
                continue
            result.append({
                **row,
                "broker": "schwab",
                "acc_id": account_hash,
                "security_firm": "SCHWAB",
                "trd_env": "REAL",
                "display_name": f"实盘 · 嘉信 · {masked or account_hash[:8]}",
                "selectable": True,
            })
        return result

    def portfolio(self, settings: AppSettings) -> dict[str, Any]:
        account = settings.schwab_account_hash
        if not account:
            return {"funds": [], "positions": []}
        payload = self._request(f"/trader/v1/accounts/{urllib.parse.quote(account, safe='')}", {"fields": "positions"})
        securities = payload.get("securitiesAccount", payload) if isinstance(payload, dict) else {}
        balances = securities.get("currentBalances") or {}
        funds = [{
            **balances,
            "total_assets": balances.get("liquidationValue") or balances.get("equity"),
            "cash": balances.get("cashBalance") or balances.get("availableFunds"),
            "market_val": balances.get("longMarketValue"),
            "unrealized_pl": balances.get("longNonMarginableMarketValue"),
        }]
        normalized = []
        for row in securities.get("positions") or []:
            if not isinstance(row, dict):
                continue
            instrument = row.get("instrument") or {}
            qty = (_number(row.get("longQuantity")) or 0) - (_number(row.get("shortQuantity")) or 0)
            normalized.append({
                **row,
                "code": str(instrument.get("symbol") or instrument.get("cusip") or ""),
                "stock_name": str(instrument.get("description") or instrument.get("symbol") or ""),
                "qty": qty,
                "can_sell_qty": max(qty, 0),
                "average_cost": row.get("averagePrice"),
                "nominal_price": None,
                "market_val": row.get("marketValue"),
                "unrealized_pl": None,
            })
        return {"funds": funds, "positions": normalized}

    def account_equity(self, settings: AppSettings) -> float:
        funds = self.portfolio(settings)["funds"]
        value = _number(funds[0].get("total_assets")) if funds else None
        if value is None or value <= 0:
            raise BrokerError("嘉信没有返回可用于仓位计算的账户净值")
        return value

    def orders(self, settings: AppSettings) -> list[dict[str, Any]]:
        if not settings.schwab_account_hash:
            return []
        today = datetime.now(timezone.utc)
        query = {
            "fromEnteredTime": (today - timedelta(days=1)).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "toEnteredTime": today.isoformat(timespec="seconds").replace("+00:00", "Z"),
        }
        rows = _items(
            self._request(f"/trader/v1/accounts/{urllib.parse.quote(settings.schwab_account_hash, safe='')}/orders", query)
        )
        result = []
        for row in rows:
            legs = row.get("orderLegCollection") or []
            leg = legs[0] if legs and isinstance(legs[0], dict) else {}
            instrument = leg.get("instrument") or {}
            instruction = str(leg.get("instruction") or "").upper()
            result.append({
                **row,
                "order_id": str(row.get("orderId") or ""),
                "code": str(instrument.get("symbol") or ""),
                "trd_side": "SELL" if "SELL" in instruction else "BUY",
                "qty": row.get("quantity") or 0,
                "price": row.get("price") or 0,
                "dealt_qty": row.get("filledQuantity") or 0,
                "dealt_avg_price": 0,
                "order_status": str(row.get("status") or ""),
                "create_time": str(row.get("enteredTime") or ""),
            })
        return result

    def quote(self, settings: AppSettings, code: str) -> Quote:
        symbol = code.removeprefix("US.").upper()
        payload = self._request("/marketdata/v1/quotes", {"symbols": symbol, "fields": "quote,reference"})
        row = payload.get(symbol) if isinstance(payload, dict) else None
        if not isinstance(row, dict):
            raise BrokerError(f"嘉信没有返回 {symbol} 的行情")
        quote = row.get("quote") or row
        reference = row.get("reference") or {}
        return Quote(
            code=code,
            last=_number(quote.get("lastPrice")),
            bid=_number(quote.get("bidPrice")),
            ask=_number(quote.get("askPrice")),
            name=str(reference.get("description") or symbol),
            currency="USD",
        )

    def sellable_quantity(self, settings: AppSettings, code: str) -> float:
        symbol = code.removeprefix("US.").upper()
        for row in self.portfolio(settings)["positions"]:
            if str(row.get("code") or "").upper() == symbol:
                return max(_number(row.get("can_sell_qty")) or 0.0, 0.0)
        return 0.0

    def place_order(self, settings: AppSettings, **_: Any) -> Any:
        raise BrokerError("嘉信当前为只读适配器；需完成 OAuth 自动刷新与订单预览联调后才会开放执行")


class BrokerRouter:
    """Selects the configured broker while exposing one engine-facing API."""

    credential_fields = {
        "webull": ("app_secret", "access_token"),
        "schwab": ("client_secret", "access_token", "refresh_token"),
    }

    def __init__(self) -> None:
        self.keychain = KeychainStore()
        self.moomoo = MoomooAdapter()
        self.ibkr = IBKRClientPortalAdapter()
        self.webull = WebullAdapter(self.keychain)
        self.schwab = SchwabAdapter(self.keychain)
        self.robinhood = RobinhoodMCPAdapter(self.keychain)
        self._health_lock = threading.RLock()
        self._health_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._data_lock = threading.RLock()
        self._data_cache: dict[tuple[str, str, str], tuple[float, Any]] = {}

    def _adapter(self, settings: AppSettings) -> Any:
        return getattr(self, settings.broker)

    def capabilities(self, settings: AppSettings) -> dict[str, Any]:
        if settings.broker == "moomoo":
            return {
                "accounts": True,
                "portfolio": True,
                "quotes": True,
                "orders": True,
                "execution": True,
                "leader_mirror": True,
                "streaming": True,
                "transport": "OpenD 行情订阅推送 + 本地交易缓存；查询仅作断线补偿",
                "status": "完整接入 · 推送优先",
            }
        if settings.broker == "robinhood":
            execution, status = self.robinhood.execution_status()
            return {
                "accounts": True,
                "portfolio": True,
                "quotes": True,
                "orders": True,
                "execution": execution,
                "leader_mirror": False,
                "streaming": False,
                "transport": "Robinhood 官方 Trading MCP · OAuth 2.1 / Streamable HTTP",
                "status": status,
            }
        labels = {
            "ibkr": "已接入 Gateway 账户、持仓、订单与股票快照；订单确认回复链路尚未开放",
            "webull": "已接入签名认证、账户、余额、持仓和未结订单；MQTT 行情与订单预览尚未开放",
            "schwab": "已接入 OAuth 令牌、账户、持仓、订单和股票快照；自动刷新与订单预览尚未开放",
        }
        return {
            "accounts": True,
            "portfolio": True,
            "quotes": settings.broker in {"ibkr", "schwab"},
            "orders": True,
            "execution": False,
            "leader_mirror": False,
            "streaming": settings.broker in {"ibkr", "webull", "schwab"},
            "transport": {
                "ibkr": "官方 WebSocket 可推送行情、订单、成交与账户；当前只读适配器按需 REST，不做后台轮询",
                "webull": "官方 MQTT 推送行情、gRPC 推送订单事件；当前只读适配器按需 REST，不做后台轮询",
                "schwab": "官方 Streamer WebSocket 可推送行情；当前只读适配器按需 REST，不做后台轮询",
            }[settings.broker],
            "status": labels[settings.broker],
        }

    def health(self, settings: AppSettings) -> dict[str, Any]:
        key = f"{settings.broker}:{settings.opend_host}:{settings.opend_port}:{settings.ibkr_host}:{settings.ibkr_port}:{settings.webull_environment}:{settings.webull_app_key}:{settings.schwab_client_id}:{settings.robinhood_account_id}"
        with self._health_lock:
            cached = self._health_cache.get(key)
            if cached and time.monotonic() - cached[0] < 45:
                result = dict(cached[1])
                rate_reader = getattr(self._adapter(settings), "rate_status", None)
                if callable(rate_reader):
                    result["rate_limit"] = rate_reader()
                stream_reader = getattr(self._adapter(settings), "stream_status", None)
                if callable(stream_reader):
                    result["stream"] = stream_reader()
                return result
        result = dict(self._adapter(settings).health(settings))
        result.setdefault("broker", settings.broker)
        result.setdefault("broker_label", BROKER_LABELS[settings.broker])
        result["capabilities"] = self.capabilities(settings)
        result["credential_status"] = self.credential_status(settings.broker)
        result["expiry_policy"] = broker_expiry_policy(
            settings.broker, settings.expiry_open_cutoff_minutes
        )
        rate_reader = getattr(self._adapter(settings), "rate_status", None)
        result["rate_limit"] = rate_reader() if callable(rate_reader) else {
            "broker": "OpenD",
            "local_guard": True,
            "cooldown_remaining_seconds": 0,
        }
        stream_reader = getattr(self._adapter(settings), "stream_status", None)
        if callable(stream_reader):
            result["stream"] = stream_reader()
        with self._health_lock:
            self._health_cache = {key: (time.monotonic(), dict(result))}
        return result

    def execution_status(self, settings: AppSettings) -> tuple[bool, str]:
        if settings.broker == "robinhood":
            return self.robinhood.execution_status()
        capabilities = self.capabilities(settings)
        return bool(capabilities["execution"]), str(capabilities["status"])

    def credential_status(self, broker: str) -> dict[str, bool]:
        if broker == "robinhood":
            return self.robinhood.credential_status()
        return {
            field: bool(self.keychain.get(broker, field))
            for field in self.credential_fields.get(broker, ())
        }

    def save_credentials(self, broker: str, payload: dict[str, Any]) -> dict[str, bool]:
        if broker not in self.credential_fields:
            raise ValueError("这个券商不需要在 App 内保存 API 凭证")
        for field in self.credential_fields[broker]:
            value = str(payload.get(field) or "").strip()
            if value:
                self.keychain.set(broker, field, value)
        self.invalidate()
        return self.credential_status(broker)

    def clear_credentials(self, broker: str) -> dict[str, bool]:
        if broker not in self.credential_fields:
            raise ValueError("这个券商没有保存在 App 内的 API 凭证")
        for field in self.credential_fields[broker]:
            self.keychain.delete(broker, field)
        self.invalidate()
        return self.credential_status(broker)

    def invalidate(self) -> None:
        with self._health_lock:
            self._health_cache.clear()
        with self._data_lock:
            self._data_cache.clear()

    def set_robinhood_redirect_uri(self, value: str) -> None:
        self.robinhood.set_redirect_uri(value)

    def robinhood_authorization_url(self) -> str:
        return self.robinhood.authorization_url()

    def complete_robinhood_oauth(self, code: str, state: str) -> None:
        self.robinhood.complete_oauth(code, state)
        self.invalidate()

    def disconnect_robinhood(self) -> None:
        self.robinhood.disconnect()
        self.invalidate()

    def close(self) -> None:
        closer = getattr(self.moomoo, "close", None)
        if callable(closer):
            closer()

    def _cached(self, settings: AppSettings, operation: str, ttl: float, loader: Any) -> Any:
        identity = settings.selected_account_id() or "-"
        key = (settings.broker, operation, identity)
        now = time.monotonic()
        with self._data_lock:
            cached = self._data_cache.get(key)
            if cached and now - cached[0] < ttl:
                return copy.deepcopy(cached[1])
        value = loader()
        with self._data_lock:
            self._data_cache[key] = (time.monotonic(), copy.deepcopy(value))
        return value

    @staticmethod
    def resolve_code(settings: AppSettings, code: str, instrument: dict[str, Any] | None = None) -> str:
        try:
            return broker_symbol(settings.broker, code, instrument)
        except ValueError as exc:
            raise BrokerError(str(exc)) from exc

    def accounts(self, settings: AppSettings) -> list[dict[str, Any]]:
        return self._cached(settings, "accounts", 30.0, lambda: self._adapter(settings).accounts(settings))

    def portfolio(self, settings: AppSettings) -> dict[str, Any]:
        return self._cached(settings, "portfolio", 5.2, lambda: self._adapter(settings).portfolio(settings))

    def account_equity(self, settings: AppSettings) -> float:
        funds = self.portfolio(settings).get("funds") or []
        for row in funds:
            for field in ("total_assets", "securities_assets", "net_liquidation", "equity", "cash"):
                value = _number(row.get(field))
                if value is not None and value > 0:
                    return value
        raise BrokerError(f"{BROKER_LABELS[settings.broker]} 没有返回可用于仓位计算的账户净值")

    def orders(self, settings: AppSettings) -> list[dict[str, Any]]:
        return self._cached(settings, "orders", 5.2, lambda: self._adapter(settings).orders(settings))

    def quote(self, settings: AppSettings, code: str) -> Quote:
        resolved = self.resolve_code(settings, code)
        return self._cached(settings, f"quote:{resolved}", 0.4, lambda: self._adapter(settings).quote(settings, resolved))

    def sellable_quantity(self, settings: AppSettings, code: str) -> float:
        resolved = self.resolve_code(settings, code)
        return self._adapter(settings).sellable_quantity(settings, resolved)

    def place_order(self, settings: AppSettings, **kwargs: Any) -> Any:
        try:
            return self._adapter(settings).place_order(settings, **kwargs)
        except BrokerError:
            raise
        except OpenDError as exc:
            # OpenD uses its own adapter exception for expected order-level
            # rejections (unsupported session, locked trading, invalid order,
            # etc.). Normalize it so one rejected order does not trip the
            # engine's unknown-failure circuit breaker.
            raise BrokerError(str(exc)) from exc
