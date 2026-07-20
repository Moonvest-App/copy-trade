"""Webull OpenAPI 只读适配器（HMAC-SHA1 签名认证）。"""

from __future__ import annotations

import base64
import hashlib
import hmac
import urllib.parse
import uuid
from datetime import datetime, timezone
from typing import Any

from ..api_policy import ApiPacer, EndpointRule
from ..config import AppSettings
from ..models import Quote
from .base import BROKER_LABELS, BrokerError, as_items, json_request, to_number
from .keychain import KeychainStore


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
        return json_request(url, headers=headers, pacer=self.pacer, route=path)

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
        rows = as_items(payload, "data") or as_items(payload)
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
        positions = as_items(position_payload, "data") or as_items(position_payload)
        balance_row = (as_items(balance, "data") or as_items(balance) or [{}])[0]
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
        value = to_number(funds[0].get("total_assets")) if funds else None
        if value is None or value <= 0:
            raise BrokerError("Webull 没有返回可用于仓位计算的账户资产")
        return value

    def orders(self, settings: AppSettings) -> list[dict[str, Any]]:
        if not settings.webull_account_id:
            return []
        payload = self._request(settings, "/openapi/trade/order/open", {"account_id": settings.webull_account_id})
        rows = as_items(payload, "data") or as_items(payload)
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
                return max(to_number(row.get("can_sell_qty")) or 0.0, 0.0)
        return 0.0

    def place_order(self, settings: AppSettings, **_: Any) -> Any:
        raise BrokerError("Webull 当前为只读适配器；需完成 MQTT 行情与订单预览联调后才会开放执行")
