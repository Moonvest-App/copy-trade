"""Charles Schwab（嘉信）只读适配器。"""

from __future__ import annotations

import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Any

from ..api_policy import ApiPacer, EndpointRule
from ..config import AppSettings
from ..models import Quote
from .base import BROKER_LABELS, BrokerError, as_items, json_request, to_number
from .keychain import KeychainStore


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
        return json_request(
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
        rows = as_items(self._request("/trader/v1/accounts/accountNumbers"))
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
            qty = (to_number(row.get("longQuantity")) or 0) - (to_number(row.get("shortQuantity")) or 0)
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
        value = to_number(funds[0].get("total_assets")) if funds else None
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
        rows = as_items(
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
            last=to_number(quote.get("lastPrice")),
            bid=to_number(quote.get("bidPrice")),
            ask=to_number(quote.get("askPrice")),
            name=str(reference.get("description") or symbol),
            currency="USD",
        )

    def sellable_quantity(self, settings: AppSettings, code: str) -> float:
        symbol = code.removeprefix("US.").upper()
        for row in self.portfolio(settings)["positions"]:
            if str(row.get("code") or "").upper() == symbol:
                return max(to_number(row.get("can_sell_qty")) or 0.0, 0.0)
        return 0.0

    def place_order(self, settings: AppSettings, **_: Any) -> Any:
        raise BrokerError("嘉信当前为只读适配器；需完成 OAuth 自动刷新与订单预览联调后才会开放执行")
