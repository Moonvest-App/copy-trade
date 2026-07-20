"""IBKR Client Portal Gateway 只读适配器。"""

from __future__ import annotations

import json
import ssl
import urllib.parse
from typing import Any

from .. import __version__ as APP_VERSION
from ..api_policy import ApiPacer, EndpointRule
from ..config import AppSettings
from ..models import Quote
from .base import BROKER_LABELS, BrokerError, as_items, json_request, to_number


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
        headers = {"Accept": "application/json", "User-Agent": f"Moonvest/{APP_VERSION}"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        return json_request(
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
        rows = as_items(self._request(settings, "/portfolio/accounts"))
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
        positions = as_items(self._request(settings, f"/portfolio/{encoded}/positions/0"))
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
            qty = to_number(row.get("position")) or 0.0
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
        value = to_number(funds[0].get("total_assets")) if funds else None
        if value is None or value <= 0:
            raise BrokerError("IBKR 没有返回可用于仓位计算的净值")
        return value

    def orders(self, settings: AppSettings) -> list[dict[str, Any]]:
        payload = self._request(settings, "/iserver/account/orders")
        rows = as_items(payload, "orders")
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
        rows = as_items(self._request(settings, "/iserver/secdef/search", method="POST", payload={"symbol": symbol}))
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
        rows = as_items(self._request(settings, f"/iserver/marketdata/snapshot?{query}"))
        if not rows:
            raise BrokerError("IBKR 没有返回行情快照")
        row = rows[0]
        return Quote(
            code=code,
            last=to_number(row.get("31")),
            bid=to_number(row.get("84")),
            ask=to_number(row.get("86")),
            name=str(row.get("55") or self._symbol(code)),
            currency="USD",
        )

    def sellable_quantity(self, settings: AppSettings, code: str) -> float:
        symbol = self._symbol(code)
        for row in self.portfolio(settings)["positions"]:
            row_symbol = str(row.get("code") or "").upper().split()[0]
            if row_symbol == symbol:
                return max(to_number(row.get("can_sell_qty")) or 0.0, 0.0)
        return 0.0

    def place_order(self, settings: AppSettings, **_: Any) -> Any:
        raise BrokerError("IBKR 当前为只读适配器；尚未启用订单警告回复确认链路")
