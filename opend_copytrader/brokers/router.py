"""BrokerRouter：把已配置券商映射到统一的引擎 API。"""

from __future__ import annotations

import copy
import threading
import time
from typing import Any, Callable

from ..config import AppSettings
from ..instruments import broker_expiry_policy, broker_symbol
from ..models import Quote
from ..moomoo_adapter import MoomooAdapter, OpenDError
from ..robinhood_mcp import RobinhoodMCPAdapter, RobinhoodOrderRejected
from .base import BROKER_LABELS, BrokerError, to_number
from .ibkr import IBKRClientPortalAdapter
from .keychain import KeychainStore
from .schwab import SchwabAdapter
from .webull import WebullAdapter


class BrokerRouter:
    """Selects the configured broker while exposing one engine-facing API."""

    credential_fields = {
        "webull": ("app_secret", "access_token"),
        "schwab": ("client_secret", "access_token", "refresh_token"),
    }

    def __init__(self, *, diagnostic: Callable[..., None] | None = None) -> None:
        self.keychain = KeychainStore()
        self.moomoo = MoomooAdapter()
        self.ibkr = IBKRClientPortalAdapter()
        self.webull = WebullAdapter(self.keychain)
        self.schwab = SchwabAdapter(self.keychain)
        self.robinhood = RobinhoodMCPAdapter(self.keychain, diagnostic=diagnostic)
        self._health_lock = threading.RLock()
        self._health_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._data_lock = threading.RLock()
        self._data_cache: dict[tuple[str, str, str], tuple[float, Any]] = {}

    def _adapter(self, settings: AppSettings) -> Any:
        return getattr(self, settings.broker)

    @staticmethod
    def _health_key(settings: AppSettings) -> str:
        return f"{settings.broker}:{settings.opend_host}:{settings.opend_port}:{settings.ibkr_host}:{settings.ibkr_port}:{settings.webull_environment}:{settings.webull_app_key}:{settings.schwab_client_id}:{settings.robinhood_account_id}"

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
            capability = self.robinhood.execution_capability(settings)
            return {
                "accounts": True,
                "portfolio": True,
                "quotes": True,
                "orders": True,
                "execution": bool(capability["execution"]),
                "execution_assets": list(capability.get("assets") or []),
                "agentic_account": bool(capability.get("agentic")),
                "execution_prerequisites": dict(capability.get("prerequisites") or {}),
                "leader_mirror": False,
                "streaming": False,
                "transport": "Robinhood 官方 Trading MCP · OAuth 2.1 / Streamable HTTP",
                "status": str(capability["status"]),
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
        key = self._health_key(settings)
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

    def monitor_health(self, settings: AppSettings) -> dict[str, Any]:
        """Return only cached health so diagnostics never makes a broker request."""
        key = self._health_key(settings)
        with self._health_lock:
            cached = self._health_cache.get(key)
            if cached:
                checked_at, payload = cached
                result = dict(payload)
                result["checked"] = True
                result["stale"] = time.monotonic() - checked_at >= 45
                return result
        return {
            "broker": settings.broker,
            "broker_label": BROKER_LABELS[settings.broker],
            "connected": False,
            "checked": False,
            "stale": True,
        }

    def execution_status(self, settings: AppSettings) -> tuple[bool, str]:
        if settings.broker == "robinhood":
            return self.robinhood.execution_status(settings)
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
                value = to_number(row.get(field))
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
        except RobinhoodOrderRejected as exc:
            # A definite review/schema/broker rejection is scoped to this
            # order. Ambiguous place responses remain unhandled here and trip
            # the engine's fail-closed circuit breaker.
            raise BrokerError(str(exc)) from exc
