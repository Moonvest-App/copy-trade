from __future__ import annotations

import math
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator

import moomoo
from moomoo import (
    RET_OK,
    OpenQuoteContext,
    OpenSecTradeContext,
    OrderType,
    SecurityFirm,
    StockQuoteHandlerBase,
    SubType,
    SysConfig,
    TrdEnv,
    TrdMarket,
    TrdSide,
)

from .config import AppSettings, VALID_FIRMS
from .models import OrderResult, Quote


SysConfig.enable_console_log(False)
SysConfig.set_client_info("Moonvest", 1)


class OpenDError(RuntimeError):
    pass


class _StreamingQuoteHandler(StockQuoteHandlerBase):
    def __init__(self, owner: "MoomooAdapter") -> None:
        super().__init__()
        self.owner = owner

    def on_recv_rsp(self, rsp_pb: Any) -> tuple[Any, Any]:
        ret, data = super().on_recv_rsp(rsp_pb)
        if ret == RET_OK:
            self.owner._remember_stream_quotes(data)
        return ret, data


def _safe_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if hasattr(value, "item"):
            value = value.item()
    except Exception:
        pass
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple, set)):
        return [_safe_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _safe_value(v) for k, v in value.items()}
    return str(value)


def dataframe_records(data: Any) -> list[dict[str, Any]]:
    if data is None or not hasattr(data, "to_dict"):
        return []
    return [
        {str(key): _safe_value(value) for key, value in row.items()}
        for row in data.to_dict(orient="records")
    ]


class MoomooAdapter:
    """Small, password-free OpenD adapter built on the local moomoo SDK.

    It intentionally has no unlock method. REAL trading succeeds only when the
    operator has already unlocked trading in the OpenD GUI.
    """

    firms = tuple(sorted(VALID_FIRMS))

    def __init__(self):
        self._health_lock = threading.RLock()
        self._health_cache: tuple[float, dict[str, Any]] | None = None
        self._quote_stream_lock = threading.RLock()
        self._quote_stream_ctx: OpenQuoteContext | None = None
        self._quote_stream_key = ""
        self._quote_stream_codes: set[str] = set()
        self._quote_stream_rows: dict[str, dict[str, Any]] = {}
        self._quote_stream_error = ""

    @staticmethod
    def sdk_version() -> str:
        return str(getattr(moomoo, "__version__", "unknown"))

    @staticmethod
    def _firm(name: str) -> str:
        key = name.strip().upper()
        if key not in VALID_FIRMS:
            raise OpenDError(f"未知券商区域：{key}")
        return getattr(SecurityFirm, key)

    @contextmanager
    def quote_context(self, settings: AppSettings) -> Iterator[OpenQuoteContext]:
        # security_firm on OpenQuoteContext selects the upstream crypto venue;
        # for regular securities it can reject otherwise-valid JP/CA firms.
        # CopyTrader v0.1 intentionally excludes CC.* execution, so NONE is
        # the correct broker-neutral quote context here.
        ctx = OpenQuoteContext(
            host=settings.opend_host,
            port=int(settings.opend_port),
            security_firm=SecurityFirm.NONE,
        )
        try:
            yield ctx
        finally:
            ctx.close()

    @contextmanager
    def trade_context(
        self,
        settings: AppSettings,
        *,
        firm: str | None = None,
    ) -> Iterator[OpenSecTradeContext]:
        ctx = OpenSecTradeContext(
            filter_trdmarket=TrdMarket.NONE,
            host=settings.opend_host,
            port=int(settings.opend_port),
            security_firm=self._firm(firm or settings.security_firm),
        )
        try:
            yield ctx
        finally:
            ctx.close()

    def health(self, settings: AppSettings) -> dict[str, Any]:
        cache_key = f"{settings.opend_host}:{settings.opend_port}"
        with self._health_lock:
            if self._health_cache:
                cached_at, cached = self._health_cache
                if cached.get("cache_key") == cache_key and time.monotonic() - cached_at < 12:
                    return {k: v for k, v in cached.items() if k != "cache_key"}
        try:
            with self.quote_context(settings) as ctx:
                ret, data = ctx.get_global_state()
                if ret != RET_OK:
                    raise OpenDError(str(data))
                state = _safe_value(data if isinstance(data, dict) else {})
            result = {
                "connected": True,
                "host": settings.opend_host,
                "port": settings.opend_port,
                "sdk_version": self.sdk_version(),
                "state": state,
            }
        except Exception as exc:
            result = {
                "connected": False,
                "host": settings.opend_host,
                "port": settings.opend_port,
                "sdk_version": self.sdk_version(),
                "error": str(exc),
            }
        with self._health_lock:
            cached = dict(result)
            cached["cache_key"] = cache_key
            self._health_cache = (time.monotonic(), cached)
        return result

    def accounts(self, settings: AppSettings) -> list[dict[str, Any]]:
        accounts: list[dict[str, Any]] = []
        seen: set[tuple[Any, ...]] = set()
        errors: list[str] = []
        preferred = settings.security_firm if settings.security_firm in self.firms else self.firms[0]
        firms = (preferred, *[firm for firm in self.firms if firm != preferred])
        for firm in firms:
            try:
                with self.trade_context(settings, firm=firm) as ctx:
                    ret, data = ctx.get_acc_list()
                    if ret != RET_OK:
                        errors.append(f"{firm}: {data}")
                        continue
                    for row in dataframe_records(data):
                        acc_id = int(row.get("acc_id") or 0)
                        raw_firm = str(row.get("security_firm") or "").upper()
                        actual_firm = raw_firm
                        shared_sim_account = actual_firm in {"", "N/A", "NONE"}
                        if shared_sim_account:
                            actual_firm = firm
                        env = str(row.get("trd_env") or "")
                        jp_type = str(row.get("jp_acc_type") or "")
                        key = (
                            ("shared", acc_id, env, jp_type)
                            if shared_sim_account
                            else (acc_id, actual_firm, env, jp_type)
                        )
                        if not acc_id or key in seen:
                            continue
                        seen.add(key)
                        row["acc_id"] = acc_id
                        row["security_firm"] = actual_firm
                        row["display_name"] = self._account_label(row)
                        row["selectable"] = str(row.get("acc_role", "")).upper() != "MASTER"
                        accounts.append(row)
            except Exception as exc:
                errors.append(f"{firm}: {exc}")
        accounts.sort(
            key=lambda item: (
                str(item.get("trd_env")) != "REAL",
                str(item.get("security_firm")),
                str(item.get("display_name")),
            )
        )
        return accounts

    @staticmethod
    def _account_label(row: dict[str, Any]) -> str:
        env = "实盘" if str(row.get("trd_env", "")).upper() == "REAL" else "模拟"
        firm = str(row.get("security_firm") or "")
        card = str(row.get("uni_card_num") or row.get("card_num") or "")
        card = f"尾号 {card[-4:]}" if card and card not in {"N/A", "0"} else "模拟账户"
        raw_jp_type = row.get("jp_acc_type")
        if isinstance(raw_jp_type, (list, tuple)):
            suffix = " · 日本综合账户" if raw_jp_type else ""
        else:
            jp_type = str(raw_jp_type or "")
            if jp_type.startswith("["):
                suffix = " · 日本综合账户"
            else:
                suffix = f" · {jp_type}" if jp_type and jp_type not in {"N/A", "NONE"} else ""
        return f"{env} · {firm} · {card}{suffix}"

    def quote(self, settings: AppSettings, code: str) -> Quote:
        try:
            ctx = self._ensure_quote_subscription(settings, code)
            # Once subscribed, this reads OpenD's locally maintained push
            # cache and is not subject to the snapshot request frequency cap.
            ret, data = ctx.get_stock_quote([code])
            if ret != RET_OK:
                raise OpenDError(str(data))
            rows = dataframe_records(data)
            if rows:
                self._remember_stream_quotes(data)
            with self._quote_stream_lock:
                row = dict(self._quote_stream_rows.get(code) or (rows[0] if rows else {}))
        except Exception as stream_exc:
            # Permission/quota errors can prevent subscription. A single
            # snapshot remains a safe fallback and is cached by BrokerRouter.
            with self._quote_stream_lock:
                self._quote_stream_error = str(stream_exc)
            with self.quote_context(settings) as fallback:
                ret, data = fallback.get_market_snapshot([code])
                if ret != RET_OK:
                    raise OpenDError(str(data)) from stream_exc
                rows = dataframe_records(data)
                row = rows[0] if rows else {}
        if not row:
            raise OpenDError(f"OpenD 没有返回 {code} 的行情")
        return Quote(
            code=code,
            last=self._positive(row.get("last_price")),
            bid=self._positive(row.get("bid_price")),
            ask=self._positive(row.get("ask_price")),
            name=str(row.get("name") or ""),
            currency=str(row.get("currency") or ""),
        )

    def _ensure_quote_subscription(self, settings: AppSettings, code: str) -> OpenQuoteContext:
        key = f"{settings.opend_host}:{int(settings.opend_port)}"
        with self._quote_stream_lock:
            if self._quote_stream_ctx is None or self._quote_stream_key != key:
                if self._quote_stream_ctx is not None:
                    self._quote_stream_ctx.close()
                ctx = OpenQuoteContext(
                    host=settings.opend_host,
                    port=int(settings.opend_port),
                    security_firm=SecurityFirm.NONE,
                )
                ctx.set_handler(_StreamingQuoteHandler(self))
                self._quote_stream_ctx = ctx
                self._quote_stream_key = key
                self._quote_stream_codes.clear()
                self._quote_stream_rows.clear()
                self._quote_stream_error = ""
            ctx = self._quote_stream_ctx
            if code not in self._quote_stream_codes:
                ret, data = ctx.subscribe(
                    [code],
                    [SubType.QUOTE],
                    is_first_push=True,
                    subscribe_push=True,
                    extended_time=code.startswith("US.") and settings.us_session != "RTH",
                )
                if ret != RET_OK:
                    raise OpenDError(f"OpenD 行情订阅失败：{data}")
                self._quote_stream_codes.add(code)
            return ctx

    def _remember_stream_quotes(self, data: Any) -> None:
        rows = dataframe_records(data)
        with self._quote_stream_lock:
            for row in rows:
                code = str(row.get("code") or "").strip().upper()
                if code:
                    self._quote_stream_rows[code] = row
            if rows:
                self._quote_stream_error = ""

    def stream_status(self) -> dict[str, Any]:
        with self._quote_stream_lock:
            return {
                "quote_push_ready": bool(self._quote_stream_ctx and self._quote_stream_codes),
                "quote_subscription_count": len(self._quote_stream_codes),
                "quote_push_error": self._quote_stream_error,
            }

    def close(self) -> None:
        with self._quote_stream_lock:
            if self._quote_stream_ctx is not None:
                self._quote_stream_ctx.close()
            self._quote_stream_ctx = None
            self._quote_stream_codes.clear()
            self._quote_stream_rows.clear()

    @staticmethod
    def _positive(value: Any) -> float | None:
        try:
            parsed = float(value)
            return parsed if parsed > 0 and math.isfinite(parsed) else None
        except (TypeError, ValueError):
            return None

    def portfolio(self, settings: AppSettings) -> dict[str, Any]:
        if settings.account_id <= 0:
            return {"funds": [], "positions": []}
        env = TrdEnv.REAL if settings.trading_env == "REAL" else TrdEnv.SIMULATE
        with self.trade_context(settings) as ctx:
            ret_funds, funds = ctx.accinfo_query(
                trd_env=env,
                acc_id=settings.account_id,
                refresh_cache=False,
                currency=settings.base_currency,
            )
            ret_pos, positions = ctx.position_list_query(
                trd_env=env,
                acc_id=settings.account_id,
                refresh_cache=False,
                currency=settings.base_currency,
            )
        if ret_funds != RET_OK:
            raise OpenDError(str(funds))
        if ret_pos != RET_OK:
            raise OpenDError(str(positions))
        return {"funds": dataframe_records(funds), "positions": dataframe_records(positions)}

    def account_equity(self, settings: AppSettings) -> float:
        funds = self.portfolio(settings).get("funds") or []
        for row in funds:
            for field in ("total_assets", "securities_assets", "cash", "available_funds"):
                value = self._positive(row.get(field))
                if value is not None:
                    return value
        raise OpenDError("OpenD 没有返回可用于仓位计算的账户资产")

    def orders(self, settings: AppSettings) -> list[dict[str, Any]]:
        if settings.account_id <= 0:
            return []
        env = TrdEnv.REAL if settings.trading_env == "REAL" else TrdEnv.SIMULATE
        with self.trade_context(settings) as ctx:
            ret, data = ctx.order_list_query(
                trd_env=env,
                acc_id=settings.account_id,
                refresh_cache=False,
            )
        if ret != RET_OK:
            raise OpenDError(str(data))
        return dataframe_records(data)

    def deals(
        self,
        settings: AppSettings,
        *,
        account_id: int,
        firm: str,
        trading_env: str,
    ) -> list[dict[str, Any]]:
        env = TrdEnv.REAL if trading_env == "REAL" else TrdEnv.SIMULATE
        with self.trade_context(settings, firm=firm) as ctx:
            ret, data = ctx.deal_list_query(
                trd_env=env,
                acc_id=account_id,
                refresh_cache=False,
            )
        if ret != RET_OK:
            raise OpenDError(str(data))
        return dataframe_records(data)

    def sellable_quantity(self, settings: AppSettings, code: str) -> float:
        if settings.account_id <= 0:
            return 0.0
        env = TrdEnv.REAL if settings.trading_env == "REAL" else TrdEnv.SIMULATE
        with self.trade_context(settings) as ctx:
            ret, data = ctx.position_list_query(
                code=code,
                trd_env=env,
                acc_id=settings.account_id,
                refresh_cache=False,
                currency=settings.base_currency,
            )
        if ret != RET_OK:
            raise OpenDError(str(data))
        total = 0.0
        for row in dataframe_records(data):
            for field in ("can_sell_qty", "qty"):
                try:
                    total += max(float(row.get(field) or 0), 0)
                    break
                except (TypeError, ValueError):
                    continue
        return total

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
    ) -> OrderResult:
        if settings.account_id <= 0:
            raise OpenDError("尚未选择跟单账户")
        env = TrdEnv.REAL if settings.trading_env == "REAL" else TrdEnv.SIMULATE
        kwargs: dict[str, Any] = {
            "price": price if order_type == "NORMAL" else 0.0,
            "qty": quantity,
            "code": code,
            "trd_side": TrdSide.BUY if side == "BUY" else TrdSide.SELL,
            "order_type": OrderType.NORMAL if order_type == "NORMAL" else OrderType.MARKET,
            "trd_env": env,
            "acc_id": settings.account_id,
            "remark": remark[:64],
        }
        if code.startswith("US."):
            kwargs["session"] = settings.us_session
            kwargs["fill_outside_rth"] = settings.us_session in {"ETH", "ALL", "OVERNIGHT"}
        try:
            with self.trade_context(settings) as ctx:
                ret, data = ctx.place_order(**kwargs)
        except Exception as exc:
            message = str(exc)
            if "unlock" in message.lower() or "解锁" in message:
                raise OpenDError("OpenD 尚未手动解锁交易，请在 OpenD GUI 中完成解锁后重试") from exc
            raise OpenDError(message) from exc
        if ret != RET_OK:
            message = str(data)
            if "unlock" in message.lower() or "解锁" in message:
                raise OpenDError("OpenD 尚未手动解锁交易，请在 OpenD GUI 中完成解锁后重试")
            raise OpenDError(message)
        rows = dataframe_records(data)
        row = rows[0] if rows else {}
        order_id = str(row.get("order_id") or row.get("order_id_ex") or "")
        return OrderResult(
            order_id=order_id,
            status=str(row.get("order_status") or "SUBMITTED"),
            code=code,
            side=side,
            quantity=quantity,
            price=price,
            raw=row,
        )
