from __future__ import annotations

import hashlib
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from .broker_adapters import BROKER_LABELS, BrokerError, BrokerRouter
from .config import AppSettings, SettingsStore
from .instruments import (
    broker_expiry_policy,
    broker_symbol,
    expiry_open_guard,
    parse_option_contract,
    spx_tick_size,
    valid_option_limit_tick,
)
from .models import CopySignal, INFORMATIONAL_ACTIONS, Quote, RiskDecision
from .store import LocalStore


class BrokerAdapter(Protocol):
    def quote(self, settings: AppSettings, code: str) -> Quote: ...

    def sellable_quantity(self, settings: AppSettings, code: str) -> float: ...

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
    ) -> Any: ...


class CopyEngine:
    """Moonvest-only copy engine with a volatile execution arm."""

    def __init__(
        self,
        settings_store: SettingsStore,
        store: LocalStore,
        broker: BrokerAdapter | None = None,
    ):
        self.settings_store = settings_store
        self.store = store
        self.broker = broker or BrokerRouter()
        self._lock = threading.RLock()
        self._paused = False
        self._armed_until: datetime | None = None

    def state(self) -> dict[str, Any]:
        with self._lock:
            armed = bool(self._armed_until and self._armed_until > datetime.now(timezone.utc))
            if not armed:
                self._armed_until = None
            return {
                "paused": self._paused,
                "armed": armed,
                "armed_until": self._armed_until.isoformat(timespec="seconds")
                if self._armed_until
                else None,
            }

    def pause(self, paused: bool = True) -> dict[str, Any]:
        with self._lock:
            self._paused = bool(paused)
            if paused:
                self._armed_until = None
        self.store.event(
            "engine.pause" if paused else "engine.resume",
            "跟单引擎已暂停" if paused else "跟单引擎已恢复接收",
            level="warning" if paused else "info",
        )
        return self.state()

    def arm(self, *, manual_unlock_confirmed: bool = False) -> dict[str, Any]:
        settings = self.settings_store.get()
        if not settings.moonvest_follow:
            raise ValueError("请先配置 1–2 个 Moonvest follow 用户")
        if not settings.selected_account_id():
            raise ValueError("请先选择跟单账户")
        execution_status = getattr(self.broker, "execution_status", None)
        if callable(execution_status):
            ready, reason = execution_status(settings)
            if not ready:
                raise ValueError(f"{BROKER_LABELS[settings.broker]} 暂未开放订单执行：{reason}")
        if settings.mode == "observe":
            raise ValueError("仅观察模式不需要启用执行，请先切换到人工确认或自动模式")
        if settings.broker == "moomoo" and settings.trading_env == "REAL" and not manual_unlock_confirmed:
            raise ValueError("实盘启用前必须确认已在 OpenD GUI 中手动解锁交易")
        with self._lock:
            self._paused = False
            self._armed_until = datetime.now(timezone.utc) + timedelta(hours=8)
        self.store.event(
            "engine.arm",
            f"订单执行已启用（{settings.trading_env}，8 小时后自动关闭）",
            level="warning" if settings.trading_env == "REAL" else "info",
        )
        return self.state()

    def disarm(self) -> dict[str, Any]:
        with self._lock:
            self._armed_until = None
        self.store.event("engine.disarm", "订单执行已关闭")
        return self.state()

    def submit(self, payload: dict[str, Any], *, source: str = "moonvest") -> dict[str, Any]:
        signal = CopySignal.from_payload(payload, source=source)
        row, created = self.store.insert_signal(signal)
        if not created:
            return {"signal": row, "duplicate": True}
        self.store.event(
            "signal.received",
            f"收到 Moonvest 事件：{signal.action} {signal.code} × {signal.quantity:g}",
            signal_id=row["id"],
            details={"event_id": signal.external_id, "actor": signal.leader},
        )
        processed = self._evaluate(row["id"], signal)
        return {"signal": processed, "duplicate": False}

    def _evaluate(self, signal_id: str, signal: CopySignal) -> dict[str, Any]:
        settings = self.settings_store.get()
        state = self.state()
        if signal.market not in settings.allowed_markets:
            return self._reject(signal_id, f"市场 {signal.market} 未被允许")
        if settings.allowed_symbols and signal.code not in settings.allowed_symbols:
            return self._reject(signal_id, f"标的 {signal.code} 未被允许")
        if state["paused"]:
            return self.store.update_signal(signal_id, status="PAUSED", reason="跟单引擎已暂停") or {}
        non_executable = str(signal.raw.get("non_executable_reason") or "").strip()
        if non_executable:
            return self._observe(signal_id, non_executable)
        if signal.action in INFORMATIONAL_ACTIONS:
            label = "编辑" if signal.action == "EDIT" else "到期"
            return self._observe(signal_id, f"Moonvest {label}事件已记录，不产生券商订单")

        try:
            decision, no_attempt = self._prepare_decision(settings, signal)
        except Exception as exc:
            return self._reject(signal_id, f"行情或风控检查失败：{exc}")
        if no_attempt:
            return self._observe(signal_id, no_attempt)
        if decision is None:
            return self._reject(signal_id, "无法形成执行计划")
        if not decision.ok:
            return self._reject(signal_id, decision.reason, decision=decision)

        if settings.mode == "observe":
            row = self.store.set_decision(signal_id, decision, "OBSERVED")
            self.store.event(
                "signal.observed",
                f"观察到可执行事件，未下单：{signal.code}",
                signal_id=signal_id,
                details=decision.as_dict(),
            )
            return row or {}
        if settings.mode == "confirm" or not state["armed"]:
            reason = "等待人工确认" if settings.mode == "confirm" else "等待启用订单执行"
            decision.reason = reason
            row = self.store.set_decision(signal_id, decision, "PENDING")
            self.store.event(
                "signal.pending",
                f"{reason}：{signal.side} {signal.code} × {decision.quantity:g}",
                signal_id=signal_id,
                details=decision.as_dict(),
            )
            return row or {}
        return self._execute(signal_id, signal, decision)

    def _risk_decision(
        self,
        settings: AppSettings,
        signal: CopySignal,
        quote: Quote,
        *,
        quantity_override: float | None = None,
        risk_reducing: bool = False,
    ) -> RiskDecision:
        quantity = (
            float(quantity_override)
            if quantity_override is not None
            else signal.copied_quantity(settings.copy_ratio)
        )
        if quantity < 1:
            return RiskDecision(False, "按跟单比例计算后数量小于 1", quantity, 0, 0, quote)
        live_price = quote.execution_reference(signal.side)
        if live_price is None:
            return RiskDecision(False, "没有可用的买卖盘或最新价", quantity, 0, 0, quote)
        execution_price = (
            signal.signal_price
            if signal.order_type == "NORMAL" and signal.signal_price
            else live_price
        )

        contract = parse_option_contract(signal.code, signal.raw.get("instrument"))
        if (
            contract is not None
            and signal.order_type == "NORMAL"
            and settings.reject_nonconforming_option_ticks
            and not valid_option_limit_tick(contract, execution_price)
        ):
            tick = spx_tick_size(contract, execution_price)
            return RiskDecision(
                False,
                f"{contract.root} 单腿限价 {execution_price:g} 不符合最小变动单位 {tick}；不会静默改价",
                quantity,
                execution_price,
                0,
                quote,
            )

        if signal.signal_price:
            max_slippage = settings.max_slippage_pct / 100.0
            if signal.side == "BUY" and live_price > signal.signal_price * (1 + max_slippage):
                return RiskDecision(False, "当前买入参考价超过事件价格允许滑点", quantity, live_price, 0, quote)
            if signal.side == "SELL" and live_price < signal.signal_price * (1 - max_slippage):
                return RiskDecision(False, "当前卖出参考价低于事件价格允许滑点", quantity, live_price, 0, quote)

        multiplier = 100.0 if signal.is_option else 1.0
        notional = quantity * execution_price * multiplier
        if not risk_reducing and notional > settings.max_order_notional:
            return RiskDecision(
                False,
                f"预计金额 {notional:,.2f} 超过单笔限额 {settings.max_order_notional:,.2f}",
                quantity,
                execution_price,
                notional,
                quote,
            )
        daily = self.store.daily_notional()
        if not risk_reducing and daily + notional > settings.max_daily_notional:
            return RiskDecision(False, f"本单将超过单日限额（已用 {daily:,.2f}）", quantity, execution_price, notional, quote)
        return RiskDecision(True, "风控检查通过", quantity, execution_price, notional, quote)

    def _prepare_decision(
        self,
        settings: AppSettings,
        signal: CopySignal,
    ) -> tuple[RiskDecision | None, str | None]:
        contract = parse_option_contract(signal.code, signal.raw.get("instrument"))
        if contract is not None and settings.expiry_guard_enabled and signal.action in {"OPEN", "ADD"}:
            expiry_reason = expiry_open_guard(
                contract,
                cutoff_minutes=int(
                    broker_expiry_policy(settings.broker, settings.expiry_open_cutoff_minutes)[
                        "effective_minutes"
                    ]
                ),
            )
            if expiry_reason:
                raise ValueError(expiry_reason)

        management = signal.action in {"TRIM", "CLOSE"}
        quantity_override: float | None = None
        if management:
            quantity_override, no_attempt = self._management_quantity(settings, signal)
            if no_attempt:
                return None, no_attempt
        broker_code = self._broker_code(settings, signal)
        quote = self.broker.quote(settings, broker_code)
        return (
            self._risk_decision(
                settings,
                signal,
                quote,
                quantity_override=quantity_override,
                risk_reducing=management,
            ),
            None,
        )

    def _broker_code(self, settings: AppSettings, signal: CopySignal) -> str:
        resolver = getattr(self.broker, "resolve_code", None)
        if callable(resolver):
            return str(resolver(settings, signal.code, signal.raw.get("instrument")))
        return broker_symbol(settings.broker, signal.code, signal.raw.get("instrument"))

    def _management_quantity(
        self,
        settings: AppSettings,
        signal: CopySignal,
    ) -> tuple[float, str | None]:
        order_reader = getattr(self.broker, "orders", None)
        if callable(order_reader):
            try:
                self.store.reconcile_orders(list(order_reader(settings)))
            except Exception:
                pass
        owned = self.store.managed_position(
            leader=signal.leader,
            code=signal.code,
            account_id=settings.execution_account_id(),
            firm=settings.execution_firm(),
            trading_env=settings.trading_env,
        )
        remaining = float(owned["remaining"])
        if remaining < 1:
            return 0.0, "没有由本 App 确认成交且归属于该用户的可管理仓位"

        position_side = str(signal.raw.get("position_side") or "").upper()
        if position_side in {"BUY", "SELL"} and owned["direction"] != position_side:
            return 0.0, "本机可管理仓位方向与 Moonvest 事件不一致，已安全留痕"
        expected_close_side = "SELL" if position_side == "BUY" else "BUY" if position_side == "SELL" else ""
        if expected_close_side and signal.side != expected_close_side:
            return 0.0, "Moonvest 平仓方向无效，已安全留痕"
        if signal.action == "CLOSE":
            return remaining, None
        desired = signal.copied_quantity(settings.copy_ratio)
        desired = min(desired, remaining)
        if desired < 1:
            return 0.0, "按跟单比例计算后无需提交部分平仓订单"
        return desired, None

    def approve(self, signal_id: str) -> dict[str, Any]:
        row = self.store.get_signal(signal_id)
        if not row:
            raise ValueError("信号不存在")
        if row["status"] != "PENDING":
            raise ValueError(f"当前状态 {row['status']} 不能确认")
        if not self.state()["armed"]:
            raise ValueError("请先启用订单执行")
        signal = CopySignal.from_row(row)
        settings = self.settings_store.get()
        try:
            decision, no_attempt = self._prepare_decision(settings, signal)
        except Exception as exc:
            return self._reject(signal_id, f"确认前复检失败：{exc}")
        if no_attempt:
            return self._observe(signal_id, no_attempt)
        if decision is None:
            return self._reject(signal_id, "确认前无法形成执行计划")
        if not decision.ok:
            return self._reject(signal_id, decision.reason, decision=decision)
        return self._execute(signal_id, signal, decision)

    def reject(self, signal_id: str, reason: str = "操作员拒绝") -> dict[str, Any]:
        row = self.store.get_signal(signal_id)
        if not row:
            raise ValueError("信号不存在")
        if row["status"] not in {"PENDING", "PAUSED", "RECEIVED"}:
            raise ValueError(f"当前状态 {row['status']} 不能拒绝")
        return self._reject(signal_id, reason)

    def _execute(self, signal_id: str, signal: CopySignal, decision: RiskDecision) -> dict[str, Any]:
        settings = self.settings_store.get()
        try:
            broker_code = self._broker_code(settings, signal)
        except Exception as exc:
            return self._reject(signal_id, f"券商合约解析失败：{exc}", decision=decision)
        if signal.side == "SELL" and not settings.allow_unmanaged_sells:
            try:
                sellable = self.broker.sellable_quantity(settings, broker_code)
            except Exception as exc:
                return self._reject(signal_id, f"卖出持仓检查失败：{exc}", decision=decision)
            if sellable < decision.quantity:
                return self._reject(
                    signal_id,
                    f"可卖数量 {sellable:g} 小于跟单数量 {decision.quantity:g}",
                    decision=decision,
                )
        idempotency_key = hashlib.sha256(signal.external_id.encode("utf-8")).hexdigest()[:20]
        try:
            order_kwargs: dict[str, Any] = {
                "code": broker_code,
                "side": signal.side,
                "quantity": decision.quantity,
                "price": decision.execution_price,
                "order_type": signal.order_type,
                "remark": f"mv-{idempotency_key}",
            }
            if settings.broker == "robinhood":
                order_kwargs["action"] = signal.action
            result = self.broker.place_order(settings, **order_kwargs)
            row = self.store.update_signal(
                signal_id,
                copied_quantity=decision.quantity,
                execution_price=decision.execution_price,
                notional=decision.notional,
                status="PLACED",
                reason=f"订单已提交至 {BROKER_LABELS.get(settings.broker, settings.broker)}",
                broker_order_id=result.order_id,
                filled_quantity=max(float(result.raw.get("dealt_qty") or 0), 0.0),
                execution_account_id=settings.execution_account_id(),
                execution_firm=settings.execution_firm(),
                execution_env=settings.trading_env,
            )
            self.store.event(
                "order.placed",
                f"已提交 {signal.side} {signal.code} × {decision.quantity:g}",
                level="warning" if settings.trading_env == "REAL" else "info",
                signal_id=signal_id,
                details={**result.as_dict(), "idempotency_key": signal.external_id},
            )
            return row or {}
        except BrokerError as exc:
            # Broker-declared failures are scoped to this order. Keeping the
            # execution lock armed lets later, independent signals continue;
            # the failed order remains durably rejected and visible in audit.
            self.store.event(
                "order.rejected",
                "券商拒绝或未接受该笔订单；后续订单继续运行",
                level="warning",
                signal_id=signal_id,
                details={"reason": str(exc), "execution_continues": True},
            )
            return self._reject(signal_id, f"券商下单失败：{exc}", decision=decision)
        except Exception as exc:
            self.disarm()
            return self._reject(signal_id, f"下单失败：{exc}", decision=decision)

    def _observe(self, signal_id: str, reason: str) -> dict[str, Any]:
        row = self.store.update_signal(signal_id, status="OBSERVED", reason=reason)
        self.store.event("signal.informational", reason, signal_id=signal_id)
        return row or {}

    def _reject(
        self,
        signal_id: str,
        reason: str,
        *,
        decision: RiskDecision | None = None,
    ) -> dict[str, Any]:
        fields: dict[str, Any] = {"status": "REJECTED", "reason": reason}
        if decision:
            fields.update(
                copied_quantity=decision.quantity,
                execution_price=decision.execution_price,
                notional=decision.notional,
            )
        row = self.store.update_signal(signal_id, **fields)
        self.store.event(
            "signal.rejected",
            reason,
            level="warning",
            signal_id=signal_id,
            details=decision.as_dict() if decision else {},
        )
        return row or {}
