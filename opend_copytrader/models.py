from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any


CODE_RE = re.compile(r"^(US|HK|SH|SZ|SG|MY|JP)\.[A-Z0-9][A-Z0-9.-]{0,31}$")
OPTION_RE = re.compile(r"^(US|HK)\.[A-Z0-9]{1,6}\d{6}[CP]\d{1,8}$")
EXECUTABLE_ACTIONS = {"OPEN", "ADD", "TRIM", "CLOSE"}
INFORMATIONAL_ACTIONS = {"EDIT", "EXPIRE"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(slots=True)
class CopySignal:
    source: str
    external_id: str
    code: str
    side: str
    quantity: float
    action: str
    signal_price: float | None = None
    order_type: str = "NORMAL"
    leader: str = ""
    note: str = ""
    received_at: str = field(default_factory=utc_now)
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: dict[str, Any], *, source: str | None = None) -> "CopySignal":
        code = str(payload.get("code", "")).strip().upper()
        side = str(payload.get("side", "BUY")).strip().upper()
        order_type = str(payload.get("order_type", "NORMAL")).strip().upper()
        action = str(payload.get("action") or "").strip().upper()
        signal_source = (source or str(payload.get("source", "moonvest"))).strip().lower()
        external_id = str(payload.get("external_id") or "").strip()
        leader = str(payload.get("actor") or payload.get("leader") or "").strip()
        note = str(payload.get("note") or "").strip()

        try:
            quantity = float(payload.get("quantity", 0))
        except (TypeError, ValueError) as exc:
            raise ValueError("数量必须是数字") from exc
        raw_price = payload.get("signal_price", payload.get("price"))
        if raw_price in (None, ""):
            signal_price = None
        else:
            try:
                signal_price = float(raw_price)
            except (TypeError, ValueError) as exc:
                raise ValueError("信号价格必须是数字") from exc

        if signal_source != "moonvest":
            raise ValueError("Moonvest 是唯一允许的开单信息源")
        if not external_id:
            raise ValueError("Moonvest 事件 id 不能为空")
        if not CODE_RE.fullmatch(code):
            raise ValueError("代码格式无效，例如 US.AAPL 或 US.NVDA260918C150000")
        if side not in {"BUY", "SELL"}:
            raise ValueError("方向只能是 BUY 或 SELL")
        if action not in EXECUTABLE_ACTIONS | INFORMATIONAL_ACTIONS:
            raise ValueError("事件动作只能是 OPEN、ADD、TRIM、CLOSE、EDIT 或 EXPIRE")
        if order_type not in {"NORMAL", "MARKET"}:
            raise ValueError("订单类型只能是 NORMAL 或 MARKET")
        if action in {"OPEN", "ADD", "TRIM"} and quantity <= 0:
            raise ValueError(f"{action} 数量必须大于 0")
        if action in {"CLOSE", "EDIT", "EXPIRE"} and quantity < 0:
            raise ValueError("事件数量不能小于 0")
        if signal_price is not None and signal_price <= 0:
            raise ValueError("信号价格必须大于 0")

        return cls(
            source=signal_source,
            external_id=external_id,
            code=code,
            side=side,
            quantity=quantity,
            action=action,
            signal_price=signal_price,
            order_type=order_type,
            leader=leader,
            note=note,
            raw=dict(payload),
        )

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "CopySignal":
        """从 signals 表行重建信号：raw_json 提供上下文，列值为准。"""
        try:
            raw = json.loads(row.get("raw_json") or "{}")
        except (TypeError, ValueError):
            raw = {}
        if not isinstance(raw, dict):
            raw = {}
        raw.update(
            external_id=row["external_id"],
            actor=row["leader"],
            code=row["code"],
            side=row["side"],
            quantity=row["quantity"],
            action=row.get("action") or "OPEN",
            signal_price=row["signal_price"],
            order_type=row["order_type"],
            note=row["note"],
        )
        return cls.from_payload(raw, source="moonvest")

    @property
    def market(self) -> str:
        return self.code.split(".", 1)[0]

    @property
    def is_option(self) -> bool:
        return bool(OPTION_RE.fullmatch(self.code))

    def copied_quantity(self, ratio: float) -> float:
        copied = Decimal(str(self.quantity)) * Decimal(str(ratio))
        return float(copied.quantize(Decimal("1"), rounding=ROUND_DOWN))

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Quote:
    code: str
    last: float | None
    bid: float | None
    ask: float | None
    name: str = ""
    currency: str = ""

    def execution_reference(self, side: str) -> float | None:
        if side == "BUY":
            return self.ask or self.last or self.bid
        return self.bid or self.last or self.ask

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class OrderResult:
    order_id: str
    status: str
    code: str
    side: str
    quantity: float
    price: float
    raw: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RiskDecision:
    ok: bool
    reason: str
    quantity: float
    execution_price: float
    notional: float
    quote: Quote | None = None

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["quote"] = self.quote.as_dict() if self.quote else None
        return data
