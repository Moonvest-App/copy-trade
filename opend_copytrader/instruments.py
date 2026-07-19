from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo


OPEN_OPTION_RE = re.compile(
    r"^US\.(?P<root>[A-Z0-9]{1,6})(?P<expiry>\d{6})(?P<right>[CP])(?P<strike>\d{1,8})$"
)
OCC_RE = re.compile(
    r"^(?P<root>[A-Z0-9 ]{1,6})(?P<expiry>\d{6})(?P<right>[CP])(?P<strike>\d{8})$"
)
EASTERN = ZoneInfo("America/New_York")

BROKER_EXPIRY_NOTES = {
    "moomoo": "不把券商自动处置当作退出保证；本地护栏先于最后常规交易时点生效。",
    "ibkr": "券商风控可能根据账户风险提前处置；不假设固定强平分钟数。",
    "webull": "当前适配器只读；指数期权不在 Webull OpenAPI 官方交易范围内。",
    "schwab": "当前适配器只读；不把券商到期处理当作退出信号。",
    "robinhood": "官方说明：资金/股票不足时可能在到期日收盘前最后 30 分钟尝试卖出，也可能因市况更早行动。",
}


def broker_expiry_policy(broker: str, configured_minutes: int) -> dict[str, Any]:
    broker = str(broker or "").lower()
    # 60 minutes is deliberately earlier than Robinhood's documented
    # last-30-minute attempt and remains conservative for brokers without a
    # stable public forced-liquidation time.
    effective = max(int(configured_minutes), 60 if broker in {"moomoo", "ibkr", "webull", "schwab"} else 45)
    return {
        "broker": broker,
        "configured_minutes": int(configured_minutes),
        "effective_minutes": effective,
        "note": BROKER_EXPIRY_NOTES.get(broker, "不依赖券商自动处置；使用本地保守到期窗口。"),
    }


@dataclass(frozen=True, slots=True)
class OptionContract:
    root: str
    expiry: date
    right: str
    strike_millis: int

    @property
    def strike(self) -> Decimal:
        return Decimal(self.strike_millis) / Decimal(1000)

    @property
    def is_spx_family(self) -> bool:
        return self.root in {"SPX", "SPXW"}

    @property
    def settlement(self) -> str:
        if self.root == "SPX":
            return "AM"
        if self.root == "SPXW":
            return "PM"
        return "PHYSICAL"

    @property
    def canonical_id(self) -> str:
        return f"OPT:US:{self.root}:{self.expiry.isoformat()}:{self.right}:{self.strike_millis}"

    def to_moomoo(self) -> str:
        return f"US.{self.root}{self.expiry:%y%m%d}{self.right}{self.strike_millis}"

    def to_occ(self) -> str:
        return f"{self.root:<6}{self.expiry:%y%m%d}{self.right}{self.strike_millis:08d}"

    def last_regular_trading_close(self) -> datetime:
        trading_day = self.expiry
        if self.root == "SPX":
            # Standard AM-settled SPX ordinarily stops trading on the prior
            # business day. This local guard deliberately uses RTH 16:00 ET.
            trading_day -= timedelta(days=1)
            while trading_day.weekday() >= 5:
                trading_day -= timedelta(days=1)
        return datetime.combine(trading_day, time(16, 0), tzinfo=EASTERN)


def parse_option_contract(code: str, instrument: dict[str, Any] | None = None) -> OptionContract | None:
    raw_occ = str((instrument or {}).get("occ") or "").strip().upper()
    match = OCC_RE.fullmatch(raw_occ) if raw_occ else None
    if match is None:
        match = OPEN_OPTION_RE.fullmatch(str(code or "").strip().upper())
    if match is None:
        return None
    expiry = datetime.strptime(match.group("expiry"), "%y%m%d").date()
    strike = int(match.group("strike"))
    return OptionContract(
        root=match.group("root").strip(),
        expiry=expiry,
        right=match.group("right"),
        strike_millis=strike,
    )


def broker_symbol(broker: str, code: str, instrument: dict[str, Any] | None = None) -> str:
    contract = parse_option_contract(code, instrument)
    if contract is None:
        return str(code or "").strip().upper()
    broker = str(broker or "").lower()
    if broker == "moomoo":
        return contract.to_moomoo()
    if broker == "schwab":
        return contract.to_occ()
    if broker == "webull" and contract.is_spx_family:
        raise ValueError("Webull OpenAPI 官方范围不含指数期权，不能把 SPX/SPXW 当作普通股票期权发送")
    if broker == "ibkr":
        raise ValueError(
            f"IBKR 的 {contract.root} 期权必须按 conid + tradingClass={contract.root} 解析，已阻止模糊 ticker 下单/报价"
        )
    return contract.to_occ()


def expiry_open_guard(
    contract: OptionContract,
    *,
    cutoff_minutes: int,
    now: datetime | None = None,
) -> str | None:
    moment = now or datetime.now(EASTERN)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=EASTERN)
    moment = moment.astimezone(EASTERN)
    close = contract.last_regular_trading_close()
    cutoff = close - timedelta(minutes=max(int(cutoff_minutes), 0))
    if moment >= close:
        return f"{contract.root} 合约已进入或超过最后常规交易时点（{close:%Y-%m-%d %H:%M} ET）"
    if moment >= cutoff:
        return (
            f"{contract.root} 到期风险护栏：距最后常规交易时点不足 {cutoff_minutes} 分钟，"
            "禁止新开仓；TRIM/CLOSE 仍允许"
        )
    return None


def spx_tick_size(contract: OptionContract, price: float) -> Decimal | None:
    if not contract.is_spx_family:
        return None
    return Decimal("0.05") if Decimal(str(price)) < Decimal("3") else Decimal("0.10")


def valid_option_limit_tick(contract: OptionContract, price: float) -> bool:
    tick = spx_tick_size(contract, price)
    if tick is None:
        return True
    value = Decimal(str(price))
    return value > 0 and value % tick == 0
