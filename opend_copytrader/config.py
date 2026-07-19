from __future__ import annotations

import hashlib
import json
import os
import re
import threading
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


VALID_FIRMS = {
    "FUTUSECURITIES",
    "FUTUINC",
    "FUTUSG",
    "FUTUAU",
    "FUTUCA",
    "FUTUJP",
    "FUTUMY",
}
VALID_BROKERS = {"moomoo", "ibkr", "webull", "schwab", "robinhood"}
FOLLOW_RE = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")


def default_data_dir() -> Path:
    override = os.environ.get("MOONVEST_DATA_DIR", "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return Path.home() / "Library" / "Application Support" / "Moonvest"


@dataclass(slots=True)
class AppSettings:
    broker: str = "moomoo"
    opend_host: str = "127.0.0.1"
    opend_port: int = 11111
    ibkr_host: str = "127.0.0.1"
    ibkr_port: int = 5000
    ibkr_account_id: str = ""
    webull_environment: str = "production"
    webull_account_id: str = ""
    webull_app_key: str = ""
    schwab_account_hash: str = ""
    schwab_client_id: str = ""
    schwab_callback_url: str = "https://127.0.0.1"
    robinhood_account_id: str = ""
    security_firm: str = "FUTUJP"
    account_id: int = 0
    trading_env: str = "SIMULATE"
    base_currency: str = "USD"
    us_session: str = "RTH"

    mode: str = "observe"  # observe | confirm | auto
    copy_ratio: float = 1.0
    max_order_notional: float = 1_000.0
    max_daily_notional: float = 5_000.0
    max_slippage_pct: float = 3.0
    expiry_guard_enabled: bool = True
    expiry_open_cutoff_minutes: int = 60
    reject_nonconforming_option_ticks: bool = True
    allowed_markets: list[str] = field(default_factory=lambda: ["US"])
    allowed_symbols: list[str] = field(default_factory=list)
    allow_unmanaged_sells: bool = False

    moonvest_follow: list[str] = field(default_factory=list)
    moonvest_cursor_mode: str = "header"  # header | since

    def validate(self) -> None:
        self.broker = self.broker.strip().lower()
        if self.broker not in VALID_BROKERS:
            raise ValueError("券商只能选择 moomoo、Robinhood、IBKR、Webull 或嘉信")
        self.opend_host = self.opend_host.strip()
        if self.opend_host not in {"127.0.0.1", "localhost"}:
            raise ValueError("为保证本地安全，OpenD 主机只能是 127.0.0.1 或 localhost")
        if not 1 <= int(self.opend_port) <= 65535:
            raise ValueError("OpenD 端口无效")
        self.ibkr_host = self.ibkr_host.strip().lower()
        if self.ibkr_host not in {"127.0.0.1", "localhost"}:
            raise ValueError("为防止会话泄露，IBKR Client Portal Gateway 只能连接本机")
        if not 1 <= int(self.ibkr_port) <= 65535:
            raise ValueError("IBKR Gateway 端口无效")
        self.ibkr_account_id = self.ibkr_account_id.strip()
        self.webull_environment = self.webull_environment.strip().lower()
        if self.webull_environment not in {"production", "sandbox"}:
            raise ValueError("Webull 环境只能是 production 或 sandbox")
        self.webull_account_id = self.webull_account_id.strip()
        self.webull_app_key = self.webull_app_key.strip()
        self.schwab_account_hash = self.schwab_account_hash.strip()
        self.schwab_client_id = self.schwab_client_id.strip()
        self.schwab_callback_url = self.schwab_callback_url.strip()
        self.robinhood_account_id = self.robinhood_account_id.strip()
        if self.schwab_callback_url and not self.schwab_callback_url.startswith(
            ("https://", "http://127.0.0.1", "http://localhost")
        ):
            raise ValueError("嘉信回调地址必须使用 HTTPS，或明确指向本机")
        self.security_firm = self.security_firm.strip().upper()
        if self.security_firm not in VALID_FIRMS:
            raise ValueError("券商区域标识无效")
        self.trading_env = self.trading_env.strip().upper()
        if self.trading_env not in {"SIMULATE", "REAL"}:
            raise ValueError("交易环境只能是 SIMULATE 或 REAL")
        self.mode = self.mode.strip().lower()
        if self.mode not in {"observe", "confirm", "auto"}:
            raise ValueError("执行模式只能是 observe、confirm 或 auto")
        if not 0 < float(self.copy_ratio) <= 1:
            raise ValueError("跟单比例必须大于 0% 且不超过 100%")
        if float(self.max_order_notional) <= 0 or float(self.max_daily_notional) <= 0:
            raise ValueError("单笔和单日限额必须大于 0")
        if not 0 <= float(self.max_slippage_pct) <= 100:
            raise ValueError("最大滑点必须在 0% 到 100% 之间")
        if not 15 <= int(self.expiry_open_cutoff_minutes) <= 240:
            raise ValueError("到期日前禁止开仓窗口必须在 15 到 240 分钟之间")
        self.allowed_markets = sorted(
            {str(value).strip().upper() for value in self.allowed_markets if str(value).strip()}
        )
        self.allowed_symbols = sorted(
            {str(value).strip().upper() for value in self.allowed_symbols if str(value).strip()}
        )
        self.base_currency = self.base_currency.strip().upper() or "USD"
        self.us_session = self.us_session.strip().upper() or "RTH"
        self.moonvest_cursor_mode = self.moonvest_cursor_mode.strip().lower()
        if self.moonvest_cursor_mode not in {"header", "since"}:
            raise ValueError("游标传递方式只能是 Last-Event-ID 或 since")
        self.moonvest_follow = self._validated_follows(self.moonvest_follow)

    @staticmethod
    def _validated_follows(values: Any) -> list[str]:
        if not isinstance(values, list):
            raise ValueError("Moonvest follow 配置必须是列表")
        result: list[str] = []
        seen: set[str] = set()
        for raw in values:
            # Moonvest's follow query is case-sensitive and usernames are
            # canonical lowercase. Normalize here so a UI value such as
            # "BOKUTO" cannot silently subscribe to an empty stream.
            username = str(raw or "").strip().removeprefix("@").casefold()
            if not FOLLOW_RE.fullmatch(username):
                raise ValueError("Moonvest 用户名只能包含字母、数字、点、下划线或连字符")
            if username not in seen:
                seen.add(username)
                result.append(username)
        if len(result) > 2:
            raise ValueError("每条 Moonvest SSE 连接最多允许 2 个 follow 用户")
        return result

    def selected_account_id(self) -> str:
        if self.broker == "moomoo":
            return str(self.account_id) if int(self.account_id or 0) > 0 else ""
        if self.broker == "ibkr":
            return self.ibkr_account_id
        if self.broker == "webull":
            return self.webull_account_id
        if self.broker == "schwab":
            return self.schwab_account_hash
        return self.robinhood_account_id

    def execution_account_id(self) -> int:
        """Stable SQLite identity for broker account ids that are not numeric."""
        if self.broker == "moomoo":
            return int(self.account_id or 0)
        account = self.selected_account_id()
        if not account:
            return 0
        digest = hashlib.sha256(f"{self.broker}:{account}".encode("utf-8")).digest()
        return int.from_bytes(digest[:7], "big")

    def execution_firm(self) -> str:
        return self.security_firm if self.broker == "moomoo" else self.broker.upper()

    def public_dict(self) -> dict[str, Any]:
        return asdict(self)


class SettingsStore:
    def __init__(self, path: Path | None = None):
        self.path = path or (default_data_dir() / "settings.json")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._settings = self._load()

    def _load(self) -> AppSettings:
        if not self.path.exists():
            settings = AppSettings()
            settings.validate()
            return settings
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            known = AppSettings.__dataclass_fields__
            settings = AppSettings(**{key: value for key, value in payload.items() if key in known})
            settings.validate()
            return settings
        except Exception:
            # A broken local settings file must never silently arm trading.
            settings = AppSettings()
            settings.validate()
            return settings

    def get(self) -> AppSettings:
        with self._lock:
            return AppSettings(**self._settings.public_dict())

    def update(self, payload: dict[str, Any]) -> AppSettings:
        with self._lock:
            current = self._settings.public_dict()
            for key, value in payload.items():
                if key in AppSettings.__dataclass_fields__:
                    current[key] = value
            next_settings = AppSettings(**current)
            next_settings.validate()
            temporary = self.path.with_suffix(".tmp")
            temporary.write_text(
                json.dumps(next_settings.public_dict(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            os.chmod(temporary, 0o600)
            temporary.replace(self.path)
            self._settings = next_settings
            return self.get()
