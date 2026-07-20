"""兼容 facade：实现已拆分到 opend_copytrader.brokers 包。

历史导入路径（engine / moonvest / server / tests）继续有效。
"""

from __future__ import annotations

from .brokers import (
    BROKER_LABELS,
    BrokerError,
    BrokerRouter,
    IBKRClientPortalAdapter,
    KeychainStore,
    SchwabAdapter,
    WebullAdapter,
    webull_signature,
)

__all__ = [
    "BROKER_LABELS",
    "BrokerError",
    "BrokerRouter",
    "IBKRClientPortalAdapter",
    "KeychainStore",
    "SchwabAdapter",
    "WebullAdapter",
    "webull_signature",
]
