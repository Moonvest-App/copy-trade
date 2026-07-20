"""macOS 登录钥匙串存取；API 凭证绝不落入 settings.json。"""

from __future__ import annotations

import subprocess
import threading

from .base import BrokerError


class KeychainStore:
    """Stores API material in the login Keychain, never in settings.json."""

    service_prefix = "app.moonvest.copytrader"

    # security(1) 的"未找到该项"退出码；只有这个结果才允许负缓存。
    _ERR_SEC_ITEM_NOT_FOUND = 44

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._cache: dict[tuple[str, str], str | None] = {}

    @classmethod
    def _service(cls, broker: str) -> str:
        return f"{cls.service_prefix}.{broker}"

    def get(self, broker: str, name: str) -> str | None:
        key = (broker, name)
        with self._lock:
            if key in self._cache:
                return self._cache[key]
        try:
            result = subprocess.run(
                ["/usr/bin/security", "find-generic-password", "-a", name, "-s", self._service(broker), "-w"],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except Exception:
            # 超时/钥匙串锁定等瞬时失败不写缓存，下次调用重试，
            # 避免"重启前一直显示未配置"。
            return None
        if result.returncode == 0:
            value = result.stdout.rstrip("\n")
        elif result.returncode == self._ERR_SEC_ITEM_NOT_FOUND:
            value = None
        else:
            return None
        with self._lock:
            self._cache[key] = value
        return value

    def set(self, broker: str, name: str, value: str) -> None:
        value = str(value or "").strip()
        if not value:
            return
        try:
            result = subprocess.run(
                [
                    "/usr/bin/security",
                    "add-generic-password",
                    "-U",
                    "-a",
                    name,
                    "-s",
                    self._service(broker),
                    "-w",
                    value,
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=8,
            )
        except Exception as exc:
            raise BrokerError(f"无法写入 macOS 钥匙串：{exc}") from exc
        if result.returncode != 0:
            raise BrokerError(f"无法写入 macOS 钥匙串：{result.stderr.strip() or 'security 命令失败'}")
        with self._lock:
            self._cache[(broker, name)] = value

    def delete(self, broker: str, name: str) -> None:
        try:
            subprocess.run(
                ["/usr/bin/security", "delete-generic-password", "-a", name, "-s", self._service(broker)],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
        finally:
            with self._lock:
                self._cache[(broker, name)] = None
