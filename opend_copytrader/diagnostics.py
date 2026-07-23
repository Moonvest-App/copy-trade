from __future__ import annotations

import io
import json
import os
import platform
import re
import threading
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import __version__
from .config import default_data_dir


_SENSITIVE_KEYS = {
    "password",
    "api_key",
    "app_key",
    "app_secret",
    "client_id",
    "client_secret",
    "secret",
    "token",
    "access_token",
    "refresh_token",
    "authorization",
    "cookie",
    "account_id",
    "account_number",
    "account_hash",
    "broker_order_id",
    "order_id",
    "signal_id",
    "external_id",
    "event_id",
    "cursor",
    "raw",
    "raw_json",
    "payload_json",
    "idempotency_key",
    "moonvest_follow",
    "actor",
    "leader",
}


def _scrub_text(value: str) -> str:
    text = str(value or "")
    text = re.sub(r"(?i)\bBearer\s+[^\s,;]+", "Bearer <redacted>", text)
    text = re.sub(
        r"(?i)([\"']?(?:api[_-]?key|password|secret|access[_-]?token|refresh[_-]?token|authorization|cookie)[\"']?\s*[:=]\s*[\"']?)[^\"',;\s}\]]+",
        r"\1<redacted>",
        text,
    )
    text = re.sub(r"\bsk_(?:live|test)_[A-Za-z0-9_-]+\b", "<api-key-redacted>", text)
    text = re.sub(r"/Users/[^/\s]+", "/Users/<user>", text)
    text = re.sub(r"(?i)C:\\Users\\[^\\\s]+", r"C:\\Users\\<user>", text)
    text = re.sub(r"(https?://[^\s?]+)\?[^\s]+", r"\1?<query-redacted>", text)
    text = re.sub(r"(?<![\d-])\d{6,}(?![\d-])", "<number-redacted>", text)
    return text[:4_000]


def scrub(value: Any, key: str = "") -> Any:
    if key.casefold() in _SENSITIVE_KEYS:
        return "<redacted>" if value not in (None, "", 0, False) else "<empty>"
    if isinstance(value, dict):
        return {str(child_key): scrub(child, str(child_key)) for child_key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [scrub(child) for child in value[:500]]
    if isinstance(value, str):
        return _scrub_text(value)
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return _scrub_text(str(value))


class DiagnosticRecorder:
    """Small structured log and privacy-filtered support bundle exporter."""

    def __init__(
        self,
        path: Path | None = None,
        *,
        export_dir: Path | None = None,
        max_bytes: int = 2_000_000,
    ) -> None:
        self.path = path or (default_data_dir() / "diagnostics.jsonl")
        self.export_dir = export_dir or Path.home() / "Downloads"
        self.max_bytes = max(100_000, int(max_bytes))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()

    def record(self, event: str, **fields: Any) -> None:
        row = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "event": str(event)[:160],
            **fields,
        }
        line = json.dumps(scrub(row), ensure_ascii=False, separators=(",", ":")) + "\n"
        try:
            with self._lock:
                self._rotate_if_needed(len(line.encode("utf-8")))
                with self.path.open("a", encoding="utf-8") as handle:
                    handle.write(line)
                os.chmod(self.path, 0o600)
        except OSError:
            # Diagnostics are best-effort and must never take down trading state.
            return

    def _rotate_if_needed(self, incoming: int) -> None:
        try:
            current = self.path.stat().st_size
        except OSError:
            return
        if current + incoming <= self.max_bytes:
            return
        previous = self.path.with_suffix(".1.jsonl")
        try:
            previous.unlink(missing_ok=True)
            self.path.replace(previous)
        except OSError:
            pass

    def recent(self, limit: int = 500) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 2_000))
        rows: list[dict[str, Any]] = []
        with self._lock:
            for candidate in (self.path.with_suffix(".1.jsonl"), self.path):
                try:
                    lines = candidate.read_text(encoding="utf-8", errors="replace").splitlines()
                except OSError:
                    continue
                for line in lines:
                    try:
                        item = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(item, dict):
                        rows.append(scrub(item))
        return rows[-limit:]

    def archive(self, context: dict[str, Any]) -> tuple[str, bytes]:
        stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime())
        filename = f"Moonvest-Diagnostics-{stamp}.zip"
        manifest = {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "app_version": __version__,
            "platform": platform.system(),
            "platform_release": platform.release(),
            "architecture": platform.machine(),
            "python": platform.python_version(),
            "privacy": "redacted",
        }
        readme = (
            "Moonvest 脱敏诊断包\n\n"
            "包含：版本、系统架构、连接状态、脱敏设置摘要、最近审计事件和结构化错误日志。\n"
            "不包含：Moonvest API key、券商凭证、OAuth 令牌、Cookie、账户号、订单号、持仓明细或原始配置文件。\n"
            "把整个 ZIP 发给维护者即可；不要自行补充任何密钥。\n"
        )
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("README.txt", readme)
            archive.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
            archive.writestr(
                "diagnostic-context.json",
                json.dumps(scrub(context), ensure_ascii=False, indent=2),
            )
            archive.writestr(
                "diagnostic-log.json",
                json.dumps(self.recent(), ensure_ascii=False, indent=2),
            )
        return filename, buffer.getvalue()

    def export(self, context: dict[str, Any]) -> dict[str, Any]:
        filename, payload = self.archive(context)
        self.export_dir.mkdir(parents=True, exist_ok=True)
        target = self.export_dir / filename
        temporary = target.with_suffix(".tmp")
        temporary.write_bytes(payload)
        try:
            os.chmod(temporary, 0o600)
        except OSError:
            pass
        temporary.replace(target)
        return {"filename": filename, "location": "Downloads", "bytes": len(payload)}
