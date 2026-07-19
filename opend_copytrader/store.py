from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import default_data_dir
from .models import CopySignal, RiskDecision, utc_now


class LocalStore:
    def __init__(self, path: Path | None = None):
        self.path = path or (default_data_dir() / "copytrader.sqlite3")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._migrate()

    def _migrate(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    source TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    leader TEXT NOT NULL DEFAULT '',
                    code TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    copied_quantity REAL NOT NULL DEFAULT 0,
                    signal_price REAL,
                    execution_price REAL,
                    order_type TEXT NOT NULL,
                    notional REAL NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL DEFAULT '',
                    broker_order_id TEXT NOT NULL DEFAULT '',
                    note TEXT NOT NULL DEFAULT '',
                    raw_json TEXT NOT NULL DEFAULT '{}',
                    UNIQUE(source, external_id)
                );

                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    level TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    message TEXT NOT NULL,
                    signal_id TEXT,
                    details_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS moonvest_positions (
                    position_key TEXT PRIMARY KEY,
                    actor TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    asset_type TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    side TEXT NOT NULL,
                    status TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    expiry TEXT,
                    updated_event_id TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    stale INTEGER NOT NULL DEFAULT 0,
                    payload_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_signals_created ON signals(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status);
                CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_moonvest_positions_actor
                    ON moonvest_positions(actor, status, updated_at DESC);
                """
            )
            existing = {
                str(row["name"])
                for row in self._conn.execute("PRAGMA table_info(signals)").fetchall()
            }
            additions = {
                "action": "TEXT NOT NULL DEFAULT 'ORDER'",
                "fraction": "REAL",
                "filled_quantity": "REAL NOT NULL DEFAULT 0",
                "execution_account_id": "INTEGER NOT NULL DEFAULT 0",
                "execution_firm": "TEXT NOT NULL DEFAULT ''",
                "execution_env": "TEXT NOT NULL DEFAULT ''",
            }
            for name, definition in additions.items():
                if name not in existing:
                    self._conn.execute(f"ALTER TABLE signals ADD COLUMN {name} {definition}")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def insert_signal(self, signal: CopySignal) -> tuple[dict[str, Any], bool]:
        now = utc_now()
        signal_id = uuid.uuid4().hex
        with self._lock, self._conn:
            try:
                self._conn.execute(
                    """
                    INSERT INTO signals (
                        id, created_at, updated_at, source, external_id, leader,
                        code, side, quantity, action, fraction, signal_price,
                        order_type, status, note, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'RECEIVED', ?, ?)
                    """,
                    (
                        signal_id,
                        now,
                        now,
                        signal.source,
                        signal.external_id,
                        signal.leader,
                        signal.code,
                        signal.side,
                        signal.quantity,
                        signal.action,
                        None,
                        signal.signal_price,
                        signal.order_type,
                        signal.note,
                        json.dumps(signal.raw, ensure_ascii=False, default=str),
                    ),
                )
                row = self.get_signal(signal_id)
                return row or {}, True
            except sqlite3.IntegrityError:
                row = self._conn.execute(
                    "SELECT * FROM signals WHERE source = ? AND external_id = ?",
                    (signal.source, signal.external_id),
                ).fetchone()
                return dict(row) if row else {}, False

    def update_signal(self, signal_id: str, **fields: Any) -> dict[str, Any] | None:
        allowed = {
            "copied_quantity",
            "execution_price",
            "notional",
            "status",
            "reason",
            "broker_order_id",
            "filled_quantity",
            "execution_account_id",
            "execution_firm",
            "execution_env",
        }
        clean = {key: value for key, value in fields.items() if key in allowed}
        if not clean:
            return self.get_signal(signal_id)
        clean["updated_at"] = utc_now()
        names = ", ".join(f"{key} = ?" for key in clean)
        with self._lock, self._conn:
            self._conn.execute(
                f"UPDATE signals SET {names} WHERE id = ?",
                (*clean.values(), signal_id),
            )
        return self.get_signal(signal_id)

    def set_decision(self, signal_id: str, decision: RiskDecision, status: str) -> dict[str, Any] | None:
        return self.update_signal(
            signal_id,
            copied_quantity=decision.quantity,
            execution_price=decision.execution_price,
            notional=decision.notional,
            status=status,
            reason=decision.reason,
        )

    def get_signal(self, signal_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM signals WHERE id = ?", (signal_id,)).fetchone()
            return dict(row) if row else None

    def has_signal(self, source: str, external_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM signals WHERE source = ? AND external_id = ? LIMIT 1",
                (source, external_id),
            ).fetchone()
        return row is not None

    def list_signals(self, limit: int = 100, status: str | None = None) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._lock:
            if status:
                rows = self._conn.execute(
                    "SELECT * FROM signals WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                    (status, limit),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM signals ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
        return [dict(row) for row in rows]

    def reconcile_orders(self, orders: list[dict[str, Any]]) -> int:
        changed = 0
        with self._lock, self._conn:
            for order in orders:
                order_id = str(order.get("order_id") or order.get("order_id_ex") or "").strip()
                if not order_id:
                    continue
                try:
                    dealt = max(float(order.get("dealt_qty") or 0), 0.0)
                except (TypeError, ValueError):
                    dealt = 0.0
                order_status = str(order.get("order_status") or "").upper()
                status = "FILLED" if order_status in {"FILLED_ALL", "FILLED"} else None
                cursor = self._conn.execute(
                    """
                    UPDATE signals
                    SET filled_quantity = ?,
                        status = CASE WHEN ? IS NULL THEN status ELSE ? END,
                        updated_at = ?
                    WHERE broker_order_id = ?
                      AND (filled_quantity != ? OR (? IS NOT NULL AND status != ?))
                    """,
                    (dealt, status, status, utc_now(), order_id, dealt, status, status),
                )
                changed += int(cursor.rowcount or 0)
        return changed

    def managed_position(
        self,
        *,
        leader: str,
        code: str,
        account_id: int,
        firm: str,
        trading_env: str,
    ) -> dict[str, float]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN side='BUY' THEN filled_quantity ELSE 0 END), 0) AS bought,
                    COALESCE(SUM(CASE WHEN side='SELL' THEN filled_quantity ELSE 0 END), 0) AS sold
                FROM signals
                WHERE leader = ? AND code = ?
                  AND execution_account_id = ? AND execution_firm = ? AND execution_env = ?
                """,
                (leader, code, int(account_id), firm, trading_env),
            ).fetchone()
        bought = float(row["bought"] if row else 0)
        sold = float(row["sold"] if row else 0)
        net = bought - sold
        return {
            "bought": bought,
            "sold": sold,
            "net": net,
            "remaining": abs(net),
            "direction": "BUY" if net > 0 else "SELL" if net < 0 else "",
        }

    def upsert_moonvest_position(self, position_key: str, payload: dict[str, Any]) -> None:
        event_id = str(payload.get("id") or "").strip()
        if not position_key or not event_id:
            raise ValueError("Moonvest position key 与事件 id 不能为空")
        try:
            quantity = max(float(payload.get("qty") or 0), 0.0)
        except (TypeError, ValueError) as exc:
            raise ValueError("Moonvest 持仓数量无效") from exc
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO moonvest_positions (
                    position_key, actor, symbol, asset_type, kind, side, status,
                    quantity, expiry, updated_event_id, updated_at, stale, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                ON CONFLICT(position_key) DO UPDATE SET
                    actor=excluded.actor,
                    symbol=excluded.symbol,
                    asset_type=excluded.asset_type,
                    kind=excluded.kind,
                    side=excluded.side,
                    status=excluded.status,
                    quantity=excluded.quantity,
                    expiry=excluded.expiry,
                    updated_event_id=excluded.updated_event_id,
                    updated_at=excluded.updated_at,
                    stale=0,
                    payload_json=excluded.payload_json
                """,
                (
                    position_key,
                    str(payload.get("actor") or ""),
                    str(payload.get("symbol") or "").upper(),
                    str(payload.get("asset_type") or ""),
                    str(payload.get("kind") or ""),
                    str(payload.get("side") or ""),
                    str(payload.get("status") or ""),
                    quantity,
                    payload.get("expiry"),
                    event_id,
                    utc_now(),
                    json.dumps(payload, ensure_ascii=False, default=str),
                ),
            )

    def mark_moonvest_positions_stale(self) -> int:
        with self._lock, self._conn:
            cursor = self._conn.execute(
                "UPDATE moonvest_positions SET stale = 1 WHERE stale = 0"
            )
        return int(cursor.rowcount or 0)

    def list_moonvest_positions(self, limit: int = 100) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT position_key, actor, symbol, asset_type, kind, side, status,
                       quantity, expiry, updated_event_id, updated_at, stale
                FROM moonvest_positions
                ORDER BY CASE status WHEN 'open' THEN 0 ELSE 1 END, updated_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def daily_notional(self) -> float:
        today = datetime.now(timezone.utc).date().isoformat()
        with self._lock:
            row = self._conn.execute(
                """
                SELECT COALESCE(SUM(notional), 0) AS total
                FROM signals
                WHERE substr(created_at, 1, 10) = ? AND status IN ('PLACED', 'FILLED')
                """,
                (today,),
            ).fetchone()
            return float(row["total"] if row else 0)

    def event(
        self,
        kind: str,
        message: str,
        *,
        level: str = "info",
        signal_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO events (id, created_at, level, kind, message, signal_id, details_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    uuid.uuid4().hex,
                    utc_now(),
                    level,
                    kind,
                    message,
                    signal_id,
                    json.dumps(details or {}, ensure_ascii=False, default=str),
                ),
            )

    def list_events(self, limit: int = 100) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            try:
                item["details"] = json.loads(item.pop("details_json"))
            except Exception:
                item["details"] = {}
            result.append(item)
        return result

    def get_meta(self, key: str, default: str = "") -> str:
        with self._lock:
            row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
            return str(row["value"]) if row else default

    def set_meta(self, key: str, value: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
