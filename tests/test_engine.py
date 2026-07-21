from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
import unittest.mock
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from opend_copytrader.api_policy import ApiPacer, RateLimitError
from opend_copytrader.broker_adapters import BrokerError, BrokerRouter, webull_signature
from opend_copytrader.config import AppSettings, SettingsStore
from opend_copytrader.engine import CopyEngine
from opend_copytrader.instruments import (
    broker_symbol,
    expiry_open_guard,
    parse_option_contract,
    valid_option_limit_tick,
)
from opend_copytrader.models import OrderResult, Quote
from opend_copytrader.moomoo_adapter import OpenDError
from opend_copytrader.moonvest import (
    CURSOR_META_KEY,
    MoonvestCredentials,
    MoonvestStream,
    iter_sse_frames,
    moonvest_position_key,
    normalize_trade_payload,
    trade_to_signal,
)
from opend_copytrader.robinhood_mcp import RobinhoodMCPAdapter, parse_mcp_http_body
from opend_copytrader.server import Application
from opend_copytrader.store import LocalStore
from opend_copytrader.tls import bundled_ca_file, trusted_ssl_context


class FakeBroker:
    def __init__(self):
        self.placed: list[dict] = []
        self.sellable = 100.0
        self.quote_calls = 0

    def quote(self, settings, code):
        self.quote_calls += 1
        return Quote(code=code, last=100.0, bid=99.0, ask=101.0, name="Test")

    def sellable_quantity(self, settings, code):
        return self.sellable

    def place_order(self, settings, **kwargs):
        self.placed.append(kwargs)
        return OrderResult(
            order_id=f"order-{len(self.placed)}",
            status="SUBMITTED",
            code=kwargs["code"],
            side=kwargs["side"],
            quantity=kwargs["quantity"],
            price=kwargs["price"],
            raw={"dealt_qty": kwargs["quantity"]},
        )


class CopyEngineTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.settings = SettingsStore(root / "settings.json")
        self.store = LocalStore(root / "test.sqlite3")
        self.broker = FakeBroker()
        self.engine = CopyEngine(self.settings, self.store, self.broker)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def signal(self, external_id="event-1", **overrides):
        payload = {
            "external_id": external_id,
            "actor": "alice",
            "code": "US.AAPL",
            "side": "BUY",
            "quantity": 2,
            "action": "OPEN",
            "signal_price": 100,
            "order_type": "NORMAL",
            "position_side": "BUY",
        }
        payload.update(overrides)
        return payload

    def enable_confirm(self, **extra):
        self.settings.update({
            "mode": "confirm",
            "account_id": 123,
            "trading_env": "SIMULATE",
            "moonvest_follow": ["alice"],
            **extra,
        })

    def test_observe_mode_never_places(self):
        result = self.engine.submit(self.signal())
        self.assertEqual(result["signal"]["status"], "OBSERVED")
        self.assertEqual(self.broker.placed, [])

    def test_event_id_is_persistent_idempotency_key(self):
        first = self.engine.submit(self.signal("stable-event"))
        second = self.engine.submit(self.signal("stable-event"))
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(first["signal"]["id"], second["signal"]["id"])

    def test_only_moonvest_source_is_accepted(self):
        with self.assertRaisesRegex(ValueError, "唯一允许"):
            self.engine.submit(self.signal(), source="manual")

    def test_global_copy_ratio_is_applied(self):
        self.settings.update({"copy_ratio": 0.5})
        result = self.engine.submit(self.signal(quantity=10))
        self.assertEqual(result["signal"]["copied_quantity"], 5)

    def test_notional_limit_rejects(self):
        self.settings.update({"max_order_notional": 50})
        result = self.engine.submit(self.signal())
        self.assertEqual(result["signal"]["status"], "REJECTED")
        self.assertIn("单笔限额", result["signal"]["reason"])

    def test_broker_reject_only_rejects_one_order_and_keeps_execution_armed(self):
        class RejectOnceBroker(FakeBroker):
            def __init__(self):
                super().__init__()
                self.reject_next = True

            def place_order(self, settings, **kwargs):
                if self.reject_next:
                    self.reject_next = False
                    raise BrokerError("订单被券商拒绝")
                return super().place_order(settings, **kwargs)

        self.broker = RejectOnceBroker()
        self.engine = CopyEngine(self.settings, self.store, self.broker)
        self.settings.update({
            "mode": "auto",
            "account_id": 123,
            "trading_env": "SIMULATE",
            "moonvest_follow": ["alice"],
            "max_order_notional": 1_000_000,
        })
        self.engine.arm()

        rejected = self.engine.submit(self.signal("broker-reject", quantity=1))["signal"]
        self.assertEqual(rejected["status"], "REJECTED")
        self.assertTrue(self.engine.state()["armed"])

        placed = self.engine.submit(self.signal("next-order", quantity=1))["signal"]
        self.assertEqual(placed["status"], "PLACED")
        self.assertTrue(self.engine.state()["armed"])

    def test_unexpected_order_exception_still_disarms_execution(self):
        class CrashingBroker(FakeBroker):
            def place_order(self, settings, **kwargs):
                raise RuntimeError("unexpected adapter failure")

        self.broker = CrashingBroker()
        self.engine = CopyEngine(self.settings, self.store, self.broker)
        self.settings.update({
            "mode": "auto",
            "account_id": 123,
            "trading_env": "SIMULATE",
            "moonvest_follow": ["alice"],
            "max_order_notional": 1_000_000,
        })
        self.engine.arm()
        rejected = self.engine.submit(self.signal("unexpected-failure", quantity=1))["signal"]
        self.assertEqual(rejected["status"], "REJECTED")
        self.assertFalse(self.engine.state()["armed"])

    def test_expired_option_open_is_rejected_before_quote(self):
        result = self.engine.submit(self.signal(code="US.SPXW200101C6000000", signal_price=2.95))
        self.assertEqual(result["signal"]["status"], "REJECTED")
        self.assertIn("最后常规交易时点", result["signal"]["reason"])

    def test_confirm_mode_requires_arm_then_places(self):
        self.enable_confirm()
        result = self.engine.submit(self.signal())
        self.assertEqual(result["signal"]["status"], "PENDING")
        self.engine.arm()
        placed = self.engine.approve(result["signal"]["id"])
        self.assertEqual(placed["status"], "PLACED")
        expected = hashlib.sha256(b"event-1").hexdigest()[:20]
        self.assertEqual(self.broker.placed[0]["remark"], f"mv-{expected}")

    def test_sell_guard_prevents_unmanaged_sell(self):
        self.enable_confirm()
        self.broker.sellable = 1
        result = self.engine.submit(self.signal(side="SELL", quantity=2, position_side="SELL"))
        self.engine.arm()
        rejected = self.engine.approve(result["signal"]["id"])
        self.assertEqual(rejected["status"], "REJECTED")
        self.assertIn("可卖数量", rejected["reason"])

    def test_edit_and_expire_are_recorded_without_quote(self):
        edited = self.engine.submit(self.signal(action="EDIT", quantity=8))
        expired = self.engine.submit(self.signal("expired", action="EXPIRE", quantity=0))
        self.assertEqual(edited["signal"]["status"], "OBSERVED")
        self.assertEqual(expired["signal"]["status"], "OBSERVED")
        self.assertEqual(self.broker.quote_calls, 0)

    def test_partial_close_uses_source_delta_and_local_owned_position(self):
        self.enable_confirm(copy_ratio=0.5)
        opening = self.engine.submit(self.signal("open", quantity=4))
        self.engine.arm()
        opened = self.engine.approve(opening["signal"]["id"])
        self.assertEqual(opened["filled_quantity"], 2)
        trim = self.engine.submit(self.signal(
            "trim",
            side="SELL",
            action="TRIM",
            quantity=2,
            signal_price=99,
            position_side="BUY",
        ))
        self.assertEqual(trim["signal"]["status"], "PENDING")
        self.assertEqual(trim["signal"]["copied_quantity"], 1)

    def test_close_exits_all_locally_owned_quantity(self):
        self.enable_confirm()
        opening = self.engine.submit(self.signal("open", quantity=3))
        self.engine.arm()
        self.engine.approve(opening["signal"]["id"])
        closing = self.engine.submit(self.signal(
            "close", side="SELL", action="CLOSE", quantity=0, signal_price=99, position_side="BUY"
        ))
        self.assertEqual(closing["signal"]["copied_quantity"], 3)

    def test_vertical_is_durable_but_non_executable(self):
        result = self.engine.submit(self.signal(
            non_executable_reason="vertical 已记录，当前执行层仅支持单腿",
        ))
        self.assertEqual(result["signal"]["status"], "OBSERVED")
        self.assertEqual(self.broker.quote_calls, 0)


def stock_event(event_id="evt-001", action="opened", **overrides):
    payload = {
        "id": event_id,
        "actor": "alice",
        "action": action,
        "symbol": "NVDA",
        "asset_type": "stock",
        "side": "buy",
        "kind": "single",
        "status": "open",
        "expiry": None,
        "legs": [],
        "qty": 10,
        "entry_price": 100.0,
        "qty_added": None,
        "qty_closed": None,
        "changes": [],
        "exit_price": None,
        "realized_pnl": None,
        "subscriber_only": False,
        "note": None,
    }
    payload.update(overrides)
    return payload


class MoonvestMappingTest(unittest.TestCase):
    def test_sse_parser_ignores_keepalive_and_joins_data_lines(self):
        frames = list(iter_sse_frames([
            b": keepalive\n",
            b"event: trade\n",
            b"id: evt-1\n",
            b"data: {\"id\":\n",
            b"data: \"evt-1\"}\n",
            b"\n",
        ]))
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0].event, "trade")
        self.assertEqual(frames[0].event_id, "evt-1")
        self.assertIn("\n", frames[0].data)

    def test_all_actions_normalize_and_map(self):
        cases = {
            "opened": "OPEN",
            "added_to": "ADD",
            "partially_closed": "TRIM",
            "closed": "CLOSE",
            "edited": "EDIT",
            "expired": "EXPIRE",
        }
        for index, (action, expected) in enumerate(cases.items()):
            overrides = {}
            if action == "added_to": overrides = {"qty_added": 2, "qty": 12, "entry_price": 101}
            if action == "partially_closed": overrides = {"qty_closed": 2, "qty": 8, "exit_price": 110}
            if action == "closed": overrides = {"qty": 0, "status": "closed", "exit_price": 110}
            if action == "expired": overrides = {"qty": 0, "status": "expired"}
            normalized = normalize_trade_payload(stock_event(f"evt-{index}", action, **overrides))
            self.assertEqual(trade_to_signal(normalized)["action"], expected)

    def test_option_single_builds_structured_occ_contract(self):
        payload = normalize_trade_payload({
            **stock_event(),
            "asset_type": "option",
            "expiry": "2026-09-18",
            "legs": [{"strike": 150, "right": "call", "side": "buy"}],
        })
        signal = trade_to_signal(payload)
        self.assertEqual(signal["code"], "US.NVDA260918C150000")
        self.assertEqual(signal["instrument"]["occ"], "NVDA  260918C00150000")

    def test_vertical_is_preserved_and_marked_non_executable(self):
        payload = normalize_trade_payload({
            **stock_event(),
            "asset_type": "option",
            "kind": "vertical",
            "expiry": "2026-09-18",
            "legs": [
                {"strike": 150, "right": "call", "side": "buy"},
                {"strike": 155, "right": "call", "side": "sell"},
            ],
        })
        signal = trade_to_signal(payload)
        self.assertIn("vertical", signal["non_executable_reason"])
        self.assertEqual(len(signal["instrument"]["legs"]), 2)

    def test_frame_id_must_match_payload_id(self):
        with self.assertRaisesRegex(Exception, "不一致"):
            normalize_trade_payload(stock_event("one"), "two")


class FakeCredentials:
    def api_key(self):
        return "test-key"

    def status(self):
        return {"api_key_configured": True, "credential_source": "test"}


class FakeStreamResponse:
    def __init__(self, lines):
        self.lines = lines
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True
        return False

    def __iter__(self):
        return iter(self.lines)

    def close(self):
        self.closed = True


class MoonvestStreamTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.settings = SettingsStore(root / "settings.json")
        self.settings.update({"moonvest_follow": ["alice", "bob"]})
        self.store = LocalStore(root / "events.sqlite3")
        self.broker = FakeBroker()
        self.engine = CopyEngine(self.settings, self.store, self.broker)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    @staticmethod
    def event_lines(payload):
        encoded = json.dumps(payload, separators=(",", ":")).encode()
        return [b": keepalive\n", b"event: trade\n", f"id: {payload['id']}\n".encode(), b"data: " + encoded + b"\n", b"\n"]

    def test_last_event_id_header_replay_and_dedup(self):
        requests = []
        payload = stock_event()

        def opener(request, timeout=0):
            requests.append(request)
            return FakeStreamResponse(self.event_lines(payload))

        self.store.set_meta(CURSOR_META_KEY, "older")
        stream = MoonvestStream(self.settings, self.store, self.engine, FakeCredentials(), opener=opener)
        stream._session("test-key")
        stream._session("test-key")
        self.assertEqual(requests[0].headers.get("Last-event-id"), "older")
        self.assertIn("follow=alice&follow=bob", requests[0].full_url)
        self.assertEqual(len(self.store.list_signals()), 1)
        self.assertEqual(self.store.get_meta(CURSOR_META_KEY), payload["id"])
        self.assertEqual(len(self.store.list_moonvest_positions()), 1)

    def test_since_query_cursor_mode(self):
        requests = []

        def opener(request, timeout=0):
            requests.append(request)
            return FakeStreamResponse([])

        self.settings.update({"moonvest_cursor_mode": "since"})
        self.store.set_meta(CURSOR_META_KEY, "cursor-9")
        stream = MoonvestStream(self.settings, self.store, self.engine, FakeCredentials(), opener=opener)
        stream._session("test-key")
        self.assertIn("since=cursor-9", requests[0].full_url)
        self.assertIsNone(requests[0].headers.get("Last-event-id"))

    def test_replace_cursor_persists_disarms_and_wakes_replay(self):
        self.settings.update({"moonvest_cursor_mode": "since", "mode": "confirm", "account_id": 123})
        self.engine.arm()
        stream = MoonvestStream(self.settings, self.store, self.engine, FakeCredentials())
        status = stream.replace_cursor(" 6a5ac2000000000000000000 ")
        self.assertEqual(self.store.get_meta(CURSOR_META_KEY), "6a5ac2000000000000000000")
        self.assertEqual(status["cursor"], "6a5ac2000000000000000000")
        self.assertFalse(self.engine.state()["armed"])
        self.assertTrue(stream._wake.is_set())

        cleared = stream.replace_cursor("")
        self.assertEqual(cleared["cursor"], "")
        self.assertEqual(self.store.get_meta(CURSOR_META_KEY), "")

    def test_cursor_change_during_connect_forces_request_rebuild(self):
        requests = []
        stream = None

        def opener(request, timeout=0):
            requests.append(request)
            if len(requests) == 1:
                stream.replace_cursor("cursor-during-connect")
            return FakeStreamResponse([])

        self.settings.update({"moonvest_cursor_mode": "since"})
        stream = MoonvestStream(self.settings, self.store, self.engine, FakeCredentials(), opener=opener)
        self.assertTrue(stream._session("test-key"))
        self.assertFalse(stream._session("test-key"))
        self.assertNotIn("since=", requests[0].full_url)
        self.assertIn("since=cursor-during-connect", requests[1].full_url)

    def test_follow_usernames_are_normalized_to_lowercase(self):
        settings = self.settings.update({"moonvest_follow": ["@BOKUTO", "BoKuTo"]})
        self.assertEqual(settings.moonvest_follow, ["bokuto"])

    def test_resync_marks_state_stale_clears_cursor_and_disarms(self):
        payload = normalize_trade_payload(stock_event("existing"))
        self.store.upsert_moonvest_position(moonvest_position_key(payload), payload)
        self.store.set_meta(CURSOR_META_KEY, "old-cursor")
        self.settings.update({"mode": "confirm", "account_id": 123})
        self.engine.arm()
        called = []
        lines = [
            b"event: resync\n",
            b"data: {\"control\":\"resync\",\"reason\":\"cursor_invalid_or_expired\"}\n",
            b"\n",
        ]
        stream = MoonvestStream(
            self.settings,
            self.store,
            self.engine,
            FakeCredentials(),
            opener=lambda request, timeout=0: FakeStreamResponse(lines),
            resync_handler=lambda: called.append(True) or {"position_count": 1},
        )
        stream._session("test-key")
        self.assertEqual(self.store.get_meta(CURSOR_META_KEY), "")
        self.assertTrue(stream.status()["resync_required"])
        self.assertFalse(self.engine.state()["armed"])
        self.assertEqual(self.store.list_moonvest_positions()[0]["stale"], 1)
        self.assertEqual(called, [True])


class CredentialTest(unittest.TestCase):
    def setUp(self):
        # 宿主 shell 可能导出了真实的 MOONVEST_API_KEY；测试必须与之隔离，
        # 否则环境覆盖优先级会掩盖钥匙串行为，还会把真实 key 打进断言输出。
        patcher = unittest.mock.patch.dict(os.environ)
        patcher.start()
        self.addCleanup(patcher.stop)
        os.environ.pop("MOONVEST_API_KEY", None)

    def test_api_key_uses_keychain_without_entering_settings(self):
        class MemoryKeychain:
            def __init__(self): self.values = {}
            def get(self, broker, name): return self.values.get((broker, name))
            def set(self, broker, name, value): self.values[(broker, name)] = value
            def delete(self, broker, name): self.values.pop((broker, name), None)

        keychain = MemoryKeychain()
        credentials = MoonvestCredentials(keychain)
        credentials.save("secret-test-key")
        self.assertEqual(credentials.api_key(), "secret-test-key")
        self.assertTrue(credentials.status()["api_key_configured"])
        settings = AppSettings().public_dict()
        self.assertNotIn("api_key", settings)
        credentials.clear()
        self.assertIsNone(credentials.api_key())


class BrokerAdapterTest(unittest.TestCase):
    def test_moomoo_order_rejection_is_normalized_as_broker_error(self):
        class RejectingOpenD:
            def place_order(self, settings, **kwargs):
                raise OpenDError("模拟交易暂不支持夜盘时段")

        router = BrokerRouter()
        router.moomoo = RejectingOpenD()
        settings = AppSettings(broker="moomoo", account_id=123, moonvest_follow=["alice"])
        with self.assertRaisesRegex(BrokerError, "模拟交易暂不支持夜盘时段"):
            router.place_order(
                settings,
                code="US.MU",
                side="BUY",
                quantity=10,
                price=848.95,
                order_type="NORMAL",
                remark="mv-test",
            )

    def test_webull_signature_matches_official_vector(self):
        signature = webull_signature(
            path="/trade/place_order",
            query={"a1": "webull", "a2": "123", "a3": "xxx", "q1": "yyy"},
            body='{"k1":123,"k2":"this is the api request body","k3":true,"k4":{"foo":[1,2]}}',
            app_key="776da210ab4a452795d74e726ebd74b6",
            app_secret="0f50a2e853334a9aae1a783bee120c1f",
            host="api.webull.com",
            timestamp="2022-01-04T03:55:31Z",
            nonce="48ef5afed43d4d91ae514aaeafbc29ba",
        )
        self.assertEqual(signature, "kvlS6opdZDhEBo5jq40nHYXaLvM=")

    def test_non_numeric_broker_account_has_stable_local_identity(self):
        first = AppSettings(broker="ibkr", ibkr_account_id="DU123456")
        second = AppSettings(broker="ibkr", ibkr_account_id="DU123456")
        first.validate(); second.validate()
        self.assertGreater(first.execution_account_id(), 0)
        self.assertEqual(first.execution_account_id(), second.execution_account_id())

    def test_non_moomoo_execution_is_locked(self):
        router = BrokerRouter()
        settings = AppSettings(broker="schwab", schwab_account_hash="hash-123")
        settings.validate()
        ready, reason = router.execution_status(settings)
        self.assertFalse(ready)
        self.assertIn("OAuth", reason)

    def test_robinhood_account_identity_is_stable(self):
        first = AppSettings(broker="robinhood", robinhood_account_id="agentic-123")
        second = AppSettings(broker="robinhood", robinhood_account_id="agentic-123")
        first.validate(); second.validate()
        self.assertGreater(first.execution_account_id(), 0)
        self.assertEqual(first.execution_account_id(), second.execution_account_id())

    def test_api_pacer_enters_local_cooldown_after_429(self):
        clock = [100.0]
        pacer = ApiPacer(
            "测试券商",
            max_calls=10,
            period_seconds=1,
            clock=lambda: clock[0],
            sleeper=lambda seconds: clock.__setitem__(0, clock[0] + seconds),
        )
        pacer.acquire("/quotes")
        pacer.record_429(12)
        with self.assertRaises(RateLimitError): pacer.acquire("/quotes", max_wait=1)
        self.assertEqual(pacer.status()["cooldown_remaining_seconds"], 12.0)


class RobinhoodMCPTest(unittest.TestCase):
    class MemoryKeychain:
        def __init__(self): self.values = {}
        def get(self, broker, name): return self.values.get((broker, name))
        def set(self, broker, name, value): self.values[(broker, name)] = value
        def delete(self, broker, name): self.values.pop((broker, name), None)

    def test_streamable_http_sse_response_is_parsed(self):
        payload = parse_mcp_http_body(
            b"event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{}}\n\n",
            "text/event-stream",
        )
        self.assertEqual(payload["id"], 7)

    def test_only_agentic_robinhood_account_is_selectable(self):
        adapter = RobinhoodMCPAdapter(self.MemoryKeychain())
        adapter._call_tool = lambda name, arguments=None: {  # type: ignore[method-assign]
            "accounts": [
                {"account_number": "regular-1", "type": "individual", "agentic_allowed": False},
                {"account_number": "agentic-2", "type": "Agentic", "agentic_allowed": True},
            ]
        }
        accounts = adapter.accounts(AppSettings())
        self.assertEqual(len(accounts), 2)
        self.assertFalse(accounts[0]["selectable"])
        self.assertTrue(accounts[1]["selectable"])

    def test_equity_order_is_reviewed_before_it_is_placed(self):
        adapter = RobinhoodMCPAdapter(self.MemoryKeychain())
        calls = []

        def fake_call(name, arguments=None):
            calls.append((name, arguments))
            if name == "place_equity_order":
                return {"order_id": "rh-order-1", "status": "queued"}
            return {"warnings": []}

        adapter._call_tool = fake_call  # type: ignore[method-assign]
        result = adapter.place_order(
            AppSettings(broker="robinhood", robinhood_account_id="agentic-2"),
            code="US.AAPL",
            side="BUY",
            quantity=2,
            price=201.25,
            order_type="NORMAL",
            remark="mv-event",
        )
        self.assertEqual([call[0] for call in calls], ["review_equity_order", "place_equity_order"])
        self.assertEqual(calls[0][1]["limit_price"], 201.25)
        self.assertEqual(result.order_id, "rh-order-1")


class InstrumentPolicyTest(unittest.TestCase):
    def test_spx_and_spxw_remain_distinct(self):
        spx = parse_option_contract("", {"occ": "SPX   260717C06000000"})
        spxw = parse_option_contract("", {"occ": "SPXW  260717C06000000"})
        self.assertIsNotNone(spx); self.assertIsNotNone(spxw)
        self.assertNotEqual(spx.canonical_id, spxw.canonical_id)
        self.assertEqual(broker_symbol("schwab", spxw.to_moomoo()), "SPXW  260717C06000000")

    def test_spx_tick_and_expiry_cutoff_rules(self):
        contract = parse_option_contract("US.SPXW260717C6000000")
        self.assertTrue(valid_option_limit_tick(contract, 2.95))
        self.assertFalse(valid_option_limit_tick(contract, 2.97))
        reason = expiry_open_guard(
            contract,
            cutoff_minutes=60,
            now=datetime(2026, 7, 17, 15, 30, tzinfo=ZoneInfo("America/New_York")),
        )
        self.assertIn("禁止新开仓", reason)


class SharedAppRegressionTest(unittest.TestCase):
    def test_account_discovery_can_target_an_unsaved_broker(self):
        saved = AppSettings(broker="moomoo")
        app = Application.__new__(Application)
        app.settings = SimpleNamespace(get=lambda: AppSettings(**saved.public_dict()))
        discovered = []
        app.adapter = SimpleNamespace(accounts=lambda settings: discovered.append(settings) or [])

        self.assertEqual(app.discover_accounts("robinhood"), [])
        self.assertEqual(discovered[0].broker, "robinhood")
        self.assertEqual(saved.broker, "moomoo")
        with self.assertRaisesRegex(ValueError, "券商只能选择"):
            app.discover_accounts("unknown-broker")

    def test_settings_ui_uses_explicit_save_click_and_selected_broker_discovery(self):
        root = Path(__file__).resolve().parents[1]
        javascript = (root / "opend_copytrader" / "static" / "app.js").read_text(encoding="utf-8")
        html = (root / "opend_copytrader" / "static" / "index.html").read_text(encoding="utf-8")

        self.assertIn('/api/accounts?broker=${encodeURIComponent(broker)}', javascript)
        self.assertNotIn("event.submitter", javascript)
        self.assertIn('$("#save-settings").addEventListener("click"', javascript)
        save_button = next(line for line in html.splitlines() if 'id="save-settings"' in line)
        self.assertIn('type="button"', save_button)

    def test_share_build_uses_bundled_ca_for_python_https(self):
        root = Path(__file__).resolve().parents[1]
        ca_file = bundled_ca_file()
        self.assertIsNotNone(ca_file)
        self.assertTrue(Path(ca_file).is_file())
        context = trusted_ssl_context()
        self.assertTrue(context.check_hostname)
        self.assertGreater(context.cert_store_stats()["x509_ca"], 100)

        swift = (root / "packaging" / "MacApp.swift").read_text(encoding="utf-8")
        build_script = (root / "scripts" / "build_macos_app.sh").read_text(encoding="utf-8")
        self.assertIn('environment["SSL_CERT_FILE"] = caFile.path', swift)
        self.assertIn('environment["REQUESTS_CA_BUNDLE"] = caFile.path', swift)
        self.assertIn("--collect-data certifi", build_script)


if __name__ == "__main__":
    unittest.main()
