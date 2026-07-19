from __future__ import annotations

import json
import mimetypes
import signal
import socket
import threading
import urllib.parse
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .broker_adapters import BrokerRouter
from .config import SettingsStore
from .engine import CopyEngine
from .moonvest import MoonvestCredentials, MoonvestStream
from .store import LocalStore


STATIC_DIR = Path(__file__).with_name("static")
LOCAL_HEADER = "moonvest"


class Application:
    def __init__(self, *, settings_path: Path | None = None, database_path: Path | None = None):
        self.settings = SettingsStore(settings_path)
        self.store = LocalStore(database_path)
        self.adapter = BrokerRouter()
        self.engine = CopyEngine(self.settings, self.store, self.adapter)
        self.moonvest_credentials = MoonvestCredentials(self.adapter.keychain)
        self.moonvest_stream = MoonvestStream(
            self.settings,
            self.store,
            self.engine,
            self.moonvest_credentials,
            resync_handler=self._broker_resync_snapshot,
        )
        self.moonvest_stream.start()

    def close(self) -> None:
        self.moonvest_stream.stop()
        self.adapter.close()
        self.store.close()

    def _broker_resync_snapshot(self) -> dict[str, Any]:
        settings = self.settings.get()
        if not settings.selected_account_id():
            return {"account_configured": False}
        portfolio = self.adapter.portfolio(settings)
        snapshot = {
            "account_configured": True,
            "broker": settings.broker,
            "position_count": len(portfolio.get("positions") or []),
            "positions": portfolio.get("positions") or [],
        }
        self.store.set_meta(
            "moonvest.last_resync_broker_snapshot",
            json.dumps(snapshot, ensure_ascii=False, default=str),
        )
        return {key: value for key, value in snapshot.items() if key != "positions"}

    def dashboard(self) -> dict[str, Any]:
        settings = self.settings.get()
        return {
            "health": self.adapter.health(settings),
            "settings": settings.public_dict(),
            "engine": self.engine.state(),
            "moonvest": self.moonvest_stream.status(),
            "moonvest_positions": self.store.list_moonvest_positions(100),
            "daily_notional": self.store.daily_notional(),
            "signals": self.store.list_signals(60),
            "events": self.store.list_events(50),
        }


class LocalServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], app: Application):
        self.app = app
        super().__init__(address, RequestHandler)


class RequestHandler(BaseHTTPRequestHandler):
    server: LocalServer
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:  # noqa: N802
        if not self._local_host():
            return self._json({"error": "invalid host"}, HTTPStatus.MISDIRECTED_REQUEST)
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)
        try:
            if path == "/api/robinhood/oauth/callback":
                code = str(query.get("code", [""])[0])
                state = str(query.get("state", [""])[0])
                error = str(query.get("error_description", query.get("error", [""]))[0])
                if error:
                    raise ValueError(f"Robinhood 拒绝授权：{error}")
                if not code or not state:
                    raise ValueError("Robinhood OAuth 回调缺少 code 或 state")
                self.server.app.adapter.complete_robinhood_oauth(code, state)
                self.server.app.engine.disarm()
                self.server.app.store.event(
                    "robinhood.oauth_connected", "Robinhood Trading MCP OAuth 已连接"
                )
                return self._redirect("/api/robinhood/oauth/complete")
            if path == "/api/robinhood/oauth/complete":
                return self._html(
                    "<!doctype html><meta charset='utf-8'><title>Moonvest</title>"
                    "<style>body{margin:0;display:grid;place-items:center;height:100vh;background:#070e1c;color:#edf3fb;font:16px -apple-system}div{text-align:center}p{color:#8291a7}</style>"
                    "<div><h1>Robinhood 已连接</h1><p>可以关闭此窗口并返回 Moonvest。</p></div>"
                )
            if path == "/api/dashboard":
                return self._json(self.server.app.dashboard())
            if path == "/api/health":
                return self._json(self.server.app.adapter.health(self.server.app.settings.get()))
            if path == "/api/settings":
                return self._json(self.server.app.settings.get().public_dict())
            if path == "/api/engine":
                return self._json(self.server.app.engine.state())
            if path == "/api/moonvest/status":
                return self._json(self.server.app.moonvest_stream.status())
            if path == "/api/moonvest/positions":
                return self._json({"positions": self.server.app.store.list_moonvest_positions(300)})
            if path == "/api/robinhood/status":
                return self._json(self.server.app.adapter.robinhood.health(self.server.app.settings.get()))
            if path == "/api/accounts":
                return self._json(
                    {"accounts": self.server.app.adapter.accounts(self.server.app.settings.get())}
                )
            if path == "/api/broker/credentials":
                broker = str(
                    query.get("broker", [self.server.app.settings.get().broker])[0]
                ).strip().lower()
                return self._json(
                    {"broker": broker, "status": self.server.app.adapter.credential_status(broker)}
                )
            if path == "/api/portfolio":
                return self._json(self.server.app.adapter.portfolio(self.server.app.settings.get()))
            if path == "/api/orders":
                orders = self.server.app.adapter.orders(self.server.app.settings.get())
                self.server.app.store.reconcile_orders(orders)
                return self._json({"orders": orders})
            if path == "/api/signals":
                status = query.get("status", [None])[0]
                limit = int(query.get("limit", ["100"])[0])
                return self._json({"signals": self.server.app.store.list_signals(limit, status)})
            if path == "/api/events":
                limit = int(query.get("limit", ["100"])[0])
                return self._json({"events": self.server.app.store.list_events(limit)})
            return self._static(path)
        except ValueError as exc:
            return self._json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            return self._json({"error": str(exc)}, HTTPStatus.SERVICE_UNAVAILABLE)

    def do_POST(self) -> None:  # noqa: N802
        if not self._local_host():
            return self._json({"error": "invalid host"}, HTTPStatus.MISDIRECTED_REQUEST)
        if self.headers.get("X-Local-App") != LOCAL_HEADER:
            return self._json({"error": "缺少本地应用请求头"}, HTTPStatus.FORBIDDEN)
        path = urllib.parse.urlparse(self.path).path
        try:
            payload = self._body()
            if path == "/api/moonvest/credentials":
                if bool(payload.get("clear")):
                    result = self.server.app.moonvest_credentials.clear()
                    self.server.app.engine.disarm()
                    self.server.app.store.event(
                        "moonvest.credentials_cleared", "已从 macOS 钥匙串清除 Moonvest API key"
                    )
                else:
                    result = self.server.app.moonvest_credentials.save(
                        str(payload.get("api_key") or "")
                    )
                    self.server.app.store.event(
                        "moonvest.credentials_saved", "已将 Moonvest API key 保存到 macOS 钥匙串"
                    )
                self.server.app.moonvest_stream.wake()
                return self._json(result)
            if path == "/api/robinhood/oauth/start":
                authorization_url = self.server.app.adapter.robinhood_authorization_url()
                opened = webbrowser.open(authorization_url)
                return self._json({"started": bool(opened)})
            if path == "/api/robinhood/oauth/disconnect":
                self.server.app.adapter.disconnect_robinhood()
                self.server.app.engine.disarm()
                self.server.app.store.event(
                    "robinhood.oauth_disconnected", "Robinhood Trading MCP OAuth 已断开"
                )
                return self._json({"connected": False})
            if path.startswith("/api/signals/") and path.endswith("/approve"):
                signal_id = path.split("/")[3]
                return self._json({"signal": self.server.app.engine.approve(signal_id)})
            if path.startswith("/api/signals/") and path.endswith("/reject"):
                signal_id = path.split("/")[3]
                reason = str(payload.get("reason") or "操作员拒绝")
                return self._json({"signal": self.server.app.engine.reject(signal_id, reason)})
            if path == "/api/engine/arm":
                if not self.server.app.moonvest_credentials.api_key():
                    raise ValueError("请先配置 Moonvest API key")
                return self._json(
                    self.server.app.engine.arm(
                        manual_unlock_confirmed=bool(payload.get("manual_unlock_confirmed"))
                    )
                )
            if path == "/api/engine/disarm":
                return self._json(self.server.app.engine.disarm())
            if path == "/api/engine/pause":
                return self._json(self.server.app.engine.pause(bool(payload.get("paused", True))))
            if path == "/api/broker/credentials":
                broker = str(
                    payload.get("broker") or self.server.app.settings.get().broker
                ).strip().lower()
                if bool(payload.get("clear")):
                    status = self.server.app.adapter.clear_credentials(broker)
                    self.server.app.engine.disarm()
                    self.server.app.store.event(
                        "broker.credentials_cleared", f"已从 macOS 钥匙串清除 {broker} API 凭证"
                    )
                else:
                    status = self.server.app.adapter.save_credentials(broker, payload)
                    self.server.app.store.event(
                        "broker.credentials_saved", f"已将 {broker} API 凭证保存到 macOS 钥匙串"
                    )
                return self._json({"broker": broker, "status": status})
            return self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)
        except ValueError as exc:
            return self._json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            return self._json({"error": str(exc)}, HTTPStatus.SERVICE_UNAVAILABLE)

    def do_PUT(self) -> None:  # noqa: N802
        if not self._local_host():
            return self._json({"error": "invalid host"}, HTTPStatus.MISDIRECTED_REQUEST)
        if self.headers.get("X-Local-App") != LOCAL_HEADER:
            return self._json({"error": "缺少本地应用请求头"}, HTTPStatus.FORBIDDEN)
        path = urllib.parse.urlparse(self.path).path
        try:
            if path != "/api/settings":
                return self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)
            settings = self.server.app.settings.update(self._body())
            self.server.app.engine.disarm()
            self.server.app.adapter.invalidate()
            self.server.app.moonvest_stream.wake()
            self.server.app.store.event("settings.updated", "设置已保存，订单执行已自动关闭")
            return self._json(settings.public_dict())
        except ValueError as exc:
            return self._json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            return self._json({"error": str(exc)}, HTTPStatus.SERVICE_UNAVAILABLE)

    def _body(self) -> dict[str, Any]:
        size = int(self.headers.get("Content-Length", "0") or 0)
        if size > 1_000_000:
            raise ValueError("请求体过大")
        if size == 0:
            return {}
        raw = self.rfile.read(size)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise ValueError("请求必须是 JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("请求 JSON 必须是对象")
        return payload

    def _local_host(self) -> bool:
        host = (self.headers.get("Host") or "").split(":", 1)[0].strip("[]").lower()
        return host in {"127.0.0.1", "localhost", "::1"}

    def _static(self, path: str) -> None:
        relative = "index.html" if path in {"", "/"} else path.lstrip("/")
        candidate = (STATIC_DIR / relative).resolve()
        if STATIC_DIR.resolve() not in candidate.parents and candidate != STATIC_DIR.resolve():
            return self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)
        if not candidate.is_file():
            if "." not in Path(relative).name:
                candidate = STATIC_DIR / "index.html"
            else:
                return self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)
        data = candidate.read_bytes()
        content_type = mimetypes.guess_type(candidate.name)[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header(
            "Content-Type",
            f"{content_type}; charset=utf-8" if content_type.startswith("text/") else content_type,
        )
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; style-src 'self'; script-src 'self'; img-src 'self' data:; connect-src 'self'",
        )
        self.end_headers()
        self.wfile.write(data)

    def _json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        data = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(data)

    def _html(self, markup: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        data = markup.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(data)

    def _redirect(self, location: str) -> None:
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()


def available_port(host: str, preferred: int) -> int:
    for port in range(preferred, preferred + 20):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, port))
            except OSError:
                continue
            return port
    raise RuntimeError("找不到可用的本地端口")


def run(
    host: str = "127.0.0.1",
    port: int = 8899,
    *,
    open_browser: bool = True,
    ready_file: Path | None = None,
) -> None:
    if host not in {"127.0.0.1", "localhost"}:
        raise ValueError("应用只允许绑定本机回环地址")
    actual_port = available_port(host, port)
    app = Application()
    server = LocalServer((host, actual_port), app)
    url = f"http://127.0.0.1:{actual_port}"
    app.adapter.set_robinhood_redirect_uri(
        f"{url}/api/robinhood/oauth/callback"
    )
    print(f"Moonvest running at {url}", flush=True)
    if ready_file is not None:
        ready_file.parent.mkdir(parents=True, exist_ok=True)
        temporary_ready_file = ready_file.with_name(f".{ready_file.name}.tmp")
        temporary_ready_file.write_text(str(actual_port), encoding="utf-8")
        temporary_ready_file.replace(ready_file)
    if open_browser:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()

    previous_sigterm = signal.getsignal(signal.SIGTERM)

    def stop_server(_signum: int, _frame: object) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop_server)
    try:
        server.serve_forever(poll_interval=0.4)
    except KeyboardInterrupt:
        pass
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)
        server.server_close()
        app.close()
        if ready_file is not None:
            ready_file.unlink(missing_ok=True)
