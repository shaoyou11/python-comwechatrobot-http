from typing import Any, Callable, Dict, Optional, Tuple
import json
import logging
import os
import socketserver
import threading
import time

import requests

from .Api import Api
from .Bus import EventBus

Bus = EventBus()
logger = logging.getLogger(__name__)

MESSAGE_TYPES = {
    0: "eventnotify",
    1: "text",
    3: "image",
    9: "scancashmoney",
    34: "voice",
    35: "qqmail",
    37: "friendrequest",
    42: "card",
    43: "video",
    47: "animatedsticker",
    48: "location",
    49: "share",
    50: "voip",
    51: "phone",
    106: "sysnotify",
    1009: "eventnotify",
    1010: "eventnotify",
    2000: "transfer",
    2001: "redpacket",
    2002: "miniprogram",
    2003: "groupinvite",
    2004: "file",
    2005: "revokemsg",
    2006: "groupannouncement",
    10000: "sysmsg",
    10002: "other",
}


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError:
        logger.warning("Invalid integer environment variable %s; using %s", name, default)
        return default
    if parsed < minimum:
        logger.warning("Environment variable %s is below %s; using %s", name, minimum, default)
        return default
    return parsed


class _ThreadingTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class _ReceiveMsgHandler(socketserver.BaseRequestHandler):
    def _handle_frame(self, frame: bytes) -> None:
        try:
            message = json.loads(frame.decode("utf-8"))
            self.server.robot._receive_callback(message)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError, ValueError) as exc:
            logger.warning("Ignored invalid TCP callback payload: %s", exc)
        except Exception:
            logger.exception("TCP message dispatch failed")
        finally:
            try:
                self.request.sendall(b"200 OK")
            except OSError:
                pass

    def handle(self) -> None:
        buffer = b""
        while True:
            try:
                data = self.request.recv(4096)
            except OSError:
                break
            if not data:
                break

            buffer += data
            while b"\n" in buffer:
                frame, buffer = buffer.split(b"\n", 1)
                frame = frame.rstrip(b"\r")
                if frame:
                    self._handle_frame(frame)

        if buffer.strip():
            self._handle_frame(buffer.strip())


class WeChatRobot:
    BASE_PATH = "C:\\Users\\user\\My Documents\\WeChat Files"
    MESSAGE_MODES = frozenset(("tcp", "bridge", "auto"))

    def __init__(
        self,
        ip: str = "0.0.0.0",
        port: int = 23456,
        comwechat_port: int = 18888,
        message_mode: Optional[str] = None,
        bridge_api_base: Optional[str] = None,
        pull_wait_ms: Optional[int] = None,
        pull_batch_size: Optional[int] = None,
    ):
        self.ip = ip
        self.port = port
        self.api = Api(comwechat_port)
        self.url = "http://{}:{}/".format(ip, port)

        configured_mode = message_mode or os.environ.get("WECHATROBOT_MESSAGE_MODE", "tcp")
        configured_mode = configured_mode.strip().lower()
        if configured_mode not in self.MESSAGE_MODES:
            if message_mode is not None:
                raise ValueError(
                    "message_mode must be one of: {}".format(", ".join(sorted(self.MESSAGE_MODES)))
                )
            logger.warning(
                "Invalid WECHATROBOT_MESSAGE_MODE=%s; using tcp",
                configured_mode,
            )
            configured_mode = "tcp"

        self.configured_message_mode = configured_mode
        self.active_message_mode: Optional[str] = None
        self.bridge_api_base = (
            bridge_api_base
            or os.environ.get("WECHATROBOT_BRIDGE_API_BASE", "http://127.0.0.1:19088")
        ).rstrip("/")
        self.pull_wait_ms = (
            pull_wait_ms
            if pull_wait_ms is not None
            else _env_int("WECHATROBOT_PULL_WAIT_MS", 15000, minimum=0)
        )
        self.pull_batch_size = (
            pull_batch_size
            if pull_batch_size is not None
            else _env_int("WECHATROBOT_PULL_BATCH_SIZE", 50, minimum=1)
        )

        self._stop_event = threading.Event()
        self._server: Optional[_ThreadingTCPServer] = None
        self._run_thread: Optional[threading.Thread] = None

    def on(self, *event_type: str) -> Callable:
        def deco(func: Callable) -> Callable:
            for _type in event_type:
                Bus.subscribe(_type, func)
            return func
        return deco

    def _receive_callback(self, msg: Dict[str, Any]) -> None:
        raw_type = msg.get("type")
        msg["type"] = MESSAGE_TYPES.get(raw_type, "unhandled{}".format(raw_type))

        message = str(msg.get("message") or "")
        sender = str(msg.get("sender") or "")
        if msg["type"] == "friendrequest":
            Bus.emit("frdver_msg", msg)
        elif msg["type"] == "card":
            Bus.emit("card_msg", msg)
        elif '<sysmsg type="revokemsg">' in message:
            Bus.emit("revoke_msg", msg)
        elif "微信转账" in message and "<paysubtype>1</paysubtype>" in message:
            Bus.emit("transfer_msg", msg)
        elif msg.get("isSendMsg") == 1:
            if msg.get("isSendByPhone") == 1:
                Bus.emit("self_msg", msg)
            else:
                Bus.emit("sent_msg", msg)
        elif "chatroom" in sender:
            Bus.emit("group_msg", msg)
        else:
            Bus.emit("friend_msg", msg)

    def _pull_once(
        self,
        wait_ms: Optional[int] = None,
        request_timeout: Optional[Tuple[float, float]] = None,
        log_failure: bool = True,
    ) -> bool:
        effective_wait_ms = self.pull_wait_ms if wait_ms is None else wait_ms
        timeout = request_timeout or (3, effective_wait_ms / 1000 + 5)
        try:
            response = requests.post(
                "{}/v1/messages/pull".format(self.bridge_api_base),
                json={
                    "max_items": self.pull_batch_size,
                    "wait_ms": effective_wait_ms,
                },
                timeout=timeout,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("bridge response must be an object")
            messages = payload.get("messages", [])
            if not isinstance(messages, list):
                raise ValueError("bridge response messages must be a list")
        except (requests.RequestException, ValueError, TypeError) as exc:
            if log_failure:
                logger.warning("Bridge pull failed: %s", exc)
            return False

        for msg in messages:
            if not isinstance(msg, dict):
                logger.warning("Ignored non-object bridge message")
                continue
            try:
                self._receive_callback(msg)
            except Exception:
                logger.exception("Bridge message dispatch failed")
        return True

    def _select_message_mode(self) -> str:
        if self.configured_message_mode != "auto":
            return self.configured_message_mode

        bridge_ready = self._pull_once(
            wait_ms=0,
            request_timeout=(1, 2),
            log_failure=False,
        )
        if bridge_ready:
            logger.info("Auto mode selected bridge message transport")
            return "bridge"

        logger.info("Auto mode selected TCP callback message transport")
        return "tcp"

    def _consume_forever(self) -> None:
        retry_delay = 1
        while not self._stop_event.is_set():
            if self._pull_once():
                retry_delay = 1
                continue
            self._stop_event.wait(retry_delay)
            retry_delay = min(retry_delay * 2, 30)

    def _start_bridge(self, main_thread: bool) -> Optional[int]:
        if main_thread:
            try:
                self._consume_forever()
            except KeyboardInterrupt:
                pass
            return None

        self._run_thread = threading.Thread(
            target=self._consume_forever,
            name="wechatrobot-bridge-consumer",
            daemon=True,
        )
        self._run_thread.start()
        return self._run_thread.ident

    def _start_tcp(self, main_thread: bool) -> Optional[int]:
        try:
            server = _ThreadingTCPServer((self.ip, self.port), _ReceiveMsgHandler)
            server.robot = self
            self._server = server

            self.StartMsgHook(port=self.port)
            self.StartImageHook(save_path=self.BASE_PATH)
            self.StartVoiceHook(save_path=self.BASE_PATH)
        except Exception:
            if self._server is not None:
                self._server.server_close()
                self._server = None
            logger.exception("TCP callback startup failed")
            return None

        if main_thread:
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
                self._server = None
            return None

        self._run_thread = threading.Thread(
            target=server.serve_forever,
            name="wechatrobot-tcp-callback",
            daemon=True,
        )
        self._run_thread.start()
        return self._run_thread.ident

    def run(self, main_thread: bool = True) -> Optional[int]:
        if self._run_thread is not None and self._run_thread.is_alive():
            return self._run_thread.ident

        self._stop_event.clear()
        self.active_message_mode = self._select_message_mode()
        if self.active_message_mode == "bridge":
            return self._start_bridge(main_thread)
        return self._start_tcp(main_thread)

    def shutdown(self) -> None:
        self._stop_event.set()

        server = self._server
        if server is not None:
            server.shutdown()
            server.server_close()
            self._server = None

        run_thread = self._run_thread
        if (
            run_thread is not None
            and run_thread is not threading.current_thread()
            and run_thread.is_alive()
        ):
            run_thread.join(timeout=5)
        self._run_thread = None

    def get_base_path(self):
        return self.BASE_PATH

    def __getattr__(self, item: str):
        return self.api.exec_command(item)
