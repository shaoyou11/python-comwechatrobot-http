from typing import Any, Callable, Dict, Optional, Tuple
from collections import OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import os
import socketserver
import threading
import time

import requests

from .Api import Api
from .BridgeReceiptStore import BridgeReceiptStore
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


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        parsed = float(value)
    except ValueError:
        logger.warning("Invalid float environment variable %s; using %s", name, default)
        return default
    return parsed if parsed > 0 else default


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
        receipt_db_path: Optional[str] = None,
        consumer_id: Optional[str] = None,
        dispatch_workers: Optional[int] = None,
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
            else _env_int("WECHATROBOT_PULL_BATCH_SIZE", 5, minimum=1)
        )
        self.consumer_id = (
            consumer_id
            or os.environ.get("WECHATROBOT_CONSUMER_ID", "efb")
        ).strip()[:128] or "efb"
        self.dispatch_workers = min(
            16,
            dispatch_workers
            if dispatch_workers is not None
            else _env_int("WECHATROBOT_DISPATCH_WORKERS", 4, minimum=1),
        )
        self.receipt_store = None
        if self.configured_message_mode in ("bridge", "auto"):
            receipt_path = (
                receipt_db_path
                or os.environ.get("WECHATROBOT_RECEIPT_DB", ":memory:")
            )
            self.receipt_store = BridgeReceiptStore(
                receipt_path,
                retention_seconds=_env_int(
                    "WECHATROBOT_RECEIPT_RETENTION_SECONDS",
                    7 * 24 * 60 * 60,
                    minimum=1,
                ),
            )

        self._stop_event = threading.Event()
        self._server: Optional[_ThreadingTCPServer] = None
        self._run_thread: Optional[threading.Thread] = None
        self.retry_max_attempts = max(
            0, _env_int("WECHATROBOT_DISPATCH_RETRY_TIMES", 3)
        )
        self.retry_base_seconds = max(
            0.1,
            _env_float("WECHATROBOT_DISPATCH_RETRY_BASE_SECONDS", 2.0),
        )
        self._retry_queue = deque()
        self._login_state: Optional[bool] = None

    def on(self, *event_type: str) -> Callable:
        def deco(func: Callable) -> Callable:
            for _type in event_type:
                Bus.subscribe(_type, func)
            return func
        return deco

    def _receive_callback(self, msg: Dict[str, Any]):
        raw_type = msg.get("type")
        msg["type"] = MESSAGE_TYPES.get(raw_type, "unhandled{}".format(raw_type))

        message = str(msg.get("message") or "")
        sender = str(msg.get("sender") or "")

        def emit_required(event: str):
            if not Bus.has_subscribers(event):
                raise RuntimeError("No handler subscribed for {}".format(event))
            return Bus.emit(event, msg)

        if msg["type"] == "friendrequest":
            return emit_required("frdver_msg")
        elif msg["type"] == "card":
            return emit_required("card_msg")
        elif '<sysmsg type="revokemsg">' in message:
            return emit_required("revoke_msg")
        elif "微信转账" in message and "<paysubtype>1</paysubtype>" in message:
            return emit_required("transfer_msg")
        elif msg.get("isSendMsg") == 1:
            if msg.get("isSendByPhone") == 1:
                return emit_required("self_msg")
            else:
                return emit_required("sent_msg")
        elif "chatroom" in sender:
            return emit_required("group_msg")
        else:
            return emit_required("friend_msg")

    @staticmethod
    def _source_chat_key(msg: Dict[str, Any]) -> str:
        for key in (
            "chat_id", "chatId", "conversation_id", "conversationId",
            "roomid", "room_id", "group_id", "chatroom_id",
            "from_user", "fromUser", "sender", "wxid", "chat",
        ):
            value = str(msg.get(key) or "").strip()
            if value:
                return value
        return "unknown"

    def _dispatch_delivery_group(self, items):
        ack_ids = []
        nack_ids = []
        failure_reasons = []
        for msg, delivery_id, dedup_key in items:
            try:
                if self.receipt_store is None or not self.receipt_store.is_processed(dedup_key):
                    self._receive_callback(msg)
                    if self.receipt_store is not None:
                        self.receipt_store.record_processed(dedup_key)
                ack_ids.append(delivery_id)
            except Exception as exc:
                self.api.invalidate_db_handles()
                logger.exception("Bridge message dispatch failed")
                nack_ids.append(delivery_id)
                failure_reasons.append(type(exc).__name__)
        return ack_ids, nack_ids, failure_reasons

    def _post_delivery_outcome(
        self,
        endpoint: str,
        delivery_ids,
        reason: str = "",
    ) -> bool:
        if not delivery_ids:
            return True
        body = {
            "delivery_ids": list(delivery_ids),
            "consumer_id": self.consumer_id,
        }
        if endpoint == "nack":
            body["reason"] = reason
        try:
            response = requests.post(
                "{}/v1/messages/{}".format(self.bridge_api_base, endpoint),
                json=body,
                timeout=(3, 5),
            )
            response.raise_for_status()
            payload = response.json()
            count = payload.get("acked" if endpoint == "ack" else "nacked")
            if not isinstance(count, int) or count < len(delivery_ids):
                raise ValueError("bridge outcome count mismatch")
            return True
        except (requests.RequestException, ValueError, TypeError) as exc:
            logger.warning("Bridge %s failed: %s", endpoint.upper(), exc)
            return False

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
                    "ack_mode": True,
                    "consumer_id": self.consumer_id,
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
            deliveries = payload.get("deliveries")
            if deliveries is not None:
                if not isinstance(deliveries, list) or len(deliveries) != len(messages):
                    raise ValueError("bridge response deliveries must align with messages")
        except (requests.RequestException, ValueError, TypeError) as exc:
            self.api.invalidate_db_handles()
            if log_failure:
                logger.warning("Bridge pull failed: %s", exc)
            return False

        if deliveries is None:
            for msg in messages:
                if not isinstance(msg, dict):
                    logger.warning("Ignored non-object bridge message")
                    continue
                self._dispatch_with_retry(msg)
            return True

        ack_ids = []
        nack_ids = []
        grouped = OrderedDict()
        for msg, delivery in zip(messages, deliveries):
            if not isinstance(msg, dict):
                logger.warning("Ignored non-object bridge message")
                continue
            if not isinstance(delivery, dict):
                logger.warning("Ignored bridge message with invalid delivery metadata")
                continue
            delivery_id = delivery.get("delivery_id")
            dedup_key = delivery.get("dedup_key")
            if not isinstance(delivery_id, str) or not delivery_id:
                logger.warning("Ignored bridge message without delivery ID")
                continue
            if not isinstance(dedup_key, str) or not dedup_key:
                logger.warning("Ignored bridge message without dedup key")
                nack_ids.append(delivery_id)
                continue
            trace_id = str(delivery.get("trace_id") or "").strip()
            if trace_id:
                msg["_bridge_trace_id"] = trace_id[:12]
            grouped.setdefault(self._source_chat_key(msg), []).append(
                (msg, delivery_id, dedup_key)
            )

        failure_reasons = []
        if grouped:
            with ThreadPoolExecutor(
                max_workers=min(self.dispatch_workers, len(grouped)),
                thread_name_prefix="wechatrobot-chat",
            ) as executor:
                futures = [
                    executor.submit(self._dispatch_delivery_group, items)
                    for items in grouped.values()
                ]
                for future in futures:
                    group_acked, group_nacked, group_reasons = future.result()
                    ack_ids.extend(group_acked)
                    nack_ids.extend(group_nacked)
                    failure_reasons.extend(group_reasons)

        ack_ok = self._post_delivery_outcome("ack", ack_ids)
        nack_ok = self._post_delivery_outcome(
            "nack",
            nack_ids,
            reason=failure_reasons[0] if failure_reasons else "dispatch failed",
        )
        return ack_ok and nack_ok

    def _native_logged_in(self) -> bool:
        try:
            data = self.api.GetSelfInfo().get("data")
            ready = isinstance(data, dict) and bool(data.get("wxId"))
        except Exception:
            ready = False
        if ready != self._login_state:
            self._login_state = ready
            if ready:
                logger.info("Native WeChat login state changed: ready=True")
            else:
                logger.warning("Native WeChat login state changed: ready=False")
        return ready

    def _dispatch_with_retry(self, msg: Dict[str, Any], attempts: int = 0) -> None:
        try:
            self._receive_callback(msg)
            return
        except Exception as exc:
            self.api.invalidate_db_handles()
            next_attempts = attempts + 1
            if not self._native_logged_in():
                delay = min(
                    60.0,
                    self.retry_base_seconds * (2 ** min(next_attempts, 5)),
                )
                self._retry_queue.append(
                    (msg, next_attempts, time.monotonic() + delay)
                )
                logger.warning(
                    "Bridge message deferred until login, retry_in=%.1fs: %s",
                    delay,
                    exc,
                )
                return
            if self.retry_max_attempts <= 0 or next_attempts > self.retry_max_attempts:
                logger.error(
                    "Bridge message dispatch failed permanently, attempts=%s: %s",
                    next_attempts,
                    exc,
                )
                return
            delay = self.retry_base_seconds * (2 ** (next_attempts - 1))
            self._retry_queue.append(
                (msg, next_attempts, time.monotonic() + delay)
            )
            logger.warning(
                "Bridge message dispatch failed, retry %s/%s in %.1fs: %s",
                next_attempts,
                self.retry_max_attempts,
                delay,
                exc,
            )

    def _process_retry_queue(self) -> None:
        while self._retry_queue:
            msg, attempts, due_at = self._retry_queue[0]
            if time.monotonic() < due_at:
                return
            self._retry_queue.popleft()
            self._dispatch_with_retry(msg, attempts)

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
            if not self._native_logged_in():
                self._stop_event.wait(max(1.0, self.pull_wait_ms / 1000))
                continue
            self._process_retry_queue()
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
        self._retry_queue.clear()
        if self.receipt_store is not None:
            self.receipt_store.close()
            self.receipt_store = None

    def get_base_path(self):
        return self.BASE_PATH

    def __getattr__(self, item: str):
        return self.api.exec_command(item)
