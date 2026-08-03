import importlib
import json
import socket
import time

import pytest

from wechatrobot import WeChatRobot
from wechatrobot.Api import Api, is_openim_contact_id


robot_module = importlib.import_module("wechatrobot.WeChatRobot")


def message(**overrides):
    payload = {
        "type": 1,
        "message": "hello",
        "sender": "wxid_friend",
        "isSendMsg": 0,
        "isSendByPhone": 0,
    }
    payload.update(overrides)
    return payload


@pytest.mark.parametrize(
    ("payload", "expected_event"),
    [
        (message(), "friend_msg"),
        (message(sender="room@chatroom"), "group_msg"),
        (message(isSendMsg=1, isSendByPhone=1), "self_msg"),
        (message(isSendMsg=1, isSendByPhone=0), "sent_msg"),
        (message(type=37), "frdver_msg"),
        (message(type=42), "card_msg"),
        (message(message='<sysmsg type="revokemsg">x'), "revoke_msg"),
        (
            message(message="微信转账<paysubtype>1</paysubtype>"),
            "transfer_msg",
        ),
    ],
)
def test_dispatches_message_events(monkeypatch, payload, expected_event):
    emitted = []
    monkeypatch.setattr(robot_module.Bus, "emit", lambda event, msg: emitted.append((event, msg)))

    robot = WeChatRobot()
    robot._receive_callback(payload)

    assert emitted[0][0] == expected_event


def test_preserves_configurable_comwechat_port():
    robot = WeChatRobot(comwechat_port=19999)

    assert robot.api.port == 19999


def test_openim_contact_id_detection_includes_customer_service_ids():
    assert is_openim_contact_id("25984993499793938@kefu.openim")
    assert is_openim_contact_id("some-user@openim")
    assert not is_openim_contact_id("wxid_friend")


def test_openim_customer_service_lookup_uses_openim_database():
    api = Api.__new__(Api)
    calls = []
    api.GetDBHandle = lambda db_name="MicroMsg.db": db_name

    def query_database(*, db_handle, sql):
        calls.append((db_handle, sql))
        return {"data": [["UserName", "Alias", "Remark", "NickName", "Type"],
                          ["25984993499793938@kefu.openim", "", "", "国开客服", "3"]]}

    api.QueryDatabase = query_database
    result = api.GetContactBySql("25984993499793938@kefu.openim")

    assert result[3] == "国开客服"
    assert calls[0][0] == "OpenIMContact.db"
    assert "OpenIMContact" in calls[0][1]


def test_openim_customer_service_lookup_returns_none_when_record_is_missing():
    api = Api.__new__(Api)
    api.GetDBHandle = lambda db_name="MicroMsg.db": db_name
    api.QueryDatabase = lambda **kwargs: {
        "data": [["UserName", "Alias", "Remark", "NickName", "Type"]]
    }

    assert api.GetContactBySql("25984993499793938@kefu.openim") is None


def test_event_decorator_returns_original_function():
    robot = WeChatRobot()

    def handler(msg):
        return msg

    assert robot.on("test_event")(handler) is handler


def test_rejects_invalid_explicit_message_mode():
    with pytest.raises(ValueError):
        WeChatRobot(message_mode="invalid")


def test_invalid_environment_mode_falls_back_to_tcp(monkeypatch):
    monkeypatch.setenv("WECHATROBOT_MESSAGE_MODE", "invalid")

    robot = WeChatRobot()

    assert robot.configured_message_mode == "tcp"


def test_bridge_pull_dispatches_all_messages(monkeypatch, tmp_path):
    emitted = []
    request = {}

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"messages": [message(), message(sender="room@chatroom")]}

    def fake_post(url, json, timeout):
        request.update(url=url, json=json, timeout=timeout)
        return Response()

    monkeypatch.setattr(robot_module.requests, "post", fake_post)
    monkeypatch.setattr(robot_module.Bus, "emit", lambda event, msg: emitted.append(event))
    robot = WeChatRobot(
        message_mode="bridge",
        bridge_api_base="http://bridge:19088/",
        pull_wait_ms=12000,
        pull_batch_size=25,
        receipt_db_path=str(tmp_path / "receipts.db"),
    )

    assert robot._pull_once() is True
    assert request["url"] == "http://bridge:19088/v1/messages/pull"
    assert request["json"] == {
        "max_items": 25,
        "wait_ms": 12000,
        "ack_mode": True,
        "consumer_id": "efb",
    }
    assert emitted == ["friend_msg", "group_msg"]


def test_reliable_bridge_acks_after_dispatch(monkeypatch, tmp_path):
    calls = []
    emitted = []
    payload = message(msgid="100")
    delivery = {
        "delivery_id": "lease-1",
        "dedup_key": "msg:100|1|wxid_friend|0",
    }

    class Response:
        def __init__(self, body):
            self.body = body

        def raise_for_status(self):
            return None

        def json(self):
            return self.body

    def fake_post(url, json, timeout):
        calls.append((url, json))
        if url.endswith("/pull"):
            return Response({"messages": [payload], "deliveries": [delivery]})
        return Response({"ok": True, "acked": 1})

    monkeypatch.setattr(robot_module.requests, "post", fake_post)
    monkeypatch.setattr(robot_module.Bus, "emit", lambda event, msg: emitted.append(event))
    robot = WeChatRobot(
        message_mode="bridge",
        bridge_api_base="http://bridge:19088",
        receipt_db_path=str(tmp_path / "receipts.db"),
    )

    assert robot._pull_once(wait_ms=0) is True
    assert emitted == ["friend_msg"]
    assert calls[1] == (
        "http://bridge:19088/v1/messages/ack",
        {"delivery_ids": ["lease-1"], "consumer_id": "efb"},
    )


def test_reliable_bridge_nacks_dispatch_failure(monkeypatch, tmp_path):
    calls = []
    delivery = {"delivery_id": "lease-2", "dedup_key": "msg:200"}

    class Response:
        def __init__(self, body):
            self.body = body

        def raise_for_status(self):
            return None

        def json(self):
            return self.body

    def fake_post(url, json, timeout):
        calls.append((url, json))
        if url.endswith("/pull"):
            return Response(
                {
                    "messages": [message(msgid="200")],
                    "deliveries": [delivery],
                }
            )
        return Response({"ok": True, "nacked": 1})

    monkeypatch.setattr(robot_module.requests, "post", fake_post)
    monkeypatch.setattr(
        robot_module.Bus,
        "emit",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("private detail")),
    )
    robot = WeChatRobot(
        message_mode="bridge",
        bridge_api_base="http://bridge:19088",
        receipt_db_path=str(tmp_path / "receipts.db"),
    )

    assert robot._pull_once(wait_ms=0) is True
    assert calls[1][0] == "http://bridge:19088/v1/messages/nack"
    assert calls[1][1]["delivery_ids"] == ["lease-2"]
    assert calls[1][1]["reason"] == "RuntimeError"


def test_processed_receipt_skips_duplicate_and_repairs_ack(monkeypatch, tmp_path):
    calls = []
    emitted = []
    receipt_path = str(tmp_path / "receipts.db")
    store = robot_module.BridgeReceiptStore(receipt_path)
    store.record_processed("msg:already")
    store.close()

    class Response:
        def __init__(self, body):
            self.body = body

        def raise_for_status(self):
            return None

        def json(self):
            return self.body

    def fake_post(url, json, timeout):
        calls.append((url, json))
        if url.endswith("/pull"):
            return Response(
                {
                    "messages": [message(msgid="300")],
                    "deliveries": [
                        {
                            "delivery_id": "lease-3",
                            "dedup_key": "msg:already",
                        }
                    ],
                }
            )
        return Response({"ok": True, "acked": 1})

    monkeypatch.setattr(robot_module.requests, "post", fake_post)
    monkeypatch.setattr(robot_module.Bus, "emit", lambda event, msg: emitted.append(event))
    robot = WeChatRobot(
        message_mode="bridge",
        bridge_api_base="http://bridge:19088",
        receipt_db_path=receipt_path,
    )

    assert robot._pull_once(wait_ms=0) is True
    assert emitted == []
    assert calls[1][0].endswith("/v1/messages/ack")


def test_bridge_pull_rejects_invalid_response(monkeypatch):
    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"messages": "invalid"}

    monkeypatch.setattr(robot_module.requests, "post", lambda *args, **kwargs: Response())

    assert WeChatRobot(message_mode="bridge")._pull_once() is False


def test_auto_mode_selects_bridge_when_probe_succeeds(monkeypatch):
    robot = WeChatRobot(message_mode="auto")
    monkeypatch.setattr(robot, "_pull_once", lambda **kwargs: True)

    assert robot._select_message_mode() == "bridge"


def test_auto_mode_falls_back_to_tcp_when_probe_fails(monkeypatch):
    robot = WeChatRobot(message_mode="auto")
    monkeypatch.setattr(robot, "_pull_once", lambda **kwargs: False)

    assert robot._select_message_mode() == "tcp"


def test_tcp_mode_starts_hooks_and_receives_callback(monkeypatch):
    emitted = []
    hook_calls = []

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    monkeypatch.setattr(robot_module.Bus, "emit", lambda event, msg: emitted.append(event))
    robot = WeChatRobot(ip="127.0.0.1", port=port, message_mode="tcp")
    monkeypatch.setattr(
        robot,
        "StartMsgHook",
        lambda **kwargs: hook_calls.append(("message", kwargs)),
    )
    monkeypatch.setattr(
        robot,
        "StartImageHook",
        lambda **kwargs: hook_calls.append(("image", kwargs)),
    )
    monkeypatch.setattr(
        robot,
        "StartVoiceHook",
        lambda **kwargs: hook_calls.append(("voice", kwargs)),
    )

    try:
        thread_id = robot.run(main_thread=False)
        assert thread_id is not None

        with socket.create_connection(("127.0.0.1", port), timeout=2) as client:
            client.sendall(json.dumps(message()).encode("utf-8") + b"\n")
            assert client.recv(32) == b"200 OK"

        deadline = time.time() + 2
        while not emitted and time.time() < deadline:
            time.sleep(0.01)
        assert emitted == ["friend_msg"]
        assert hook_calls[0] == ("message", {"port": port})
        assert [name for name, _ in hook_calls] == ["message", "image", "voice"]
    finally:
        robot.shutdown()


def test_run_does_not_start_second_background_thread(monkeypatch):
    robot = WeChatRobot(message_mode="bridge")
    monkeypatch.setattr(robot, "_pull_once", lambda **kwargs: robot._stop_event.wait(0.01) or True)

    try:
        first_thread_id = robot.run(main_thread=False)
        second_thread_id = robot.run(main_thread=False)
        assert first_thread_id == second_thread_id
    finally:
        robot.shutdown()
