import importlib
import json
import socket
import time

import pytest

from wechatrobot import WeChatRobot


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


def test_bridge_pull_dispatches_all_messages(monkeypatch):
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
    )

    assert robot._pull_once() is True
    assert request["url"] == "http://bridge:19088/v1/messages/pull"
    assert request["json"] == {"max_items": 25, "wait_ms": 12000}
    assert emitted == ["friend_msg", "group_msg"]


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
