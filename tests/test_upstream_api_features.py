import json
import importlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from wechatrobot.Api import Api
from wechatrobot.WeChatRobot import WeChatRobot


api_module = importlib.import_module("wechatrobot.Api")


class FakeResponse:
    def __init__(self, payload):
        self.content = json.dumps(payload).encode("utf-8")

    def raise_for_status(self):
        return None


class ApiFeatureTest(unittest.TestCase):
    def test_mark_as_read_posts_type_49_without_automatic_use(self):
        api = Api()
        with patch.object(
            api_module.requests,
            "post",
            return_value=FakeResponse({"result": "OK"}),
        ) as post:
            post.assert_not_called()
            response = api.MarkAsRead(wxid="wxid_a")

        self.assertEqual(response, {"result": "OK"})
        self.assertTrue(post.call_args.args[0].endswith("/api/?type=49"))
        self.assertEqual(json.loads(post.call_args.kwargs["data"]), {"wxid": "wxid_a"})

    def test_send_quote_text_posts_type_51_only_when_explicitly_called(self):
        api = Api()
        with patch.object(
            api_module.requests,
            "post",
            return_value=FakeResponse({"result": "OK"}),
        ) as post:
            post.assert_not_called()
            response = api.SendQuoteText(
                wxid="wxid_a",
                msg="reply",
                target_msgid="123456",
            )

        self.assertEqual(response, {"result": "OK"})
        self.assertTrue(post.call_args.args[0].endswith("/api/?type=51"))
        self.assertEqual(
            json.loads(post.call_args.kwargs["data"]),
            {"wxid": "wxid_a", "msg": "reply", "target_msgid": "123456"},
        )
        self.assertEqual(post.call_args.kwargs["timeout"], 60.0)

    def test_control_and_send_requests_use_separate_timeouts(self):
        api = Api()
        with patch.object(
            api_module.requests,
            "post",
            return_value=FakeResponse({"result": "OK"}),
        ) as post:
            api.GetSelfInfo()
            self.assertEqual(post.call_args.kwargs["timeout"], 5.0)
            api.SendText(wxid="wxid_a", msg="hello")
            self.assertEqual(post.call_args.kwargs["timeout"], 60.0)

    def test_database_query_refreshes_handle_by_database_name(self):
        api = Api()
        api.invalidate_db_handles = Mock()
        api.GetDBHandle = Mock(side_effect=[101, 202])
        api.post = Mock(
            side_effect=[
                {"result": "ERROR", "err_msg": "database handle unavailable", "data": []},
                {"result": "OK", "data": [["value"]]},
            ]
        )

        response = api.QueryDatabase(db_name="MicroMsg.db", sql="select 1")

        self.assertEqual(response["result"], "OK")
        self.assertEqual(api.post.call_count, 2)
        self.assertEqual(api.post.call_args_list[0].args[0], 34)
        self.assertEqual(api.post.call_args_list[1].args[1].db_handle, "202")
        api.invalidate_db_handles.assert_called_once_with()


class BridgeReliabilityTest(unittest.TestCase):
    def test_login_gate_holds_bridge_pull_until_native_login(self):
        with tempfile.TemporaryDirectory() as directory:
            robot = WeChatRobot(
                message_mode="bridge",
                receipt_db_path=str(Path(directory) / "receipts.db"),
            )
            pulls = Mock()

            def stop_after_wait(_timeout):
                robot._stop_event.set()

            robot._stop_event.wait = stop_after_wait
            with patch.object(robot, "_native_logged_in", return_value=False), patch.object(
                robot, "_pull_once", pulls
            ):
                robot._consume_forever()

            pulls.assert_not_called()
            robot.shutdown()

    def test_legacy_dispatch_retry_is_bounded_and_can_recover(self):
        with tempfile.TemporaryDirectory() as directory:
            robot = WeChatRobot(
                message_mode="bridge",
                receipt_db_path=str(Path(directory) / "receipts.db"),
            )
            robot.retry_max_attempts = 2
            robot.retry_base_seconds = 0.1
            robot.api.invalidate_db_handles = Mock()
            calls = {"count": 0}

            def receive(_message):
                calls["count"] += 1
                if calls["count"] == 1:
                    raise RuntimeError("temporary")

            robot._receive_callback = receive
            with patch.object(robot, "_native_logged_in", return_value=True):
                robot._dispatch_with_retry({"msgid": "1"})
                self.assertEqual(len(robot._retry_queue), 1)
                message, attempts, _ = robot._retry_queue.popleft()
                robot._retry_queue.append((message, attempts, 0))
                robot._process_retry_queue()

            self.assertEqual(calls["count"], 2)
            self.assertFalse(robot._retry_queue)
            robot.shutdown()


if __name__ == "__main__":
    unittest.main()
