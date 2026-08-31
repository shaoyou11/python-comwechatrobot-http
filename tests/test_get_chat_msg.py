import json
import importlib
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from wechatrobot.Api import Api
from wechatrobot.Modles import WECHAT_MSG_GET_BY_SVR_ID
from wechatrobot.WeChatRobot import WeChatRobot


api_module = importlib.import_module("wechatrobot.Api")


class FakeResponse:
    def __init__(self, content):
        self.content = content


class GetChatMsgBySvrIdTest(unittest.TestCase):
    def test_posts_server_message_id_to_native_xml_lookup(self):
        api = Api()
        with patch.object(
            api_module.requests,
            "post",
            return_value=FakeResponse(b'{"result":"OK","data":{"xml":"<msg/>"}}'),
        ) as post:
            response = api.GetChatMsgBySvrId(msgid="123456789012345678")

        self.assertEqual(response["data"]["xml"], "<msg/>")
        self.assertTrue(post.call_args.args[0].endswith(f"/api/?type={WECHAT_MSG_GET_BY_SVR_ID}"))
        self.assertEqual(json.loads(post.call_args.kwargs["data"]), {"msgid": "123456789012345678"})

    def test_robot_normalizes_native_message_type_without_changing_xml(self):
        robot = WeChatRobot()
        robot.api.GetChatMsgBySvrId = lambda **_params: {
            "result": "OK",
            "data": {"msgid": "123", "type": 49, "xml": "<msg><appmsg/></msg>"},
        }

        response = robot.GetChatMsgBySvrId(msgid="123")

        self.assertEqual(response["data"]["type"], "share")
        self.assertEqual(response["data"]["xml"], "<msg><appmsg/></msg>")


if __name__ == "__main__":
    unittest.main()
