import sys
import unittest
import importlib
from pathlib import Path
from unittest.mock import patch

import requests


sys.path.insert(0, str(Path(__file__).parents[1]))

from wechatrobot.Api import Api
from wechatrobot.WeChatRobot import WeChatRobot


robot_module = importlib.import_module("wechatrobot.WeChatRobot")


class DbHandleLifecycleTest(unittest.TestCase):
    def test_bridge_failure_invalidates_cached_handles(self):
        robot = WeChatRobot()
        robot.api.db_handle = {"MicroMsg.db": 0x12345678}

        with patch.object(
            robot_module.requests,
            "post",
            side_effect=requests.ConnectionError("bridge stopped"),
        ):
            self.assertFalse(robot._pull_once())

        self.assertEqual(robot.api.db_handle, {})

    def test_missing_database_handle_refreshes_cache(self):
        api = Api()
        api.db_handle = {"OpenIMContact.db": 2}
        api.GetDatabaseHandles = lambda: {
            "data": [
                {"db_name": "MicroMsg.db", "handle": 1},
                {"db_name": "OpenIMContact.db", "handle": 2},
            ]
        }

        self.assertEqual(api.GetDBHandle("MicroMsg.db"), 1)

    def test_rejected_database_handle_invalidates_cache(self):
        api = Api()
        api.db_handle = {"MicroMsg.db": 1}
        with patch.object(api, "post", return_value={
            "result": "ERROR",
            "err_msg": "database handle unavailable",
            "data": [],
        }):
            response = api.QueryDatabase(db_handle=1, sql="select 1")

        self.assertEqual(response["result"], "ERROR")
        self.assertEqual(api.db_handle, {})

    def test_unavailable_database_handle_returns_zero(self):
        api = Api()
        api.GetDatabaseHandles = lambda: {"data": []}

        self.assertEqual(api.GetDBHandle("MicroMsg.db"), 0)


if __name__ == "__main__":
    unittest.main()
