import importlib
import json
import unittest
from unittest.mock import Mock, patch

from pydantic.v1 import ValidationError
from wechatrobot.Api import Api


api_module = importlib.import_module("wechatrobot.Api")


class GetCdnTest(unittest.TestCase):
    def test_preserves_large_ids_as_strings_on_wire(self):
        api = Api()
        for value in (7744938917580967084, '7744938917580967084', 2 ** 64 - 1):
            with self.subTest(value=value), patch.object(
                api_module.requests, 'post',
                return_value=Mock(content=b'{"result":"OK","path":"C:/image.png"}'),
            ) as post:
                result = api.GetCdn(msgid=value)
                self.assertEqual(result['result'], 'OK')
                self.assertEqual(json.loads(post.call_args.kwargs['data']), {'msgid': str(value)})
                self.assertIsInstance(json.loads(post.call_args.kwargs['data'])['msgid'], str)

    def test_unsupported_references_never_reach_old_native_api(self):
        api = Api()
        for value in ('local:7:123', '', 'abc', -1, 2 ** 64, '１２３', 1.5):
            with self.subTest(value=value), patch.object(api_module.requests, 'post') as post:
                with self.assertRaises(ValidationError):
                    api.GetCdn(msgid=value)
                post.assert_not_called()
