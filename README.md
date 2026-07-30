# python-comwechatrobot-http

`shaoyou11` 维护的兼容分支，在原有 ComWechat TCP 回调基础上加入 Bridge API
长轮询接收方式，同时保留当前 EFB 所依赖的端口参数和数据库扩展。

## 主要特点

- 默认仍使用稳定的 TCP 回调，不改变现有 EFB 行为。
- 支持上游新增的 Bridge API 长轮询接收。
- 支持启动时自动探测 Bridge；不可用时回到 TCP。
- 保留 `comwechat_port`、自定义回调端口、`sent_msg` 和群成员 SQL 接口。
- 自动模式只在启动时选择一次，不会运行中反复切换，避免重复收消息。
- 提供可停止的后台线程，便于容器平滑退出和自动化测试。

## 安装

固定使用 `v1.1.0`：

```bash
pip install "git+https://github.com/shaoyou11/python-comwechatrobot-http.git@v1.1.0"
```

跟随默认分支：

```bash
pip install "git+https://github.com/shaoyou11/python-comwechatrobot-http.git@master"
```

## 基本用法

```python
from wechatrobot import WeChatRobot

bot = WeChatRobot()


@bot.on("friend_msg")
def on_friend_msg(msg):
    bot.SendText(wxid=msg["sender"], msg=msg["message"])


@bot.on("group_msg")
def on_group_msg(msg):
    print("on_group_msg:", msg)


@bot.on("self_msg", "sent_msg")
def on_self_msg(msg):
    print("on_self_msg:", msg)


bot.run()
```

## 消息模式

| 模式 | 设置 | 行为 | 适用场景 |
| --- | --- | --- | --- |
| TCP | `tcp` | 启动 ComWechat Hook，并监听回调端口 | 现有 EFB，默认且最稳妥 |
| Bridge | `bridge` | 从 `/v1/messages/pull` 长轮询消息 | 已部署新版 Bridge API |
| 自动 | `auto` | 启动时探测 Bridge，失败则使用 TCP | 同一镜像兼容两类后端 |

环境变量示例：

```yaml
environment:
  WECHATROBOT_MESSAGE_MODE: tcp
```

Bridge 模式示例：

```yaml
environment:
  WECHATROBOT_MESSAGE_MODE: bridge
  WECHATROBOT_BRIDGE_API_BASE: http://127.0.0.1:19088
  WECHATROBOT_PULL_WAIT_MS: "15000"
  WECHATROBOT_PULL_BATCH_SIZE: "50"
```

也可以通过构造参数设置：

```python
bot = WeChatRobot(
    comwechat_port=18888,
    message_mode="bridge",
    bridge_api_base="http://127.0.0.1:19088",
)
```

## EFB 兼容说明

当前 EFB 镜像建议先保持：

```yaml
environment:
  WECHATROBOT_MESSAGE_MODE: tcp
```

这与原稳定提交 `3df22af` 的接收路径一致，不要求 ComWechat 容器提供 Bridge
接口。公众号过滤、Watchdog、附件处理等上层定制功能不在本库中实现，因此升级
本库不会主动删除或重写这些功能。

只有后端真正提供 `POST /v1/messages/pull` 时，才应切换到 `bridge`。本项目是
消息消费者，不会自行创建、持久化或确认 Bridge 队列；队列可靠性由 Bridge
服务负责。

## 自动模式边界

`auto` 会在进程启动时执行一次无等待探测：

- 探测成功：本次进程固定使用 Bridge。
- 探测失败：本次进程固定使用 TCP。
- 运行中不会自动来回切换，避免 Hook 与长轮询同时接收造成重复消息。

需要改变模式时，应修改配置后正常重启 EFB。

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WECHATROBOT_MESSAGE_MODE` | `tcp` | `tcp`、`bridge` 或 `auto` |
| `WECHATROBOT_BRIDGE_API_BASE` | `http://127.0.0.1:19088` | Bridge API 根地址 |
| `WECHATROBOT_PULL_WAIT_MS` | `15000` | 长轮询等待时间，毫秒 |
| `WECHATROBOT_PULL_BATCH_SIZE` | `50` | 每批最多获取的消息数 |

## 事件

常用事件包括：

- `friend_msg`
- `group_msg`
- `self_msg`
- `sent_msg`
- `frdver_msg`
- `card_msg`
- `revoke_msg`
- `transfer_msg`

## 回退

如果 Bridge 后端尚未准备好，将 `WECHATROBOT_MESSAGE_MODE` 改回 `tcp` 即可。
也可以在 EFB Dockerfile 中重新固定原稳定提交：

```text
3df22af9a6e77e8032681800af4ffb90a3981b4c
```

## 来源

本仓库基于
[`jiz4oh/python-comwechatrobot-http`](https://github.com/jiz4oh/python-comwechatrobot-http)
维护。`v1.1.0` 以原稳定提交 `3df22af` 为基础，移植并加固上游 Bridge API
接收能力。
