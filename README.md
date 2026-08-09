# python-comwechatrobot-http

这是 `shaoyou11` 维护的 ComWechat 兼容分支。在原有 TCP 回调基础上加入 Bridge API 长轮询接收方式，
同时保留 EFB 依赖的端口参数、回调事件和数据库扩展。

## 功能范围

- 默认使用 TCP 回调，不改变现有 EFB 行为。
- 支持 Bridge API 长轮询接收。
- 支持启动时自动探测 Bridge；探测失败时回退到 TCP。
- 保留 `comwechat_port`、自定义回调端口、`sent_msg` 和群成员 SQL 接口。
- 自动模式只在启动时选择一次，避免运行中两条路径同时收消息。
- 提供可停止的后台线程，支持容器平滑退出和自动化测试。

## 安装

生产环境固定使用 `v1.1.0`：

```bash
pip install "git+https://github.com/shaoyou11/python-comwechatrobot-http.git@v1.1.0"
```

开发或测试默认分支：

```bash
pip install "git+https://github.com/shaoyou11/python-comwechatrobot-http.git@master"
```

## 消息模式

| 模式 | 配置值 | 行为 | 适用场景 |
| --- | --- | --- | --- |
| TCP | `tcp` | 启动 ComWechat Hook 并监听回调端口。 | 现有 EFB，默认且最稳妥。 |
| Bridge | `bridge` | 从 `/v1/messages/pull` 长轮询消息。 | 已部署新版 Bridge API。 |
| 自动 | `auto` | 启动时探测 Bridge，失败后使用 TCP。 | 同一镜像兼容两类后端。 |

TCP 模式：

```yaml
environment:
  WECHATROBOT_MESSAGE_MODE: tcp
```

Bridge 模式：

```yaml
environment:
  WECHATROBOT_MESSAGE_MODE: bridge
  WECHATROBOT_BRIDGE_API_BASE: http://127.0.0.1:19088
  WECHATROBOT_PULL_WAIT_MS: "15000"
  WECHATROBOT_PULL_BATCH_SIZE: "50"
  WECHATROBOT_CONSUMER_ID: efb
  WECHATROBOT_RECEIPT_DB: /data/operations/state/bridge-consumer.db
```

也可以通过构造参数设置：

```python
bot = WeChatRobot(
    comwechat_port=18888,
    message_mode="bridge",
    bridge_api_base="http://127.0.0.1:19088",
)
```

## Bridge 确认与去重

只有后端提供可靠版 `POST /v1/messages/pull` 时，才应切换到 `bridge`。可靠模式会请求租约元数据，
消息分发成功后调用 ACK，失败时调用 NACK。消费成功去重键写入本地 SQLite 回执数据库；如果 ACK 请求丢失，
消息再次投递时只补 ACK，不重复分发。

生产环境必须把 `WECHATROBOT_RECEIPT_DB` 指向持久化目录。未配置时使用内存回执，只适合开发测试。
旧 Bridge 响应不包含租约元数据时仍可兼容消费，但没有 ACK 保障。

同一微信会话按 FIFO 处理；附件、联系人优先级和公众号过滤等策略由 EFB 或 ComWechat 上层组件负责，本库不重写这些策略。

## 自动模式边界

`auto` 只在进程启动时执行一次探测：

- 探测成功：本次进程固定使用 Bridge。
- 探测失败：本次进程固定使用 TCP。
- 运行中不会在 Bridge 和 TCP 之间来回切换，避免 Hook 与长轮询同时接收造成重复消息。

需要改变模式时，修改配置后正常重启 EFB。

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WECHATROBOT_MESSAGE_MODE` | `tcp` | `tcp`、`bridge` 或 `auto`。 |
| `WECHATROBOT_BRIDGE_API_BASE` | `http://127.0.0.1:19088` | Bridge API 根地址。 |
| `WECHATROBOT_PULL_WAIT_MS` | `15000` | 长轮询等待时间，单位为毫秒。 |
| `WECHATROBOT_PULL_BATCH_SIZE` | `50` | 每批最多获取的消息数。 |
| `WECHATROBOT_CONSUMER_ID` | `efb` | Bridge 租约消费者名称。 |
| `WECHATROBOT_RECEIPT_DB` | `:memory:` | 成功消费回执 SQLite 路径。 |
| `WECHATROBOT_RECEIPT_RETENTION_SECONDS` | `604800` | 消费去重回执保留时间。 |

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

## EFB 兼容与回退

旧 ComWechat 后端可保持：

```yaml
environment:
  WECHATROBOT_MESSAGE_MODE: tcp
```

该路径与原稳定提交 `3df22af` 的接收方式一致，不要求 ComWechat 提供 Bridge 接口。公众号过滤、Watchdog、
附件处理等上层定制功能不在本库中实现，因此升级本库不会主动删除或重写这些功能。

如果 Bridge 后端尚未准备好，将 `WECHATROBOT_MESSAGE_MODE` 改回 `tcp`。也可以在 EFB Dockerfile 中重新固定：

```text
3df22af9a6e77e8032681800af4ffb90a3981b4c
```

## 来源

本仓库基于
[`jiz4oh/python-comwechatrobot-http`](https://github.com/jiz4oh/python-comwechatrobot-http)
维护。`v1.1.0` 以原稳定提交 `3df22af` 为基础，移植并加固上游 Bridge API 接收能力。
