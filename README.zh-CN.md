# Hermes RingCentral 插件

这个插件把 RingCentral Team Messaging 接入 Hermes Agent。你可以让 Hermes
通过 RingCentral bot 回复私聊和群聊、在 thread 里回复、按 owner 权限读取群聊或私聊历史并总结，也可以把 cron 通知投递到 RingCentral。

[English README](README.md)

## 快速安装

使用 Hermes 官方插件管理命令安装：

```sh
hermes plugins install ringclaw/hermes-ringcentral --enable
```

安装器会提示填写 `RC_BOT_TOKEN`，并保存到 `~/.hermes/.env`。如果插件已经安装但没有启用，执行：

```sh
hermes plugins enable ringcentral-platform
```

安装或修改凭证后重启 gateway：

```sh
hermes gateway restart
```

第一次启动 gateway 时也可以用：

```sh
hermes gateway start
```

## RingCentral 应用准备

先在 RingCentral Developer Portal 创建 bot：

1. 打开 <https://developers.ringcentral.com/> 并登录目标账号。
2. 创建 **Bot** 类型应用。
3. 至少授予这些权限：
   - `TeamMessaging`：读取和发送 Team Messaging 消息
   - `ReadAccounts`：读取 bot extension 信息
   - `WebSocketsSubscription`：监听实时消息事件
4. 将 bot 安装或发布到目标 RingCentral 账号。
5. 复制 bot JWT，作为 `RC_BOT_TOKEN`。

如果需要 owner 专属的历史消息总结，或者 bot 不在某个群里时让 owner 代发消息，还需要给 owner 用户准备 JWT/OAuth 应用，并配置完整的 `RC_USER_*` 三个变量。

## 配置

最小配置：

```sh
export RC_BOT_TOKEN="<bot JWT>"
```

常用可选配置：

```sh
# 默认使用生产环境。Sandbox 账号使用 devtest URL。
export RC_SERVER_URL="https://platform.ringcentral.com"

# Owner 模式：用于 owner-only 历史读取和 fallback 发送。
export RC_USER_CLIENT_ID="<owner app client id>"
export RC_USER_CLIENT_SECRET="<owner app client secret>"
export RC_USER_JWT_TOKEN="<owner JWT>"
export RC_HISTORY_MESSAGE_LIMIT=250

# 用户权限。未设置时，如果启用了 owner 模式，插件会自动只允许 owner 邮箱。
export RC_ALLOWED_USER_EMAILS="owner@example.com,teammate@example.com"
export RC_ALLOW_ALL_USERS=false

# 群聊/Team 权限和触发方式。
export RC_ALLOWED_CHANNELS="g-abc123,g-def456"
export RC_IGNORED_CHANNELS="g-muted"
export RC_REQUIRE_MENTION=true
export RC_FREE_RESPONSE_CHANNELS="g-abc123"
export RC_THREAD_REQUIRE_MENTION=false

# Thread 回复和通知投递。
export RC_REPLY_TO_MODE=first
export RC_NO_THREAD_CHANNELS="g-announcements"
export RC_PROCESSING_EMOJI_ENABLED=true
export RC_PROCESSING_EMOJI_EDIT_DELAY_SECONDS=5
export RC_HOME_CHANNEL="g-abc123"
export RC_HOME_CHANNEL_NAME="Hermes Updates"

# 入站附件只会在消息通过访问策略后下载。
export RC_ATTACHMENT_DOWNLOAD_ENABLED=true
export RC_ATTACHMENT_MAX_COUNT=5
export RC_ATTACHMENT_MAX_BYTES=5242880
```

长期使用建议把这些变量写到 `~/.hermes/.env`，这样不需要每次手动 export。

## 怎么使用

### 私聊 bot

直接给 RingCentral bot 发私聊，像正常使用 Hermes 一样提问：

```text
帮我写一段给团队的发布通知。
```

只有 owner 或 `RC_ALLOWED_USER_EMAILS` 中的用户可以触发 bot。未授权用户的私聊会被静默忽略。

### 群聊或 Team 中使用

在群聊里 mention bot：

```text
@Hermes 帮我总结一下这个 thread 里的结论
```

默认群聊必须 mention bot 才会触发。你可以通过 `RC_FREE_RESPONSE_CHANNELS` 允许指定群无需 mention，或通过 `RC_REQUIRE_MENTION=false` 全局关闭 mention 要求。

### 让 owner 总结群聊或私聊历史

在 owner 和 bot 的私聊里，用自然语言提出需求：

```text
总结 Project Team 从昨天到现在的消息。
```

```text
总结我和 Alice Wang 今天的聊天
```

Hermes 会根据需要调用 `ringcentral_get_recent_messages` 工具，使用 owner 的 `RC_USER_*` 凭证读取 owner 可见的最近消息。插件只返回结构化消息材料；目标识别、时间范围判断和最终总结都交给 Hermes Agent 完成。非 owner 用户不能使用这个工具读取历史。

### Cron 和通知投递

配置 `RC_HOME_CHANNEL` 后，Hermes 的 cron 任务和通知可以直接投递到 RingCentral：

```text
每天上午 9 点提醒我检查 standup，并发送到 RingCentral。
```

## 特色功能

- **官方插件管理安装**：直接使用 `hermes plugins install`。
- **Bot 优先对话**：普通对话都走 `RC_BOT_TOKEN`。
- **Owner-only 历史总结**：只允许 owner 从 bot 私聊触发群聊/私聊历史读取。
- **总结逻辑交给 Hermes Agent**：插件提供消息材料，不硬编码意图、目标和时间段解析。
- **Owner fallback 发送**：bot 不在某个群或权限不足时，可用 owner 身份 fallback 发送。
- **Thread 回复**：支持 RingCentral Team Messaging 的 `parentPostId` / `threadId`。
- **Thread 等待 emoji**：Hermes 会先回复 `👀`，短暂等待后 edit 为 `⏳`，最终回复送达后删除等待消息。
- **Discord 风格权限控制**：支持 allowed users、allowed channels、ignored channels、mention required、free-response channels 和 thread follow-up 策略。
- **附件处理**：图片、音频、文档会下载到 Hermes 缓存，供视觉或文件工具继续处理。
- **Cron 投递**：通过 `RC_HOME_CHANNEL` 把定时任务和通知送到 RingCentral。
- **Webhook 空 text fallback**：当新 Team Messaging posts API 对 integration/webhook 消息返回空 text 时，会尝试旧 Glip 接口补齐文本。

## 常见问题

| 现象 | 可能原因 | 处理方式 |
| --- | --- | --- |
| 插件没有加载 | 已安装但未启用 | 执行 `hermes plugins enable ringcentral-platform` 并重启 gateway |
| 日志提示 `RC_BOT_TOKEN not configured` | 缺少 bot JWT | 在 `~/.hermes/.env` 设置 `RC_BOT_TOKEN` |
| 日志提示 `RingCentral rejected bot token` | JWT 错误或过期 | 在 RingCentral Developer Portal 重新签发 bot JWT |
| owner 历史总结提示缺少凭证 | `RC_USER_*` 不完整 | 设置 `RC_USER_CLIENT_ID`、`RC_USER_CLIENT_SECRET`、`RC_USER_JWT_TOKEN` |
| 群聊里 bot 不回复 | 没 mention、用户未授权或群不在允许列表 | 检查 mention、`RC_ALLOWED_USER_EMAILS`、`RC_ALLOWED_CHANNELS`、`RC_IGNORED_CHANNELS` |
| 没有回复到 thread | RingCentral UI/API 行为或该 chat 禁用了 thread | 检查 `RC_REPLY_TO_MODE` 和 `RC_NO_THREAD_CHANNELS` |

## 开发测试

本地跑 RingCentral 测试：

```sh
PYTHONPATH=/root/workspace/github/NousResearch/hermes-agent \
  uv run --with PyYAML --extra dev pytest -q tests/test_ringcentral.py
```
