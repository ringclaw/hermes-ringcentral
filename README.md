# Hermes RingCentral Plugin

RingCentral Team Messaging support for Hermes Agent. This plugin lets a Hermes
agent talk through a RingCentral bot, reply in threads, read owner-visible chat
history when explicitly requested by the owner, and deliver scheduled
notifications into RingCentral.

[中文说明](README.zh-CN.md)

## Quick Start

Install with the Hermes plugin manager:

```sh
hermes plugins install ringclaw/hermes-ringcentral --enable
```

The installer prompts for `RC_BOT_TOKEN` and saves it to `~/.hermes/.env`. If
the plugin is already installed but disabled, enable it with:

```sh
hermes plugins enable ringcentral-platform
```

Restart the gateway after installing or changing credentials:

```sh
hermes gateway restart
```

For first-time gateway setup, `hermes gateway start` is also fine.

## RingCentral App Setup

Create a RingCentral bot app in the RingCentral developer portal:

1. Sign in at <https://developers.ringcentral.com/>.
2. Create an app with the **Bot** platform type.
3. Grant at least these permissions:
   - `TeamMessaging` for reading and writing team messages
   - `ReadAccounts` for resolving the bot extension
   - `WebSocketsSubscription` for live message events
4. Install or publish the bot to the target RingCentral account.
5. Copy the bot JWT and use it as `RC_BOT_TOKEN`.

For owner-only history summaries or fallback sends, also configure a user JWT
app for the owner account and set all three `RC_USER_*` variables.

## Configuration

Minimum:

```sh
export RC_BOT_TOKEN="<bot JWT>"
```

Common optional settings:

```sh
# Production is the default. Use devtest for sandbox accounts.
export RC_SERVER_URL="https://platform.ringcentral.com"

# Owner mode: enables owner-only history reads and fallback sends.
export RC_USER_CLIENT_ID="<owner app client id>"
export RC_USER_CLIENT_SECRET="<owner app client secret>"
export RC_USER_JWT_TOKEN="<owner JWT>"
export RC_HISTORY_MESSAGE_LIMIT=250

# User access control. If owner mode is configured and this is unset,
# the plugin auto-seeds the owner email as the only allowed user.
export RC_ALLOWED_USER_EMAILS="owner@example.com,teammate@example.com"
export RC_ALLOW_ALL_USERS=false

# Group/team channel controls.
export RC_ALLOWED_CHANNELS="g-abc123,g-def456"
export RC_IGNORED_CHANNELS="g-muted"
export RC_REQUIRE_MENTION=true
export RC_FREE_RESPONSE_CHANNELS="g-abc123"
export RC_THREAD_REQUIRE_MENTION=false

# Threading and delivery.
export RC_REPLY_TO_MODE=first
export RC_NO_THREAD_CHANNELS="g-announcements"
export RC_PROCESSING_EMOJI_ENABLED=true
export RC_PROCESSING_EMOJI_EDIT_DELAY_SECONDS=5
export RC_HOME_CHANNEL="g-abc123"
export RC_HOME_CHANNEL_NAME="Hermes Updates"
```

Put persistent values in `~/.hermes/.env` if you do not want to export them in
your shell each time.

## How To Use

### Direct messages

DM the RingCentral bot and talk to Hermes normally:

```text
Can you draft a deployment update for the team?
```

Only the configured owner or users in `RC_ALLOWED_USER_EMAILS` can trigger the
bot. Unauthorized DMs are ignored.

### Group and team chats

Mention the bot in a group/team chat:

```text
@Hermes summarize the decision in this thread
```

By default, group messages require a bot mention. You can allow selected
channels to trigger without mentions by setting `RC_FREE_RESPONSE_CHANNELS`, or
disable mention requirements globally with `RC_REQUIRE_MENTION=false`.

### Owner history summaries

From the owner-bot DM, ask Hermes for a group or DM summary in natural language:

```text
Summarize Project Team since yesterday.
```

```text
总结我和 Alice Wang 今天的聊天
```

Hermes may call the `ringcentral_get_recent_messages` tool to fetch recent
source messages visible to the owner. The plugin returns structured history;
Hermes Agent decides the intent, target, time window, and final summary. Other
users cannot use this tool to read history.

### Cron and notifications

Set `RC_HOME_CHANNEL` to route Hermes cron jobs and notifications into
RingCentral:

```text
Create a daily 9am reminder and deliver it to RingCentral.
```

## Feature Highlights

- **Hermes-native plugin install** via `hermes plugins install`.
- **Bot-first messaging** for normal conversations, with optional owner fallback
  when the bot is not in a target chat.
- **Owner-only chat history tool** for group and direct-message summaries,
  with intent and summarization handled by Hermes Agent.
- **Thread replies** using RingCentral Team Messaging `parentPostId` /
  `threadId` where supported.
- **Waiting emoji in threads**: Hermes posts `👀`, edits it to `⏳` after a
  short delay, then deletes it when the final reply is delivered.
- **Discord-style controls** for allowed users, allowed channels, ignored
  channels, mention requirements, free-response channels, and thread follow-up
  behavior.
- **Attachment handling** for inbound images, audio, and documents so Hermes
  tools can work with downloaded files.
- **Cron delivery** through `RC_HOME_CHANNEL`, including out-of-process cron
  sender support.
- **Webhook text fallback** for integration posts where the modern Team
  Messaging posts API returns empty text.

## Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Plugin does not load | Installed but not enabled | Run `hermes plugins enable ringcentral-platform` and restart the gateway |
| Gateway logs `RC_BOT_TOKEN not configured` | Missing bot JWT | Set `RC_BOT_TOKEN` in `~/.hermes/.env` |
| Gateway logs `RingCentral rejected bot token` | Bad or expired JWT | Re-issue the bot JWT in the RingCentral developer portal |
| Owner history request says credentials are missing | `RC_USER_*` is incomplete | Set `RC_USER_CLIENT_ID`, `RC_USER_CLIENT_SECRET`, and `RC_USER_JWT_TOKEN` |
| Bot does not reply in a group | No mention, blocked user, or blocked channel | Mention the bot and check `RC_ALLOWED_USER_EMAILS`, `RC_ALLOWED_CHANNELS`, and `RC_IGNORED_CHANNELS` |
| Replies are not threaded in a chat | RingCentral UI/API behavior or channel disabled threads | Check `RC_REPLY_TO_MODE` and `RC_NO_THREAD_CHANNELS` |

## Development

Run the RingCentral test suite with Hermes Agent on `PYTHONPATH`:

```sh
PYTHONPATH=/root/workspace/github/NousResearch/hermes-agent \
  uv run --with PyYAML --extra dev pytest -q tests/test_ringcentral.py
```
