# RingCentral Team Messaging Plugin

A Hermes Agent gateway adapter that connects the bot to RingCentral Team
Messaging (the chat surface in RingCentral / RingEX). Uses the v1 Team
Messaging REST API for outbound posts + file uploads, and the platform
WebSocket subscription stream for inbound events.

## Setup

### 1. Create a bot in the RingCentral developer portal

1. Sign in at <https://developers.ringcentral.com/> with a RingCentral account
   that has Super Admin rights on the target company / sandbox.
2. **Create App** → choose **Bot** (server-only, JWT) as the platform type.
3. Grant the bot the following permissions (minimum):
   - `TeamMessaging` — read + write chats and posts
   - `ReadAccounts` — required to resolve the bot's own extension ID
   - `SubscriptionWebhook` — required to open the WebSocket subscription
4. After publishing the bot to your account, copy the **bot JWT token** from
   the app credentials page. This is the value used as `RC_BOT_TOKEN`.

### 2. Configure Hermes

Set the bot token (and optional knobs) via env vars or `hermes config`:

```sh
export RC_BOT_TOKEN="<bot JWT>"
# Optional — defaults to production. Use the devtest URL for sandbox accounts:
#   https://platform.devtest.ringcentral.com
export RC_SERVER_URL="https://platform.ringcentral.com"

# Optional access control — comma-separated RC person IDs.
export RC_ALLOWED_USERS="123456789,987654321"
# Or for dev environments only:
export RC_ALLOW_ALL_USERS=true

# Optional — default chat/group ID for cron + notification delivery.
export RC_HOME_CHANNEL="g-abc123"
export RC_HOME_CHANNEL_NAME="Hermes-Updates"
```

### 3. Start the gateway

```sh
hermes gateway start
```

The plugin is auto-discovered from `plugins/platforms/ringcentral/`. The
adapter connects, fetches its own extension ID (used to filter echoes),
opens a WebSocket subscription on `/team-messaging/v1/posts`, and starts
relaying messages.

## Behavior

* **DMs** — every inbound post is forwarded to the agent.
* **Group / Team chats** — the bot only responds when explicitly addressed
  via an inline `![:Person](<bot id>)` mention. The mention prefix is
  stripped before the message is handed to the agent.
* **Edits / deletes** — handled through `edit_message` / `delete_message`
  hooks the agent uses for streaming-style responses.
* **Files** — inbound attachments (images, audio, documents) are downloaded
  to the Hermes cache so the vision tool can pick them up by path.
  Outbound media is uploaded via `POST /team-messaging/v1/files` and
  attached to a follow-up post for the caption.
* **Rate limiting** — HTTP 429 responses are retried up to twice, honoring
  the server's `Retry-After` header (capped at 30 s).
* **Reconnect** — WebSocket disconnects trigger exponential backoff
  (2 s → 60 s) with jitter. Permanent auth failures (HTTP 401 / 403)
  stop the loop instead of looping forever.

## Cron / notification delivery

Set `RC_HOME_CHANNEL` to a chat or group ID and Hermes' cron scheduler
will route `deliver=ringcentral` jobs there automatically — even when
cron runs out-of-process from the gateway, courtesy of the plugin's
standalone-sender hook.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Gateway logs `RC_BOT_TOKEN not configured` | env var missing | `export RC_BOT_TOKEN=…` and restart |
| Gateway logs `RingCentral rejected bot token` | bad / expired token | Re-issue the JWT from the dev portal |
| WS keeps disconnecting with `HTTP 401` | bot lacks `SubscriptionWebhook` scope | Add the scope, reinstall the bot |
| Posts succeed but bot never replies in a group | bot wasn't addressed | Use `![:Person](<bot id>)` to mention |
| Files arrive but no media surfaces | attachment download blocked by SSRF rule | Verify the chat is accessible to the bot |

## Dependencies

```text
aiohttp>=3.9.0
websockets>=12.0
```

Both are typically already installed; `aiohttp` ships with Hermes core and
`websockets` is a transitive dep of several other adapters.
