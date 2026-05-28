# RingCentral Team Messaging Plugin

A Hermes Agent gateway adapter that connects RingCentral Team Messaging
(the chat surface in RingCentral / RingEX) to Hermes. The bot identity is
the primary conversation identity. An optional owner identity can observe
owner-visible groups and send when the bot is not a member.

## Setup

### 1. Create a bot in the RingCentral developer portal

1. Sign in at <https://developers.ringcentral.com/> with a RingCentral account
   that has Super Admin rights on the target company / sandbox.
2. **Create App** → choose **Bot** (server-only, JWT) as the platform type.
3. Grant the bot the following permissions (minimum):
   - `TeamMessaging` — read + write chats and posts
   - `ReadAccounts` — required to resolve the bot's own extension ID
   - `WebSocketsSubscription` — required to open the WebSocket subscription
4. After publishing the bot to your account, copy the **bot JWT token** from
   the app credentials page. This is the value used as `RC_BOT_TOKEN`.

### 2. Configure Hermes

Set the bot token (and optional knobs) via env vars or `hermes config`:

```sh
export RC_BOT_TOKEN="<bot JWT>"
# Optional — defaults to production. Use the devtest URL for sandbox accounts:
#   https://platform.devtest.ringcentral.com
export RC_SERVER_URL="https://platform.ringcentral.com"

# Optional owner mode. These three vars are only used together.
# Owner mode lets Hermes observe owner-visible groups and fallback-send as
# the owner when the bot is not in a target group.
export RC_USER_CLIENT_ID="<owner app client id>"
export RC_USER_CLIENT_SECRET="<owner app client secret>"
export RC_USER_JWT_TOKEN="<owner JWT>"
# Optional owner-summary window. The plugin fetches this many recent chat
# messages and lets Hermes Agent produce the actual summary.
export RC_SUMMARY_MESSAGE_LIMIT=250

# Optional access control — comma-separated RC person IDs. If unset and
# owner mode is configured, Hermes auto-seeds this to the owner person ID.
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
adapter connects the bot, fetches its own extension ID, opens a WebSocket
subscription on `/team-messaging/v1/posts`, and starts relaying messages.
When owner mode is configured, it also opens an owner WebSocket and resolves
the owner person ID.

## Behavior

* **DMs** — authorized inbound posts are forwarded to the agent.
* **Owner DM summaries** — the owner can DM the bot with
  `/summarize <group/person/chat id>` or `总结 <群名或人名>`. The plugin uses
  `RC_USER_*` to resolve and read recent messages from that owner-visible
  group or direct chat, then passes the formatted history to Hermes Agent.
  Natural-language target extraction falls back to Hermes plugin LLM access;
  the plugin does not generate summaries itself.
* **Group / Team chats** — without owner mode, the bot responds when
  explicitly addressed via `![:Person](<bot id>)`. With owner mode, only the
  owner can trigger Hermes; group messages must mention the bot or start with
  `/`. Non-owner group chatter is stored as observed context for later owner
  requests, but never triggers the agent. Group summary commands are blocked
  and should be sent from the owner DM instead.
* **Edits / deletes** — handled through `edit_message` / `delete_message`
  hooks the agent uses for streaming-style responses. If a message was sent
  via owner fallback, later edits/deletes use the owner identity too.
* **Files** — inbound attachments (images, audio, documents) are downloaded
  to the Hermes cache so the vision tool can pick them up by path.
  Outbound media is uploaded via `POST /team-messaging/v1/files` and
  attached to a follow-up post for the caption.
* **Owner fallback** — outbound sends try the bot first. On permission or
  membership failures (`401`, `403`, `404`), Hermes retries with the owner
  identity when owner mode is configured.
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
| Gateway logs owner auth failed | one of `RC_USER_*` is wrong or expired | Re-issue the owner JWT and verify client id/secret |
| `/summarize <target>` says no owner credentials | `RC_USER_*` is incomplete | Set all three owner vars and restart |
| `/summarize <target>` cannot find a chat/person | owner token cannot see the target, or the name is ambiguous | Use the exact group/person name, Person mention, or numeric ID |
| WS keeps disconnecting with `HTTP 401` | bot lacks `WebSocketsSubscription` scope | Add the scope, reinstall the bot |
| Posts succeed but bot never replies in a group | bot wasn't addressed, or sender is not owner in owner mode | Owner should mention the bot or use a `/` command |
| Files arrive but no media surfaces | attachment download blocked by SSRF rule | Verify the chat is accessible to the bot |

## Dependencies

```text
aiohttp>=3.9.0
websockets>=12.0
```

Both are typically already installed; `aiohttp` ships with Hermes core and
`websockets` is a transitive dep of several other adapters.
