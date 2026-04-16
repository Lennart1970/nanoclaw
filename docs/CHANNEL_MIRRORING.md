# Cross-Channel Mirroring: WhatsApp ↔ Slack

NanoClaw can mirror conversations across channels. Messages sent in a WhatsApp group appear in a linked Slack channel and vice versa — same agent, same memory, same context. Agent replies go to both.

## How it works

A new `channel_links` DB table maps a **primary** registered group (e.g. your WhatsApp group) to one or more **linked** channels (e.g. a Slack channel). When a message arrives on either side:

- **Inbound**: Messages from all linked channels are aggregated into a single prompt for the agent. Sender names from linked channels get attribution tags (e.g. `[slack] John`) so the agent knows where each message came from.
- **Outbound**: Agent replies fan out to the primary channel AND all linked channels simultaneously.
- **Context**: All messages share a single cursor and group folder — the agent sees one unified conversation regardless of which channel people are talking in.

## Setup steps

### 1. Prerequisites

- Both channels must be installed and running (e.g. `/add-whatsapp` and `/add-slack`)
- Your primary group must be registered (e.g. a WhatsApp group)
- Your Slack bot must be invited to the target Slack channel

### 2. Get the Slack channel ID

The channel ID is the last segment of the Slack channel URL:

```
https://yourworkspace.slack.com/archives/C0AU5JSDPU0
                                         ^^^^^^^^^^^
```

The NanoClaw JID format is `slack:<channel_id>`, e.g. `slack:C0AU5JSDPU0`.

### 3. Link the channels

From your main group, the agent can issue a `link_channel` IPC command:

```json
{
  "type": "link_channel",
  "primaryJid": "120363XXXXXXXXX@g.us",
  "linkedJid": "slack:C0XXXXXXXXX"
}
```

Or insert directly via SQLite:

```sql
INSERT INTO channel_links (primary_jid, linked_jid, created_at)
VALUES ('120363XXXXXXXXX@g.us', 'slack:C0XXXXXXXXX', datetime('now'));
```

### 4. Restart NanoClaw

```bash
systemctl --user restart nanoclaw
```

Look for `Channel links loaded, linkCount: 1` in the logs to confirm.

## What gets mirrored

| Feature | Supported |
|---------|-----------|
| Text messages (both directions) | Yes |
| Agent replies to both channels | Yes |
| Trigger detection from either channel | Yes |
| Scheduled task output to both channels | Yes |
| Typing indicators on both channels | Yes |
| Channel attribution on sender names | Yes |
| Unlinking via IPC (`unlink_channel`) | Yes |

## Architecture

The implementation touches four files:

- **`src/db.ts`** — New `channel_links` table, CRUD functions, and `getMessagesSinceMulti()` for querying messages across multiple JIDs in one query
- **`src/index.ts`** — `getEffectiveRegisteredGroups()` makes linked JIDs visible to channels without needing separate registration. `sendToGroup()` handles outbound fan-out. The message loop merges linked JID messages into primary group buckets before processing
- **`src/ipc.ts`** — `link_channel` and `unlink_channel` commands (main-group only)

Design principle: linked channels are **not** registered as separate groups. They exist only in the `channel_links` table and get virtual entries at runtime via `getEffectiveRegisteredGroups()`. This means one group folder, one agent session, one memory — just multiple windows into the same conversation.
