# Telegram Notification Integration Guide

## 1. Objective
This document explains how to connect FleetCron Agent to Telegram so that routine job status messages arrive silently while failure alerts trigger push notifications. The guide covers bot creation, multi-recipient group setup, MongoDB-based credential storage, and sample payloads for both silent and high-priority messages.

## 2. Prerequisites
- Telegram account with access to the mobile or desktop app.
- Ability to install the official *Telegram* app on recipients' smartphones (push control is managed per chat).
- Access to the MongoDB instance used by FleetCron (default database: `fleetcron`).
- `mongosh` or `mongo` shell for inserting configuration documents.

## 3. Create a Telegram Bot
1. Open Telegram and start a chat with **@BotFather**.
2. Send `/newbot` and follow the prompts to pick a name and username.
3. BotFather returns an HTTP API token. Record it; this value is needed in later steps.

## 4. Create a Shared Chat for Multiple Recipients
1. In Telegram, create a new group and invite every recipient who should receive notifications.
2. Add the newly created bot to the group as a member.
3. Promote the group to a *supergroup* (Telegram does this automatically after adding the bot or when the member count grows).
4. Obtain the chat ID:
   - Send any message into the group (e.g., `ping`).
   - Visit `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates` in a browser.
   - Locate the JSON payload for your test message and grab `message.chat.id`. For supergroups the ID is negative (e.g., `-1001234567890`).

## 5. Store Telegram Credentials in MongoDB
Credentials can be stored in a dedicated collection so that FleetCron can read them without using environment variables. The example below uses a `notification_configs` collection.

```shell
mongosh "mongodb://localhost:27017/fleetcron"
```

Inside the shell, insert or update the document:

```javascript
db.notification_configs.updateOne(
  { _id: "telegram" },
  {
    $set: {
      bot_token: "123456789:ABCDEF-your-telegram-token",
      chat_id: "-1001234567890",
      default_parse_mode: "MarkdownV2", // optional
      updated_at: new Date()
    }
  },
  { upsert: true }
);
```

If you prefer to keep normal and error notifications in separate chats, add extra fields (e.g., `chat_id_silent`, `chat_id_alert`).

## 6. Sending Messages from FleetCron
Use the Telegram sendMessage API endpoint:

```
POST https://api.telegram.org/bot<bot_token>/sendMessage
```

### 6.1 Routine (Silent) Messages
Set `disable_notification` to `true` so the group receives the text without a push alert.

```json
{
  "chat_id": "-1001234567890",
  "text": "FleetCron Warmup succeeded at 23:55",
  "disable_notification": true
}
```

### 6.2 Failure Alerts with Push
Omit `disable_notification` (or set it to `false`). This forces Telegram to deliver the message with a push notification.

```json
{
  "chat_id": "-1001234567890",
  "text": "🚨 FleetCron Warmup failed: Too many open files",
  "parse_mode": "MarkdownV2"
}
```

### 6.3 Optional Enhancements
- Use `parse_mode` (`MarkdownV2` or `HTML`) for formatting.
- Attach inline keyboards via `reply_markup` if you need quick-action buttons.
- Log each API response; Telegram returns error codes that help diagnose permission or token issues.

## 7. Testing Checklist
1. Run a silent (normal) notification and confirm it appears in the group without triggering a push alert.
2. Trigger a failure path (or temporarily send a simulated alert) and verify the push notification arrives on all recipients' phones.
3. If pushes still appear on normal messages, open the group in Telegram, tap the chat name, and ensure notifications are set to *Muted* on every member’s device. Telegram honors local mute settings before the `disable_notification` flag.

## 8. Operational Tips
- Rotate the bot token whenever someone leaves the team.
- Restrict who can add the bot to other chats to avoid accidental disclosures.
- Consider storing a secondary alert chat ID for catastrophic failures so that paging messages bypass standard mute rules.
- Back up the `notification_configs` document alongside other FleetCron configuration data.

With these steps in place, FleetCron can deliver routine updates quietly and escalate only when a job actually fails.
