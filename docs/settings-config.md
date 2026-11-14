
# Settings

Most settings can be changed from the user profile menu in the hub UI. The configuration file is located at `data/server-config.json`. It is not recommended to edit the file directly unless necessary. If running the hub via Docker, mount the data directory and retain it between upgrades.


## Server


### Server Location
This is used in emails/notifications to enable you to link back to the server. You can change this if you utilize a reverse proxy server to provide HTTPS, or an alternate URL.

| Name      | Description | Example  |
|---        |---|---|
| Protocol   | Server protocol, e.g. http or https   | http  |
| Hostname   | Server name/ip  | 192.168.1.20  |
| Port       | Server Port     | 8082  |


### Server Properties

| Name      | Description | Example  |
|---        |---|---|
| Timezone  | Server timezone for displaying jobs | Europe/London |
| Log Level | Numeric value (0=Error, 1=Warn, 2=Info, 3=Verbose, 4=Debug). Higher = more output. | 2 |

### Definitions Storage

Orchelium supports three storage backends for schedule/job and orchestration definitions.

Default from `2026.06.06.02` onward: `fs`.

Add this block under `server` in `data/server-config.json`:

```json
"definitions": {
  "backend": "fs",
  "jobsDir": "./data/jobs",
  "orchestrationsDir": "./data/orchestrations",
  "stateDir": "./data/.state",
  "watcherDebounceMs": 500,
  "reconciliationIntervalSeconds": 60
}
```

Backend modes:

| Backend | Behavior | Typical use |
|---|---|---|
| `db` | Reads/writes definitions from LevelDB keys only. | Legacy compatibility mode. |
| `hybrid` | Reads merge DB and file definitions (file wins on conflicts). Writes persist to file and mirror to DB. | Safe transition period while moving to file-backed definitions. |
| `fs` | Reads/writes definitions from filesystem assets only. | Default mode from `2026.06.06.02` and long-term target mode. |

Definitions properties:

| Property | Purpose |
|---|---|
| `backend` | Selects active definitions backend (`db`, `hybrid`, `fs`). |
| `jobsDir` | Directory containing schedule/job definition files (`*.job.json`). |
| `orchestrationsDir` | Directory containing orchestration definition files (`*.orch.json`). |
| `stateDir` | Internal operational state directory for definition-store metadata/locks. |
| `watcherDebounceMs` | Debounce window (ms) for filesystem watcher reload events. |
| `reconciliationIntervalSeconds` | Periodic full-rescan interval (seconds) to recover missed watcher events. |

Notes:

| Note | Detail |
|---|---|
| Directory creation | In `fs` and `hybrid` modes, missing `jobsDir`, `orchestrationsDir`, and `stateDir` are created automatically at startup. |
| Environment override | `DEFINITIONS_BACKEND` environment variable overrides `server.definitions.backend` when set. |
| Git tracking | Track `jobsDir` and `orchestrationsDir` in git. Do not track `stateDir` contents. |

Warning for users adopting `2026.06.06.02` onward:

- If your existing schedules/orchestrations were created before filesystem-backed definitions and you have not migrated yet, set `backend` to `hybrid`, run the migration endpoint, verify files, then switch back to `fs`.

### Migration from versions earlier than `2026.06.06.01`

If you are upgrading from a version before `2026.06.06.01`, your schedule/orchestration definitions are likely still DB-backed only.

Use this short migration flow:

1. Set backend to `hybrid` in `data/server-config.json` under `server.definitions.backend`.
2. Restart the hub.
3. Trigger migration once:

```bash
curl -X POST http://localhost:8082/rest/definitions/migrate-db-to-fs \
  -H "Content-Type: application/json" \
  -d '{"deleteSource":false}' \
  -b cookie.txt
```

4. Verify status:

```bash
curl -X GET http://localhost:8082/rest/definitions/status -b cookie.txt
```

5. Confirm files exist in `data/jobs` and `data/orchestrations`.
6. Switch backend to `fs` and restart the hub.

Rollback safety:

- Keeping `deleteSource:false` preserves DB definitions so you can temporarily switch back to `db` if required.
- Only use `deleteSource:true` after you have validated file-backed operation.

---
---


## WebSocket
Properties for the WebSocket server that can be used for agent/hub communication. This is the recommended protocol for agent communication as it requires no external software and works out of the box.
| Name      | Description | Example  |
|---|---|---|
| Enable    | Whether websocket connection is on/off | on |
| Hostname  | Hostname of WebSocket server, typically set to localhost | localhost |
| Port      | WebSocket server port, by default set to 49981 | 49981 |

---
---


## MQTT
Properties for the MQTT server (such as [Eclipse Mosquitto](https://mosquitto.org/)) that can be used for agent/hub communication. This is a secondary and alternative protocol for agent communication where there exists a very large number of agents.

| Name      | Description | Example  |
|---|---|---|
| Enable    | Whether MQTT connection is on/off | on |
| Hostname  | Hostname of MQTT server, typically set to localhost | localhost |
| Port      | MQTT server port, by default set to 1883 | 1883 |
| Username  | Username for authenticated MQTT Server | user |
| Password  | Password for authenticated MQTT Server | Password |

When a username is set, the agent deploy command and the agent installer receive it. See [Agent connection tokens](./agent-connection-token.md) for the broker login and for the separate per-agent token.

---
---

## Notifications
Configuration of notifications for job errors, disconnect/reconnect of agents, etc.

### Alerting

Alerts can be sent to either a webhook, an email address, or just to the console (depending on the log level also)
| Name      | Description | Example  |
|---|---|---|
| Notification Type    | Webhook, Email or Console| Console|


### Configuration
| Name      | Description | Example  |
|---|---|---|
| Disconnect duration before notification | Time (in seconds) between a disconnect/reconnect before a notification is sent. If the server reconnects in this time range, no notification is sent. Default: 5 | 5 (secs) |


### SMTP
Settings for SMTP server, used if the alert notification type is set to email.
| Name      | Description | Example  |
|---|---|---|
| Enable    | Whether SMTP connection is on/off | on |
| SMTP Host | Hostname of SMTP server, typically set to localhost | localhost |
| SMTP Port | SMTP server port | 587 |
| Secure    | Whether SMTP server uses secure comms or not | true |
| SMTP Username | Username for SMTP Server | user |
| SMTP Password | Password for SMTP Server | Password |
| Email from | Sender email address | Orchelium@email.com |
| Email to | Recipient email address | receiver@email.com |


### Webhook
Settings for webhook URL, used if the alert notification type is set to webhook.
| Name      | Description | Example  |
|---|---|---|
| webhook URL    | URL for a webhook that receives the notification text via a POST | http://192.168.1.49:1880/mywebhook |

---
---



## Rule-Based Thresholds (Metric Rules)
Orchelium  uses a flexible rule-based system for threshold jobs. You define rules for each job or agent. Rules can trigger jobs based on metrics like CPU, storage, file count, file age, and more.

When a rule triggers a job, the metric data and condition information are passed to the job as **trigger context**, making it available to scripts and orchestrations.

### How Trigger Context Works

When a rule fires and triggers a job:

1. **Scripts receive environment variables** containing the metric data:
   - `$ORCHELIUM_METRIC_TYPE` - Type of metric (cpu, mount_usage, file_count, etc.)
   - `$ORCHELIUM_METRIC_VALUE` - Actual value (e.g., 92.5)
   - `$ORCHELIUM_METRIC_PATH` - Path being monitored (if applicable)
   - `$ORCHELIUM_CONDITION_OPERATOR` - The operator used (>=, <=, etc.)
   - `$ORCHELIUM_CONDITION_THRESHOLD` - Threshold value
   - And more (see [TRIGGER_CONTEXT_GUIDE.md](../TRIGGER_CONTEXT_GUIDE.md))

2. **Orchestrations can use template substitution** in parameters:
   - `#{context.metric.value}` - Gets replaced with actual metric value
   - `#{context.metric.path}` - Gets replaced with the path
   - Example: `--cleanup-percent #{context.metric.value}` becomes `--cleanup-percent 92.5`

3. **Full JSON context** available as `$ORCHELIUM_TRIGGER_CONTEXT` for advanced processing

### Example Rule Configuration
Add a `rules` section to your job or agent config:

```json
{
	"rules": [
		{
			"metric": {
				"type": "mount_usage",
				"agent": "agent1",
				"path": "/mnt/data"
			},
			"condition": {
				"operator": ">=",
				"threshold": 90
			},
			"cooldown": 60, // minutes between triggers
			"job": "backup-job-on-high-usage"
		}
	]
}
```

#### Supported Metrics
- `cpu` (CPU usage %)
- `mount_usage` (disk usage % for a mount path)
- `dir_size` (directory size in bytes)
- `file_size` (file size in bytes)
- `file_count` (number of files in a directory)
- `file_age` (age of newest/oldest file)

#### Supported Operators
- `>`, `>=`, `<`, `<=`, `==`, `!=`

#### Cooldown
Prevents the rule from triggering repeatedly within the cooldown period (in minutes).

#### Migration Note
Old threshold job settings are deprecated. Migrate to the new rule format for all threshold-based automations.

---
---

## Agent Concurrency
You can control how many jobs each agent can run at the same time. By default, each agent allows up to 3 concurrent jobs. To change this, set the `concurrency` property in the agent configuration:

```json
{
	"name": "agent1",
	"concurrency": 5
}
```

If not set, the default is 3. When the limit is reached, new jobs will be queued or skipped until capacity is available.

---
---

## Job Icons
Provide and add any icon name from [here](https://marella.me/material-icons/demo/) into the list by pressing the [+] button, then the icon can be used to identify any schedule you create.  Additionally, you can press the trash can icon to delete an icon you don't want to use in schedule configuration.

---
---

## Backup & Restore

Orchelium provides comprehensive backup and restore functionality to protect your configuration and data. Backups can be created and restored through the Settings page.

### What Gets Backed Up

The following items can be included in backups:

| Item | Description |
|---|---|
| **Server Configuration** | MQTT, SMTP, WebSocket, Notifications, Thresholds, Icons settings |
| **Agents Configuration** | All registered agent configurations |
| **Job Schedules** | All configured job schedules |
| **Job Execution History** | Historical logs of all job executions |
| **Orchestration Jobs** | All configured orchestration job definitions |
| **Orchestration Execution History** | Historical records of orchestration job executions |
| **Agent Connection History** | Historical logs of agent connections |
| **User Accounts** | All registered user accounts with bcrypt-hashed passwords |

### Creating a Backup

1. Navigate to **Settings** (gear icon in top menu)
2. Scroll to **Backup & Restore** section
3. Click **Create Backup**
4. Select which items to include in the backup
5. Click **Create** — a `.zip` file will download automatically
6. Store the backup file in a safe location

### Restoring from Backup

1. Navigate to **Settings** (gear icon in top menu)
2. Scroll to **Backup & Restore** section under **Restore from Backup**
3. Click **Choose File** and select a previously saved backup `.zip` file
4. Click **Restore Backup**
5. The system will restore all selected items from the backup
6. **Restart the Orchelium** to ensure all changes take effect

### Important Notes

- **No encryption key needed**: User passwords are bcrypt-hashed (not encrypted), so backups can be restored to any Orchelium installation
- **User accounts included**: When you restore a backup, all user accounts are restored with their original login credentials
- **Historical data**: Job history and execution logs are included, so you maintain a complete record of past executions
- **Server restart required**: After restoring a backup, restart Orchelium for all changes to take effect
- **Backup location**: Keep backups in a secure location; treat them as sensitive data since they contain service credentials

---
---

## Webhook Management

Webhooks allow external systems to trigger Orchelium jobs with custom data. Detailed webhook API documentation is available in the [REST API Reference](./REST_API_REFERENCE.md).

### Using the Web Interface (Recommended)

**For most users**, manage webhooks through the Orchelium settings interface:

1. **Navigate to Settings → Webhooks tab**
2. **Create Webhook** - Click "Create Webhook" button
   - Enter webhook name and optional description
   - System generates and displays API key (copy immediately - you won't see it again!)
   - Webhook is instantly ready to use
3. **Edit Webhook** - Click edit icon to rename or update description
4. **Rotate API Key** - Click key icon to generate a new key (old key immediately invalidated)
5. **Delete Webhook** - Click delete icon with confirmation
6. **View Statistics** - See system-wide webhook trigger counts and last triggered times

The UI shows for each webhook:
- **Name & Description** - Identify and document the webhook
- **Status** - Active (green) or Inactive (grey)
- **Trigger Count** - Total times this webhook has been triggered
- **Last Triggered** - Timestamp of most recent trigger

**Webhook Table in Settings:**
```
Name                    | Description              | Status   | Triggers | Last Triggered
External Alert          | Monitoring system alert  | ● Active | 12       | 2 hours ago
GitHub Actions          | CI/CD pipeline trigger   | ● Active | 5        | 1 day ago
Backup Sync             | File sync webhook        | ● Inactive | 0      | Never
```

### Creating a Webhook via API

To manually create webhooks via API (for automation/scripting):

```bash
curl -X POST "http://your-server:8082/rest/webhooks/my-job" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "External System Alert",
    "description": "Webhook for external monitoring system"
  }'
```

Response includes the newly generated API key (UUID):
```json
{
  "success": true,
  "webhook": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "External System Alert",
    "apiKey": "a1b2c3d4-e5f6-4a5b-8c9d-e1f2g3h4i5j6",
    "isActive": true,
    "createdAt": "2026-04-04T10:30:00.000Z"
  }
}
```

### Managing Webhooks

**List all webhooks for a job:**
```bash
curl "http://your-server:8082/rest/webhooks/my-job" \
  -H "Authorization: Bearer session-cookie"
```

**Disable/enable webhook:**
```bash
curl -X PUT "http://your-server:8082/rest/webhooks/my-job/webhook-id" \
  -d '{"isActive": false}'
```

**Rotate API key (generate new one):**
```bash
curl -X POST "http://your-server:8082/rest/webhooks/my-job/webhook-id/rotate-key" \
  -d '{"oldKey": "old-uuid-here"}'
```

**Delete webhook:**
```bash
curl -X DELETE "http://your-server:8082/rest/webhooks/my-job/webhook-id"
```

### Using Webhook API Keys

- **Keys are UUID v4 format** - example: `550e8400-e29b-41d4-a716-446655440000`
- **Keys are only shown once** when created - save them securely
- **Keys can be rotated** anytime without affecting webhook metadata
- **Active/Inactive status** - disable webhooks without deleting them
- **Trigger tracking** - system records last trigger time and total count

### Webhook Payload in Jobs

When external systems trigger a webhook, the JSON payload becomes available to:

**Scripts:** Via environment variable
```bash
# $ORCHELIUM_TRIGGER_CONTEXT contains full JSON payload
payload=$(echo "$ORCHELIUM_TRIGGER_CONTEXT" | jq '.payload')
```

**Orchestrations:** Via template substitution
```json
{
  "parameters": "--event #{context.payload.event} --severity #{context.payload.severity}"
}
```

For complete webhook examples and API reference, see [REST_API_REFERENCE.md](./REST_API_REFERENCE.md#webhook-triggers) and [TRIGGER_CONTEXT_GUIDE.md](../TRIGGER_CONTEXT_GUIDE.md).

For developers integrating with the webhook UI, see [Phase 3: Webhook Management UI Developer Guide](./Developers/phase3-webhook-ui-guide.md).

---
---

Configuration file can be found in `data/server-config.json`.

The majority of the settings can be changed from the user experience. It is not recommended to change the configuration file directly. If running the hub via Docker, mount the data directory and retain it between upgrades of the hub.

The data directory contains all the configuration files, as well as data stores.

---

## Related Documentation

- [Installation](./installation.md): Setting up Orchelium
- [Agent connection tokens](./agent-connection-token.md): How agents prove their identity
- [Backup Schedules](./backup-schedules.md): Creating and managing schedules
- [Orchestrations](./orchestrations.md): Building complex workflows
- [User Management](./user-management.md): User accounts and access control
- [REST API Reference](./REST_API_REFERENCE.md): API endpoints
- [Back to Documentation Index](./README.MD)
