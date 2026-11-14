# Developer Database Reference

This document describes all persistent databases used in Orchelium, their structure, and provides code samples for accessing each. All databases use [LevelDB](https://github.com/Level/level) via the `level` Node.js package, with JSON value encoding.

---

## 1. Main Data Database (`data.db`)
- **Location:** `./data/data.db/`
- **Purpose:** Stores general application data, including:
  - Schedules
  - Orchestration jobs
  - Notification data
  - Other key-value data

### Key Structures
- `SCHEDULES_CONFIG` (Array of schedule objects)
- `ORCHESTRATION_JOBS` (Object: jobId → orchestration job definition)
- `NOTIFICATIONS_DATA` (Array of notification objects)
- `JOB_HISTORY` (Array of job history items)
- `AGENTS_CONFIG` (Object: agent name → agent record)
- `AGENT_CREDENTIALS` (Object: agent name → connection-token hash and encrypted token). See [Agent connection tokens](../agent-connection-token.md). The plain token is not stored in this record.

The Express session signing secret is not a database key. It is the file `data/session-secret`, created on first startup when the file is missing.

The hub encryption key for SMTP, MQTT, and stored agent tokens is the file `data/encryption-key`. On first startup, when that file is missing: use `ORCHELIUM_ENCRYPTION_KEY` if set; otherwise write `CHANGEIT` when encrypted secrets already exist and recommend a change in the notification list; otherwise write a random key.

### Example: Accessing Data
```js
const db = require('./db.js');

// Get all schedules
const schedules = await db.getData('SCHEDULES_CONFIG');

// Put new schedules
await db.putData('SCHEDULES_CONFIG', schedulesArray);

// Get orchestration jobs
const jobs = await db.getData('ORCHESTRATION_JOBS');

// Add a notification
let notifications = await db.getData('NOTIFICATIONS_DATA');
notifications.push({ type: 'info', message: 'Job completed' });
await db.putData('NOTIFICATIONS_DATA', notifications);
```

---

## 2. User Database (`user.db`)
- **Location:** `./data/user.db/`
- **Purpose:** Stores user credentials and metadata.
- **Structure:**
  - Key: `username` (string)
  - Value: `{ username, email, password (bcrypt hash), ... }`

### Example: Accessing Users
```js
const Users = require('./models/user.js');

// Create user
await Users.createUser('alice', 'alice@example.com', 'password123');

// Get user by username
const user = await Users.getUserByUsername('alice');

// Update password
await Users.updatePassword('alice', 'newPassword');
```

---

## 3. Agent History Database (`agentHistory.db`)
- **Location:** `./data/agentHistory.db/`
- **Purpose:** Tracks agent status changes and history.
- **Structure:**
  - Key: `status:<agentId>:<ISODate>`
  - Value: `{ date, message, job }`

### Example: Accessing Agent History
```js
const agentHistory = require('./agentHistory.js');

// Add a status change
await agentHistory.addStatus('agent1', new Date(), 'online', 'BackupDB');

// Get all status changes for an agent
const statusList = await agentHistory.getStatus('agent1');
```

---

## 4. Data Access Patterns

### General DB API (`db.js`)
- `initializeDB(dbPath)` — Initialize the database
- `getData(key)` — Get value by key (returns JSON)
- `putData(key, value)` — Store value by key
- `deleteData(key)` — Delete value by key

### Example: Custom Data
```js
const db = require('./db.js');

// Store custom data
await db.putData('MY_KEY', { foo: 'bar' });

// Retrieve custom data
const myData = await db.getData('MY_KEY');
```

---


---

## 5. Schedule Data Structure

**Key:** `SCHEDULES_CONFIG`

**Each record (schedule object):**

| Field           | Type     | Description                                 |
|-----------------|----------|---------------------------------------------|
| jobName         | string   | Unique job name                             |
| description     | string   | Description of the schedule                 |
| scheduleType    | string   | Type: 'daily', 'weekly', 'monthly', etc.    |
| scheduleTime    | string   | Time of day (e.g., '02:00')                 |
| dayOfWeek       | string   | (Optional) Day of week for weekly jobs      |
| dayInMonth      | string   | (Optional) Day in month for monthly jobs    |
| agent           | string   | Agent name (for classic mode)               |
| command         | string   | Command to run (for classic mode)           |
| commandParams   | object   | Command parameters (for classic mode)       |
| color           | string   | Color for UI                                |
| icon            | string   | Icon for UI                                 |
| scheduleMode    | string   | 'classic' or 'orchestration'                |
| orchestrationId | string   | (Orchestration mode) Linked orchestration   |
| lastUpdated     | string   | ISO date string                             |

**Example:**
```json
{
  "jobName": "BackupDB",
  "description": "Nightly DB backup",
  "scheduleType": "daily",
  "scheduleTime": "02:00",
  "agent": "agent1",
  "command": "backupDB.sh",
  "commandParams": { "db": "main" },
  "color": "#2196f3",
  "icon": "database",
  "scheduleMode": "classic",
  "lastUpdated": "2026-03-27T01:00:00Z"
}
```

---

## 6. Orchestration Job Structure

**Key:** `ORCHESTRATION_JOBS`

**Each record (object property):**

| Field         | Type     | Description                                 |
|-------------- |----------|---------------------------------------------|
| id            | string   | Unique job ID                               |
| name          | string   | Name of orchestration job                   |
| description   | string   | Description                                 |
| type          | string   | 'orchestration'                             |
| createdAt     | string   | ISO date string (created)                   |
| updatedAt     | string   | ISO date string (last updated)              |
| currentVersion| number   | Current version number                      |
| versions      | array    | Array of versioned job definitions          |

**Each version object:**
| Field      | Type     | Description                |
|------------|----------|----------------------------|
| version    | number   | Version number             |
| nodes      | array    | Node definitions           |
| edges      | array    | Edge definitions           |
| createdAt  | string   | ISO date string            |

**Example:**
```json
{
  "jobId123": {
    "id": "jobId123",
    "name": "Nightly Backup",
    "description": "Runs all backup scripts",
    "type": "orchestration",
    "createdAt": "2026-03-26T20:00:00Z",
    "updatedAt": "2026-03-27T01:00:00Z",
    "currentVersion": 2,
    "versions": [
      {
        "version": 1,
        "nodes": [ ... ],
        "edges": [ ... ],
        "createdAt": "2026-03-26T20:00:00Z"
      },
      {
        "version": 2,
        "nodes": [ ... ],
        "edges": [ ... ],
        "createdAt": "2026-03-27T01:00:00Z"
      }
    ]
  }
}
```

---

## 7. Notification Data Structure

**Key:** `NOTIFICATIONS_DATA`

**Each record (notification object):**

| Field       | Type     | Description                        |
|-------------|----------|------------------------------------|
| type        | string   | Notification type ('info', 'warn') |
| title       | string   | Title of notification              |
| description | string   | Description/details                |
| runDate     | string   | ISO date string                    |
| url         | string   | (Optional) Related URL             |

**Example:**
```json
{
  "type": "info",
  "title": "Backup Completed",
  "description": "Nightly backup finished successfully.",
  "runDate": "2026-03-27T02:00:00Z",
  "url": "http://example.com/backup/123"
}
```

---

## 8. Job History Structure

**Key:** `JOB_HISTORY`

**Each record (job run history object):**

| Field        | Type     | Description                        |
|--------------|----------|------------------------------------|
| jobName      | string   | Name of the job                    |
| runDate      | string   | ISO date string (run time)         |
| returnCode   | number   | Exit code (0 = success)            |
| runTime      | number   | Duration in seconds                |
| log          | string   | Log output                         |
| manual       | boolean  | Was run manually                   |
| executionId  | string   | (Optional) Orchestration exec ID   |

**Example:**
```json
{
  "jobName": "BackupDB",
  "runDate": "2026-03-27T02:00:00Z",
  "returnCode": 0,
  "runTime": 120,
  "log": "Job completed successfully",
  "manual": false,
  "executionId": "exec-abc-123"
}
```

---

## 9. User Data Structure

**Database:** `user.db` (LevelDB)

**Each record (user object):**

| Field                | Type     | Description                        |
|----------------------|----------|------------------------------------|
| username             | string   | Username (key)                     |
| email                | string   | User email                         |
| password             | string   | Bcrypt password hash               |
| resetPasswordToken   | string   | (Optional) Password reset token    |
| resetPasswordExpires | number   | (Optional) Expiry timestamp (ms)   |

**Example:**
```json
{
  "username": "alice",
  "email": "alice@example.com",
  "password": "$2b$10$...",
  "resetPasswordToken": "abc123",
  "resetPasswordExpires": 1764350400000
}
```

---

## 10. Agent History Data Structure

**Database:** `agentHistory.db` (LevelDB)

**Each record:**

| Field   | Type     | Description                        |
|---------|----------|------------------------------------|
| date    | string   | ISO date string                    |
| message | string   | Status message                     |
| job     | string   | Job name (if applicable)           |

**Key format:** `status:<agentId>:<ISODate>`

**Example:**
```json
{
  "date": "2026-03-27T02:00:00Z",
  "message": "online",
  "job": "BackupDB"
}
```

---

## 11. Custom Key-Value Data

**Database:** `data.db` (LevelDB)

**Each record:**

| Key   | Value (any JSON-serializable type) |
|-------|------------------------------------|
| e.g.  | string, number, object, array      |

**Example:**
```json
{
  "MY_KEY": { "foo": "bar" },
  "ANOTHER_KEY": [1,2,3]
}
```

---

## Additional Notes

- All LevelDB databases are initialized with `{ valueEncoding: 'json' }`.
- Keys are case-sensitive.
- For iterating all records (e.g., all users), use the LevelDB iterator API.
- For more details, see the code in `db.js`, `models/user.js`, `agentHistory.js`, `history.js`, `notificationData.js`, `scheduler.js`, and `orchestration.js`.
