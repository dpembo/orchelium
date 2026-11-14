# Orchelium REST API Reference

This document describes the main REST API endpoints exposed by the Orchelium server (see `server.js`). Endpoints are grouped by functional area. All endpoints require authentication unless otherwise noted.

---


## Table of Contents
- [Authorization & Authentication](#authorization--authentication)

- [Backup Management](#backup-management)
  - [Backup Items](#backup-items)
  - [Backup Create](#backup-create)
  - [Backup Restore](#backup-restore)

- [Webhook Triggers](#webhook-triggers)
  - [Webhook Trigger Job](#webhook-trigger-job)
  - [Check Webhook Execution Status](#check-webhook-execution-status)

- [Webhook Management](#webhook-management)
  - [List All Webhooks (System-wide)](#list-all-webhooks-system-wide)
  - [List Webhooks for Job](#list-webhooks-for-job)
  - [Create Webhook](#create-webhook)
  - [Update Webhook](#update-webhook)
  - [Rotate Webhook Key](#rotate-webhook-key)
  - [Delete Webhook](#delete-webhook)
  - [Get Webhook Statistics](#get-webhook-statistics)

- [Notifications](#notifications)

  - [Notifications List](#notifications-list)
  - [Notifications Create](#notifications-create)
  - [Notifications Update](#notifications-update)
  - [Notifications Count](#notifications-count)
  - [Notifications Delete All](#notifications-delete-all)
  - [Notifications Delete by Index](#notifications-delete-by-index)
  - [Notifications Test](#notifications-test)

  - [Job History List](#job-history-list)
  - [Job History Get](#job-history-get)
  - [Job History Create](#job-history-create)
  - [Job History Update](#job-history-update)
  - [Job History Delete](#job-history-delete)

  - [Running Jobs List](#running-jobs-list)
  - [Running Jobs Delete by Index](#running-jobs-delete-by-index)
  - [Running Jobs Delete by Name](#running-jobs-delete-by-name)
  - [Running Jobs Delete by Execution ID](#running-jobs-delete-by-execution-id)
  - [Running Jobs Delete All](#running-jobs-delete-all)

  - [User List](#user-list)
  - [User Get](#user-get)
  - [User Create](#user-create)
  - [User Update](#user-update)
  - [User Delete](#user-delete)

  - [Generic Key-Value Data](#generic-key-value-data)

- [Agent Management](#agent-management)
  - [Agent Details](#agent-details)
  - [Agent Ping](#agent-ping)
  - [Agent History & Stats](#agent-history--stats)
  - [Agent Query by Name](#agent-query-by-name)
  - [Agent Update Status (Is Running)](#agent-update-status-is-running)
  - [Agent Update Status (Detailed)](#agent-update-status-detailed)
  - [Agent Start Update Job](#agent-start-update-job)

- [Templates](#templates)
  - [Templates List](#templates-list)
  - [Templates Refresh](#templates-refresh)

- [Debug & System](#debug--system)
  - [Debug Info](#debug-info)
  - [Debug Logs](#debug-logs)
  - [Debug Logger Trace](#debug-logger-trace)
  - [Debug Logger Warn](#debug-logger-warn)
  - [Server Time](#server-time)

- [Other REST Endpoints](#other-rest-endpoints)
  - [Jobs Config](#jobs-config)
  - [ETA Config](#eta-config)
  - [Script Details](#script-details)
  - [Delete a Script](#delete-a-script)
  - [Jobs Using a Script](#jobs-using-a-script)

- [Orchestration Management](#orchestration-management)
  - [List All Orchestration Jobs](#list-all-orchestration-jobs)
  - [Get an Orchestration Job](#get-an-orchestration-job)
  - [Create an Orchestration Job](#create-an-orchestration-job)
  - [Update an Orchestration Job](#update-an-orchestration-job)
  - [Delete an Orchestration Job](#delete-an-orchestration-job)
  - [Execute an Orchestration Job](#execute-an-orchestration-job)
  - [Get Execution History](#get-execution-history)
  - [Get Orchestration Job Metadata](#get-orchestration-job-metadata)
  - [Get a Specific Version of an Orchestration Job](#get-a-specific-version-of-an-orchestration-job)
  - [List Available Scripts](#list-available-scripts-for-orchestration-palette)
  - [List Available Agents](#list-available-agents-for-orchestration-palette)

- [Script Test Management](#script-test-management)
  - [Start a Script Test](#start-a-script-test)
  - [Get Script Test Execution State](#get-script-test-execution-state)
  - [Get a Script Test Execution](#get-a-script-test-execution)
  - [Acknowledge a Completed Test](#acknowledge-a-completed-test)
  - [Discard a Retained Test Result](#discard-a-retained-test-result)
  - [Terminate an Active Test](#terminate-an-active-test)

- [Job History](#job-history)
  - [Job History List](#job-history-list)
  - [Job History Get](#job-history-get)
  - [Job History Create](#job-history-create)
  - [Job History Update](#job-history-update)
  - [Job History Delete](#job-history-delete)
  - [Job History Clear All](#job-history-clear-all)

- [Schedule Management](#schedule-management)
  - [List All Schedules](#list-all-schedules)
  - [Get a Schedule by Job Name](#get-a-schedule-by-job-name)
  - [Delete a Schedule by Job Name](#delete-a-schedule-by-job-name)
  - [Delete All Schedules](#delete-all-schedules)

---

## Authorization & Authentication

All REST API endpoints require authentication unless otherwise noted. Orchelium uses session-based authentication and CSRF protection for form-based routes. Key points:

- **Session Authentication:**
  - Users log in via `/login.html` using a username and password.
  - On successful login, a session is created and stored using `express-session`.
  - The session is maintained via a session cookie.
  - Most API routes are protected by the `User.isAuthenticated` middleware, which checks for a valid session (`req.session.user`).
  - If not authenticated, the user is redirected to the login page.

- **User Management:**
  - User credentials are stored in a LevelDB database with passwords hashed using bcrypt.
  - Registration is available via `/register.html` if no users exist.
  - Password reset is supported via email with a time-limited token.

- **CSRF Protection:**
  - Form-based routes (e.g., settings, registration) require a CSRF token, generated per session and validated on submission.
  - REST API routes (those starting with `/rest` or `/api`) do **not** require CSRF tokens and are excluded from CSRF validation.

- **Public vs. Protected Routes:**
  - Public: `/login.html`, `/register.html`, `/forgot.html`, `/reset/...`
  - `POST /saveScript` requires a session with `scripts.create` or `scripts.edit`, and an `X-CSRF-Token` header.
  - All other routes require authentication.

- **Example Authentication Flow:**
  1. User visits `/login.html` and submits credentials.
  2. If valid, `req.session.user` is set and the user is redirected to the app.
  3. All subsequent requests include the session cookie for authentication.

---

## Backup (Internal data) Management

#### Backup Items
`GET /api/backup/items` | List available backup data items that can be included in a backup.

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | JSON object with available backup items | `{ "serverConfig": {...}, "agentsConfig": {...}, "userAccounts": {...} }` |

**Available items:**
- `serverConfig`: Server Configuration (MQTT, SMTP, WebSocket, Notifications, Thresholds, Icons)
- `agentsConfig`: Agents Configuration
- `schedules`: Backup Schedules
- `jobHistory`: Job Execution History
- `orchestrationJobs`: Orchestration Job Definitions
- `orchestrationExecutions`: Orchestration Execution History
- `agentHistory`: Agent Connection History
- `userAccounts`: User Accounts (with bcrypt-hashed passwords)

#### Create Backup
`POST /api/backup/create` | Create a backup zip file with selected items.

| Property | Value | Example |
|---|---|---|
| **Input** | JSON body with boolean flags for each item | `{ "serverConfig": true, "userAccounts": true, "jobHistory": false, ... }` |
| **Output** | Binary zip file attachment | `orchelium-backup-20260328.zip` |

**Request body options:**
```json
{
  "serverConfig": true,
  "agentsConfig": true,
  "schedules": true,
  "jobHistory": true,
  "orchestrationJobs": true,
  "orchestrationExecutions": true,
  "agentHistory": true,
  "userAccounts": true
}
```

#### Restore Backup
`POST /api/backup/restore` | Restore Orchelium settings and data from a backup zip file.

| Property | Value | Example |
|---|---|---|
| **Input** | Multipart form with `backupFile` | Upload `.zip` file created by `/api/backup/create` |
| **Output** | JSON with restore results | `{ "success": true, "itemsRestored": [...], "warnings": [...] }` |

**Response example:**
```json
{
  "success": true,
  "itemsRestored": [
    "Server Configuration",
    "Agents Configuration",
    "Job Schedules",
    "User Accounts"
  ],
  "warnings": [],
  "recommendations": [
    "Restart the Orchelium to ensure all changes take effect."
  ]
}
```

**Important:** After restoring a backup, restart Orchelium for all changes to take effect.

---

## Webhook Triggers

### Public Endpoint (No Authentication Required)

Webhooks allow external systems to trigger jobs and pass custom data that becomes available to scripts and orchestrations.

#### Webhook Trigger Job
`POST /api/webhook/trigger/:jobName?key=<webhook-key>` | Trigger a job via webhook with custom JSON payload.

| Property | Value | Example |
|---|---|---|
| **Authentication** | Webhook API Key (UUID v4 format) | `550e8400-e29b-41d4-a716-446655440000` |
| **Key Location** | Query parameter `?key=` OR header `X-Webhook-Key` | `?key=UUID` or header value |
| **Input** | JSON object with arbitrary payload | `{ "event": "alert", "usage": 95, "path": "/mnt/data" }` |
| **Response Status** | 202 Accepted (job queued) | Async execution - returns immediately |
| **Output** | JSON with execution details and status URL | `{ "success": true, "executionId": "...", "statusUrl": "..." }` |

**Request examples:**

Via Query Parameter:
```bash
curl -X POST "http://your-server:8082/api/webhook/trigger/backup-job?key=550e8400-e29b-41d4-a716-446655440000" \
  -H "Content-Type: application/json" \
  -d '{
    "status": "alert",
    "metric": "disk_usage",
    "value": 95,
    "mount": "/mnt/data"
  }'
```

Via Header:
```bash
curl -X POST "http://your-server:8082/api/webhook/trigger/backup-job" \
  -H "Content-Type: application/json" \
  -H "X-Webhook-Key: 550e8400-e29b-41d4-a716-446655440000" \
  -d '{
    "status": "alert",
    "value": 95
  }'
```

**Response example (202 Accepted - Job Queued):**
```json
{
  "success": true,
  "jobName": "backup-job",
  "executionId": "a1b2c3d4e5f6g7h8",
  "message": "Job triggered via webhook and queued for execution",
  "statusUrl": "/rest/orchestration/executions/a1b2c3d4e5f6g7h8"
}
```

**Response example (Error - 401/400):**
```json
{
  "error": "Invalid webhook API key for this job"
}
```

**Important Notes:**

- **Async Execution**: Webhook returns **immediately** (202 Accepted) before job completes
- **No Authentication Required**: Webhooks are publicly accessible — security relies on the UUID key
- **Long-Running Jobs**: Perfect for orchestrations that take hours to complete
- **Check Status**: Use the `statusUrl` to poll job execution status if needed
- **Payload Available**: The webhook JSON payload is available to scripts and orchestrations as environment variables or through trigger context
- **Trigger Context**: Webhook triggers create a trigger context that's passed to jobs, similar to rule-triggered jobs
- **For scripts**: Payload data is available via `$ORCHELIUM_TRIGGER_CONTEXT` environment variable (as JSON)
- **For orchestrations**: Payload data accessible via template substitution `#{context.payload.*}`

#### Check Webhook Execution Status
`GET /orchestration/execution/details?jobId=<jobId>&executionId=<executionId>` | Check the status and details of a webhook-triggered orchestration execution.

| Property | Value | Example |
|---|---|---|
| **Authentication** | Required (User session) | |
| **Input** | Query parameters: `jobId`, `executionId` | `?jobId=backup-db&executionId=a1b2c3d4e5f6g7h8` |
| **Output** | JSON with execution details, nodes, and status | Full orchestration execution state |

**Request example:**
```bash
curl -X GET "http://your-server:8082/orchestration/execution/details?jobId=backup-db&executionId=a1b2c3d4e5f6g7h8" \
  -H "Cookie: session=<session-cookie>"
```

**Response example (Still Running - 200 OK):**
```json
{
  "jobId": "backup-db",
  "jobName": "Database Backup",
  "execution": {
    "executionId": "a1b2c3d4e5f6g7h8",
    "status": "running",
    "finalStatus": null,
    "startTime": "2026-04-04T14:30:00.000Z",
    "endTime": null,
    "duration": 125000
  },
  "nodes": [...],
  "edges": [...],
  "visitedNodes": ["start", "execute-backup"],
  "nodeScriptOutputs": {...}
}
```

**Response example (Completed - 200 OK):**
```json
{
  "jobId": "backup-db",
  "jobName": "Database Backup",
  "execution": {
    "executionId": "a1b2c3d4e5f6g7h8",
    "status": "completed",
    "finalStatus": "success",
    "startTime": "2026-04-04T14:30:00.000Z",
    "endTime": "2026-04-04T14:32:15.000Z",
    "duration": 135000
  },
  "nodes": [...],
  "edges": [...],
  "visitedNodes": ["start", "execute-backup", "end"],
  "nodeScriptOutputs": {
    "execute-backup": {
      "status": "executed",
      "exitCode": 0,
      "output": "Backup completed successfully"
    }
  }
}
```

**Common Status Values:**
- `status`: `running` | `completed`
- `finalStatus`: `success` | `error` | `null` (if still running)

**Use Case:** Poll this endpoint to monitor webhook-triggered orchestration progress in real-time.

---

## Webhook Management

### List All Webhooks (System-wide)
`GET /rest/webhooks-all` | List all webhooks across all jobs with job name included.

| Property | Value | Example |
|---|---|---|
| **Authentication** | Required (User session) | |
| **Input** | None | |
| **Output** | JSON array of webhooks with jobName | `[{ "id": "...", "jobName": "job1", "name": "Alert Webhook", "isActive": true }]` |

**Response example:**
```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "jobName": "backup-database",
    "name": "Alert Webhook",
    "description": "External alerts",
    "isActive": true,
    "createdAt": "2026-04-04T10:30:00.000Z",
    "lastTriggeredAt": "2026-04-04T12:15:00.000Z",
    "triggerCount": 3
  },
  {
    "id": "a1b2c3d4-e5f6-4a5b-8c9d-e1f2g3h4i5j6",
    "jobName": "backup-files",
    "name": "Jenkins webhook",
    "description": "CI/CD pipeline trigger",
    "isActive": true,
    "createdAt": "2026-04-01T14:20:00.000Z",
    "lastTriggeredAt": "2026-04-04T09:00:00.000Z",
    "triggerCount": 15
  }
]
```

**Use Case**: View all webhooks configured across the entire system, useful for auditing and management.

---

### List Webhooks for Job
`GET /rest/webhooks/:jobName` | List all webhooks configured for a specific job.

| Property | Value | Example |
|---|---|---|
| **Authentication** | Required (User session) | |
| **Input** | None (URL parameter only) | |
| **Output** | JSON array of webhooks | `[{ "id": "...", "name": "Alert Webhook", "isActive": true }]` |

**Response example:**
```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Alert Webhook",
    "description": "External alerts",
    "isActive": true,
    "createdAt": "2026-04-04T10:30:00.000Z",
    "lastTriggeredAt": "2026-04-04T12:15:00.000Z",
    "triggerCount": 3
  }
]
```

**Note**: API keys are not returned in list responses for security. Keys are only shown when creating a new webhook.

---

### Create Webhook
`POST /rest/webhooks/:jobName` | Create a new webhook for a job.

| Property | Value | Example |
|---|---|---|
| **Authentication** | Required (User session) | |
| **Input** | JSON with `name` and optional `description` | `{ "name": "External Alert", "description": "Slack webhook" }` |
| **Output** | JSON with new webhook details (including API key) | `{ "success": true, "webhook": {...} }` |

**Request body:**
```json
{
  "name": "External Alert Webhook",
  "description": "Sends alerts to external monitoring system"
}
```

**Response example (API key only shown on creation):**
```json
{
  "success": true,
  "webhook": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "External Alert Webhook",
    "description": "Sends alerts to external monitoring system",
    "apiKey": "550e8400-e29b-41d4-a716-446655440000",
    "isActive": true,
    "createdAt": "2026-04-04T10:30:00.000Z"
  }
}
```

**⚠️ Save the API key**: The key is only displayed once. Save it securely if you need to use it later.

---

### Update Webhook
`PUT /rest/webhooks/:jobName/:webhookId` | Update webhook name, description, or status.

| Property | Value | Example |
|---|---|---|
| **Authentication** | Required (User session) | |
| **Input** | JSON with fields to update | `{ "name": "New Name", "isActive": false }` |
| **Output** | Updated webhook object (without API key) | |

**Request body (any combination):**
```json
{
  "name": "Updated Webhook Name",
  "description": "Updated description",
  "isActive": false
}
```

**Response example:**
```json
{
  "success": true,
  "webhook": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Updated Webhook Name",
    "description": "Updated description",
    "isActive": false,
    "createdAt": "2026-04-04T10:30:00.000Z"
  }
}
```

---

### Rotate Webhook Key
`POST /rest/webhooks/:jobName/:webhookId/rotate-key` | Generate a new API key for the webhook.

| Property | Value | Example |
|---|---|---|
| **Authentication** | Required (User session) | |
| **Input** | JSON with `oldKey` (for verification) | `{ "oldKey": "550e8400-..." }` |
| **Output** | Updated webhook with new API key | |

**Request body:**
```json
{
  "oldKey": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Response example:**
```json
{
  "success": true,
  "webhook": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "External Alert Webhook",
    "apiKey": "a1b2c3d4-e5f6-4a5b-8c9d-e1f2g3h4i5j6",
    "keyRotatedAt": "2026-04-04T15:45:00.000Z"
  }
}
```

**Note**: The old key immediately becomes invalid. Update your webhook trigger code with the new key.

---

### Delete Webhook
`DELETE /rest/webhooks/:jobName/:webhookId` | Delete a webhook.

| Property | Value | Example |
|---|---|---|
| **Authentication** | Required (User session) | |
| **Input** | None (URL parameter only) | |
| **Output** | Confirmation message | |

**Response example:**
```json
{
  "success": true,
  "message": "Webhook deleted successfully"
}
```

---

### Get Webhook Statistics
`GET /rest/webhooks-stats` | Get system-wide webhook statistics.

| Property | Value | Example |
|---|---|---|
| **Authentication** | Required (User session) | |
| **Input** | None | |
| **Output** | JSON with webhook statistics | |

**Response example:**
```json
{
  "jobsWithWebhooks": 5,
  "totalWebhooks": 12,
  "activeWebhooks": 10,
  "inactiveWebhooks": 2
}
```

---



## Notifications

#### Notifications List
`GET /rest/notifications` | Get all notifications.

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | JSON array of notifications | `[ { "type": "info", "message": "Backup completed" } ]` |

---

## Related Documentation

- [Installation](./installation.md): Setting up Orchelium
- [Backup Schedules](./backup-schedules.md): Creating and managing schedules
- [Orchestrations](./orchestrations.md): Building complex backup workflows
- [Settings Configuration](./settings-config.md): Server configuration options
- [User Management](./user-management.md): User accounts and permissions
- [Back to Documentation Index](./README.MD)

#### Notifications Create
`POST /rest/notifications` | Create a new notification.

| Property | Value | Example |
|---|---|---|
| **Input** | JSON body: `{ type, title, description, url }` | `{ "type": "info", "title": "Backup", "description": "Backup completed", "url": "http://..." }` |
| **Output** | JSON with created item | `{ "success": true, "item": { ... } }` |

#### Notifications Update
`PUT /rest/notifications/:index` | Update a notification by index.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `index`, JSON body: `{ type, title, description, url }` | `1`, `{ "type": "warn", ... }` |
| **Output** | JSON with updated item | `{ "success": true, "item": { ... } }` |

#### Notifications Count
`GET /rest/notifications/count` | Get notification count.

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | `{ count: number }` | `{ "count": 3 }` |

#### Notifications Delete All
`DELETE /rest/notifications` | Delete all notifications.

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | 200 OK | `200` |

#### Notifications Delete by Index
`DELETE /rest/notifications/:index` | Delete notification at index.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `index` | `2` |
| **Output** | 200 OK | `200` |

#### Notifications Test
`GET /rest/notifty/test` | Send a test notification.

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | 200 OK | `200` |

---

## Job History

#### Job History List
`GET /rest/history` | Get all job history items.

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | JSON array of job history items | `[ { ... }, ... ]` |

#### Job History Get
`GET /rest/history/:index` | Get a single job history item by index.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `index` | `0` |
| **Output** | JSON job history item | `{ ... }` |

#### Job History Create
`POST /rest/history` | Create a new job history item.

| Property | Value | Example |
|---|---|---|
| **Input** | JSON body: job history item | `{ ... }` |
| **Output** | JSON with created item | `{ "success": true, "item": { ... } }` |

#### Job History Update
`PUT /rest/history/:index` | Update a job history item by index.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `index`, JSON body: job history item | `1`, `{ ... }` |
| **Output** | JSON with updated item | `{ "success": true, "item": { ... } }` |

#### Job History Delete
`DELETE /rest/history/:index` | Delete a job history item by index.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `index` | `1` |
| **Output** | JSON with success | `{ "success": true }` |

#### Job History Clear All
`DELETE /rest/history/clear` | Delete all job history items.

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | `{ "success": true, "message": "History cleared successfully" }` | |

---

## Running Jobs

#### Running Jobs List
`GET /rest/running` | Retrieve all currently running jobs.

| Property | Value | Example |
|---|---|---|
| **Input** | None | N/A |
| **Output** | JSON array of running jobs | `{ "success": true, "count": 2, "jobs": [...] }` |

#### Running Jobs Delete by Index
`DELETE /rest/running/:index` | Delete a running job by its index in the queue.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `index` | `0` |
| **Output** | JSON with success message | `{ "success": true, "message": "Running job at index 0 removed" }` |

#### Running Jobs Delete by Name
`DELETE /rest/running/byName/:jobName` | Delete a running job by its job name.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `jobName` | `MyBackupJob` |
| **Output** | JSON with success message | `{ "success": true, "message": "Running job [MyBackupJob] removed" }` |

#### Running Jobs Delete by Execution ID
`DELETE /rest/running/byExecutionId/:executionId` | Delete a running job by its execution ID.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `executionId` | `a1b2c3d4e5f6g7h8` |
| **Output** | JSON with success message | `{ "success": true, "message": "Running job with executionId [a1b2c3d4e5f6g7h8] removed" }` |

#### Running Jobs Delete All
`DELETE /rest/running` | Delete all running jobs.

| Property | Value | Example |
|---|---|---|
| **Input** | None | N/A |
| **Output** | JSON with success and count | `{ "success": true, "message": "All running jobs deleted", "deletedCount": 5 }` |

---

## User Management

#### User List
`GET /rest/users` | List all users (usernames and emails).

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | JSON array of users | `[ { "username": "alice", "email": "alice@example.com" }, ... ]` |

#### User Get
`GET /rest/users/:username` | Get a user by username.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `username` | `alice` |
| **Output** | JSON user object | `{ "username": "alice", "email": "alice@example.com" }` |

#### User Create
`POST /rest/users` | Create a new user.

| Property | Value | Example |
|---|---|---|
| **Input** | JSON body: `{ username, email, password }` | `{ "username": "bob", "email": "bob@example.com", "password": "secret" }` |
| **Output** | JSON with success | `{ "success": true }` |

#### User Update
`PUT /rest/users/:username` | Update a user's email or password.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `username`, JSON body: `{ email?, password? }` | `alice`, `{ "email": "new@example.com" }` |
| **Output** | JSON with success | `{ "success": true }` |

#### User Delete
`DELETE /rest/users/:username` | Delete a user.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `username` | `bob` |
| **Output** | JSON with success | `{ "success": true }` |

---

## Generic Key-Value Data

#### Get Value by Key
`GET /rest/data/:key` | Get value by key.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `key` | `customKey` |
| **Output** | JSON with key and value | `{ "key": "customKey", "value": "some data" }` |

#### Create or Update Value by Key
`PUT /rest/data/:key` | Create or update value by key.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `key`, JSON body: `{ value }` | `customKey`, `{ "value": "new data" }` |
| **Output** | JSON with success | `{ "success": true }` |

#### Delete Value by Key
`DELETE /rest/data/:key` | Delete value by key.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `key` | `customKey` |
| **Output** | JSON with success | `{ "success": true }` |
| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | 200 OK | `200` |



#### Notifications Delete by Index
`DELETE /rest/notifications/:index` | Delete notification at index.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `index` | `2` |
| **Output** | 200 OK | `200` |



#### Notifications Test
`GET /rest/notifty/test` | Send a test notification.

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | 200 OK | `200` |

---

## Agent Management



#### Agent Details
`GET /rest/agent/:id` | Get agent details by ID.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `id` | `agent1` |
| **Output** | JSON agent object | `{ "id": "agent1", "status": "online" }` |



#### Agent Ping
`GET /rest/agent/:id/ping` | Ping agent by ID.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `id` | `agent1` |
| **Output** | `{ status: "ok" }` | `{ "status": "ok" }` |



#### Agent History & Stats
`GET /rest/agentdetail?agentname=...&startDate=...&endDate=...` | Get agent history and stats for a date range.

| Property | Value | Example |
|---|---|---|
| **Input** | Query params: `agentname`, `startDate`, `endDate` | `agentname=agent1&startDate=2026-03-01&endDate=2026-03-26` |
| **Output** | JSON with filter, totalDuration, totalPct, history | `{ "filter": {"startDate": "2026-03-01", "endDate": "2026-03-26"}, "totalDuration": 1234, "totalPct": 100, "history": [...] }` |



#### Agent Query by Name
`GET /rest/agentquery?name=...` | Query agent by name.

| Property | Value | Example |
|---|---|---|
| **Input** | Query param: `name` | `agent1` |
| **Output** | JSON agent object | `{ "id": "agent1", "status": "online" }` |



#### Agent Update Status (Is Running)
`GET /rest/updateAgent/:agentId/isRunning` | Check if update job is running for agent.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `agentId` | `agent1` |
| **Output** | JSON status and item | `{ "status": true, "item": {...} }` |



#### Agent Update Status (Detailed)
`GET /rest/updateAgent/:agentId/status` | Get update job status for agent.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `agentId` | `agent1` |
| **Output** | JSON status, message, agent, item | `{ "status": "COMPLETED", "message": "", "agent": {...}, "item": {...} }` |



#### Agent Start Update Job
`POST /rest/updateAgent/:agentId` | Start update job for agent.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `agentId`, JSON body `{ command: ... }` | `agent1`, `{ "command": "update.sh" }` |
| **Output** | `{ status: "ok" }` | `{ "status": "ok" }` |

---

## Templates



#### Templates List
`GET /rest/templates` | Get cached templates.

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | JSON with templates | `{ "templates": [ ... ] }` |



#### Templates Refresh
`GET /rest/templates/refresh` | Refresh templates from repository.

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | JSON with status | `{ "status": "refreshed" }` |

---

## Debug & System



#### Debug Info
`GET /rest/debug` | Get debug info (settings, connections, logger, etc).

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | JSON debug object | `{ "settings": { ... }, "logger": { ... } }` |



#### Debug Logs
`GET /rest/debug/logs` | Get recent log messages.

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | JSON array of log messages | `["Backup started", "Backup completed"]` |



#### Debug Logger Trace
`GET /rest/debug/on` | Set logger to trace level.

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | `{ logger_level: "trace" }` | `{ "logger_level": "trace" }` |



#### Debug Logger Warn
`GET /rest/debug/off` | Set logger to warn level.

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | `{ logger_level: "warn" }` | `{ "logger_level": "warn" }` |



#### Server Time
`GET /rest/servertime` | Get server time.

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | `{ dateTime: ... }` | `{ "dateTime": "2026-03-26T12:34:56.789Z" }` |

---

## Other REST Endpoints



#### Jobs Config
`GET /rest/jobs` | Get job configuration (JSON).

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | JSON config | `{ "jobs": [ ... ] }` |



#### ETA Config
`GET /rest/eta` | Get ETA/config info (JSON).

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | JSON config | `{ "eta": 120 }` |



#### Script Details
`GET /rest/script/:script` | Get script details by name.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `script` | `backupDB.sh` |
| **Output** | JSON script object | `{ "name": "backupDB.sh", "description": "Backup database" }` |

#### Delete a Script
`DELETE /rest/script/:scriptName` | Delete a saved script by filename. Requires a valid CSRF token header (`x-csrf-token`). Returns `409` if the script is currently used by any scheduled job or orchestration.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `scriptName`; Header: `x-csrf-token` | `backupDB.sh` |
| **Output** | JSON result | `{ "success": true }` |
| **409 Conflict** | Script in use | `{ "success": false, "jobs": [ ... ], "count": 2 }` |

#### Jobs Using a Script
`GET /rest/jobs-for-script/:scriptName` | List all scheduled jobs and orchestrations that reference a given script.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `scriptName` | `backupDB.sh` |
| **Output** | JSON with jobs array | `{ "success": true, "script": "backupDB.sh", "jobs": [ ... ], "count": 1 }` |

---


---

## Orchestration Management

The following endpoints manage orchestration job definitions and trigger their execution. All require authentication.

#### List All Orchestration Jobs
`GET /rest/orchestration/jobs` | Returns all defined orchestration jobs.

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | JSON array of job summaries | `[ { "id": "backup-pipeline", "name": "Backup Pipeline", ... }, ... ]` |

#### Get an Orchestration Job
`GET /rest/orchestration/jobs/:jobId` | Returns the full definition of a specific orchestration job (nodes, edges, metadata).

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `jobId` | `backup-pipeline` |
| **Output** | Full job object including `nodes`, `edges`, `name`, `description`, `icon`, `color` | |

#### Create an Orchestration Job
`POST /rest/orchestration/jobs` | Create a new orchestration job. `jobId` and `name` are required.

| Property | Value | Example |
|---|---|---|
| **Input** | JSON body: `{ jobId, name, description, nodes, edges, icon, color }` | |
| **Output** | `{ "success": true, "job": { ... } }` | |

#### Update an Orchestration Job
`PUT /rest/orchestration/jobs/:jobId` | Update an existing orchestration job's definition or metadata.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `jobId`; JSON body: `{ name, description, nodes, edges, icon, color }` | |
| **Output** | `{ "success": true, "job": { ... } }` | |

#### Delete an Orchestration Job
`DELETE /rest/orchestration/jobs/:jobId` | Delete an orchestration job and its definition.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `jobId` | `backup-pipeline` |
| **Output** | `{ "success": true, "message": "Orchestration job [backup-pipeline] deleted" }` | |

#### Execute an Orchestration Job
`POST /rest/orchestration/jobs/:jobId/execute` | Trigger an orchestration job. Returns immediately with an `executionId`; the job runs asynchronously. Optionally pass `rerunFrom` (a node ID) to re-run from a specific point in a previous execution.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `jobId`; Optional JSON body: `{ "rerunFrom": "<nodeId>" }` | |
| **Output** | `{ "success": true, "executionId": "a1b2c3d4e5f6g7h8" }` | |

Use the returned `executionId` with the execution history endpoint or the Orchestration Monitor UI to track progress.

#### Get Execution History
`GET /rest/orchestration/jobs/:jobId/executions` | Returns all stored execution records for an orchestration job.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `jobId` | `backup-pipeline` |
| **Output** | JSON array of execution records, each with `executionId`, `status`, `startTime`, `endTime`, `scriptOutputs`, `nodeMetrics`, `errors` | |

#### Get Orchestration Job Metadata
`GET /rest/orchestration/jobs/:jobId/metadata` | Returns lightweight metadata only (name, description, icon, color, version info) without the full node/edge definition.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `jobId` | `backup-pipeline` |
| **Output** | `{ "id", "name", "description", "icon", "color", "currentVersion", "totalVersions", ... }` | |

#### Get a Specific Version of an Orchestration Job
`GET /rest/orchestration/jobs/:jobId/versions/:version` | Returns the definition as it was at a specific historical version.

| Property | Value | Example |
|---|---|---|
| **Input** | URL params: `jobId`, `version` (integer) | `backup-pipeline`, `3` |
| **Output** | Full job object at that version | |

#### List Available Scripts (for Orchestration Palette)
`GET /rest/orchestration/scripts` | Returns scripts available for use as Execute Script nodes in the orchestration builder.

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | JSON array of script objects with `name` and `description` | |

#### List Available Agents (for Orchestration Palette)
`GET /rest/orchestration/agents` | Returns agents available for assignment to nodes.

| Property | Value | Example |
|---|---|---|
| **Input** | None | |
| **Output** | JSON array of `{ id, name, description, status, address, version }` | |

---


---

## Script Test Management

The following endpoints support the interactive script testing feature available in the Script Editor. All require authentication.

#### Start a Script Test
`POST /rest/script-test/start` | Run a script on a target agent for testing purposes. Returns immediately with an execution record. If a test for the same script is already active, returns `409` with the existing execution.

| Property | Value | Example |
|---|---|---|
| **Input** | JSON body: `{ agentName, scriptName?, sourceType, scriptContent?, commandParams? }` | `{ "agentName": "my-agent", "sourceType": "saved", "scriptName": "test.sh" }` |
| **Output** | `{ "success": true, "execution": { ... } }` | |
| **409 Conflict** | Active or retained test exists | `{ "success": false, "code": "ACTIVE_TEST_EXISTS", "execution": { ... } }` |

`sourceType` must be `"saved"` (runs the saved script file) or `"editor"` (runs `scriptContent` directly).

#### Get Script Test Execution State
`POST /rest/script-test/state` | Check whether a blocking execution (active or unacknowledged retained result) exists for a given script identity.

| Property | Value | Example |
|---|---|---|
| **Input** | JSON body: `{ scriptName?, sourceType, scriptContent? }` | |
| **Output** | `{ "success": true, "scriptIdentity": "...", "state": null \| { "type": "active"\|"retained", "execution": { ... } } }` | |

#### Get a Script Test Execution
`GET /rest/script-test/:executionId` | Fetch the current state and accumulated log of a specific test execution.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `executionId` | `a1b2c3d4` |
| **Output** | `{ "success": true, "execution": { "executionId", "status", "log", "returnCode", "agentName", ... } }` | |

#### Acknowledge a Completed Test
`POST /rest/script-test/:executionId/acknowledge` | Mark a completed/failed test result as seen, allowing a new test to be started for the same script.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `executionId` | |
| **Output** | `{ "success": true, "execution": { ... } }` | |

#### Discard a Retained Test Result
`POST /rest/script-test/:executionId/discard` | Remove a retained (completed/failed) test result before its 30-minute retention window expires. Returns `409` if the test is still active.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `executionId` | |
| **Output** | `{ "success": true }` | |

#### Terminate an Active Test
`POST /rest/script-test/:executionId/terminate` | Send a termination signal to the agent running the test. The test status changes to `terminating` then `terminated` once the agent confirms.

| Property | Value | Example |
|---|---|---|
| **Input** | URL param: `executionId` | |
| **Output** | `{ "success": true, "execution": { ... } }` | |

**Execution status values:** `pending` → `running` → `completed` | `failed` | `terminating` → `terminated`

---


---

## Schedule Management

The following endpoints allow you to read and delete schedule data. All endpoints require authentication.

#### List All Schedules
`GET /rest/schedules` | Get all schedules.

| Property    | Value         | Example |
|-------------|---------------|---------|
| **Input**   | None          |         |
| **Output**  | JSON array of schedules | `[ { "jobName": "BackupDB", ... }, ... ]` |

#### Get a Schedule by Job Name
`GET /rest/schedules/:jobName` | Get a specific schedule by job name.

| Property    | Value         | Example |
|-------------|---------------|---------|
| **Input**   | URL param: `jobName` | `BackupDB` |
| **Output**  | JSON schedule object | `{ "jobName": "BackupDB", ... }` |

#### Delete a Schedule by Job Name
`DELETE /rest/schedules/:jobName` | Delete a specific schedule by job name.

| Property    | Value         | Example |
|-------------|---------------|---------|
| **Input**   | URL param: `jobName` | `BackupDB` |
| **Output**  | JSON result   | `{ "success": true, "message": "Schedule [BackupDB] deleted" }` |

#### Delete All Schedules
`DELETE /rest/schedules` | Delete all schedules.

| Property    | Value         | Example |
|-------------|---------------|---------|
| **Input**   | None          |         |
| **Output**  | JSON result   | `{ "success": true, "message": "All schedules deleted" }` |

---

## Related Documentation

- [Installation](./installation.md): Setting up Orchelium
- [Backup Schedules](./backup-schedules.md): Creating and managing schedules
- [Orchestrations](./orchestrations.md): Building complex backup workflows
- [Settings Configuration](./settings-config.md): Server configuration options
- [User Management](./user-management.md): User accounts and permissions
- [Back to Documentation Index](./README.MD)

---

**Note:** Many additional HTML-rendering endpoints exist (e.g., `/login.html`, `/settings.html`), but this document focuses on REST-style JSON APIs.
