
# Orchelium Documentation

Orchelium is a lightweight yet powerful solution for managing and scheduling shell-based executions across a local area network. Designed for IT administrators, it ensures secure, encrypted communication between a central hub and remotely managed agents. Orchelium streamlines job execution, scheduling, monitoring, and notifications, making it an effective tool for backup automation and infrastructure management.

---

## Documentation Contents

- [Getting Started / Installation](installation.md)
- [Job Schedules](backup-schedules.md) — Creating schedules, running jobs, and testing scripts interactively
- [Orchestrations](orchestrations.md) — Visual workflow builder with Execute, HTTP, Wait, Notify, Condition, Split/Join nodes
- [User Management](user-management.md)
- [Configuration / Settings](settings-config.md) — Rule-based thresholds, agent concurrency, webhook management, and all server/agent options
- [Plugins](plugins.md) — Plugin Manager: browse, install, update, and remove plugins from the official registry
- [Trigger Context Guide](TRIGGER_CONTEXT_GUIDE.md) — Template substitution in scripts and orchestrations (`#{context.*}`)
- [Webhook User Guide](WEBHOOK_USER_GUIDE.md) — Complete guide to creating and using webhooks
- [REST API Reference](REST_API_REFERENCE.md) — All API endpoints including orchestration, script testing, schedules, and more
- [Developer Guides](Developers/)
- [Screenshots & UI Reference](screens/)

---

### About the Docs

- **installation.md** — How to install and set up Orchelium (Docker & manual), including all environment variables
- **backup-schedules.md** — Creating and managing job schedules; interactive script testing from the editor
- **orchestrations.md** — Building complex workflows with the visual orchestration builder, including all node types and template substitution
- **user-management.md** — User setup, login, and password management
- **settings-config.md** — Server and agent configuration options, backup & restore, rule-based thresholds, webhooks
- **plugins.md** — Plugin Manager UI: browsing the official registry, installing/updating/uninstalling plugins, categories, and writing custom plugins
- **TRIGGER_CONTEXT_GUIDE.md** — How `#{context.*}` template variables work in Execute Script, HTTP Request, and Notify nodes
- **WEBHOOK_USER_GUIDE.md** — Webhook creation, API key management, payload access in scripts and orchestrations
- **REST_API_REFERENCE.md** — API endpoints for programmatic access
- **screens/** — Visual reference for the UI and features

---

## Feature Highlights

### Job Schedules & Script Testing
- **Scheduled Jobs**: Run shell scripts on agents on a fixed schedule (daily, cron, etc.)
- **Manual Execution**: Trigger any job immediately from the schedule list
- **Script Testing**: Run a script interactively from the Script Editor with live output streaming, without creating a history entry. Supports saved scripts and unsaved editor buffers.

### Plugin Manager
- **Official Plugin Registry**: Browse 19+ community plugins from [github.com/dpembo/orchelium-plugins](https://github.com/dpembo/orchelium-plugins)
- **One-click Install / Update / Uninstall**: Plugins are downloaded, validated, and hot-loaded — no restart required
- **Categories**: Backup, Containers, Databases, File Sync, Storage, System
- **Search & filter**: Find plugins by name, description, or tag; filter by installed/updates/not-installed
- **Custom plugins**: Drop a folder into `plugins/` on the hub and it is hot-reloaded automatically

### Orchestrations
- **Visual Workflow Builder**: Drag-and-drop workflow designer with a real-time monitor
- **Node Types**: Start, Execute Script, HTTP Request, Wait, Notify, Condition, Split/Join, End (Success/Failure)
- **Parallel Execution**: Split/Join nodes fan out work across multiple branches and merge results
- **Template Substitution**: Use `#{context.*}` in HTTP URLs, headers, body, auth fields, and Notify messages to inject trigger context values at runtime

### Rule-Based Thresholds & Trigger Context
- **Rule-Based Thresholds**: Define rules to trigger jobs based on metrics (CPU, disk, file count, etc.)
- **Agent Concurrency**: Control how many jobs each agent can run in parallel (default: 3, configurable per agent)
- **Trigger Context**: Scripts and orchestrations receive metric data when triggered by rules
  - Scripts: `$ORCHELIUM_METRIC_VALUE`, `$ORCHELIUM_METRIC_PATH`, `$ORCHELIUM_CONDITION_THRESHOLD`, etc.
  - Orchestrations: `#{context.metric.value}`, `#{context.rule.name}`, etc.

### Webhooks
- **Webhook Triggers**: External systems trigger jobs via `POST /api/webhook/trigger/:jobName?key=<uuid>`
- **Payload Access**: Webhook JSON payload available in scripts as `$ORCHELIUM_TRIGGER_CONTEXT` and in orchestrations as `#{context.webhook.payload.*}`
- **Webhook Management UI**: Create, edit, rotate keys, and view activity from Settings → Webhooks
  - Delete webhooks with confirmation
  - No more curl commands needed!

**Example**: Cleanup script triggered when disk usage exceeds 90%:
```bash
if [ "$ORCHELIUM_METRIC_TYPE" = "mount_usage" ]; then
    echo "Cleaning up $ORCHELIUM_METRIC_PATH (${ORCHELIUIM_METRIC_VALUE}% usage)"
fi
```

See [TRIGGER_CONTEXT_GUIDE.md](../TRIGGER_CONTEXT_GUIDE.md) for complete examples, webhook API, and template substitution syntax.

For more advanced topics, troubleshooting, and contributing, see the [Developer Guide](Developers/developer-guide.md).

---

Copyright (C)2025 pembo.co.uk

