# Upgrading Orchelium

This section covers notes and migration steps when upgrading an existing Orchelium installation. For a new install, see [Installation](./installation.md).

## Definition storage

When using `server.definitions.backend` as `fs` (default) or `hybrid`, the data volume also contains filesystem-backed definition assets:

- `data/jobs` for schedule/job definitions (`*.job.json`)
- `data/orchestrations` for orchestration definitions (`*.orch.json`)
- `data/.state` for internal definition-store operational state

These are created automatically if missing.

Warning for users adopting `2026.06.06.02` onward:

- The default definitions backend is `fs`.
- If your existing schedules/orchestrations are still DB-backed and not migrated, temporarily set `server.definitions.backend` to `hybrid`, run migration, verify files, then return to `fs`.

## Upgrade migration (versions earlier than `2026.06.06.01`)

If upgrading from a version before `2026.06.06.01`, move definitions safely using this flow:

1. Set `server.definitions.backend` to `hybrid` in `data/server-config.json`.
2. Restart the hub.
3. Run migration once:

```bash
curl -X POST http://localhost:8082/rest/definitions/migrate-db-to-fs \
  -H "Content-Type: application/json" \
  -d '{"deleteSource":false}' \
  -b cookie.txt
```

4. Verify status and counts:

```bash
curl -X GET http://localhost:8082/rest/definitions/status -b cookie.txt
```

5. Confirm files exist under `data/jobs` and `data/orchestrations`.
6. Switch backend to `fs` and restart.

Keep `deleteSource:false` until you have validated file-backed operation.

## Agent connection tokens

Current hubs issue a connection token when an agent that supports it is approved. Agents approved before this change keep connecting with only their name. They show a warning on the agent list until a token is issued from the agent edit page.

Issuing a token starts requiring it. Update the agent first if that build cannot store `AGENT_TOKEN`. Details are in [Agent connection tokens](./agent-connection-token.md).

---

- [Back to Installation](./installation.md)
- [Back to Documentation Index](./README.MD)
