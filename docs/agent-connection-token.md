# Agent connection tokens

An agent proves who it is with a connection token issued by the hub. The shared hub encryption key does not do that job. This page describes how the token is created, how an agent presents it, and what remains possible until a token exists.

The hub encryption key encrypts the stored copy of each token, and it signs commands for an agent that does not have a token yet. That includes the enrollment message, which is how the agent receives its token. The hub keeps the key in `data/encryption-key` (or `ORCHELIUM_ENCRYPTION_KEY` when that is set). Once the agent has stored the token and the hub has seen this build's `command-key` capability, the hub signs jobs, pings, and rotation with that token, and the agent accepts no other key. An older agent keeps receiving commands signed with the hub key. MQTT and WebSocket use the same rule. Agent status messages are not signed with the hub key.

The connection token is different. The hub creates one token per approved agent. Only that agent and the hub know it.

---

## What is checked

| Direction | How identity is checked |
|---|---|
| Hub to agent, after the agent has a token and has reported `command-key` (jobs, ping, rotate) | JWT signed with that agent's connection token. The agent ignores a command whose name is not its own, and it rejects a command signed with the hub key. |
| Hub to agent, before a token exists, or on an older agent (enrollment and agents that do not report `command-key`) | JWT signed with the hub encryption key. |
| Agent to hub over WebSocket | The socket must present this agent's connection token. The `name` in each message must match that socket. |
| Agent to hub over MQTT | Each status message carries this agent's connection token. The hub checks it before the message is processed. |

A client that can reach the WebSocket port and only knows an agent name cannot register as an approved agent, and it cannot disconnect an agent that is already connected.

---

## First connection

A new agent connects with its name and no token.

The hub accepts that socket as pending:

- It can send a single registration message, which appears on the agent list for approval.
- It is not given jobs.
- It cannot close a socket that is already connected under that name.
- If that name is already approved, the connection is refused.

Approving the agent creates a random token. The hub stores a hash of the token, and an encrypted copy so the token can be shown again or rotated. The plain token is shown on that agent's edit page.

When the connected agent is a current build and the socket is still open, the hub also sends the token down that WebSocket. The agent writes `AGENT_TOKEN` into its local `settings.sh` and reconnects with the token. A later restart uses the same value.

An MQTT agent does not receive the token on the broker, because every subscriber of the command topic would see it. Copy the value from the agent edit page into `AGENT_TOKEN` and restart the agent.

---

## After a token exists

The agent sends the token on every WebSocket connection in the `Authorization: Bearer` header. On MQTT it sends the token on each status message. The hub removes the token from the message before the message is saved.

| Presented token | Result |
|---|---|
| Matches the stored token | The connection is accepted. A restart may replace the previous socket. |
| Missing or wrong | The connection is refused. The existing socket is left as it is. Status messages are ignored. |

There is no switch to turn this check off for an agent that already has a token. Removing `AGENT_TOKEN` from the agent does not make the hub accept it.

---

## Agents that already existed

Approving an agent that does not yet support connection tokens does not create a token. An approved agent with no token is not accepted. Issue a token from the agent edit page, and update the agent so it can store `AGENT_TOKEN`, before it can connect again.

Until a token is issued:

- A client that only knows the agent name cannot take the connection or submit status.
- Agents still waiting to be registered can open one pending socket.

The agent list shows the same blinking warning used for an out-of-date agent. The tooltip explains that the agent needs a token. People who can edit agents can open the edit page from that warning and issue one.

Agents still waiting to be registered do not show this warning. They are not expected to have a token yet.

Issuing a token starts requiring it immediately. Update the agent first when the installed build cannot store a token. Otherwise it will stop being able to connect until `AGENT_TOKEN` is set and the agent is restarted.

---

## On the agent edit page

The connection token sits with the other agent fields. It is masked with a fixed set of dots, so the value and its length are not on the page. The field is not submitted with **Submit**.

| Control | Action |
|---|---|
| Binoculars | Shows the current token so it can be copied into `AGENT_TOKEN`. |
| Rotate | Sends a new token to a connected agent. The current token keeps working until the agent confirms it has stored the new one. |
| Key | Shown when no token exists. Issues the first token and displays it on this page. |

The token is not shown on the agent list. After you issue one, the edit page is where it can be copied.

Rotation only runs when the agent is connected and the installed build can store a token. The new token is sealed with the current token before it is sent, including on MQTT, so other agents cannot read it. If the agent restarts before it has saved the new token, the old token still connects and the hub sends the rotation again. If the acknowledgement is lost after the agent has saved it, the next connection that presents the new token completes the rotation.

---

## MQTT broker login

The broker username and password are separate from the connection token. They are the login for the MQTT broker itself.

Set them in **Settings → MQTT**, or on the MQTT step of initial setup. When they are set, the agent deploy command includes `--mqttUsername` and `--mqttPassword`, and the agent installer writes `MQTT_USERNAME` and `MQTT_PASSWORD` into `settings.sh`.

Broker authentication matters because an MQTT status message contains the connection token. Anyone allowed to subscribe to the status topic can read it. Restrict that topic to the hub and the agents.

---

## Where the values are kept

| Location | What is stored |
|---|---|
| Hub database `data/data.db`, key `AGENT_CREDENTIALS` | Hash of each token, plus a copy encrypted with the hub key. One record for all agents. |
| Agent `settings.sh` | `AGENT_TOKEN="..."` in the clear, on that machine only. |

The hub loads the credential record into memory at startup. To clear tokens in a build environment, stop the hub, delete the `AGENT_CREDENTIALS` key, and start the hub again. The agent records stay. Approved agents with no token then show the warning and can connect by name again. An agent that still has `AGENT_TOKEN` in `settings.sh` will send it, and the hub ignores it while no credential is stored.

```bash
node -e '
const { Level } = require("level");
(async () => {
  const db = new Level("./data/data.db", { valueEncoding: "json" });
  await db.del("AGENT_CREDENTIALS");
  await db.close();
})().catch((err) => { console.error(err); process.exit(1); });
'
```

Do this only where losing issued tokens is acceptable. LevelDB will not open the database while the hub is using it.

---

## Related documentation

- [Installation](./installation.md)
- [Settings](./settings-config.md)
- [Upgrading](./Upgrade.md)
- [Database reference](./Developers/database-reference.md)
- [Back to Documentation Index](./README.md)
