# Orchelium codebase review

Review date: 12 November 2026. Scope is the hub (`server.js` and modules), the agent (`agent/src`), shared crypto, views, tests, and deployment files. No code was changed for this review.

This is a home-lab backup orchestrator: a Node.js hub schedules shell jobs, a small agent runs them, and a browser UI manages both. The product shape is clear, and newer modules (user permissions, agent token auth, plugin install path checks, orchestration engine tests) are in better shape than the original hub. The findings below are the gaps that will hurt first.

Severity is judged by what an attacker or operator can do with the current defaults, including the published Docker image, which does not set `NODE_ENV` or `ORCHELIUM_ENCRYPTION_KEY`.

---

## Findings

### 1. Unauthenticated script write

| | |
|---|---|
| Severity | Critical |
| Category | Security |
| Location | `server.js` — `POST /saveScript` (around line 958); `validateCsrf` (around line 858) |

**Problem.** `POST /saveScript` writes the request body to `scripts/<name>.sh` with no session check. `validateCsrf` also treats `/saveScript` as a public route and skips the token. The filename filter strips path separators, so this is not a path traversal, but any caller who can reach the hub can create or overwrite a shell script. Jobs and orchestrations execute those scripts on agents.

**Recommendation.** Require `User.isAuthenticated` and `PERMISSIONS.SCRIPTS_CREATE` or `SCRIPTS_EDIT`. Remove `/saveScript` from the CSRF skip list and require the token. Reject empty names and bodies.

```js
app.post('/saveScript', validateCsrf, User.isAuthenticated,
  User.requireAnyPermission([PERMISSIONS.SCRIPTS_CREATE, PERMISSIONS.SCRIPTS_EDIT]),
  asyncHandler(async (req, res) => { /* existing write */ }));
```

**Benefit.** Closes an unauthenticated remote code execution path on every agent that runs hub scripts.

**Estimated effort.** Small

---

### 2. MQTT broker password is sent to every agent-status viewer

| | |
|---|---|
| Severity | Critical |
| Category | Security |
| Location | `server.js` — `GET /agentstatus.html` (around line 3686); `views/agentstatus.ejs` — `addAgent` (around line 137) |

**Problem.** The route only requires `agents.view`. The default viewer and invited-user roles include that permission. The handler passes `serverConfig.mqtt.password` into the template, and the page embeds it in client-side JavaScript used to build an install command. Anyone who can open Agent Status can read the broker password from page source. With that password they can publish on the agent command topic and run shell on every MQTT-connected agent. The password is also placed on the agent command line (`--mqttPassword=...`), so it appears in process listings and shell history during install.

**Recommendation.** Do not render broker credentials in the browser. Build the install command on the server for `agents.create` only, or return a one-time registration token instead of the broker password. Keep the password write-only in settings.

**Benefit.** Stops a view-only user from taking over the agent command channel.

**Estimated effort.** Medium

---

### 3. Default encryption key, and one key used for every secret

| | |
|---|---|
| Severity | High |
| Category | Security |
| Location | `passman.js` — `initializeEncryptionKey`, `encrypt`, `decrypt`; `shared/keyMaterial.js` — `padStringTo256Bits`; `server.js` session setup (around line 709); `Dockerfile` |

**Problem.** If `ORCHELIUM_ENCRYPTION_KEY` is unset, the key is the string `CHANGEIT`. Enforcement defaults to `warn`, so the process still starts. The Docker image never sets the variable or `NODE_ENV`. The same key is the Express session secret, the HS256 JWT secret, and the AES key for stored MQTT, SMTP, and agent tokens.

`encrypt` uses AES-256-CBC with no MAC, so ciphertext can be tampered with without a clean failure. `padStringTo256Bits` pads short secrets with a repeated byte instead of a KDF. `deriveKey` (PBKDF2) exists in `passman.js` and is never called. A leaked session secret, a backup of `server-config.json`, or the default key decrypts every stored credential and forges agent JWTs.

**Recommendation.** Refuse to boot when the key is missing or shorter than 32 bytes (`ORCHELIUM_KEY_ENFORCE=strict` in the image). Split session, JWT, and data keys. Replace CBC with AES-256-GCM (the agent token sealer in `shared/secretBox.js` already does this). Derive keys with HKDF or `crypto.scrypt`, and delete `deriveKey`'s hard-coded salt `BackupHubSalt`.

**Benefit.** Stored credentials and agent authentication stop depending on a published default and on unauthenticated ciphertext.

**Estimated effort.** Large

---

### 4. Viewers can run and delete jobs with GET requests

| | |
|---|---|
| Severity | High |
| Category | Security |
| Location | `server.js` — `GET /runSchedule.html` (around line 3234); `GET /delete-schedule.html` (around line 2868) |

**Problem.** Running a schedule is a GET and requires only `jobs.view`. Deleting a schedule is a GET. Both take a `redir` query parameter and redirect to it. Cookies are `SameSite=strict`, which blocks classic cross-site POST CSRF, but same-site links, extensions, and anything that can open these URLs still run or delete jobs. A view-only user should not be able to start backups. GET also makes the action prefetchable and bookmarkable.

**Recommendation.** Use POST (or PUT/DELETE) with `validateCsrf`. Require `jobs.edit` to run and keep `jobs.delete` to delete. Allow `redir` only when it is a relative path on this host (see finding 5).

**Benefit.** Job execution matches the permission model, and destructive actions are no longer triggered by navigation.

**Estimated effort.** Medium

---

### 5. Open redirect after login and after job actions

| | |
|---|---|
| Severity | High |
| Category | Security |
| Location | `server.js` — `POST /login.html` (around line 1141); `GET /runSchedule.html`; `GET /delete-schedule.html` |

**Problem.** After a successful login, `res.redirect(redirect)` uses the posted `redirect` value with no allow-list. `https://evil.example` and `//evil.example` both leave the site. The same pattern exists for `redir` on run and delete. Combined with finding 4, a link can delete a job and then send the browser elsewhere.

**Recommendation.** Accept only paths that start with a single `/` and do not start with `//` or `/\`.

```js
function safeRedirect(value, fallback = '/') {
  if (typeof value !== 'string') return fallback;
  if (!value.startsWith('/') || value.startsWith('//') || value.startsWith('/\\')) return fallback;
  return value;
}
```

**Benefit.** Login and job actions cannot be used as a phishing redirector.

**Estimated effort.** Small

---

### 6. Job script path is not confined to `scripts/`

| | |
|---|---|
| Severity | High |
| Category | Security |
| Location | `scheduler.js` — `runJob` (around line 719) |

**Problem.** The script path is `"./scripts/" + command`. `command` comes from the job definition. `jobs.edit` does not imply `settings.access` (the operator role has edit and not settings). A value such as `../data/server-config.json` or `../data/user.db` makes the hub read that file and ship the bytes to the selected agent as the script body. That exposes SMTP/MQTT secrets and password hashes to anyone who can edit a job and reach one agent.

**Recommendation.** Resolve the path and require it to stay inside the scripts directory, the same way `safeResolve` already works for docs.

```js
const base = path.resolve(process.cwd(), 'scripts');
const scriptPath = path.resolve(base, command);
if (scriptPath !== base && !scriptPath.startsWith(base + path.sep)) {
  throw new AppError('Script path is outside the scripts directory', 400);
}
```

**Benefit.** Job editors can run scripts they are allowed to edit, and cannot read the credential store.

**Estimated effort.** Small

---

### 7. Agents without a connection token can be impersonated

| | |
|---|---|
| Severity | High |
| Category | Security |
| Location | `communications/agentAuth.js` — `decideConnection`, `authorizeMessage`; `communications/wsServerTransport.js` — connection handler |

**Problem.** `decideConnection` returns `legacy-accept` when the agent is already approved, has no stored token, and has no live socket. `authorizeMessage` returns `{ ok: true }` whenever that agent has no credential. A caller who knows the agent name can open a WebSocket, replace the real agent if it is offline, and submit status, logs, and completion records. Pending sockets also parse and dispatch every message; approval is only a later filter inside `processMessage`.

MQTT has the same message check. If the broker is reachable and still using a default or shared password (finding 2), the same impersonation works without a socket.

**Recommendation.** Treat missing credentials as reject for any agent whose status is not `register`. Keep the one-time enrolment path only for unknown names, and do not mark them online until an admin approves them. Stop dispatching messages from sockets that are not the approved connection. Plan a migration that issues tokens for every existing agent and then deletes the legacy branch.

**Benefit.** Agent identity is the token, not the name on the query string.

**Estimated effort.** Medium

---

### 8. Shell parameters are escaped inside `bash -c`, and newlines are not escaped

| | |
|---|---|
| Severity | High |
| Category | Security |
| Location | `agent/src/executor.js` — `executeBackupCommand` (around line 67); `scheduler.js` — `runJob` (around line 723); `thresholdJobs.js` |

**Problem.** The agent writes the script to a temp file, then runs:

```js
const runCommand = `. ${file} ${sanitizedParams} >> ${logfile}`;
spawn('bash', ['-c', runCommand], { detached: true, stdio: 'ignore', env: spawnEnv });
```

The sanitiser escapes a fixed set of metacharacters and does not escape newlines, so a parameter value can start a second shell command. `runJob` also does `commandParams += inData`. Threshold jobs pass metric text into `inData`. If `commandParams` is null, JavaScript coerces it to the string `"null"` before appending.

Sourcing an unquoted path is safe only while `file` is generated by the agent. The parameter channel is not.

**Recommendation.** Pass the script path and arguments as an argv array (`spawn(file, args, ...)` or `spawn('bash', [file, ...args])`) and drop `bash -c` for parameters. Keep trigger payload in environment variables, which the executor already supports via `contextEnvVars`. Coerce null parameters to `''`.

**Benefit.** Job arguments can no longer change which commands run. Removes a class of quoting bugs instead of extending the denylist.

**Estimated effort.** Medium

---

### 9. Arbitrary database read and write, with the extra token printed to stdout

| | |
|---|---|
| Severity | High |
| Category | Security |
| Location | `server.js` — `initializeSensitiveDataToken`, `validateSensitiveDataToken` (around line 897); `GET/PUT/DELETE /rest/data/:key` (around line 4934) |

**Problem.** These routes read, overwrite, or delete any LevelDB key, including `AGENT_CREDENTIALS`. The extra token is generated on first use, printed with `console.log`, and accepted in the `dataToken` query string. Query strings land in access logs, browser history, and `Referer`. Comparison uses `!==` rather than `crypto.timingSafeEqual`. The token is not persisted, so a restart invalidates it, but anyone with container logs holds the current one. Combined with the default encryption key, overwriting or reading `AGENT_CREDENTIALS` yields agent tokens.

**Recommendation.** Remove this generic API, or restrict keys to an explicit allow-list that excludes credentials, users, and webhooks. Persist a hash of an admin-provisioned token. Compare with `timingSafeEqual`. Never accept the token on the query string, and never print it.

**Benefit.** A logged-in session plus a log line is no longer enough to dump or replace the credential store.

**Estimated effort.** Medium

---

### 10. Webhook keys are stored and compared in plaintext, and accepted on the URL

| | |
|---|---|
| Severity | High |
| Category | Security |
| Location | `webhooksData.js` — `validateWebhookKey` (around line 161); `server.js` — `POST /api/webhook/trigger/:jobName`; `webhookManager.js` — key extraction |

**Problem.** API keys are stored in LevelDB and compared with `===`. The trigger endpoint accepts the key as `?key=`. That value is written to proxy logs and may leak via `Referer`. There is no rate limit, so guessing and log scraping are both practical. A valid key starts a job. The handler returns `err.message` to the client on some failures.

**Recommendation.** Store `sha256(key)` and compare with `timingSafeEqual`. Accept the key only from `X-Webhook-Key`. Add a per-IP and per-job limiter. Return a generic 401 body.

**Benefit.** A database backup or an access log no longer contains usable webhook secrets, and online guessing gets slower.

**Estimated effort.** Medium

---

### 11. Session holds the password hash, is not regenerated, and has no expiry

| | |
|---|---|
| Severity | Medium |
| Category | Security |
| Location | `server.js` — `POST /login.html` (around line 1131); session middleware (around line 709); `models/user.js` — `isAuthenticated` |

**Problem.** Login assigns `req.session.user = user`, and the user record includes `password`, reset tokens, and invite tokens. `isAuthenticated` reloads that full record into the session on every request. The store is the default MemoryStore: no `maxAge`, no `name` distinct from the default, and no `req.session.regenerate` after login (session fixation). `secure` is true only when `NODE_ENV === 'production'`, which the Dockerfile does not set, so the cookie is sent on cleartext HTTP. Logout uses GET, so a page that can trigger navigation can log the user out.

`isAuthenticated` answers unauthenticated API calls with a 302 to an HTML login page, which breaks non-browser clients and hides 401s.

**Recommendation.** Store `{ username }` in the session and load a public user DTO per request. Call `req.session.regenerate` after a successful password check. Set `cookie.maxAge`, `cookie.secure: true` behind TLS, and set `NODE_ENV=production` in the image. Return 401 JSON for `/rest` and `/api`.

**Benefit.** Session theft no longer includes password hashes. Sessions expire. Login cannot reuse an attacker-set session id.

**Estimated effort.** Medium

---

### 12. Security headers are unused, and a few HTML sinks are raw

| | |
|---|---|
| Severity | Medium |
| Category | Security |
| Location | `server.js` line 53 (`lusca` required, never used); `server.js` docs renderer (around line 792); `pluginManager.js` icon cache; `views/agentstatus.ejs` (`innerHTML`, `M.toast({html})`) |

**Problem.** There is no `Content-Security-Policy`, `X-Frame-Options`, `Referrer-Policy`, or `Strict-Transport-Security`. `lusca` is loaded and ignored. Docs HTML is `marked.parse` output with raw HTML kept, then interpolated into a page. Plugin SVGs are downloaded from the registry and served from `/plugin-icons` with no sanitising; an SVG opened as a document on this origin can run script. Agent status builds toast HTML and `innerHTML` from agent names, and agent names are not restricted to a safe character set (`agents.js` `registerAgent`).

**Recommendation.** Add `helmet` with a CSP that fits the current inline scripts, or remove inline scripts first. Sanitise markdown with `sanitize-html` (already a dependency) before sending docs. Reject SVG script, event handlers, and external references before caching icons. Escape agent names in the client, or pass them through `textContent`.

**Benefit.** A malicious plugin icon, doc file, or agent name cannot become a same-origin script as easily. Clickjacking gets harder.

**Estimated effort.** Medium

---

### 13. Login and password reset have no rate limit, and client IP is spoofable

| | |
|---|---|
| Severity | Medium |
| Category | Security |
| Location | `server.js` — `POST /login.html` (around line 1083); `models/user.js` — `createUser`, `isResetTokenValid`, `isInviteTokenValid` |

**Problem.** Login, forgot-password, reset, and invite acceptance have no limiter and no lockout. `createUser` accepts any password length; only invite acceptance checks for 8 characters. Reset and invite tokens are compared with `===`. The logged client address is `req.headers['x-forwarded-for'] || req.ip` even when `trust proxy` is off, so the audit trail and the optional login-failure email can be pointed at an arbitrary address. Failed login returns the same message for an unknown user and a bad password, which is good; inactive accounts return a different message, which confirms the account exists.

**Recommendation.** Add a small limiter (for example `express-rate-limit`) on the public auth routes. Use one error message for inactive and unknown users. Compare tokens with `timingSafeEqual`. Enforce the same password rules on first-user registration. Log `req.ip` only, and document that `ORCHELIUM_TRUST_PROXY=true` is required behind a proxy.

**Benefit.** Online password guessing and token guessing slow down. Audit logs stop recording attacker-chosen addresses.

**Estimated effort.** Medium

---

### 14. Backup restore extracts a zip with no path check

| | |
|---|---|
| Severity | Medium |
| Category | Security |
| Location | `backupManager.js` — `extractZip` (around line 517) |

**Problem.** Restore pipes the upload into `unzipper.Extract({ path: targetDir })`. That helper has a long history of writing entries whose names contain `..` outside the destination. Plugin install (`pluginManager.js` `installPlugin`) does check `path.resolve` and is in better shape, but that check uses `startsWith` without a trailing separator, so a destination of `/plugins/ab` would also accept `/plugins/abc`. Restore accepts up to 500 MB from any user with `settings.access`.

**Recommendation.** Use the same entry walker as plugin install, require `resolved === dest || resolved.startsWith(dest + path.sep)`, and reject links. Cap entry count and uncompressed size, not only the upload size.

**Benefit.** A crafted backup cannot write outside the restore directory.

**Estimated effort.** Medium

---

### 15. The HTTP process accepts traffic before startup finishes

| | |
|---|---|
| Severity | High |
| Category | Reliability |
| Location | `server.js` — top-level async IIFE (around line 187); `app.listen` (around line 5374) |

**Problem.** LevelDB, users, and agents start in an un-awaited IIFE. `app.listen` then accepts connections, and only the listen callback runs user migration, definition store init, and `scheduler.init`. A request in that window can hit a closed database or an empty schedule list and still pass authentication middleware. Failures in the listen callback are logged and the process stays up, so the hub looks healthy while jobs will not run. There is a second race: the IIFE and the listen callback both touch the database.

**Recommendation.** Await database, user migration, agent auth, and scheduler init, then call `listen`. Exit if any required step fails. Add a `/health` route that returns 503 until init completes.

**Benefit.** Restarts stop serving half-initialised state, and orchestrators can wait for a real ready signal.

**Estimated effort.** Medium

---

### 16. Plugin install can report success before files are written

| | |
|---|---|
| Severity | Medium |
| Category | Reliability |
| Location | `pluginManager.js` — `installPlugin` (around line 350) |

**Problem.** `unzipper.Parse()` emits `finish` when the central directory is consumed, not when every `createWriteStream` has flushed. `resolve` is tied to `finish`. `mkdir` errors reject, but other entries keep flowing. `chmod` on `.sh` files is not tied to the promise. A caller can treat the plugin as installed while scripts are still empty or not executable.

**Recommendation.** Collect per-entry promises and `await Promise.all` before resolving. Use `pipeline` from `node:stream/promises`.

**Benefit.** Install either finishes completely or fails visibly.

**Estimated effort.** Small

---

### 17. In-memory state is unbounded and dies on restart

| | |
|---|---|
| Severity | Medium |
| Category | Reliability |
| Location | `server.js` — `inProgressExecutions` (around line 5031); `scheduler.js` in-progress cache; `express-session` MemoryStore; `server.js` `loglines` |

**Problem.** Orchestration stubs live in plain objects. The cleanup path runs after a normal finish. A crash or a lost agent leaves the stub until process restart. Sessions live in MemoryStore, so a restart logs everyone out and a second hub process cannot share them. The log ring buffer keeps 250 events, but log4js is configured with `enableCallStack: true`, so each event retains a stack.

Running-job state on the agent is a `Map`, which is appropriate, but `cleanupBackgroundTasks` sends `SIGTERM` and does not wait, and child processes are `unref`'d and detached. A hub restart does not stop those shells.

**Recommendation.** Expire in-progress keys on a timer. Move sessions to a store that survives restart if more than one instance is ever planned. Turn call-stack capture on only at debug level. On agent shutdown, signal the process group and await `exit` with a timeout.

**Benefit.** Memory stops growing with abandoned runs. Restarts behave predictably.

**Estimated effort.** Medium

---

### 18. Credential writes are fire-and-forget

| | |
|---|---|
| Severity | Medium |
| Category | Reliability |
| Location | `communications/agentAuth.js` — `persist` |

**Problem.** `issue`, `beginRotation`, and `forget` mutate the in-memory map and call `db.putData(...).catch(...)` without awaiting it. A crash or a failed write leaves the database behind the memory map. The next start will reject agents that were just issued a token, or accept a token that was supposed to be forgotten. Concurrent rotations can overwrite each other because nothing serialises updates per agent.

**Recommendation.** `await` the write and keep a per-agent queue. If the write fails, roll the memory map back and surface the error to the admin action.

**Benefit.** Issued tokens survive a crash, and a failed disk write does not look like success.

**Estimated effort.** Small

---

### 19. `server.js` is the application

| | |
|---|---|
| Severity | High |
| Category | Architecture |
| Location | `server.js` (about 5,400 lines) |

**Problem.** Routing, CSRF, backup, webhooks, scripts, scheduling pages, agent admin, user admin, orchestration monitor, and startup all live in one file. Route order is load-bearing. The same `require()` calls are repeated inside handlers (`webhooksData`, `scheduler`, `agentAuth`). Several handlers are copied with small differences (the running-item delete routes, the initial-setup pages). `orchestrationEngine.js` (about 1,700 lines) and `scheduler.js` (about 1,100) are smaller versions of the same pattern: one module owns persistence, policy, and I/O.

**Recommendation.** Split by feature under something like `routes/auth.js`, `routes/jobs.js`, `routes/agents.js`, `routes/scripts.js`, `routes/webhooks.js`, `routes/settings.js`, and keep `server.js` as middleware plus `listen`. Pass `serverConfig`, `logger`, and stores in as arguments instead of reading globals.

**Benefit.** A change to webhooks no longer requires reading the whole hub. Tests can load one router.

**Estimated effort.** Large

---

### 20. Implicit globals couple every module

| | |
|---|---|
| Severity | High |
| Category | Architecture |
| Location | `server.js` lines 1–115 (`logger =`, `serverConfig =`, `db =`, `scheduler =`, and the rest); most modules read those names with no `require` |

**Problem.** The file is not in strict mode. Assignments without `const` create globals. `db.js`, `scheduler.js`, `agentAuth.js`, `pluginManager.js`, and the transports all call `logger` and `serverConfig` as free variables. Modules cannot be loaded in isolation, a typo creates a new global instead of throwing, and tests must install globals before `require`. `passman.js` calls `logger` during `initializeEncryptionKey` at load time, which only works because `server.js` assigns `logger` first.

**Recommendation.** Add `'use strict'` at the top of each file. Export a `createApp({ logger, config, db })` function. Have each module require its own logger or accept it as an argument.

**Benefit.** Load order stops being part of the design. Unit tests stop needing a fake global environment.

**Estimated effort.** Large

---

### 21. One process owns all state

| | |
|---|---|
| Severity | Medium |
| Category | Architecture |
| Location | `db.js`; `models/user.js`; `definitionStore.js`; session middleware |

**Problem.** Users, agent credentials, webhooks, and history sit in separate LevelDB files opened by one process. Definitions are files plus a watcher. Sessions are in memory. That fits a single home-lab container. It does not fit two replicas, a rolling restart, or a worker process for long orchestrations. `mongoose`, `passport`, `passport-local`, and `passport-local-mongoose` are dependencies and are not referenced anywhere, which suggests an abandoned second persistence plan still sitting in the tree.

**Recommendation.** Keep LevelDB if single-process is the product decision, and write that down so the next change does not add a second database by accident. Remove the unused Mongo/Passport dependencies now. If a second instance is a goal, the session store and the credential record are the first things that must move; job execution can stay in-process.

**Benefit.** The deployment model matches the code, and unused database clients stop expanding the install.

**Estimated effort.** Small to drop unused deps. Large to support more than one hub.

---

### 22. Dead code, dead dependencies, and two copies of the agent

| | |
|---|---|
| Severity | Medium |
| Category | Maintainability |
| Location | `package.json`; `HashMap.js`; `db.js` callback wrappers; `passman.js` `deriveKey`; `server.js` `DEFAULT_WEBSOCKER_SERVER_PORT`; `agent/agent.js` vs `agent/src/` |

**Problem.** Direct dependencies with no `require` in this repo include `mongoose`, `passport`, `passport-local`, `passport-local-mongoose`, `jws` (alongside `jsonwebtoken`), and `text-encoding-utf-8` (unnecessary on Node 22). `lusca` is required and unused. `body-parser` is required but is not a direct dependency; it works only because Express depends on it. `HashMap.js` reimplements `Map` with a `startsWith` scan. `db.js` still exports callback wrappers marked deprecated. `deriveKey` is unused. `DEFAULT_WEBSOCKER_SERVER_PORT` is misspelled, includes a stray quote (`'49981"'`), and is never read.

`agent/agent.js` still contains its own `spawn('bash')` and `execSync('du ...')` while `agent/src/executor.js` and `agent/src/metrics.js` contain the maintained copies. Two implementations will drift.

`package.json` says MIT. `README.md` says Apache 2.0.

**Recommendation.** Delete unused dependencies and dead helpers. Depend on `express` built-in `express.json()` and `express.urlencoded()`, or add `body-parser` on purpose. Make `Map` the prefix index, or keep one small helper. Generate `agent/agent.js` in CI from `agent/src` and stop editing it by hand, or stop committing it. Pick one license and make both files match.

**Benefit.** Installs get smaller. The next reader does not have to decide which agent or which license is real.

**Estimated effort.** Medium

---

### 23. Duplicated orchestration bookkeeping

| | |
|---|---|
| Severity | Medium |
| Category | Maintainability |
| Location | `server.js` — `GET /runSchedule.html` stub execution (around line 3271); `scheduler.js` — `runJob` orchestration branch (around line 608) |

**Problem.** Both paths build the same in-progress execution object, push a running-queue item, register `onNodeComplete`, and clear the stub. Field lists are copied (`visitedNodes`, `scriptOutputs`, `conditionEvaluations`, `nodeMetrics`, and the rest). They already disagree in small ways (icon and colour are loaded only in the scheduler path). The next status field will be updated in one place.

**Recommendation.** One function, for example `startOrchestrationRun({ job, trigger, rerunFrom })`, should own id allocation, the stub, the running row, and the call to `executeJob`.

**Benefit.** Manual runs, cron runs, and webhook runs share one lifecycle.

**Estimated effort.** Medium

---

### 24. `upsertSchedule` takes fifteen positional arguments

| | |
|---|---|
| Severity | Low |
| Category | Maintainability |
| Location | `scheduler.js` — `upsertSchedule` / `_upsertScheduleAsync` (around line 362) |

**Problem.** Callers pass a long positional list (`colour`, `description`, `scheduleType`, `scheduleTime`, days, agent, command, params, icon, mode, orchestration id, trigger, rule fields, disabled). A swapped argument is a silent behaviour change. The same shape is repeated in `getScheduleObject`.

**Recommendation.** Take one object. Validate it in one place.

**Benefit.** Call sites become readable, and new schedule fields do not break every caller by position.

**Estimated effort.** Medium

---

### 25. Call stacks on every log line, and synchronous work on request and agent paths

| | |
|---|---|
| Severity | Medium |
| Category | Performance |
| Location | `server.js` log4js config (`enableCallStack: true`, around line 39); `GET /agentstatus.html`; `db.js` — `getManyData`, `searchData`; `agent/src/metrics.js`; `agent/src/executor.js` log interval |

**Problem.** `enableCallStack: true` makes log4js capture a stack for every line, including debug, and the memory appender keeps those objects. Agent status loads history for every agent with `await` inside a loop, so latency grows linearly with agent count and history size. `getManyData` does the same one-key-at-a-time pattern. `searchData` and `Users.getUserByEmail` / `getUserByToken` walk the whole database. On the agent, `computeMetric` for `dir_size` uses `execSync`, which blocks the agent event loop, and the string is interpolated into a shell even though a character allow-list currently blocks most metacharacters. The log follower calls `statSync` every second and opens a new read stream per chunk.

None of this will trouble a handful of agents. It will show up as a stuck UI and a stalled agent when many jobs log at once or a directory walk is large.

**Recommendation.** Enable call stacks only when the log level is `debug` or `trace`. Load agent history in parallel with a cap, or store the last status on the agent record. Replace `execSync` with `execFile` and pass the path as an argument. Tail logs with `fs.watch` or a single file handle and an offset.

**Benefit.** The hub event loop stays responsive under log load. Agent metric checks stop freezing heartbeats.

**Estimated effort.** Medium

---

### 26. HTTP orchestration nodes can call any URL the hub can reach

| | |
|---|---|
| Severity | Medium |
| Category | Security |
| Location | `orchestrationEngine.js` — HTTP execute node (around line 499) |

**Problem.** The node performs `axios` with a URL and headers taken from the graph, after template substitution. That is a useful feature and also server-side request forgery: a user who can edit an orchestration can make the hub call cloud metadata addresses, other containers, or internal admin ports. Bearer tokens and basic passwords from the node are stored in the job definition.

**Recommendation.** Document this as an admin-level action and require `orchestrations.edit` (already required to save a graph). Optionally deny link-local, metadata, and non-http(s) URLs unless the node sets an explicit “allow private” flag. Keep auth header values out of execution logs (`scriptOutputs`).

**Benefit.** A compromised editor account, or a shared operator login, cannot silently pivot through the hub without an extra decision.

**Estimated effort.** Medium

---

### 27. Tests miss the routes where the security bugs live

| | |
|---|---|
| Severity | High |
| Category | Testing |
| Location | `jest.config.js` — `collectCoverageFrom`; `tests/routes/server.test.js` |

**Problem.** There are real unit tests for the scheduler, orchestration engine, webhooks, agent auth decisions, and the agent. Coverage collection does not include `server.js`, `models/user.js`, `pluginManager.js`, `backupManager.js`, `orchestrationEngine.js`, or `webhookManager.js`. `tests/routes/server.test.js` stubs a request object and does not boot Express, so it cannot see that `/saveScript` has no auth. `forceExit: true` hides open handles. Modules that close over globals are awkward to test without loading the whole hub, which is why the largest file has the least protection.

**Recommendation.** Add HTTP tests with `supertest` (or `app` exported without listening) for: unauthenticated `/saveScript` rejected, viewer cannot `GET /runSchedule.html`, login redirect rejects absolute URLs, webhook trigger rejects a key in the query string once that is fixed, and `/rest/data` cannot read `AGENT_CREDENTIALS`. Include those files in coverage and drop `forceExit` once the listen-before-init race is gone.

**Benefit.** The failures in this review become regression tests instead of a reread of `server.js`.

**Estimated effort.** Large

---

### 28. Testability is blocked by construction, not by missing mocks

| | |
|---|---|
| Severity | Medium |
| Category | Testing |
| Location | `server.js` startup; `passman.js` load-time key init; `communications/wsServerTransport.js` (creates a `WebSocket.Server` on require) |

**Problem.** Requiring the WebSocket transport binds a port as a side effect. Requiring `passman.js` reads the environment and may log or exit. Requiring `server.js` starts listening. Tests cannot construct these objects with fakes. That pushes people toward mocking entire modules and away from testing the real middleware order.

**Recommendation.** Export `start(config)` functions. Build the WebSocket server inside `init`, which is already the pattern the browser transport uses. Read the encryption key from an argument with the environment as the default at the edge only.

**Benefit.** Route and auth tests can run without ports, and without a particular `require` order.

**Estimated effort.** Large, and it falls out of findings 19 and 20 if those are done first.

---

### 29. Operational security docs contradict the code

| | |
|---|---|
| Severity | Medium |
| Category | Documentation |
| Location | `SECURITY.md`; `README.md` Docker example; `Dockerfile`; `docs/installation.md` (related) |

**Problem.** `SECURITY.md` tells people to file a public GitHub issue for vulnerabilities. The README Docker snippet publishes `49991:49981` while the image exposes `49981` and the WebSocket default in code is unused and malformed (finding 22). The image does not mention that `ORCHELIUM_ENCRYPTION_KEY` and `NODE_ENV` change the threat model. CSRF behaviour (skipped for every `/rest` path and for `/saveScript`) is not documented, so a later change to `SameSite` would silently remove the only browser mitigation those routes have.

**Recommendation.** Point vulnerability reports at a private channel. Make the install doc and the Dockerfile agree on ports, and state that the encryption key is required. Document which routes are CSRF-exempt and why.

**Benefit.** The next operator does not deploy the default key thinking the warning in the log is optional, and vulnerability reports do not land in public issues.

**Estimated effort.** Small

---

### 30. Container runs as root, with no healthcheck

| | |
|---|---|
| Severity | Medium |
| Category | Security |
| Location | `Dockerfile` |

**Problem.** The image is `node:22-slim`, installs packages as root, and has no `USER`. The hub process can read and write the whole container. There is no `HEALTHCHECK`, which hides the startup race in finding 15 from orchestrators. `apt-get` does not clean the lists, so the image is larger than it needs to be.

**Recommendation.** Run as a non-root user, chown the data, scripts, logs, and plugins volumes, and add a healthcheck against the ready endpoint from finding 15. Delete apt lists in the same `RUN`.

**Benefit.** A script or restore bug has a smaller filesystem to damage. Orchestrators stop sending traffic to a process that is still starting.

**Estimated effort.** Medium

---

### 31. Frontend stacks are vendored and old

| | |
|---|---|
| Severity | Low |
| Category | Maintainability |
| Location | `public/js/jquery/`, `public/js/materialize/materialize.js`, `public/js/jquery/pnotify/` |

**Problem.** jQuery UI, Materialize 1.0, and pnotify are copied into the repo (and some CSS exists twice, under `public/js` and `public/css`). They are no longer maintained upstream. Inline event handlers and `innerHTML` in the EJS views are written against that style, which makes a Content Security Policy (finding 12) painful. Monaco is served from `node_modules` as a whole tree.

**Recommendation.** Leave the UI framework in place for now. Stop adding new `innerHTML` and `M.toast({html})` call sites. When a page is touched, move its script out of the EJS file so a CSP can drop `unsafe-inline` later.

**Benefit.** New pages do not inherit the old injection habits. A later UI replacement has a smaller, safer surface.

**Estimated effort.** Large as a migration. Small to stop the pattern on new code.

---

### 32. ESLint in CI is not the project's ESLint

| | |
|---|---|
| Severity | Low |
| Category | Maintainability |
| Location | `.github/workflows/eslint.yml`; `package.json` |

**Problem.** The workflow `npm install`s ESLint 8.10.0 on the runner. The project has no `eslint` devDependency and no config that this review could tie to the codebase style (`var` vs `const`, implicit globals). CI can pass while `server.js` keeps the patterns in findings 19 and 20. ESLint 8 is also out of date relative to Node 22.

**Recommendation.** Add ESLint 9 (flat config) as a devDependency with a small rule set: `no-undef`, `eqeqeq`, `no-var` for new files, and a ban on `res.redirect` of request input unless it goes through `safeRedirect`. Run `npm test` and `npm run lint` from the repo scripts.

**Benefit.** The next unauthenticated route or open redirect fails CI instead of a review.

**Estimated effort.** Medium

---

### 33. Error middleware always returns JSON, and one route references a missing `config`

| | |
|---|---|
| Severity | Low |
| Category | Reliability |
| Location | `utils/errorHandler.js` — `errorHandlerMiddleware`; `server.js` — `GET /rest/eta` (around line 3737) |

**Problem.** The error middleware sends JSON for every failure. A thrown error on an HTML form post becomes a JSON body in the browser. `GET /rest/eta` calls `JSON.stringify(config)`, but `config` is not defined in that scope (`const { config } = require('process')` inside the startup IIFE does not create a global). The route is authenticated and has no permission check, and it throws `ReferenceError` today.

**Recommendation.** Delete `/rest/eta` or return a real status DTO. In the error middleware, render an error page when `req.accepts('html')` and the path is not under `/rest` or `/api`.

**Benefit.** Broken pages stay readable. A dead route stops looking like a data API.

**Estimated effort.** Small

---

### 34. Settings pages echo secrets back into the HTML

| | |
|---|---|
| Severity | Medium |
| Category | Security |
| Location | `views/settings.ejs` — MQTT and SMTP password inputs (around lines 408 and 528); `configuration.js` — `processServerConfig` |

**Problem.** Password inputs are filled with `value="<%= serverConfig.mqtt.password %>"`. That is limited to `settings.access`, which is better than finding 2, but the cleartext still sits in the DOM, in browser autofill, and in any screen-share of the settings page. Passwords are decrypted into the in-memory config at startup and stay there. `db.putData` debug-logs the stored value.

**Recommendation.** Render password fields empty and update them only when the admin types a new value. Log key names, not values. Avoid `logger.debug` of decrypted config.

**Benefit.** Settings access no longer means the secret is sitting in the page, and debug logs stop becoming a second credential store.

**Estimated effort.** Small

---

## Quick wins

These are safe to do on their own and remove the worst exposure.

1. Put authentication and a script permission on `POST /saveScript`, and stop skipping CSRF for it (finding 1).
2. Stop embedding the MQTT password in `agentstatus.ejs`. Restrict the install command to users who can create agents (finding 2).
3. Reject absolute and protocol-relative `redirect` / `redir` values (finding 5).
4. Confine `runJob` script paths to the scripts directory (finding 6).
5. Set `NODE_ENV=production` and require `ORCHELIUM_ENCRYPTION_KEY` in the Docker image so cookies are `Secure` and the default key cannot boot (findings 3 and 11).
6. Stop printing the data-API token and stop accepting it on the query string (finding 9).
7. Compare webhook, reset, and invite tokens with `timingSafeEqual`, and store webhook keys as hashes when you next touch that code (findings 10 and 13).
8. Delete `/rest/eta` or fix the undefined `config` reference (finding 33).
9. Remove unused npm dependencies (`mongoose`, `passport*`, `jws`, `text-encoding-utf-8`, `lusca`) (finding 22).
10. Await agent-credential persistence (finding 18).

## Technical debt

1. Split `server.js` into routers and replace implicit globals with explicit dependencies (findings 19 and 20).
2. Replace AES-CBC and the padded key with AES-GCM and a real KDF, with separate keys for sessions, JWT, and data (finding 3).
3. Retire legacy agent connections that have no token (finding 7).
4. Move job run and job delete off GET, and align them with edit and delete permissions (finding 4).
5. Pass agent arguments as an argv array and stop appending trigger data onto shell parameters (finding 8).
6. Session regeneration, `maxAge`, and a session user object that has no password hash (finding 11).
7. Rate limiting on login, reset, invite, and webhooks (finding 13).
8. One orchestration start function shared by the UI and the scheduler (finding 23).
9. ESLint as a project dependency with `no-undef` at least (finding 32).
10. Non-root container user and a real healthcheck (findings 15 and 30).

## Architecture improvements

1. **Edge versus core.** Keep HTTP, WebSocket, and MQTT at the edge. Pass a command object into the scheduler and the engine. The edge is where auth, CSRF, and path checks belong; they are currently scattered through `server.js`.
2. **Finish the agent protocol split.** `agent/src` plus `shared/protocol.js` is the right direction. Delete the parallel logic in `agent/agent.js` so security fixes in the executor land in one place (finding 22).
3. **Make startup a single function.** `createApp()` returns the Express app. `start()` opens databases, migrates users, starts the scheduler, then listens (findings 15 and 28).
4. **Treat shell execution as a boundary.** Scripts, parameters, environment, and working directory should be a typed request. The agent should not reassemble a shell string (finding 8). The hub should not read arbitrary files to fill that request (finding 6).
5. **Write down the single-node decision.** LevelDB plus MemoryStore is coherent for one container. Either document that a second replica is unsupported, or move sessions and credential records first (finding 21).
6. **Permissions should match effects.** `jobs.view` must not run jobs. `agents.view` must not reveal broker secrets. `settings.access` is currently a single switch for server, MQTT, SMTP, webhooks, backup, and the generic data API; splitting it back apart is only worth it after the secrets are no longer rendered (findings 2 and 4). The granular settings permissions were collapsed on purpose in `models/user.js`; do not revive them until the routes check them.

## Performance opportunities

Ranked by expected impact on a busy hub or agent, not by micro-optimisations.

1. **Stop capturing a stack on every log line** (`enableCallStack: true`). This taxes every request and every agent message, and the memory appender retains the stacks (finding 25).
2. **Stop the agent-status N+1 history load.** One page does a sequential history read per agent (finding 25).
3. **Replace `execSync` in agent metrics** with non-blocking `execFile`, especially `dir_size` (finding 25).
4. **Stop full LevelDB scans** for email lookup, token lookup, and `searchData`. Keep a secondary key or do the lookup in memory for the small user set (finding 25).
5. **Cap and expire in-progress execution objects** so a stuck run does not retain full `scriptOutputs` until restart (finding 17).
6. **Batch definition and schedule writes** the way `agents.js` already debounces config writes by 50 ms. Scheduler saves that rewrite the whole schedule list on every toggle will dominate before CPU does.

## Security concerns

Ranked by risk under the default Docker deployment.

1. Unauthenticated overwrite of scripts that agents execute (finding 1).
2. MQTT password delivered to every user who can view agents, which is the default invited role (finding 2).
3. Default encryption key `CHANGEIT`, shared with the session secret, AES-CBC without a MAC, and an image that does not set a key (finding 3).
4. Viewers can run jobs, and job editors can read files outside `scripts/` (findings 4 and 6).
5. Approved agents with no token can be impersonated (finding 7).
6. Shell argument injection through `bash -c` (finding 8).
7. Generic `/rest/data` plus a token printed to container logs (finding 9).
8. Plaintext webhook keys on the query string, with no rate limit (finding 10).
9. Open redirect on login (finding 5).
10. Missing security headers, unsanitised SVG and markdown, session cookie not `Secure` unless `NODE_ENV` is set (findings 11 and 12).
11. Backup zip extraction without a path jail (finding 14).
12. HTTP nodes as SSRF from the hub network (finding 26).

## Overly complex code, and what should replace it

- **`server.js` route handlers** that reimplement validation, redirects, and execution bookkeeping. Express routers plus a `safeRedirect` helper and one `startOrchestrationRun` function remove more complexity than any new abstraction.
- **`HashMap.js`.** `Map` plus a single prefix lookup is enough. The class logs on every get.
- **`padStringTo256Bits` and the unused PBKDF2 helper.** Node's `crypto.hkdfSync` or `scryptSync`, then AES-GCM, replaces both. `shared/secretBox.js` is already the pattern to copy.
- **Metacharacter denylist in `executeBackupCommand`.** An argument vector replaces the denylist. Denylists in this file have already missed newlines.
- **Callback wrappers in `db.js`.** Callers should use the async functions. The wrappers are marked deprecated and still exported.
- **`thresholdJobs.js`.** The scheduler already logs these trigger types as deprecated. Delete the module once stored jobs are migrated.
- **Vendored jQuery UI / Materialize / pnotify** for new UI. Do not add features there. Server-rendered EJS is fine; the risk is the raw HTML sinks, not the template engine.
- **Positional `upsertSchedule`.** One options object.

## Overall score

Scores are out of 10 for a single-node home-lab orchestrator, not for a multi-tenant control plane. A 10 would mean the area is safe to extend without a structural fix. The newer agent token code, permission model, and orchestration tests are why this is not lower.

| Area | Score | Why |
|---|---|---|
| Architecture | 4 | Real module boundaries exist (scheduler, engine, agent, plugins, users), but the hub is one 5,400-line script held together by implicit globals and startup races. |
| Code quality | 4 | Mixed. `agent/src`, `pluginManager.js`, and `models/user.js` are readable. `server.js` and `scheduler.js` rely on positional arguments, copied blocks, dead code, and `var`. |
| Performance | 6 | Fine for a small lab. The log stack capture, per-agent history loop, and blocking `execSync` are the items that will hurt first. No algorithmic disaster on the job runner itself. |
| Security | 3 | Unauthenticated script write, broker password in a low-privilege page, and a default key that the published image will use. Permissions and CSRF exist and are bypassed by specific routes. |
| Test coverage | 5 | Good unit tests around the engine, scheduler, webhooks, and agent decisions. Almost no coverage of HTTP authz, the routes in this review, backup restore, or plugin install. |
| Maintainability | 4 | A new contributor can change a job or an agent message. They cannot safely change startup, sessions, or a route in `server.js` without reading the whole file. Two agent implementations and two licenses add noise. |

**Overall: 4 / 10.** The product direction is sound, and several recent pieces (connection tokens, permission constants, plugin path checks, shared AES-GCM for token rotation) show the right instincts. They are not yet applied on the paths that accept untrusted input: script upload, the agent-status page, job execution, and the default deploy.
