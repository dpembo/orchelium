# Building Orchelium

The hub runs from the repository root. The agent is developed as several modules and shipped as one `agent/agent.js` file.

## Layout

| Path | Role |
|---|---|
| `shared/` | Key derivation and the hub/agent message contract. Safe to bundle into the agent. |
| `agent/src/` | Agent-only source: shell execution, metrics, local HTTP, MQTT and WebSocket clients. |
| `agent/version.js` | Version string. Release replaces `%%UNDEFINED%%` before the agent build. |
| `agent/agent.js` | Generated bundle. Not edited by hand, and not committed. |

`shared/` does not sign JWTs, store passwords, or open server sockets. The hub keeps signing and password encryption in `passman.js`. Both sides call `shared/keyMaterial.js` for the 32-byte key and `shared/protocol.js` for topic names and message fields.

MQTT broker topics and the `topic` field inside messages are `orchelium/agent/command` and `orchelium/agent/status`.

## Hub

```bash
npm ci
npm test
npm start
```

The hub image copies the repository, including `shared/`, and runs `node server.js`.

## Agent

From `agent/`:

```bash
npm ci
npm test
npm run build
node agent.js --help
```

`npm run build` (`agent/build.js`) uses esbuild. It inlines `agent/src` and `shared/` into `agent/agent.js` and leaves runtime packages (`express`, `mqtt`, `ws`, `reconnecting-websocket`, `jsonwebtoken`) external. Install those with `npm ci` next to the bundle. `agent/.npmrc` sets `omit=dev`, so a developer install that needs the tests or the bundler is:

```bash
npm ci --include=dev
```

The build refuses to write `agent.js` if the bundle contains `jwt.sign`.

Stamp `agent/version.js` before `npm run build` when the banner should show a release version. The build copies that string into the bundle.

## Release packaging

`.github/workflows/Release.yml` does this before it creates `public/agent/agent.tar`:

1. Write the release version into `version.js` and `agent/version.js`.
2. Run `npm run build --prefix agent`.
3. Tar the `agent/` directory, which now contains the single `agent.js`.

The agent container build context is the repository root, so the image can see `shared/`. `agent/Dockerfile` installs dependencies, runs `npm run build`, then drops dev dependencies. The container starts with `node agent.js`.

## Local agent image

From the repository root, after `agent/package-lock.json` includes esbuild:

```bash
docker build -f agent/Dockerfile -t orchelium-agent .
```
