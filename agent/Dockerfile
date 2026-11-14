FROM node:22-slim

LABEL org.opencontainers.image.authors="https://github.com/dpembo/orchelium/issues"
LABEL org.opencontainers.image.url="ghcr.io/dpembo/orchelium/agent:latest"
LABEL org.opencontainers.image.source="https://github.com/dpembo/orchelium"
LABEL org.opencontainers.image.vendor="Orchelium.com"
LABEL org.opencontainers.image.title="Orchelium (Agent)"
LABEL org.opencontainers.image.description="Orchelium agent executes scheduled jobs and reports results securely to the Orchelium hub."

# Build context is the repository root so shared/ can be bundled into agent.js.
WORKDIR /usr/src/app

COPY agent/package.json agent/package-lock.json ./
# agent/.npmrc sets omit=dev. Dev dependencies are required to bundle agent.js.
RUN npm ci --include=dev

COPY shared /usr/src/shared
COPY agent/ ./
RUN npm run build && npm prune --omit=dev && rm -rf src tests coverage build.js jest.config.js

# Install curl (healthcheck) and rsync for backup scripts
RUN apt-get update && apt-get install -y --no-install-recommends curl rsync && rm -rf /var/lib/apt/lists/*

EXPOSE 49991

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 CMD curl --fail http://localhost:49991/ | grep '"status":"ok"' || exit 1

ENTRYPOINT ["node", "agent.js"]
