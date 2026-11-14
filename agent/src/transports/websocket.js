const websocket = require('ws');
const ReconnectingWebSocket = require('reconnecting-websocket');
const { RetryBackoffManager } = require('./retry');
const { publishStatusUpdate } = require('../publish');
const { acceptFirstEnrollment, markRemovedFromHub } = require('../commands');
const runtime = require('../runtime');
const { debug, DEBUG_LEVEL } = require('../log');

// reconnecting-websocket removes its socket listeners and then closes a socket
// that is still connecting. The ws library emits 'error' on the next tick for
// that close. With no listener left, Node treats it as fatal and the agent
// exits instead of retrying. This wrapper keeps one listener for the life of
// the socket. reconnecting-websocket checks CLOSING === 2 on the constructor.
function SafeWebSocket(url, protocols) {
  const options = {};
  if (runtime.AGENT_TOKEN) {
    options.headers = { Authorization: `Bearer ${runtime.AGENT_TOKEN}` };
  }
  const socket = protocols === undefined
    ? new websocket(url, options)
    : new websocket(url, protocols, options);
  socket.on('error', (err) => {
    debug(DEBUG_LEVEL.TRACE, `WebSocket socket error: ${err && err.message ? err.message : err}`);
  });
  return socket;
}
SafeWebSocket.CONNECTING = websocket.CONNECTING;
SafeWebSocket.OPEN = websocket.OPEN;
SafeWebSocket.CLOSING = websocket.CLOSING;
SafeWebSocket.CLOSED = websocket.CLOSED;

class WebSocketHandler {
  constructor(server, port, agentId, processMessageCallback, options = {}) {
    this.server = server;
    this.port = port;
    this.agentId = agentId;
    this.client = null;
    this.isConnected = false;
    this.isConnecting = false;
    this.isReconnecting = false;
    this.connectionTimeout = options.connectionTimeout || 5000;
    this.processMessageCallback = processMessageCallback;
    this.reconnectTimeoutId = null;
    this.connectionGeneration = 0;
    this.heartbeatIntervalId = null;
    this.lastActivityTime = null;
    this.retryManager = new RetryBackoffManager(this.agentId, 'WebSocket');
  }

  checkConnected() {
    return this.isConnected;
  }

  connect() {
    return new Promise((resolve, reject) => {
      if (this.isConnected) {
        return resolve(this.client);
      }
      if (this.isConnecting) {
        return reject(new Error('WebSocket connection attempt in progress'));
      }

      this.isConnecting = true;
      const connId = `${encodeURIComponent(this.agentId)}`;
      const connectUrl = `ws://${this.server}:${this.port}?name=${connId}`;

      if (this.client) {
        try { this.client.close(); } catch (e) { /* stale client */ }
        this.client = null;
      }

      const generation = ++this.connectionGeneration;

      const options = {
        WebSocket: SafeWebSocket,
        connectionTimeout: this.connectionTimeout,
        maxRetries: 0,
        reconnectInterval: 1000,
      };

      debug(DEBUG_LEVEL.TRACE, `Initiating WebSocket connection for [${connId}] (gen ${generation})`);
      this.client = new ReconnectingWebSocket(connectUrl, [], options);

      this.client.addEventListener('open', () => {
        if (generation !== this.connectionGeneration) return;
        this.isConnected = true;
        this.isConnecting = false;
        this.retryManager.reset();
        this.isReconnecting = false;
        this.lastActivityTime = Date.now();
        this._startHeartbeat(generation);
        debug(DEBUG_LEVEL.INFO, `WebSocket server connected [${this.agentId}]`);
        resolve(this.client);
      });

      this.client.addEventListener('close', (event) => {
        if (generation !== this.connectionGeneration) return;
        this.isConnected = false;
        this.isConnecting = false;
        if (runtime.removedByHub) return;
        const code = event && (event.code || (event.detail && event.detail.code));
        if (code === 4001) {
          markRemovedFromHub();
          return;
        }
        if (!this.isReconnecting) {
          this.triggerReconnect();
        }
      });

      this.client.addEventListener('error', (err) => {
        if (generation !== this.connectionGeneration) return;
        this.isConnected = false;
        this.isConnecting = false;
        debug(DEBUG_LEVEL.TRACE, `WebSocket error: ${err.message}`);
        if (!this.isReconnecting) {
          this.triggerReconnect();
        }
        reject(err);
      });

      this.client.addEventListener('message', (event) => {
        if (generation !== this.connectionGeneration) return;
        this.lastActivityTime = Date.now();
        if (acceptFirstEnrollment(event.data)) return;
        const message = `"${event.data}"`;
        debug(DEBUG_LEVEL.TRACE, `Received message on [${connId}]`);
        this.processMessageCallback(message);
      });

      setTimeout(() => {
        if (generation !== this.connectionGeneration) return;
        if (!this.isConnected && this.isConnecting) {
          this.isConnecting = false;
          debug(DEBUG_LEVEL.WARN, `WebSocket [${connId}] initial connection timeout`);
          if (!this.isReconnecting) {
            this.triggerReconnect();
          }
          reject(new Error('Connection timeout'));
        }
      }, this.connectionTimeout);
    });
  }

  _startHeartbeat(generation) {
    if (this.heartbeatIntervalId) {
      clearInterval(this.heartbeatIntervalId);
      this.heartbeatIntervalId = null;
    }
    const HEARTBEAT_INTERVAL_MS = 30 * 1000;
    const HEARTBEAT_STALE_MS = 90 * 1000;
    this.heartbeatIntervalId = setInterval(() => {
      if (generation !== this.connectionGeneration) {
        clearInterval(this.heartbeatIntervalId);
        this.heartbeatIntervalId = null;
        return;
      }
      if (!this.isConnected) return;
      const elapsed = Date.now() - this.lastActivityTime;
      if (elapsed > HEARTBEAT_STALE_MS) {
        debug(DEBUG_LEVEL.WARN, `[WebSocket] ${this.agentId} - No activity for ${Math.round(elapsed / 1000)}s, connection appears stale. Forcing reconnect.`);
        clearInterval(this.heartbeatIntervalId);
        this.heartbeatIntervalId = null;
        this.isConnected = false;
        this.isConnecting = false;
        try { this.client.close(); } catch (e) { /* already closed */ }
        if (!this.isReconnecting) {
          this.triggerReconnect();
        }
      }
    }, HEARTBEAT_INTERVAL_MS);
    runtime.activeIntervals.push(this.heartbeatIntervalId);
  }

  triggerReconnect() {
    if (runtime.removedByHub || this.isReconnecting) return;

    this.isReconnecting = true;

    if (this.reconnectTimeoutId) {
      clearTimeout(this.reconnectTimeoutId);
      this.reconnectTimeoutId = null;
    }

    const retryDelay = this.retryManager.getNextRetryDelay();

    this.reconnectTimeoutId = setTimeout(() => {
      this.retryManager.recordAttempt();
      this.isReconnecting = false;
      this.connect()
        .then(() => {
          debug(DEBUG_LEVEL.INFO, `WebSocket server reconnected [${this.agentId}]`);
          publishStatusUpdate('register', 'Agent Online - Awaiting provision');
        })
        .catch(() => {
          // Reconnect will be triggered again by close/error events
        });
    }, retryDelay);
  }

  reconnectNow() {
    this.connectionGeneration += 1;
    const old = this.client;
    this.isConnected = false;
    this.isConnecting = false;
    this.isReconnecting = false;
    this.client = null;
    if (old) {
      try { old.close(); } catch (e) { /* already closed */ }
    }
    return this.connect();
  }

  sendMessage(message) {
    if (this.isConnected && this.client && this.client.readyState === 1) {
      this.client.send(message);
      debug(DEBUG_LEVEL.TRACE, `Sent message on [${this.agentId}]`);
    } else {
      debug(DEBUG_LEVEL.WARN, `Cannot send on [${this.agentId}]: WebSocket not connected (state: ${this.client?.readyState})`);
      if (!this.isConnecting && !this.isReconnecting) {
        this.triggerReconnect();
      }
    }
  }

  disconnect() {
    if (this.heartbeatIntervalId) {
      clearInterval(this.heartbeatIntervalId);
      this.heartbeatIntervalId = null;
    }
    if (this.reconnectTimeoutId) {
      clearTimeout(this.reconnectTimeoutId);
      this.reconnectTimeoutId = null;
    }
    if (this.client) {
      this.client.close();
      this.isConnected = false;
      this.isConnecting = false;
      this.isReconnecting = false;
    }
  }
}

module.exports = { WebSocketHandler };
