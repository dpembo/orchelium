const mqtt = require('mqtt');
const { RetryBackoffManager } = require('./retry');
const { publishStatusUpdate } = require('../publish');
const runtime = require('../runtime');
const { debug, DEBUG_LEVEL } = require('../log');

class MqttHandler {
  constructor(server, port, options = {}) {
    this.server = server;
    this.port = port;
    this.client = null;
    this.isConnected = false;
    this.isReconnecting = false;
    this.isConnecting = false;
    this.connectionTimeout = options.connectionTimeout || 5000;
    this.username = options.username || '';
    this.password = options.password || '';
    this.connectionTimeoutHandler = null;
    this.retryManager = new RetryBackoffManager(this.server, 'MQTT');
  }

  checkConnected() {
    return this.isConnected;
  }

  connect() {
    if (this.isConnected && this.client) {
      debug(DEBUG_LEVEL.TRACE, 'MQTT connection exists and is connected');
      return Promise.resolve(this.client);
    }
    if (this.isConnecting) {
      return Promise.reject(new Error('MQTT connection attempt in progress'));
    }
    this.isConnecting = true;
    return new Promise((resolve, reject) => {
      const connectUrl = `mqtt://${this.server}:${this.port}`;
      const clientOptions = {};
      if (this.username) {
        clientOptions.username = this.username;
        clientOptions.password = this.password;
      }
      this.client = Object.keys(clientOptions).length
        ? mqtt.connect(connectUrl, clientOptions)
        : mqtt.connect(connectUrl);
      this.connectionTimeoutHandler = setTimeout(() => {
        if (!this.isConnected) {
          this.client.end();
          this.isConnecting = false;
          this.retryConnection();
          reject(new Error('Connection timeout'));
        }
      }, this.connectionTimeout);

      this.client.on('connect', () => {
        this.isConnected = true;
        this.retryManager.reset();
        clearTimeout(this.connectionTimeoutHandler);
        this.isReconnecting = false;
        debug(DEBUG_LEVEL.INFO, `MQTT server connected [${this.server}:${this.port}]`);
        this.isConnecting = false;
        resolve(this.client);
      });

      this.client.on('error', (err) => {
        debug(DEBUG_LEVEL.ERROR, `MQTT connection error: ${err.message}`);
        this.client.end();
        this.isConnected = false;
        clearTimeout(this.connectionTimeoutHandler);
        this.isConnecting = false;
        this.retryConnection();
        reject(err);
      });

      this.client.on('close', () => {
        this.isConnected = false;
        this.isConnecting = false;
        if (runtime.removedByHub) return;
        this.retryConnection();
      });
    });
  }

  retryConnection() {
    if (runtime.removedByHub || this.isReconnecting) return;
    this.isReconnecting = true;

    const retryDelay = this.retryManager.getNextRetryDelay();

    setTimeout(() => {
      this.retryManager.recordAttempt();
      this.connect().then(() => {
        this.isReconnecting = false;
        publishStatusUpdate('register', 'Agent Online - Awaiting provision');
      }).catch(() => this.retryConnection());
    }, retryDelay);
  }

  disconnect() {
    if (this.client) {
      this.client.end();
      this.isConnected = false;
    }
  }

  publish(topic, message) {
    if (!this.isConnected) return Promise.reject(new Error('Not connected to MQTT server'));
    return new Promise((resolve, reject) => {
      this.client.publish(topic, message, (err) => {
        err ? reject(err) : resolve();
      });
    });
  }

  subscribe(topic, callback) {
    if (this.isConnected && this.client) {
      this.client.subscribe(topic, (err) => {
        if (err) debug(DEBUG_LEVEL.ERROR, `Failed to subscribe to ${topic}: ${err.message}`);
        else {
          debug(DEBUG_LEVEL.TRACE, `Subscribed to ${topic}`);
          this.client.on('message', (receivedTopic, message) => {
            if (receivedTopic === topic) callback(message.toString());
          });
        }
      });
    } else {
      debug(DEBUG_LEVEL.ERROR, 'Cannot subscribe, MQTT client not connected');
    }
  }
}

module.exports = { MqttHandler };
