const crypto = require('crypto');
const express = require('express');
const runtime = require('./runtime');
const { debug, DEBUG_LEVEL } = require('./log');
const { publishStatusUpdate } = require('./publish');
const { livenessProbe } = require('./liveness');

const app = express();

function generateDebugToken() {
  const token = crypto.randomBytes(8).toString('hex');
  runtime.debugToken = token;
  runtime.debugTokenExpiry = Date.now() + runtime.DEBUG_TOKEN_EXPIRY_MS;
  debug(DEBUG_LEVEL.WARN, `Debug access requested. Token: ${token}`);
  return token;
}

function isValidDebugToken(token) {
  if (!runtime.debugToken || !runtime.debugTokenExpiry) {
    return false;
  }

  if (Date.now() > runtime.debugTokenExpiry) {
    runtime.debugToken = null;
    runtime.debugTokenExpiry = null;
    return false;
  }

  if (token === runtime.debugToken) {
    runtime.debugTokenExpiry = Date.now() + runtime.DEBUG_TOKEN_EXPIRY_MS;
    debug(DEBUG_LEVEL.TRACE, 'Debug token validated. Expiry reset.');
    return true;
  }

  return false;
}

function registerRoutes() {
  app.get('/', async (req, res) => {
    try {
      res.set('Content-Type', 'application/json');
      res.send(await livenessProbe());
    } catch (error) {
      res.status(500).json({ status: 'error', message: 'Internal Server Error' });
    }
  });

  app.get('/debug', async (req, res) => {
    const token = req.query.token || req.headers['x-debug-token'];

    if (!token) {
      generateDebugToken();
      return res.status(401).json({
        error: 'Token required',
        message: 'Debug access token required. Check logs for token.',
        current_state: runtime.DEBUG_MODE,
      });
    }

    if (!isValidDebugToken(token)) {
      return res.status(401).json({
        error: 'Invalid or expired token',
        message: 'Token is invalid or has expired. Request new token.',
        current_state: runtime.DEBUG_MODE,
      });
    }

    res.json({ DEBUG_MODE: runtime.DEBUG_MODE, message: 'Token valid for next 10 minutes' });
  });

  app.get('/debug/on', async (req, res) => {
    const token = req.query.token || req.headers['x-debug-token'];

    if (!token) {
      generateDebugToken();
      return res.status(401).json({
        error: 'Token required',
        message: 'Debug access token required. Check logs for token.',
      });
    }

    if (!isValidDebugToken(token)) {
      return res.status(401).json({
        error: 'Invalid or expired token',
        message: 'Token is invalid or has expired. Request new token.',
      });
    }

    runtime.DEBUG_MODE = true;
    debug(DEBUG_LEVEL.WARN, 'DEBUG_MODE enabled via API');
    res.json({ DEBUG_MODE: true, message: 'Debug mode enabled. Token valid for next 10 minutes' });
  });

  app.get('/debug/off', async (req, res) => {
    const token = req.query.token || req.headers['x-debug-token'];

    if (!token) {
      generateDebugToken();
      return res.status(401).json({
        error: 'Token required',
        message: 'Debug access token required. Check logs for token.',
      });
    }

    if (!isValidDebugToken(token)) {
      return res.status(401).json({
        error: 'Invalid or expired token',
        message: 'Token is invalid or has expired. Request new token.',
      });
    }

    runtime.DEBUG_MODE = false;
    debug(DEBUG_LEVEL.WARN, 'DEBUG_MODE disabled via API');
    res.json({ DEBUG_MODE: false, message: 'Debug mode disabled. Token valid for next 10 minutes' });
  });

  app.post('/api/server-restart', async (req, res) => {
    try {
      debug(DEBUG_LEVEL.INFO, 'Server restart notification received, resetting connection backoff');

      if (runtime.useMQTT && runtime.mqttClient && runtime.mqttClient.retryManager) {
        runtime.mqttClient.retryManager.reset();
        if (!runtime.mqttClient.isConnected) {
          debug(DEBUG_LEVEL.INFO, 'Triggering MQTT reconnection after server restart');
          runtime.mqttClient.connect().then(() => {
            publishStatusUpdate('register', 'Agent Online - Awaiting provision');
          }).catch((err) => {
            debug(DEBUG_LEVEL.WARN, `MQTT reconnection failed: ${err.message}`);
            runtime.mqttClient.retryConnection();
          });
        }
      } else if (!runtime.useMQTT && runtime.wsClient && runtime.wsClient.retryManager) {
        runtime.wsClient.retryManager.reset();
        if (!runtime.wsClient.isConnected) {
          debug(DEBUG_LEVEL.INFO, 'Triggering WebSocket reconnection after server restart');
          runtime.wsClient.connect().then(() => {
            publishStatusUpdate('register', 'Agent Online - Awaiting provision');
          }).catch((err) => {
            debug(DEBUG_LEVEL.WARN, `WebSocket reconnection failed: ${err.message}`);
            runtime.wsClient.triggerReconnect();
          });
        }
      }

      res.json({
        status: 'success',
        message: 'Server restart notification received, connection backoff reset',
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      debug(DEBUG_LEVEL.ERROR, `Error processing server restart notification: ${error.message}`);
      res.status(500).json({
        status: 'error',
        message: 'Failed to process server restart notification',
        error: error.message,
      });
    }
  });
}

function listen() {
  return app.listen(runtime.PORT, () => {
    debug(DEBUG_LEVEL.TRACE, `Express server listening on port ${runtime.PORT}`);
    runtime.status = 'idle';
  });
}

module.exports = { registerRoutes, listen };
