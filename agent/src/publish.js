const { TOPICS, buildStatusMessage, buildLogMessage } = require('../../shared/protocol');
const runtime = require('./runtime');
const { debug, DEBUG_LEVEL } = require('./log');

function publishStatusUpdate(status, description, data, jobName, callback, isManual, executionId = null, retryCount = 0, executionContext = null) {
  const message = buildStatusMessage({
    name: runtime.AGENT_ID,
    server: runtime.SERVER,
    manual: isManual,
    commsType: runtime.commsType,
    description,
    data,
    status,
    jobName,
    executionId,
    executionContext,
  });

  runtime.pubCount++;
  const maxRetries = 6;

  if (runtime.useMQTT) {
    runtime.mqttClient.connect().then(() => {
      if (!runtime.mqttClient.isConnected) {
        throw new Error('MQTT connection lost after initial connect');
      }
      runtime.mqttClient.publish(TOPICS.status, JSON.stringify(message));
      debug(DEBUG_LEVEL.TRACE, `Published status update over MQTT: ${status}`);
    }).catch((err) => {
      debug(DEBUG_LEVEL.TRACE, `MQTT publish failed: ${err.message}`);
      if (retryCount < maxRetries) {
        const retryDelay = Math.pow(2, retryCount) * 1000;
        debug(DEBUG_LEVEL.TRACE, `Retrying MQTT publish in ${retryDelay}ms (attempt ${retryCount + 1}/${maxRetries})`);
        setTimeout(() => publishStatusUpdate(status, description, data, jobName, callback, isManual, executionId, retryCount + 1, executionContext), retryDelay);
      } else {
        debug(DEBUG_LEVEL.WARN, `MQTT publish failed after ${maxRetries} retries`);
      }
    });
  } else if (runtime.wsClient.isConnected) {
    if (runtime.wsClient.client && runtime.wsClient.client.readyState === 1) {
      runtime.wsClient.sendMessage(JSON.stringify(message));
      debug(DEBUG_LEVEL.TRACE, `Sent status update: ${status}`);
    } else {
      runtime.wsClient.isConnected = false;
      debug(DEBUG_LEVEL.WARN, 'WebSocket connection state mismatch, reconnecting...');
      publishStatusUpdate(status, description, data, jobName, callback, isManual, executionId, retryCount, executionContext);
    }
  } else {
    runtime.wsClient.connect().then(() => {
      runtime.wsClient.sendMessage(JSON.stringify(message));
      debug(DEBUG_LEVEL.TRACE, `Connected and sent status update: ${status}`);
    }).catch((err) => {
      debug(DEBUG_LEVEL.TRACE, `Failed to send status update: ${err.message}`);
      if (retryCount < maxRetries) {
        const retryDelay = Math.pow(2, retryCount) * 1000;
        debug(DEBUG_LEVEL.TRACE, `Retrying WebSocket publish in ${retryDelay}ms (attempt ${retryCount + 1}/${maxRetries})`);
        setTimeout(() => publishStatusUpdate(status, description, data, jobName, callback, isManual, executionId, retryCount + 1, executionContext), retryDelay);
      } else {
        debug(DEBUG_LEVEL.WARN, `WebSocket publish failed after ${maxRetries} retries`);
      }
    });
  }
}

function publishLogData(logData, jobName, eta, returnCode, callback, manual, executionId = null, executionContext = null) {
  const message = buildLogMessage({
    name: runtime.AGENT_ID,
    server: runtime.SERVER,
    jobName,
    manual,
    eta,
    returnCode,
    data: logData,
    executionId,
    executionContext,
  });

  runtime.pubCount++;
  if (runtime.useMQTT) {
    runtime.mqttClient.connect().then(() => {
      runtime.mqttClient.publish(TOPICS.status, JSON.stringify(message));
      if (callback) callback();
    }).catch((err) => debug(DEBUG_LEVEL.TRACE, `MQTT publish failed: ${err.message}`));
  } else if (runtime.wsClient.isConnected && runtime.wsClient.client && runtime.wsClient.client.readyState === 1) {
    runtime.wsClient.sendMessage(JSON.stringify(message));
    debug(DEBUG_LEVEL.TRACE, `Sent log update: ${message.status}`);
  } else {
    runtime.wsClient.isConnected = false;
    runtime.wsClient.connect().then(() => {
      runtime.wsClient.sendMessage(JSON.stringify(message));
      debug(DEBUG_LEVEL.TRACE, `Connected and sent log update: ${message.status}`);
    }).catch((err) => debug(DEBUG_LEVEL.TRACE, `Failed to send log update: ${err.message}`));
  }
}

module.exports = { publishStatusUpdate, publishLogData };
