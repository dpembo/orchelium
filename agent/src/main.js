const { TOPICS } = require('../../shared/protocol');
const versionInfo = require('../version.js');
const runtime = require('./runtime');
const { AGENT_STATUS, helpInfo } = require('./constants');
const { debug, DEBUG_LEVEL } = require('./log');
const { loadEncryptionKey } = require('./key');
const { parseSettingsFile } = require('./settings');
const { printBanner, printConfiguration } = require('./banner');
const { handleCommand } = require('./commands');
const { MqttHandler } = require('./transports/mqtt');
const { WebSocketHandler } = require('./transports/websocket');
const { registerRoutes, listen } = require('./http');
const { checkExecutePermissionCapability } = require('./files');
const { cleanupBackgroundTasks } = require('./executor');
const { publishStatusUpdate } = require('./publish');

runtime.version = versionInfo.getVersion();
loadEncryptionKey();

process.on('SIGINT', () => {
  debug(DEBUG_LEVEL.INFO, 'SIGINT received, cleaning up...');
  cleanupBackgroundTasks();
  process.exit(0);
});

process.on('SIGTERM', () => {
  debug(DEBUG_LEVEL.INFO, 'SIGTERM received, cleaning up...');
  cleanupBackgroundTasks();
  process.exit(0);
});

if (process.argv.includes('-?') || process.argv.includes('--help')) {
  console.log('Usage: node agent.js [options]\nOptions:');
  for (const [option, desc] of Object.entries(helpInfo)) console.log(`  ${option.padEnd(20)} ${desc}`);
  process.exit(0);
}

runtime.status = AGENT_STATUS.INITIALIZING;
printBanner();
parseSettingsFile('settings.sh');

debug(DEBUG_LEVEL.INFO, `Transport: ${runtime.useMQTT ? `MQTT → ${runtime.MQTT_SERVER}:${runtime.MQTT_SERVER_PORT}` : `WebSocket → ${runtime.WS_SERVER}:${runtime.WS_SERVER_PORT}`}`);

if (process.argv.includes('--debug')) {
  runtime.DEBUG_MODE = true;
  debug(DEBUG_LEVEL.WARN, 'DEBUG ENABLED');
}

if (runtime.AGENT_REMOVED === 'true' && !runtime.AGENT_TOKEN) {
  debug(DEBUG_LEVEL.ERROR, 'This agent was removed from the hub. Deploy it again to connect.');
} else if (runtime.useMQTT) {
  runtime.mqttClient = new MqttHandler(runtime.MQTT_SERVER, runtime.MQTT_SERVER_PORT, {
    connectionTimeout: runtime.RETRY_TIMEOUT,
    username: runtime.MQTT_USERNAME,
    password: runtime.MQTT_PASSWORD,
  });
  runtime.mqttClient.connect().then(() => {
    runtime.mqttClient.subscribe(TOPICS.command, handleCommand);
    publishStatusUpdate('register', 'Agent Online - Awaiting provision');
  }).catch((err) => debug(DEBUG_LEVEL.ERROR, `MQTT connection failed: ${err.message}`));
} else {
  runtime.wsClient = new WebSocketHandler(runtime.WS_SERVER, runtime.WS_SERVER_PORT, runtime.AGENT_ID, handleCommand, {
    connectionTimeout: runtime.RETRY_TIMEOUT,
  });
  debug(DEBUG_LEVEL.INFO, 'Establishing WebSocket Connection');
  runtime.wsClient.connect().then(() => {
    publishStatusUpdate('register', 'Agent Online - Awaiting provision');
  }).catch((err) => {
    debug(DEBUG_LEVEL.WARN, `WebSocket connection failed: ${err.message} - triggering reconnection`);
    runtime.wsClient.triggerReconnect();
  });
}

registerRoutes();
listen();
printConfiguration();
checkExecutePermissionCapability(runtime.workingDir);

process.on('beforeExit', (code) => {
  publishStatusUpdate('offline', 'Script exited', null, () => {
    cleanupBackgroundTasks();
    process.exit(code);
  });
});

process.on('SIGINT', () => {
  cleanupBackgroundTasks();
  runtime.useMQTT ? runtime.mqttClient.disconnect().then(() => process.exit(0)) : process.exit(0);
});
