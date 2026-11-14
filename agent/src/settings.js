const fs = require('fs');
const runtime = require('./runtime');
const { debug, DEBUG_LEVEL } = require('./log');

function parseSettingsFile(filePath) {
  try {
    debug(DEBUG_LEVEL.TRACE, `Loading Settings file [${filePath}]`);
    const data = fs.readFileSync(filePath, 'utf8');
    const lines = data.split('\n');
    lines.forEach(line => {
      const match = line.match(/^(\w+)="(.*)"$/);
      if (match) {
        const [, key, value] = match;
        switch (key) {
          case 'AGENT_NAME':
            if (runtime.agentNameIndex === -1) {
              runtime.agentName = value;
              runtime.AGENT_ID = value;
            }
            break;
          case 'MQTT_SERVER': if (runtime.mqttServerIndex === -1) runtime.MQTT_SERVER = value; break;
          case 'MQTT_PORT': if (runtime.mqttPortIndex === -1) runtime.MQTT_SERVER_PORT = value; break;
          case 'MQTT_USERNAME': if (runtime.mqttUsernameIndex === -1) runtime.MQTT_USERNAME = value; break;
          case 'MQTT_PASSWORD': if (runtime.mqttPasswordIndex === -1) runtime.MQTT_PASSWORD = value; break;
          case 'AGENT_TOKEN': if (runtime.agentTokenIndex === -1) runtime.AGENT_TOKEN = value; break;
          case 'AGENT_REMOVED': runtime.AGENT_REMOVED = value; break;
          case 'MQTT_ENABLED': if (!process.argv.includes('--mqttServer')) runtime.MQTT_ENABLED = value.toUpperCase() === 'TRUE'; break;
          case 'WS_SERVER': if (runtime.wsServerIndex === -1) runtime.WS_SERVER = value; break;
          case 'WS_PORT': if (runtime.wsPortIndex === -1) runtime.WS_SERVER_PORT = value; break;
          case 'WORKING_DIR': if (runtime.workingDirIndex === -1) runtime.workingDir = value; break;
          case 'INSTALL_DIR': runtime.INSTALL_DIR = value; break;
          case 'STARTUP_TYPE': runtime.STARTUP_TYPE = value; break;
          case 'CONNECTION_RETRIES': runtime.RETRY_ATTEMPTS = parseInt(value, 10); break;
          case 'CONNECTION_DELAY': runtime.RETRY_DELAY = parseInt(value, 10); break;
          case 'CONNECTION_TIMEOUT': runtime.RETRY_TIMEOUT = parseInt(value, 10); break;
          default:
            break;
        }
      }
    });
    runtime.useMQTT = process.argv.includes('--mqttServer') || runtime.MQTT_ENABLED;
    runtime.commsType = runtime.useMQTT ? 'mqtt' : 'websocket';
  } catch (err) {
    debug(DEBUG_LEVEL.TRACE, 'Error reading settings file');
    debug(DEBUG_LEVEL.TRACE, err);
  }
}

function writeSetting(key, value) {
  const filePath = 'settings.sh';
  const safeValue = String(value).replace(/"/g, '');
  let data = '';
  try {
    data = fs.readFileSync(filePath, 'utf8');
  } catch (err) {
    data = '';
  }
  const line = `${key}="${safeValue}"`;
  const pattern = new RegExp(`^${key}=".*"$`, 'm');
  if (pattern.test(data)) {
    data = data.replace(pattern, line);
  } else {
    data = `${data.replace(/\s*$/, '')}\n${line}\n`;
  }
  fs.writeFileSync(filePath, data, { encoding: 'utf8', mode: 0o600 });
  if (key === 'AGENT_TOKEN') runtime.AGENT_TOKEN = safeValue;
}

module.exports = { parseSettingsFile, writeSetting };
