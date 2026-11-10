const runtime = require('./runtime');
const { checkExecutePermissionCapability } = require('./files');
const { getCPULoadPercentage, getFileSystemUsagePercentage } = require('./metrics');

function getUptime() {
  let uptime = process.uptime();
  const days = Math.floor(uptime / (24 * 60 * 60)); uptime %= (24 * 60 * 60);
  const hours = Math.floor(uptime / (60 * 60)); uptime %= (60 * 60);
  const minutes = Math.floor(uptime / 60);
  const seconds = Math.floor(uptime % 60);
  return `${days}d ${hours}h ${minutes}m ${seconds}s`;
}

async function checkMqttLiveness() {
  return new Promise((resolve) => {
    if (runtime.useMQTT) {
      runtime.mqttClient.connect().then(() => {
        runtime.mqttClient.publish('test/liveness', 'test');
        resolve('MQTT test message published successfully');
      }).catch(() => resolve('ERROR: MQTT Connection issues'));
    } else {
      resolve(runtime.wsClient.checkConnected() ? 'WebSocket connection is active' : 'ERROR: WebSocket connection issues');
    }
  });
}

async function livenessProbe() {
  const mqttLivenessResult = await checkMqttLiveness();
  const filePermLivenessResult = checkExecutePermissionCapability(runtime.workingDir, true);
  const overallStatus = (mqttLivenessResult.startsWith('ERROR:') || filePermLivenessResult !== 'ok') ? 'unhealthy' : 'ok';

  return JSON.stringify({
    status: overallStatus,
    identifier: runtime.AGENT_ID,
    uptime: getUptime(),
    version: runtime.version,
    installDir: runtime.INSTALL_DIR,
    workingDir: runtime.workingDir,
    agentStatus: runtime.status,
    mqttServer: runtime.MQTT_SERVER,
    mqttPort: runtime.MQTT_SERVER_PORT,
    wsServer: runtime.WS_SERVER,
    wsServerPort: runtime.WS_SERVER_PORT,
    connectionMode: runtime.commsType,
    startupType: runtime.STARTUP_TYPE,
    jobCount: runtime.jobCount,
    failJobCount: runtime.failJobCount,
    successJobCount: runtime.successJobCount,
    pubCount: runtime.pubCount,
    subCount: runtime.subCount,
    commsStatus: mqttLivenessResult,
    filePermissionStatus: filePermLivenessResult,
    fileSystemUsagePct: getFileSystemUsagePercentage(),
    cpuPct: getCPULoadPercentage(),
  });
}

module.exports = { livenessProbe, getUptime };
