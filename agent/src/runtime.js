const os = require('os');

const DEFAULT_MQTT_SERVER = 'localhost';
const DEFAULT_MQTT_SERVER_PORT = '1883';
const DEFAULT_WS_SERVER = 'localhost';
const DEFAULT_WS_SERVER_PORT = '49981';

const HOSTNAME = os.hostname();
const agentNameIndex = process.argv.indexOf('--agent');
const agentNameFromArgv = agentNameIndex !== -1 ? process.argv[agentNameIndex + 1] : null;
const workingDirIndex = process.argv.indexOf('--workingDir');
const mqttServerIndex = process.argv.indexOf('--mqttServer');
const mqttPortIndex = process.argv.indexOf('--mqttPort');
const mqttUsernameIndex = process.argv.indexOf('--mqttUsername');
const mqttPasswordIndex = process.argv.indexOf('--mqttPassword');
const agentTokenIndex = process.argv.indexOf('--agentToken');
const wsServerIndex = process.argv.indexOf('--wsServer');
const wsPortIndex = process.argv.indexOf('--wsPort');
const retryTimeoutIndex = process.argv.indexOf('--retryTimeout');
const fileSystemIndex = process.argv.indexOf('--fileSystems');
const fileSystemThresholdIndex = process.argv.indexOf('--fileSystemsThreshold');
const cpuThresholdIndex = process.argv.indexOf('--cpuThreshold');

const FILE_SYSTEMS = fileSystemIndex !== -1 ? process.argv[fileSystemIndex + 1] : '/,/home,/var,/tmp,/boot,/usr';

const runtime = {
  PORT: 49991,
  HOSTNAME,
  SERVER: `${HOSTNAME}`,
  agentNameIndex,
  workingDirIndex,
  mqttServerIndex,
  mqttPortIndex,
  mqttUsernameIndex,
  mqttPasswordIndex,
  agentTokenIndex,
  wsServerIndex,
  wsPortIndex,
  retryTimeoutIndex,
  fileSystemIndex,
  fileSystemThresholdIndex,
  cpuThresholdIndex,

  connId: 'anonymous',
  mqttClient: null,
  wsClient: null,
  jobCount: 0,
  successJobCount: 0,
  failJobCount: 0,
  pubCount: 0,
  subCount: 0,

  RETRY_TIMEOUT: retryTimeoutIndex !== -1 ? process.argv[retryTimeoutIndex + 1] : 5000,
  FILE_SYSTEMS,
  FILE_SYSTEMS_THRESHOLD: fileSystemThresholdIndex !== -1 ? parseInt(process.argv[fileSystemThresholdIndex + 1], 10) : 80,
  CPU_THRESHOLD: cpuThresholdIndex !== -1 ? parseInt(process.argv[cpuThresholdIndex + 1], 10) : 75,
  criticalPaths: FILE_SYSTEMS.split(','),
  DEBUG_MODE: false,

  INSTALL_DIR: '/opt/OrcheliumAgent',
  STARTUP_TYPE: 'INSTALL',
  agentName: agentNameFromArgv,
  AGENT_ID: agentNameFromArgv ? agentNameFromArgv : `agent_${HOSTNAME}`,
  workingDir: workingDirIndex !== -1 ? process.argv[workingDirIndex + 1] : '/tmp',
  MQTT_SERVER: mqttServerIndex !== -1 ? process.argv[mqttServerIndex + 1] : DEFAULT_MQTT_SERVER,
  MQTT_SERVER_PORT: mqttPortIndex !== -1 ? process.argv[mqttPortIndex + 1] : DEFAULT_MQTT_SERVER_PORT,
  MQTT_USERNAME: mqttUsernameIndex !== -1 ? process.argv[mqttUsernameIndex + 1] : '',
  MQTT_PASSWORD: mqttPasswordIndex !== -1 ? process.argv[mqttPasswordIndex + 1] : '',
  AGENT_TOKEN: agentTokenIndex !== -1 ? process.argv[agentTokenIndex + 1] : '',
  previousAgentToken: '',
  MQTT_ENABLED: process.argv.includes('--mqttServer'),
  WS_SERVER: wsServerIndex !== -1 ? process.argv[wsServerIndex + 1] : DEFAULT_WS_SERVER,
  WS_SERVER_PORT: wsPortIndex !== -1 ? process.argv[wsPortIndex + 1] : DEFAULT_WS_SERVER_PORT,

  useMQTT: false,
  commsType: 'websocket',
  activeJobs: new Map(),
  activeIntervals: [],

  debugToken: null,
  debugTokenExpiry: null,
  DEBUG_TOKEN_EXPIRY_MS: 10 * 60 * 1000,

  enckey: null,
  pushVerificationNotification: true,
  status: 'offline',
  version: null,
  RETRY_ATTEMPTS: undefined,
  RETRY_DELAY: undefined,
};

module.exports = runtime;
