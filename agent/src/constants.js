const AGENT_STATUS = Object.freeze({
  OFFLINE: 'offline',
  IDLE: 'idle',
  RUNNING: 'running',
  INITIALIZING: 'initializing',
  ERROR: 'error',
});

const helpInfo = {
  '--agent': 'Specify the agent name.',
  '--workingDir': 'Specify the working directory for file operations. Default is /tmp.',
  '--debug': 'Enable debug mode for additional logging.',
  '--mqttServer': 'Specify the mqtt server address',
  '--mqttPort': 'Specify the mqtt server port',
  '--wsServer': 'Specify the WebSocket server address',
  '--wsPort': 'Specify the WebSocket server port',
  '--retryCount': 'Number of connection retries',
  '--retryDelay': 'Time in Milliseconds of wait between retries',
  '--retryTimeout': 'Time in Milliseconds for connection to timeout',
  '-? / --help': 'Display this help message.',
};

module.exports = { AGENT_STATUS, helpInfo };
