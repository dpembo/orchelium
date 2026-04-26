const versionInfo = require('./version.js');
const version = versionInfo.getVersion();
const fs = require('fs');
const path = require('path');
const mqtt = require('mqtt');
const websocket = require('ws');
const ReconnectingWebSocket = require('reconnecting-websocket');
const { spawn } = require('child_process');
const { execSync } = require('child_process');
const express = require('express');
const jwt = require('jsonwebtoken');
const os = require('os');
const crypto = require('crypto');

const PORT = 49991;
const app = express();

// Define critical filesystem paths
let connId = "anonymous";
let mqttClient;
let wsClient;

let jobCount = 0;
let successJobCount = 0;
let failJobCount = 0;
let pubCount = 0;
let subCount = 0;

// Configuration
const DEFAULT_MQTT_SERVER = 'localhost';
const DEFAULT_MQTT_SERVER_PORT = '1883';
const DEFAULT_WS_SERVER = 'localhost';
const DEFAULT_WS_SERVER_PORT = '49981';
const TOPIC_COMMAND = 'orchelium/agent/command';
const TOPIC_STATUS = 'orchelium/agent/status';
const LOG_STATUS = 'orchelium/agent/log';

let RETRY_TIMEOUT = 5000;
let FILE_SYSTEMS = "/,/home,/var,/tmp,/boot,/usr";
let FILE_SYSTEMS_THRESHOLD = 80;
let CPU_THRESHOLD = 75;
let DEBUG_MODE = false;

const AGENT_STATUS = Object.freeze({
  OFFLINE: "offline",
  IDLE: "idle",
  RUNNING: "running",
  INITIALIZING: "initializing",
  ERROR: "error"
});
let status = AGENT_STATUS.OFFLINE;

// Generate agent ID using hostname
let INSTALL_DIR = "/opt/OrcheliumAgent";
let STARTUP_TYPE = "INSTALL";
const HOSTNAME = os.hostname();
const agentNameIndex = process.argv.indexOf('--agent');
let agentName = agentNameIndex !== -1 ? process.argv[agentNameIndex + 1] : null;
let AGENT_ID = agentName ? agentName : `agent_${HOSTNAME}`;
const SERVER = `${HOSTNAME}`;

const workingDirIndex = process.argv.indexOf('--workingDir');
let workingDir = workingDirIndex !== -1 ? process.argv[workingDirIndex + 1] : '/tmp';

const mqttServerIndex = process.argv.indexOf('--mqttServer');
let MQTT_SERVER = mqttServerIndex !== -1 ? process.argv[mqttServerIndex + 1] : DEFAULT_MQTT_SERVER;

const mqttPortIndex = process.argv.indexOf('--mqttPort');
let MQTT_SERVER_PORT = mqttPortIndex !== -1 ? process.argv[mqttPortIndex + 1] : DEFAULT_MQTT_SERVER_PORT;

let MQTT_ENABLED = process.argv.includes('--mqttServer');

const wsServerIndex = process.argv.indexOf('--wsServer');
let WS_SERVER = wsServerIndex !== -1 ? process.argv[wsServerIndex + 1] : DEFAULT_WS_SERVER;

const wsPortIndex = process.argv.indexOf('--wsPort');
let WS_SERVER_PORT = wsPortIndex !== -1 ? process.argv[wsPortIndex + 1] : DEFAULT_WS_SERVER_PORT;

const retryTimeoutIndex = process.argv.indexOf('--retryTimeout');
RETRY_TIMEOUT = retryTimeoutIndex !== -1 ? process.argv[retryTimeoutIndex + 1] : RETRY_TIMEOUT;

const fileSystemIndex = process.argv.indexOf('--fileSystems');
FILE_SYSTEMS = fileSystemIndex !== -1 ? process.argv[fileSystemIndex + 1] : FILE_SYSTEMS;
let criticalPaths = FILE_SYSTEMS.split(",");

const fileSystemThresholdIndex = process.argv.indexOf('--fileSystemsThreshold');
FILE_SYSTEMS_THRESHOLD = fileSystemThresholdIndex !== -1 ? parseInt(process.argv[fileSystemThresholdIndex + 1]) : FILE_SYSTEMS_THRESHOLD;

const cpuThresholdIndex = process.argv.indexOf('--cpuThreshold');
CPU_THRESHOLD = cpuThresholdIndex !== -1 ? parseInt(process.argv[cpuThresholdIndex + 1]) : CPU_THRESHOLD;

let useMQTT = false;
let commsType = "websocket";
let activeJobs = new Map();  // Track active jobs by executionId or jobName
let activeIntervals = [];  // Track intervals for cleanup

// Debug token management (10 minute expiry, resets on each use)
let debugToken = null;
let debugTokenExpiry = null;
const DEBUG_TOKEN_EXPIRY_MS = 10 * 60 * 1000;  // 10 minutes

function initializeEncryptionKey() {
  let key = process.env.ORCHELIUM_ENCRYPTION_KEY;
  const keyEnforceLevel = process.env.ORCHELIUM_KEY_ENFORCE || 'warn'; // 'strict', 'warn', or 'silent'
  
  if(!key || key.length === 0) {
    const DEFAULT_KEY = "CHANGEIT";
    key = DEFAULT_KEY;
    
    switch(keyEnforceLevel) {
      case 'strict':
        console.error("═══════════════════════════════════════════════════════════════");
        console.error("CRITICAL: Default encryption key in use. Agent will not start.");
        console.error("═══════════════════════════════════════════════════════════════");
        console.error("Set ORCHELIUM_ENCRYPTION_KEY environment variable before starting.");
        console.error("This key must match the key configured on the Server.");
        console.error("═══════════════════════════════════════════════════════════════");
        process.exit(1);
      case 'warn':
        console.warn("═══════════════════════════════════════════════════════════════");
        console.warn("⚠️  WARNING: Using default encryption key (CHANGEIT)");
        console.warn("═══════════════════════════════════════════════════════════════");
        console.warn("This key should ONLY be used for development/testing.");
        console.warn("For production, set ORCHELIUM_ENCRYPTION_KEY environment variable.");
        console.warn("The same key must be set on the Server for communications.");
        console.warn("═══════════════════════════════════════════════════════════════");
        break;
      case 'silent':
        debug(DEBUG_LEVEL.DEBUG, "Using default encryption key (not recommended for production)");
        break;
      default:
        console.warn("Unknown ORCHELIUM_KEY_ENFORCE value: " + keyEnforceLevel);
    }
  } else {
    debug(DEBUG_LEVEL.INFO, "Custom encryption key loaded from ORCHELIUM_ENCRYPTION_KEY");
  }
  
  return padStringTo256Bits(key);
}

let enckey = initializeEncryptionKey();
let pushVerificationNotification = true;

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

// Utility Functions

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
          // CLI args take priority — only apply settings file value if not already set via argv
          case 'AGENT_NAME': if (agentNameIndex === -1) { agentName = AGENT_ID = value; } break;
          case 'MQTT_SERVER': if (mqttServerIndex === -1) MQTT_SERVER = value; break;
          case 'MQTT_PORT': if (mqttPortIndex === -1) MQTT_SERVER_PORT = value; break;
          case 'MQTT_ENABLED': if (!process.argv.includes('--mqttServer')) MQTT_ENABLED = value.toUpperCase() === 'TRUE'; break;
          case 'WS_SERVER': if (wsServerIndex === -1) WS_SERVER = value; break;
          case 'WS_PORT': if (wsPortIndex === -1) WS_SERVER_PORT = value; break;
          case 'WORKING_DIR': if (workingDirIndex === -1) workingDir = value; break;
          case 'INSTALL_DIR': INSTALL_DIR = value; break;
          case 'STARTUP_TYPE': STARTUP_TYPE = value; break;
          case 'CONNECTION_RETRIES': RETRY_ATTEMPTS = parseInt(value); break;
          case 'CONNECTION_DELAY': RETRY_DELAY = parseInt(value); break;
          case 'CONNECTION_TIMEOUT': RETRY_TIMEOUT = parseInt(value); break;
        }
      }
    });
    useMQTT = process.argv.includes('--mqttServer') || MQTT_ENABLED;
    commsType = useMQTT ? "mqtt" : "websocket";
  } catch (err) {
    debug(DEBUG_LEVEL.TRACE, 'Error reading settings file');
    debug(DEBUG_LEVEL.TRACE, err);
  }
}

// Debug level enumeration
const DEBUG_LEVEL = Object.freeze({
  TRACE: 'trace',      // Verbose debug info, only shown when DEBUG_MODE enabled
  INFO: 'info',        // General information (default for most messages)
  WARN: 'warn',        // Warning messages
  ERROR: 'error',      // Error messages
  CRITICAL: 'critical' // Critical errors with visual alert
});

// Debug styles with ANSI codes
const DEBUG_STYLES = Object.freeze({
  trace: { code: '\x1b[37m\x1b[2m', label: '[TRACE]' },         // dim white
  info: { code: '\x1b[37m\x1b[1m', label: '[INFO]' },           // bright white
  warn: { code: '\x1b[33m', label: '[WARN]' },                  // yellow
  error: { code: '\x1b[31m', label: '[ERROR]' },                // red
  critical: { code: '\x1b[37m\x1b[41m', label: '[CRITICAL]' },  // white on red background
  reset: '\x1b[0m',
  dim: '\x1b[2m'                                                 // dim text for timestamps
});

function debug(level, ...args) {
  // Validate level is recognized
  if (!Object.values(DEBUG_LEVEL).includes(level)) {
    console.error(`Invalid debug level: ${level}. Valid levels: ${Object.values(DEBUG_LEVEL).join(', ')}`);
    return;
  }

  // Skip trace logs unless DEBUG_MODE is enabled
  if (level === DEBUG_LEVEL.TRACE && !DEBUG_MODE) {
    return;
  }

  // Get style for this level
  const style = DEBUG_STYLES[level];

  // Format timestamp as HH:mm:ss.sss
  const now = new Date();
  const timestamp = `${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}:${now.getSeconds().toString().padStart(2, '0')}.${now.getMilliseconds().toString().padStart(3, '0')}`;

  // Format message from all arguments (supports multiple args like console.log)
  const message = args.map(arg => {
    if (typeof arg === 'string') return arg;
    if (arg instanceof Error) return `${arg.name}: ${arg.message}`;
    return JSON.stringify(arg);
  }).join(' ');

  // Output: [HH:mm:ss.sss] [LEVEL] message
  console.log(`${style.code}${DEBUG_STYLES.dim}[${timestamp}]${DEBUG_STYLES.reset}${style.code} ${style.label} ${message}${DEBUG_STYLES.reset}`);
}

function validateJWTToken(inToken) {
  return new Promise((resolve, reject) => {
    jwt.verify(JSON.parse(inToken), enckey, { algorithm: 'HS256' }, (err, decoded) => {
      err ? reject(err) : resolve(decoded);
    });
  });
}

function checkExecutePermissionCapability(directoryPath, liveness) {
  try {
    fs.accessSync(directoryPath, fs.constants.W_OK);
    const tempFilePath = path.join(directoryPath, '.tmp_permission_test');
    fs.writeFileSync(tempFilePath, 'Temporary file content', 'utf8');
    fs.chmodSync(tempFilePath, '755');
    fs.unlinkSync(tempFilePath);
    debug(DEBUG_LEVEL.TRACE, 'Confirmed execute permissions capability.');
    return "ok";
  } catch (err) {
    debug(DEBUG_LEVEL.ERROR, `Permission capability check failed: ${err.message}`);
    debug(DEBUG_LEVEL.CRITICAL, 'No execute permissions capability.');
    return liveness ? "No execute permissions capability." : process.exit(1);
  }
}

function addExecutePermission(filePath) {
  fs.chmod(filePath, '755', (err) => {
    err ? debug(DEBUG_LEVEL.CRITICAL, `Error setting execute permission: ${err}`) : debug(DEBUG_LEVEL.TRACE, 'Execute permission added.');
  });
}

function generateRandomFileName() {
  return `${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;
}

function writeFileAndWait(filePath, dataToWrite) {
  try {
    fs.writeFileSync(filePath, dataToWrite, 'utf8');
    debug(DEBUG_LEVEL.TRACE, 'File written successfully!');
  } catch (err) {
    debug(DEBUG_LEVEL.ERROR, `Error writing file: ${err}`);
  }
}

function createEmptyFile(filePath) {
  try {
    // Must be synchronous so the file is fully initialized before the script starts
    // appending output. Async creation can race and truncate early log lines.
    fs.writeFileSync(filePath, '', 'utf8');
    debug(DEBUG_LEVEL.TRACE, `Empty file created: ${filePath}`);
  } catch (err) {
    debug(DEBUG_LEVEL.ERROR, `Error creating file: ${filePath} (${err.message})`);
  }
}

function deleteFile(filePathToDelete) {
  try {
    fs.unlinkSync(filePathToDelete);
    debug(DEBUG_LEVEL.TRACE, `File removed: ${filePathToDelete}`);
  } catch (err) {
    debug(DEBUG_LEVEL.ERROR, `Error deleting file: ${err}`);
  }
}

function publishStatusUpdate(status, description, data, jobName, callback, isManual, executionId = null, retryCount = 0, executionContext = null) {
  const message = {
    name: AGENT_ID,
    topic: TOPIC_STATUS,
    server: SERVER,
    manual: isManual,
    commsType: commsType,
    description: description,
    data: data,
    status: status,
    jobName: jobName,
    executionId: executionId,  // Include execution ID for tracking
    lastStatusReport: new Date().toISOString(),
  };

  if (executionContext && typeof executionContext === 'object') {
    message.executionMode = executionContext.executionMode || null;
    message.scriptName = executionContext.scriptName || null;
    message.scriptIdentity = executionContext.scriptIdentity || null;
    message.sourceType = executionContext.sourceType || null;
    message.scriptLabel = executionContext.scriptLabel || null;
  }

  pubCount++;
  const maxRetries = 6;
  
  if (useMQTT) {
    mqttClient.connect().then(() => {
      // Verify connection state before publishing
      if (!mqttClient.isConnected) {
        throw new Error('MQTT connection lost after initial connect');
      }
      mqttClient.publish(TOPIC_STATUS, JSON.stringify(message));
      debug(DEBUG_LEVEL.TRACE, `Published status update over MQTT: ${status}`);
    }).catch((err) => {
      debug(DEBUG_LEVEL.TRACE, `MQTT publish failed: ${err.message}`);
      // Retry with exponential backoff
      if (retryCount < maxRetries) {
        const retryDelay = Math.pow(2, retryCount) * 1000; // 1s, 2s, 4s, 8s, 16s, 32s
        debug(DEBUG_LEVEL.TRACE, `Retrying MQTT publish in ${retryDelay}ms (attempt ${retryCount + 1}/${maxRetries})`);
        setTimeout(() => publishStatusUpdate(status, description, data, jobName, callback, isManual, executionId, retryCount + 1, executionContext), retryDelay);
      } else {
        debug(DEBUG_LEVEL.WARN, `MQTT publish failed after ${maxRetries} retries`);
      }
    });
  } else {
    if (wsClient.isConnected) {
      // Verify connection state is still good
      if (wsClient.client && wsClient.client.readyState === 1) {
        wsClient.sendMessage(JSON.stringify(message));
        debug(DEBUG_LEVEL.TRACE, `Sent status update: ${status}`);
      } else {
        // Connection state mismatch, trigger reconnection and retry
        wsClient.isConnected = false;
        debug(DEBUG_LEVEL.WARN, `WebSocket connection state mismatch, reconnecting...`);
        publishStatusUpdate(status, description, data, jobName, callback, isManual, executionId, retryCount, executionContext);
      }
    } else {
      wsClient.connect().then(() => {
        wsClient.sendMessage(JSON.stringify(message));
        debug(DEBUG_LEVEL.TRACE, `Connected and sent status update: ${status}`);
      }).catch((err) => {
        debug(DEBUG_LEVEL.TRACE, `Failed to send status update: ${err.message}`);
        // Retry with exponential backoff
        if (retryCount < maxRetries) {
          const retryDelay = Math.pow(2, retryCount) * 1000; // 1s, 2s, 4s, 8s, 16s, 32s
          debug(DEBUG_LEVEL.TRACE, `Retrying WebSocket publish in ${retryDelay}ms (attempt ${retryCount + 1}/${maxRetries})`);
          setTimeout(() => publishStatusUpdate(status, description, data, jobName, callback, isManual, executionId, retryCount + 1, executionContext), retryDelay);
        } else {
          debug(DEBUG_LEVEL.WARN, `WebSocket publish failed after ${maxRetries} retries`);
        }
      });
    }
  }
}

function getCurrentDateTimeFormatted() {
  const now = new Date();
  return `${now.getFullYear().toString().padStart(4, '0')}-${(now.getMonth() + 1).toString().padStart(2, '0')}-${now.getDate().toString().padStart(2, '0')}_${now.getHours().toString().padStart(2, '0')}${now.getMinutes().toString().padStart(2, '0')}${now.getSeconds().toString().padStart(2, '0')}`;
}

function getActiveJobKey(executionId, jobName) {
  return executionId || jobName;
}

function getReturnCodeFromExit(code, signal) {
  if (signal === 'SIGKILL') {
    return 999;
  }
  if (signal === 'SIGTERM') {
    return 998;
  }
  if (code === null || code === undefined) {
    return 99999;
  }
  return code;
}

function removeIntervalReference(intervalId) {
  activeIntervals = activeIntervals.filter(id => id !== intervalId);
}

function signalTrackedJob(jobRecord, signal) {
  if (!jobRecord || !jobRecord.pid) {
    return false;
  }

  try {
    process.kill(-jobRecord.pid, signal);
    return true;
  } catch (groupErr) {
    debug(DEBUG_LEVEL.TRACE, `Process group signal failed for PID [${jobRecord.pid}]: ${groupErr.message}`);
  }

  try {
    process.kill(jobRecord.pid, signal);
    return true;
  } catch (processErr) {
    debug(DEBUG_LEVEL.WARN, `Failed to signal PID [${jobRecord.pid}] with [${signal}]: ${processErr.message}`);
    return false;
  }
}

function terminateExecution(executionId) {
  const jobRecord = activeJobs.get(executionId);
  if (!jobRecord) {
    debug(DEBUG_LEVEL.WARN, `No active execution found for termination [${executionId}]`);
    return false;
  }

  jobRecord.terminationRequested = true;
  return signalTrackedJob(jobRecord, 'SIGTERM');
}

function executeBackupCommand(command, commandParams, jobName, callback, manual, executionId = null, triggerContext = null, contextEnvVars = {}, executionContext = null) {
  jobCount++;
  
  // Validate and sanitize commandParams to prevent shell injection
  const sanitizedParams = String(commandParams || '').replace(/[;&|`$()\\\"'<>]/g, '\\$&');
  const activeJobKey = getActiveJobKey(executionId, jobName);
  
  // Track this job - key by executionId so concurrent same-job runs don't overwrite each other
  activeJobs.set(activeJobKey, {
    startTime: Date.now(),
    status: 'running',
    executionId: executionId,
    jobName: jobName,
    pid: null,
    childProcess: null,
    logStreamInterval: null,
    tempScriptPath: null,
    logfile: null,
    executionContext: executionContext || null,
    terminationRequested: false,
    returnCode: null,
  });
  
  status = AGENT_STATUS.RUNNING;
  publishStatusUpdate('running', `Job in progress [${jobName}]`, null, jobName, undefined, manual, executionId, 0, executionContext);

  const file = `${workingDir}/${generateRandomFileName()}.sh`;
  const logfile = `${workingDir}/${sanitizeUnixFilename(jobName)}_${getCurrentDateTimeFormatted()}.log`;
  createEmptyFile(logfile);
  writeFileAndWait(file, command);
  addExecutePermission(file);

  const jobRecord = activeJobs.get(activeJobKey);
  if (jobRecord) {
    jobRecord.tempScriptPath = file;
    jobRecord.logfile = logfile;
  }

  const runCommand = `. ${file} ${sanitizedParams} >> ${logfile}`;
  const start = new Date().getTime();
  debug(DEBUG_LEVEL.INFO, `Starting job: ${jobName}`);

  // Prepare environment for child process
  // Start with current process environment, then overlay trigger context variables
  const spawnEnv = { ...process.env, ...contextEnvVars };
  if (Object.keys(contextEnvVars).length > 0) {
    debug(DEBUG_LEVEL.INFO, `[TRIGGER CONTEXT] Injecting ${Object.keys(contextEnvVars).length} environment variables into script`);
  }

  const childProcess = spawn('bash', ['-c', runCommand], { 
    detached: true, 
    stdio: 'ignore',
    env: spawnEnv  // Pass merged environment with context variables
  });
  childProcess.unref();

  if (jobRecord) {
    jobRecord.childProcess = childProcess;
    jobRecord.pid = childProcess.pid;
  }
  
  // Add error handler for spawn failures
  childProcess.on('error', (err) => {
    debug(DEBUG_LEVEL.ERROR, `Failed to spawn process for job [${jobName}]: ${err.message}`);
    if (jobRecord && jobRecord.logStreamInterval) {
      clearInterval(jobRecord.logStreamInterval);
      removeIntervalReference(jobRecord.logStreamInterval);
    }
    activeJobs.delete(activeJobKey);
    publishStatusUpdate('error', `Job failed to start: ${err.message}`, null, jobName, undefined, manual, executionId, 0, executionContext);
  });

  let lastOffset = 0;
  let logStreamInterval;
  let childProcessExited = false;
  let localReturnCode = null;

  logStreamInterval = setInterval(() => {
    if (!fs.existsSync(logfile)) {
      debug(DEBUG_LEVEL.TRACE, `Log file [${logfile}] not accessible`);
    } else {
      try {
        const stats = fs.statSync(logfile);
        const currentSize = stats.size;

        if (lastOffset > currentSize) {
          debug(DEBUG_LEVEL.TRACE, `Size reset: ${currentSize} < ${lastOffset}`);
          lastOffset = currentSize;
        }

        if (lastOffset < currentSize) {
          const readStream = fs.createReadStream(logfile, { start: lastOffset, end: currentSize - 1 });
          readStream.on('data', (chunk) => {
            const logData = chunk.toString('utf8');
            lastOffset += chunk.length;
            publishLogData(logData, jobName, undefined, undefined, undefined, manual, executionId, executionContext);
          });
          readStream.on('error', (err) => debug(DEBUG_LEVEL.TRACE, `Read stream error: ${err.message}`));
        }

        if (childProcessExited && lastOffset >= currentSize) {
          clearInterval(logStreamInterval);
          removeIntervalReference(logStreamInterval);
          const stop = new Date().getTime();
          debug(DEBUG_LEVEL.INFO, `Job completed: ${jobName} [exit code: ${localReturnCode}]`);
          localReturnCode !== 0 ? failJobCount++ : successJobCount++;
          activeJobs.delete(activeJobKey);
          status = AGENT_STATUS.IDLE;
          deleteFile(file);
          callback(jobName, (stop - start) / 1000, manual, executionId, executionContext, localReturnCode);
        }
      } catch (err) {
        debug(DEBUG_LEVEL.TRACE, `Logfile read error: ${err.message}`);
      }
    }
  }, 1000);
  
  activeIntervals.push(logStreamInterval);
  if (jobRecord) {
    jobRecord.logStreamInterval = logStreamInterval;
  }

  childProcess.on('exit', (code, signal) => {
    childProcessExited = true;
    localReturnCode = getReturnCodeFromExit(code, signal);
    const currentJobRecord = activeJobs.get(activeJobKey);
    if (currentJobRecord) {
      currentJobRecord.returnCode = localReturnCode;
    }
  });
}

function publishLogData(logData, jobName, eta, returnCode, callback, manual, executionId = null, executionContext = null) {
  const message = {
    name: AGENT_ID,
    server: SERVER,
    status: eta !== undefined ? "eta_submission" : "log_submission",
    jobName: jobName,
    manual: manual,
    eta: eta,
    returnCode: returnCode,
    data: logData,
    executionId: executionId,  // Include execution ID for tracking
    lastStatusReport: new Date().toISOString(),
  };

  if (executionContext && typeof executionContext === 'object') {
    message.executionMode = executionContext.executionMode || null;
    message.scriptName = executionContext.scriptName || null;
    message.scriptIdentity = executionContext.scriptIdentity || null;
    message.sourceType = executionContext.sourceType || null;
    message.scriptLabel = executionContext.scriptLabel || null;
  }

  pubCount++;
  if (useMQTT) {
    mqttClient.connect().then(() => {
      mqttClient.publish(TOPIC_STATUS, JSON.stringify(message));
      if (callback) callback();
    }).catch((err) => debug(DEBUG_LEVEL.TRACE, `MQTT publish failed: ${err.message}`));
  } else {
    if (wsClient.isConnected && wsClient.client && wsClient.client.readyState === 1) {
      wsClient.sendMessage(JSON.stringify(message));
      debug(DEBUG_LEVEL.TRACE, `Sent log update: ${message.status}`);
    } else {
      wsClient.isConnected = false; // Force reconnection
      wsClient.connect().then(() => {
        wsClient.sendMessage(JSON.stringify(message));
        debug(DEBUG_LEVEL.TRACE, `Connected and sent log update: ${message.status}`);
      }).catch((err) => debug(DEBUG_LEVEL.TRACE, `Failed to send log update: ${err.message}`));
    }
  }
}

function backupComplete(jobName, eta, manual, executionId = null, executionContext = null, completionReturnCode = null) {
  status = AGENT_STATUS.IDLE;
  publishLogData("", jobName, eta, completionReturnCode, undefined, manual, executionId, executionContext);
  debug(DEBUG_LEVEL.INFO, `Job completed in ${eta} seconds`);
}

function sanitizeUnixFilename(filename) {
  return filename.replace(/[^a-zA-Z0-9_.-]/g, '_');
}

// Debug token management functions
function generateDebugToken() {
  const token = crypto.randomBytes(8).toString('hex');
  debugToken = token;
  debugTokenExpiry = Date.now() + DEBUG_TOKEN_EXPIRY_MS;
  debug(DEBUG_LEVEL.WARN, `Debug access requested. Token: ${token}`);
  return token;
}

function isValidDebugToken(token) {
  // Check if token exists and is not expired
  if (!debugToken || !debugTokenExpiry) {
    return false;
  }
  
  if (Date.now() > debugTokenExpiry) {
    debugToken = null;
    debugTokenExpiry = null;
    return false;
  }
  
  // Token matches and is still valid
  if (token === debugToken) {
    // Reset expiry timer on successful validation
    debugTokenExpiry = Date.now() + DEBUG_TOKEN_EXPIRY_MS;
    debug(DEBUG_LEVEL.TRACE, `Debug token validated. Expiry reset.`);
    return true;
  }
  
  return false;
}

async function handleCommand(message) {
  debug(DEBUG_LEVEL.TRACE, "Received Command from Orchelium Server");
  subCount++;
  validateJWTToken(message).then(async decodedPayload => {
    const {
      name: agentId,
      command,
      manual = false,
      commandParams = "",
      jobName,
      executionId = null,
      triggerContext = null,
      contextEnvVars = {},
      executionMode = null,
      scriptName = null,
      scriptIdentity = null,
      sourceType = null,
      scriptLabel = null,
    } = decodedPayload;
    debug(DEBUG_LEVEL.TRACE, `Job: [${jobName}] for agent: [${agentId}] with executionId: [${executionId}]`);
    debug(DEBUG_LEVEL.TRACE, `Command: ${command}, Manual: ${manual}, Params: ${commandParams}`);
    debug(DEBUG_LEVEL.TRACE, `Context Env Vars: ${JSON.stringify(contextEnvVars)}`);

    if (agentId !== AGENT_ID) return;

    const executionContext = executionMode
      ? {
          executionMode,
          scriptName,
          scriptIdentity,
          sourceType,
          scriptLabel,
        }
      : null;

    if (command === 'ping') {
      const statusJsonStr = await livenessProbe();
      publishStatusUpdate('pong', 'ping response', statusJsonStr);
      return;
    }

    if (command === 'queryMetric') {
      debug(DEBUG_LEVEL.INFO, `[queryMetric] Received request: ${commandParams}`);
      let metricConfig;
      try {
        metricConfig = JSON.parse(commandParams);
      } catch (parseErr) {
        debug(DEBUG_LEVEL.ERROR, `[queryMetric] Invalid JSON in commandParams: ${parseErr.message}`);
        publishStatusUpdate('metric_result', 'metric query error', JSON.stringify({ error: `Invalid config: ${parseErr.message}` }), jobName);
        return;
      }
      const result = await computeMetric(metricConfig);
      debug(DEBUG_LEVEL.INFO, `[queryMetric] Result: ${JSON.stringify(result)}`);
      publishStatusUpdate('metric_result', 'metric query result', JSON.stringify(result), jobName);
      return;
    }

    if (command === 'terminateExecution') {
      const targetExecutionId = commandParams || executionId;
      if (!targetExecutionId) {
        debug(DEBUG_LEVEL.WARN, 'terminateExecution received without a target execution ID');
        return;
      }

      const terminated = terminateExecution(targetExecutionId);
      debug(DEBUG_LEVEL.INFO, `Termination request for execution [${targetExecutionId}] result: ${terminated}`);
      return;
    }

    // Log trigger context details if present
    if (triggerContext || Object.keys(contextEnvVars).length > 0) {
      debug(DEBUG_LEVEL.INFO, `[TRIGGER CONTEXT] Job triggered with context. Type: [${triggerContext?.type || 'none'}], Env vars: [${Object.keys(contextEnvVars).length}]`);
      debug(DEBUG_LEVEL.trace, `[TRIGGER CONTEXT] Job triggered with context. Type: [${triggerContext?.type || 'none'}], Env vars: [${JSON.stringify(contextEnvVars)}], Context: [${JSON.stringify(triggerContext)}]`);
    }

    executeBackupCommand(command, commandParams, jobName, backupComplete, manual, executionId, triggerContext, contextEnvVars, executionContext);
    pushVerificationNotification = true;
  }).catch(error => {
    debug(DEBUG_LEVEL.ERROR, `Token validation failed: ${error.message}`);
    if (pushVerificationNotification) {
      publishStatusUpdate("notification", "Message failed signature verification.", message.toString());
      pushVerificationNotification = false;
    }
  });
}

function getCPULoadPercentage() {
  try {
    const loadAvgData = fs.readFileSync('/proc/loadavg', 'utf-8');
    const [oneMinLoad] = loadAvgData.split(' ').map(parseFloat);
    const cpuCount = os.cpus().length;
    return (oneMinLoad / cpuCount) * 100;
  } catch (error) {
    debug(DEBUG_LEVEL.ERROR, "Error reading load average:", error);
    return 0;
  }
}

function getFileSystemUsagePercentage() {
  try {
    const output = execSync(`df -h --output=pcent,target -x tmpfs -x devtmpfs`).toString();
    const lines = output.split('\n').slice(1).filter(line => line.trim() !== '');
    const result = lines.map(line => {
      const [usedPercentage, mountPoint] = line.trim().split(/\s+/);
      return { mount: mountPoint, usage: parseInt(usedPercentage.replace('%', ''), 10) };
    });
    return result;
  } catch (error) {
    debug(DEBUG_LEVEL.ERROR, `Error checking disk usage: ${error.message}`);
    return [];
  }
}

/**
 * Compute a metric value on the agent.
 * All metrics are gathered locally and returned as { type, value, unit, ... }.
 * Paths are validated to contain only safe characters before being passed to child processes.
 * @param {{ type: string, path?: string, pattern?: string }} config
 * @returns {Promise<Object>}
 */
async function computeMetric(config) {
  const { type, path: targetPath, pattern } = config;

  // Validate path to prevent command injection: allow standard filesystem chars only
  if (targetPath !== undefined && targetPath !== null) {
    if (!/^[a-zA-Z0-9_./ -]+$/.test(targetPath)) {
      return { type, error: `Invalid path: contains disallowed characters` };
    }
  }

  // Validate pattern (glob) similarly
  if (pattern !== undefined && pattern !== null) {
    if (!/^[a-zA-Z0-9_.*?[\]-]+$/.test(pattern)) {
      return { type, error: `Invalid pattern: contains disallowed characters` };
    }
  }

  try {
    const { execFileSync } = require('child_process');

    switch (type) {
      case 'cpu': {
        return { type, value: getCPULoadPercentage(), unit: 'percent' };
      }

      case 'mount_usage': {
        const all = getFileSystemUsagePercentage();
        const mount = targetPath ? all.find(m => m.mount === targetPath) : null;
        return { type, path: targetPath || null, value: mount ? mount.usage : null, unit: 'percent', allMounts: all };
      }

      case 'dir_size': {
        if (!targetPath) return { type, error: 'path is required for dir_size' };
        try {
          // Use shell redirection to suppress permission denied errors on protected dirs
          // du will still return total of accessible portions
          const { execSync } = require('child_process');
          const output = execSync(`du -sb "${targetPath}" 2>/dev/null || echo "0\\t${targetPath}"`).toString().trim();
          const bytes = parseInt(output.split('\t')[0], 10);
          return { type, path: targetPath, value: bytes, unit: 'bytes' };
        } catch (duErr) {
          return { type, path: targetPath, error: duErr.message };
        }
      }

      case 'file_size': {
        if (!targetPath) return { type, error: 'path is required for file_size' };
        const stat = require('fs').statSync(targetPath);
        return { type, path: targetPath, value: stat.size, unit: 'bytes' };
      }

      case 'file_count': {
        if (!targetPath) return { type, error: 'path is required for file_count' };
        const findArgs = pattern
          ? [targetPath, '-maxdepth', '1', '-name', pattern]
          : [targetPath, '-maxdepth', '1', '-type', 'f'];
        const findOutput = execFileSync('find', findArgs).toString().trim();
        const count = findOutput ? findOutput.split('\n').filter(l => l.length > 0).length : 0;
        return { type, path: targetPath, pattern: pattern || null, value: count, unit: 'count' };
      }

      case 'file_age': {
        if (!targetPath) return { type, error: 'path is required for file_age' };
        const mtimeStr = execFileSync('stat', ['-c', '%Y', targetPath]).toString().trim();
        const ageSeconds = Math.floor(Date.now() / 1000) - parseInt(mtimeStr, 10);
        return { type, path: targetPath, value: ageSeconds, unit: 'seconds' };
      }

      default:
        return { type, error: `Unknown metric type: ${type}. Supported: cpu, mount_usage, dir_size, file_size, file_count, file_age` };
    }
  } catch (err) {
    return { type, path: targetPath || null, error: err.message };
  }
}

// Cleanup function for graceful shutdown
function cleanupBackgroundTasks() {
  activeIntervals.forEach(intervalId => {
    try {
      clearInterval(intervalId);
    } catch (err) {
      debug(DEBUG_LEVEL.TRACE, `Error clearing interval: ${err.message}`);
    }
  });
  activeIntervals = [];
  
  activeJobs.forEach((job, jobName) => {
    debug(DEBUG_LEVEL.WARN, `Cleaning up active job on shutdown: ${jobName}`);
    signalTrackedJob(job, 'SIGTERM');
  });
  activeJobs.clear();
}

// Graceful shutdown on termination signals
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

// Retry Backoff Manager
// Implements exponential backoff strategy for connection retries
class RetryBackoffManager {
  constructor(agentId, handlerType) {
    this.agentId = agentId;
    this.handlerType = handlerType; // 'MQTT' or 'WebSocket'
    this.currentAttempt = 0;
    this.startTime = Date.now();
    this.lastBackoffChangeTime = this.startTime;
    this.currentBackoffStage = 0;
    
    // Define backoff stages: { durationMs, intervalMs, stageName }
    this.backoffStages = [
      { durationMs: 10 * 60 * 1000, intervalMs: 1 * 60 * 1000, stageName: '1-min backoff (10 mins)' },
      { durationMs: 10 * 60 * 1000, intervalMs: 5 * 60 * 1000, stageName: '5-min backoff (10 mins)' },
      { durationMs: 60 * 60 * 1000, intervalMs: 10 * 60 * 1000, stageName: '10-min backoff (1 hour)' },
      { durationMs: 60 * 60 * 1000, intervalMs: 20 * 60 * 1000, stageName: '20-min backoff (1 hour)' },
      { durationMs: 60 * 60 * 1000, intervalMs: 30 * 60 * 1000, stageName: '30-min backoff (1 hour)' },
      { durationMs: Infinity, intervalMs: 60 * 60 * 1000, stageName: '1-hour backoff (indefinite)' }
    ];
  }

  getNextRetryDelay() {
    const now = Date.now();
    const currentStage = this.backoffStages[this.currentBackoffStage];
    const timeInStage = now - this.lastBackoffChangeTime;

    // Check if we need to move to the next stage
    if (timeInStage > currentStage.durationMs && this.currentBackoffStage < this.backoffStages.length - 1) {
      this.currentBackoffStage++;
      this.lastBackoffChangeTime = now;
      const nextStage = this.backoffStages[this.currentBackoffStage];
      debug(DEBUG_LEVEL.INFO, `Backoff stage changed to: ${nextStage.stageName}`);
      return nextStage.intervalMs;
    }

    return currentStage.intervalMs;
  }

  recordAttempt() {
    this.currentAttempt++;
    const elapsed = Math.floor((Date.now() - this.startTime) / 1000);
    const currentStage = this.backoffStages[this.currentBackoffStage];
    debug(DEBUG_LEVEL.INFO, `[${this.handlerType}] ${this.agentId} - Retry attempt ${this.currentAttempt} after ${elapsed}s (${currentStage.stageName})`);
  }

  reset() {
    this.currentAttempt = 0;
    this.startTime = Date.now();
    this.lastBackoffChangeTime = this.startTime;
    this.currentBackoffStage = 0;
    debug(DEBUG_LEVEL.INFO, `[${this.handlerType}] ${this.agentId} - Connection successful, backoff reset`);
  }
}

// Communication Handlers

class MqttHandler {
  constructor(server, port, options = {}) {
    this.server = server;
    this.port = port;
    this.client = null;
    this.isConnected = false;
    this.isReconnecting = false;
    this.isConnecting = false;
    this.connectionTimeout = options.connectionTimeout || 5000;
    this.connectionTimeoutHandler = null;
    this.retryManager = new RetryBackoffManager(this.server, 'MQTT');
  }

  checkConnected() {
    return this.isConnected;
  }

  connect() {
    if (this.isConnected && this.client) {
      debug(DEBUG_LEVEL.TRACE, "MQTT connection exists and is connected");
      return Promise.resolve(this.client);
    }
    if (this.isConnecting) {
      return Promise.reject(new Error("MQTT connection attempt in progress"));
    }
    this.isConnecting = true;
    return new Promise((resolve, reject) => {
      const connectUrl = `mqtt://${this.server}:${this.port}`;
      this.client = mqtt.connect(connectUrl);
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
        this.retryConnection();
      });
    });
  }

  retryConnection() {
    if (this.isReconnecting) return;
    this.isReconnecting = true;

    // Get next retry delay from backoff manager
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

      const options = {
        WebSocket: websocket,
        connectionTimeout: this.connectionTimeout,
        maxRetries: 0, // Disable library's built-in retry, we'll handle it manually
        reconnectInterval: 1000
      };

      debug(DEBUG_LEVEL.TRACE, `Initiating WebSocket connection for [${connId}]`);
      debug(DEBUG_LEVEL.TRACE, `Initiating WebSocket connection for [${connId}]`);
      this.client = new ReconnectingWebSocket(connectUrl, [], options);

      this.client.addEventListener('open', () => {
        this.isConnected = true;
        this.isConnecting = false;
        this.retryManager.reset();
        this.isReconnecting = false;
        debug(DEBUG_LEVEL.INFO, `WebSocket server connected [${this.agentId}]`);
        resolve(this.client);
      });

      this.client.addEventListener('close', () => {
        this.isConnected = false;
        this.isConnecting = false;
        if (!this.isReconnecting) {
          this.triggerReconnect();
        }
      });

      this.client.addEventListener('error', (err) => {
        this.isConnected = false;
        this.isConnecting = false;
        debug(DEBUG_LEVEL.TRACE, `WebSocket error: ${err.message}`);
        if (!this.isReconnecting) {
          this.triggerReconnect();
        }
        reject(err);
      });

      this.client.addEventListener('message', (event) => {
        const message = `"${event.data}"`;
        debug(DEBUG_LEVEL.TRACE, `Received message on [${connId}]: ${message}`);
        this.processMessageCallback(message);
      });

      // Use a timeout to handle initial connection failures
      const connectionTimeoutId = setTimeout(() => {
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

  triggerReconnect() {
    if (this.isReconnecting) return;
    
    this.isReconnecting = true;
    const connId = `${encodeURIComponent(this.agentId)}`;

    // Clear any existing timeout (resource cleanup - recommendation 3)
    if (this.reconnectTimeoutId) {
      clearTimeout(this.reconnectTimeoutId);
      this.reconnectTimeoutId = null;
    }

    // Get next retry delay from backoff manager
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

  sendMessage(message) {
    if (this.isConnected && this.client && this.client.readyState === 1) { // readyState 1 = OPEN
      this.client.send(message);
      debug(DEBUG_LEVEL.TRACE, `Sent message on [${this.agentId}]`);
    } else {
      debug(DEBUG_LEVEL.WARN, `Cannot send on [${this.agentId}]: WebSocket not connected (state: ${this.client?.readyState})`);
      // Trigger reconnection if not already reconnecting
      if (!this.isConnecting && !this.isReconnecting) {
        this.triggerReconnect();
      }
    }
  }

  disconnect() {
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

// Main Execution

if (process.argv.includes('-?') || process.argv.includes('--help')) {
  console.log('Usage: node agent.js [options]\nOptions:');
  for (const [option, desc] of Object.entries(helpInfo)) console.log(`  ${option.padEnd(20)} ${desc}`);
  process.exit(0);
}

status = AGENT_STATUS.INITIALIZING;
console.log('\x1b[32m                                888                                                  888      ');
console.log('                                888                                                  888      ');
console.log('                                888                                                  888      ');
console.log('88888b.   .d88b.  88888b.d88b.  88888b.   .d88b.       .d8888b  .d88b.      888  888 888  888 ');
console.log('888 "88b d8P  Y8b 888 "888 "88b 888 "88b d88""88b     d88P"    d88""88b     888  888 888 .88P ');
console.log('888  888 88888888 888  888  888 888  888 888  888     888      888  888     888  888 888888K  ');
console.log('888 d88P Y8b.     888  888  888 888 d88P Y88..88P d8b Y88b.    Y88..88P d8b Y88b 888 888 "88b ');
console.log('88888P"   "Y8888  888  888  888 88888P"   "Y88P"  Y8P  "Y8888P  "Y88P"  Y8P  "Y88888 888  888 ');
console.log('888                                                                                           ');
console.log('888                                                                                           ');
console.log('888                                                                                           ');
console.log('\x1b[36m _________________');
console.log('|# \x1b[30m\x1b[47m:           :\x1b[0m\x1b[36m #|');
console.log('|  \x1b[30m\x1b[47m: Orchelium :\x1b[0m\x1b[36m  |');
console.log('|  \x1b[30m\x1b[47m:   Agent   :\x1b[0m\x1b[36m  |');
console.log('|  \x1b[30m\x1b[47m:           :\x1b[0m\x1b[36m  |       \x1b[32mVersion: ' + version + '\x1b[36m');
console.log('|  \x1b[30m\x1b[47m:___________:\x1b[0m\x1b[36m  |');
console.log('|    \x1b[90m _________\x1b[36m   |');
console.log('|    \x1b[90m| __      |\x1b[36m  |');
console.log('|    \x1b[90m||  |     |\x1b[36m  |');
console.log('\\____\x1b[90m||__|_____|\x1b[36m__|\x1b[0m');
console.log("");
console.log("\x1b[Orcheliuim Agent\x1b[33m");
console.log("\x1b[32m----------------------------------------------------------------------------------------------\x1b[0m\n");

parseSettingsFile("settings.sh");

debug(DEBUG_LEVEL.INFO, `Transport: ${useMQTT ? `MQTT → ${MQTT_SERVER}:${MQTT_SERVER_PORT}` : `WebSocket → ${WS_SERVER}:${WS_SERVER_PORT}`}`);

if (process.argv.includes('--debug')) {
  DEBUG_MODE = true;
  debug(DEBUG_LEVEL.WARN, "DEBUG ENABLED");
}

if (useMQTT) {
  mqttClient = new MqttHandler(MQTT_SERVER, MQTT_SERVER_PORT, {
    connectionTimeout: RETRY_TIMEOUT,
  });
  mqttClient.connect().then(() => {
    mqttClient.subscribe(TOPIC_COMMAND, handleCommand);
    publishStatusUpdate('register', 'Agent Online - Awaiting provision');
  }).catch((err) => debug(DEBUG_LEVEL.ERROR, `MQTT connection failed: ${err.message}`));
} else {
  wsClient = new WebSocketHandler(WS_SERVER, WS_SERVER_PORT, AGENT_ID, handleCommand, {
    connectionTimeout: RETRY_TIMEOUT,
  });
  debug(DEBUG_LEVEL.INFO, `Establishing WebSocket Connection`);
  wsClient.connect().then(() => {
    publishStatusUpdate('register', 'Agent Online - Awaiting provision');
  }).catch((err) => {
    debug(DEBUG_LEVEL.WARN, `WebSocket connection failed: ${err.message} - triggering reconnection`);
    wsClient.triggerReconnect();
  });
}

app.get('/', async (req, res) => {
  try {
    res.set('Content-Type', 'application/json');
    res.send(await livenessProbe());
  } catch (error) {
    res.status(500).json({ status: "error", message: "Internal Server Error" });
  }
});

app.get('/debug', async (req, res) => {
  const token = req.query.token || req.headers['x-debug-token'];
  
  if (!token) {
    // Generate a new token and log it
    generateDebugToken();
    return res.status(401).json({ 
      error: 'Token required',
      message: 'Debug access token required. Check logs for token.',
      current_state: DEBUG_MODE
    });
  }
  
  if (!isValidDebugToken(token)) {
    return res.status(401).json({ 
      error: 'Invalid or expired token',
      message: 'Token is invalid or has expired. Request new token.',
      current_state: DEBUG_MODE
    });
  }
  
  res.json({ DEBUG_MODE, message: 'Token valid for next 10 minutes' });
});

app.get('/debug/on', async (req, res) => {
  const token = req.query.token || req.headers['x-debug-token'];
  
  if (!token) {
    generateDebugToken();
    return res.status(401).json({ 
      error: 'Token required',
      message: 'Debug access token required. Check logs for token.'
    });
  }
  
  if (!isValidDebugToken(token)) {
    return res.status(401).json({ 
      error: 'Invalid or expired token',
      message: 'Token is invalid or has expired. Request new token.'
    });
  }
  
  DEBUG_MODE = true;
  debug(DEBUG_LEVEL.WARN, 'DEBUG_MODE enabled via API');
  res.json({ DEBUG_MODE: true, message: 'Debug mode enabled. Token valid for next 10 minutes' });
});

app.get('/debug/off', async (req, res) => {
  const token = req.query.token || req.headers['x-debug-token'];
  
  if (!token) {
    generateDebugToken();
    return res.status(401).json({ 
      error: 'Token required',
      message: 'Debug access token required. Check logs for token.'
    });
  }
  
  if (!isValidDebugToken(token)) {
    return res.status(401).json({ 
      error: 'Invalid or expired token',
      message: 'Token is invalid or has expired. Request new token.'
    });
  }
  
  DEBUG_MODE = false;
  debug(DEBUG_LEVEL.WARN, 'DEBUG_MODE disabled via API');
  res.json({ DEBUG_MODE: false, message: 'Debug mode disabled. Token valid for next 10 minutes' });
});

// Anonymous endpoint to handle server restart notifications
app.post('/api/server-restart', async (req, res) => {
  try {
    debug(DEBUG_LEVEL.INFO, 'Server restart notification received, resetting connection backoff');
    
    // Reset backoff for both MQTT and WebSocket handlers
    if (useMQTT && mqttClient && mqttClient.retryManager) {
      mqttClient.retryManager.reset();
      if (!mqttClient.isConnected) {
        debug(DEBUG_LEVEL.INFO, 'Triggering MQTT reconnection after server restart');
        mqttClient.connect().then(() => {
          publishStatusUpdate('register', 'Agent Online - Awaiting provision');
        }).catch((err) => {
          debug(DEBUG_LEVEL.WARN, `MQTT reconnection failed: ${err.message}`);
          mqttClient.retryConnection();
        });
      }
    } else if (!useMQTT && wsClient && wsClient.retryManager) {
      wsClient.retryManager.reset();
      if (!wsClient.isConnected) {
        debug(DEBUG_LEVEL.INFO, 'Triggering WebSocket reconnection after server restart');
        wsClient.connect().then(() => {
          publishStatusUpdate('register', 'Agent Online - Awaiting provision');
        }).catch((err) => {
          debug(DEBUG_LEVEL.WARN, `WebSocket reconnection failed: ${err.message}`);
          wsClient.triggerReconnect();
        });
      }
    }
    
    res.json({ 
      status: 'success', 
      message: 'Server restart notification received, connection backoff reset',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    debug(DEBUG_LEVEL.ERROR, `Error processing server restart notification: ${error.message}`);
    res.status(500).json({ 
      status: 'error', 
      message: 'Failed to process server restart notification',
      error: error.message
    });
  }
});

const server = app.listen(PORT, () => {
  debug(DEBUG_LEVEL.TRACE, `Express server listening on port ${PORT}`);
  status = AGENT_STATUS.IDLE;
});

// Display startup configuration with colors and horizontal separators
const colors = {
  brightYellow: '\x1b[93m',
  yellow: '\x1b[33m',
  reset: '\x1b[0m'
};

const separator = `${colors.brightYellow}${'═'.repeat(63)}${colors.reset}`;
const divider = `${colors.yellow}${'─'.repeat(63)}${colors.reset}`;

console.log(`\n${separator}`);
console.log(`${colors.brightYellow}  🤖 ORCHELIUM AGENT CONFIGURATION${colors.reset}`);
console.log(`${separator}\n`);

// Agent Information Section
console.log(`${colors.brightYellow}  📋 Agent Information${colors.reset}`);
console.log(`${divider}`);
console.log(`${colors.brightYellow}    🔹 Agent Name:${colors.reset}          ${colors.yellow}${AGENT_ID}${colors.reset}`);
console.log(`${colors.brightYellow}    🔹 Host Name:${colors.reset}           ${colors.yellow}${HOSTNAME}${colors.reset}`);


// Connection Settings Section
console.log(`\n${colors.brightYellow}  🔌 Connection Settings${colors.reset}`);
console.log(`${divider}`);
if (useMQTT) {
  console.log(`${colors.brightYellow}    🔹 Mode:${colors.reset}                ${colors.yellow}MQTT${colors.reset}`);
  console.log(`${colors.brightYellow}    🔹 Server:${colors.reset}              ${colors.yellow}${MQTT_SERVER}${colors.reset}`);
  console.log(`${colors.brightYellow}    🔹 Port:${colors.reset}                ${colors.yellow}${MQTT_SERVER_PORT}${colors.reset}`);
} else {
  console.log(`${colors.brightYellow}    🔹 Mode:${colors.reset}                ${colors.yellow}WebSocket${colors.reset}`);
  console.log(`${colors.brightYellow}    🔹 Server:${colors.reset}              ${colors.yellow}${WS_SERVER}${colors.reset}`);
  console.log(`${colors.brightYellow}    🔹 Port:${colors.reset}                ${colors.yellow}${WS_SERVER_PORT}${colors.reset}`);
}
console.log(`${colors.brightYellow}    🔹 Webserver Port:${colors.reset}      ${colors.yellow}${PORT}${colors.reset}`);
console.log(`${colors.brightYellow}    🔹 Connection Timeout:${colors.reset}  ${colors.yellow}${RETRY_TIMEOUT / 1000} secs${colors.reset}`);

// Environment Section
console.log(`\n${colors.brightYellow}  🌍 Environment${colors.reset}`);
console.log(`${divider}`);
console.log(`${colors.brightYellow}    🔹 Working Dir:${colors.reset}        ${colors.yellow}${workingDir}${colors.reset}`);
console.log(`${colors.brightYellow}    🔹 Install Dir:${colors.reset}        ${colors.yellow}${INSTALL_DIR}${colors.reset}`);

console.log(`\n${separator}\n`);

checkExecutePermissionCapability(workingDir);

process.on('beforeExit', (code) => {
  publishStatusUpdate('offline', 'Script exited', null, () => {
    cleanupBackgroundTasks();
    process.exit(code);
  });
});

process.on('SIGINT', () => {
  cleanupBackgroundTasks();
  useMQTT ? mqttClient.disconnect().then(() => process.exit(0)) : process.exit(0);
});

function padStringTo256Bits(inputString) {
  const blockSize = 32;
  const inputLength = Buffer.from(inputString, 'utf8').length;
  const paddingLength = blockSize - (inputLength % blockSize);
  return Buffer.concat([Buffer.from(inputString, 'utf8'), Buffer.alloc(paddingLength, paddingLength)]).toString('utf8');
}

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
    if (useMQTT) {
      mqttClient.connect().then(() => {
        mqttClient.publish('test/liveness', 'test');
        resolve("MQTT test message published successfully");
      }).catch(() => resolve("ERROR: MQTT Connection issues"));
    } else {
      resolve(wsClient.checkConnected() ? "WebSocket connection is active" : "ERROR: WebSocket connection issues");
    }
  });
}

async function livenessProbe() {
  const jsonObj = {};
  const mqttLivenessResult = await checkMqttLiveness();
  const filePermLivenessResult = checkExecutePermissionCapability(workingDir, true);
  const overallStatus = (mqttLivenessResult.startsWith("ERROR:") || filePermLivenessResult !== "ok") ? "unhealthy" : "ok";

  Object.assign(jsonObj, {
    status: overallStatus,
    identifier: AGENT_ID,
    uptime: getUptime(),
    version,
    installDir: INSTALL_DIR,
    workingDir,
    agentStatus: status,
    mqttServer: MQTT_SERVER,
    mqttPort: MQTT_SERVER_PORT,
    wsServer: WS_SERVER,
    wsServerPort: WS_SERVER_PORT,
    connectionMode: commsType,
    startupType: STARTUP_TYPE,
    jobCount,
    failJobCount,
    successJobCount,
    pubCount,
    subCount,
    commsStatus: mqttLivenessResult,
    filePermissionStatus: filePermLivenessResult,
    fileSystemUsagePct: getFileSystemUsagePercentage(),
    cpuPct: getCPULoadPercentage()
  });

  return JSON.stringify(jsonObj);
}