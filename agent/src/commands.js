const jwt = require('jsonwebtoken');
const { COMMANDS, executionContextFromPayload } = require('../../shared/protocol');
const runtime = require('./runtime');
const { debug, DEBUG_LEVEL } = require('./log');
const { validateJWTToken } = require('./auth');
const { publishStatusUpdate } = require('./publish');
const { executeBackupCommand, backupComplete, terminateExecution } = require('./executor');
const { computeMetric } = require('./metrics');
const { livenessProbe } = require('./liveness');
const { writeSetting } = require('./settings');
const { openSecret } = require('../../shared/secretBox');

function storeEnrollment(commandParams) {
  writeSetting('AGENT_TOKEN', commandParams);
  debug(DEBUG_LEVEL.INFO, 'Stored connection token');
  if (runtime.wsClient && runtime.wsClient.reconnectNow) {
    runtime.wsClient.reconnectNow().catch((err) => {
      debug(DEBUG_LEVEL.WARN, `Reconnect after enrollment failed: ${err.message}`);
    });
  }
}

/**
 * A newly approved agent receives its token on the WebSocket it opened.
 * That message is signed with the hub key, which a new hub does not share.
 * Accept this one enrollment before a token exists. Later commands still
 * require a valid signature, and MQTT does not use this path.
 */
function acceptFirstEnrollment(rawMessage) {
  if (runtime.AGENT_TOKEN) return false;
  const encoded = Buffer.isBuffer(rawMessage) ? rawMessage.toString('utf8') : rawMessage;
  if (typeof encoded !== 'string' || !encoded) return false;
  let payload;
  try {
    payload = jwt.decode(encoded);
  } catch (err) {
    return false;
  }
  if (!payload || payload.command !== COMMANDS.ENROLL) return false;
  if (payload.name !== runtime.AGENT_ID) return false;
  if (typeof payload.commandParams !== 'string' || !payload.commandParams) return false;
  storeEnrollment(payload.commandParams);
  return true;
}

async function handleCommand(message) {
  debug(DEBUG_LEVEL.TRACE, 'Received Command from Orchelium Server');
  runtime.subCount++;
  validateJWTToken(message).then(async decodedPayload => {
    const {
      name: agentId,
      command,
      manual = false,
      commandParams = '',
      jobName,
      executionId = null,
      triggerContext = null,
      contextEnvVars = {},
    } = decodedPayload;
    debug(DEBUG_LEVEL.TRACE, `Job: [${jobName}] for agent: [${agentId}] with executionId: [${executionId}]`);
    debug(DEBUG_LEVEL.TRACE, `Command: ${command}, Manual: ${manual}`);
    debug(DEBUG_LEVEL.TRACE, `Context Env Vars: ${JSON.stringify(contextEnvVars)}`);

    if (agentId !== runtime.AGENT_ID) return;

    const executionContext = executionContextFromPayload(decodedPayload);

    if (command === COMMANDS.ENROLL) {
      if (!commandParams) {
        debug(DEBUG_LEVEL.ERROR, 'Enrollment command did not include a token');
        return;
      }
      storeEnrollment(commandParams);
      return;
    }

    if (command === COMMANDS.ROTATE_TOKEN) {
      if (!runtime.AGENT_TOKEN) {
        debug(DEBUG_LEVEL.ERROR, 'Cannot rotate a connection token before one is stored');
        return;
      }
      let nextToken;
      try {
        nextToken = openSecret(commandParams, runtime.AGENT_TOKEN);
      } catch (err) {
        debug(DEBUG_LEVEL.ERROR, `Could not open rotated connection token: ${err.message}`);
        return;
      }
      runtime.previousAgentToken = runtime.AGENT_TOKEN;
      writeSetting('AGENT_TOKEN', nextToken);
      debug(DEBUG_LEVEL.INFO, 'Stored rotated connection token');
      publishStatusUpdate('token_ack', 'Connection token updated');
      return;
    }

    if (command === COMMANDS.PING) {
      const statusJsonStr = await livenessProbe();
      publishStatusUpdate('pong', 'ping response', statusJsonStr);
      return;
    }

    if (command === COMMANDS.QUERY_METRIC) {
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

    if (command === COMMANDS.TERMINATE_EXECUTION) {
      const targetExecutionId = commandParams || executionId;
      if (!targetExecutionId) {
        debug(DEBUG_LEVEL.WARN, 'terminateExecution received without a target execution ID');
        return;
      }

      const terminated = terminateExecution(targetExecutionId);
      debug(DEBUG_LEVEL.INFO, `Termination request for execution [${targetExecutionId}] result: ${terminated}`);
      return;
    }

    if (triggerContext || Object.keys(contextEnvVars).length > 0) {
      debug(DEBUG_LEVEL.INFO, `[TRIGGER CONTEXT] Job triggered with context. Type: [${triggerContext?.type || 'none'}], Env vars: [${Object.keys(contextEnvVars).length}]`);
      debug(DEBUG_LEVEL.trace, `[TRIGGER CONTEXT] Job triggered with context. Type: [${triggerContext?.type || 'none'}], Env vars: [${JSON.stringify(contextEnvVars)}], Context: [${JSON.stringify(triggerContext)}]`);
    }

    executeBackupCommand(command, commandParams, jobName, backupComplete, manual, executionId, triggerContext, contextEnvVars, executionContext);
    runtime.pushVerificationNotification = true;
  }).catch(error => {
    debug(DEBUG_LEVEL.ERROR, `Token validation failed: ${error.message}`);
    if (runtime.pushVerificationNotification) {
      publishStatusUpdate('notification', 'Message failed signature verification.', message.toString());
      runtime.pushVerificationNotification = false;
    }
  });
}

module.exports = { handleCommand, acceptFirstEnrollment };
