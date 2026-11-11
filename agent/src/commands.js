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
      writeSetting('AGENT_TOKEN', commandParams);
      debug(DEBUG_LEVEL.INFO, 'Stored connection token');
      if (runtime.wsClient && runtime.wsClient.reconnectNow) {
        runtime.wsClient.reconnectNow().catch((err) => {
          debug(DEBUG_LEVEL.WARN, `Reconnect after enrollment failed: ${err.message}`);
        });
      }
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

module.exports = { handleCommand };
