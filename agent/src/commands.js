const { COMMANDS, executionContextFromPayload } = require('../../shared/protocol');
const runtime = require('./runtime');
const { debug, DEBUG_LEVEL } = require('./log');
const { validateJWTToken } = require('./auth');
const { publishStatusUpdate } = require('./publish');
const { executeBackupCommand, backupComplete, terminateExecution } = require('./executor');
const { computeMetric } = require('./metrics');
const { livenessProbe } = require('./liveness');

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
    debug(DEBUG_LEVEL.TRACE, `Command: ${command}, Manual: ${manual}, Params: ${commandParams}`);
    debug(DEBUG_LEVEL.TRACE, `Context Env Vars: ${JSON.stringify(contextEnvVars)}`);

    if (agentId !== runtime.AGENT_ID) return;

    const executionContext = executionContextFromPayload(decodedPayload);

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
