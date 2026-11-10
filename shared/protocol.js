/**
 * Wire contract shared by the hub and the agent.
 * Topic strings are both the MQTT broker topics and the topic field inside messages.
 * This module does not sign tokens, open sockets, or touch the shell.
 */

const TOPICS = Object.freeze({
  command: 'orchelium/agent/command',
  status: 'orchelium/agent/status',
});

const COMMANDS = Object.freeze({
  PING: 'ping',
  QUERY_METRIC: 'queryMetric',
  TERMINATE_EXECUTION: 'terminateExecution',
});

const STATUS = Object.freeze({
  REGISTER: 'register',
  RUNNING: 'running',
  PONG: 'pong',
  OFFLINE: 'offline',
  NOTIFICATION: 'notification',
  METRIC_RESULT: 'metric_result',
  LOG_SUBMISSION: 'log_submission',
  ETA_SUBMISSION: 'eta_submission',
  ERROR: 'error',
});

const EXIT_CODES = Object.freeze({
  SIGKILL: 999,
  SIGTERM: 998,
  UNKNOWN: 99999,
});

const EXECUTION_CONTEXT_FIELDS = Object.freeze([
  'executionMode',
  'scriptName',
  'scriptIdentity',
  'sourceType',
  'scriptLabel',
]);

function applyExecutionContext(message, executionContext) {
  if (executionContext && typeof executionContext === 'object') {
    message.executionMode = executionContext.executionMode || null;
    message.scriptName = executionContext.scriptName || null;
    message.scriptIdentity = executionContext.scriptIdentity || null;
    message.sourceType = executionContext.sourceType || null;
    message.scriptLabel = executionContext.scriptLabel || null;
  }
  return message;
}

function executionContextFromPayload(payload) {
  if (!payload || !payload.executionMode) {
    return null;
  }
  return {
    executionMode: payload.executionMode,
    scriptName: payload.scriptName,
    scriptIdentity: payload.scriptIdentity,
    sourceType: payload.sourceType,
    scriptLabel: payload.scriptLabel,
  };
}

function buildStatusMessage({
  name,
  server,
  manual,
  commsType,
  description,
  data,
  status,
  jobName,
  executionId = null,
  executionContext = null,
  reportedAt = new Date(),
}) {
  const message = {
    name,
    topic: TOPICS.status,
    server,
    manual,
    commsType,
    description,
    data,
    status,
    jobName,
    executionId,
    lastStatusReport: reportedAt.toISOString(),
  };
  return applyExecutionContext(message, executionContext);
}

function buildLogMessage({
  name,
  server,
  jobName,
  manual,
  eta,
  returnCode,
  data,
  executionId = null,
  executionContext = null,
  reportedAt = new Date(),
}) {
  const message = {
    name,
    server,
    status: eta !== undefined ? STATUS.ETA_SUBMISSION : STATUS.LOG_SUBMISSION,
    jobName,
    manual,
    eta,
    returnCode,
    data,
    executionId,
    lastStatusReport: reportedAt.toISOString(),
  };
  return applyExecutionContext(message, executionContext);
}

function returnCodeFromExit(code, signal) {
  if (signal === 'SIGKILL') {
    return EXIT_CODES.SIGKILL;
  }
  if (signal === 'SIGTERM') {
    return EXIT_CODES.SIGTERM;
  }
  if (code === null || code === undefined) {
    return EXIT_CODES.UNKNOWN;
  }
  return code;
}

module.exports = {
  TOPICS,
  COMMANDS,
  STATUS,
  EXIT_CODES,
  EXECUTION_CONTEXT_FIELDS,
  applyExecutionContext,
  executionContextFromPayload,
  buildStatusMessage,
  buildLogMessage,
  returnCodeFromExit,
};
