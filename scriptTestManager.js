const crypto = require('crypto');

const RETENTION_MS = 30 * 60 * 1000;
// Enforce max log size to prevent unbounded memory growth
const MAX_LOG_SIZE_KB = 5000; // 5 MB per execution
const MAX_LOG_LINES = 10000; // Keep last 10000 lines max
const MAX_LOG_BYTES = MAX_LOG_SIZE_KB * 1024;

const testsByExecutionId = new Map();
const activeByScriptIdentity = new Map();
const retainedByScriptIdentity = new Map();

function getNowIso() {
  return new Date().toISOString();
}

function cloneExecution(record) {
  if (!record) {
    return null;
  }

  return {
    executionId: record.executionId,
    status: record.status,
    agentName: record.agentName,
    scriptName: record.scriptName,
    scriptDescription: record.scriptDescription,
    scriptParameters: record.scriptParameters || '',
    scriptLabel: record.scriptLabel,
    scriptIdentity: record.scriptIdentity,
    sourceType: record.sourceType,
    commandParams: record.commandParams,
    requestedBy: record.requestedBy,
    jobName: record.jobName,
    createdAt: record.createdAt,
    startedAt: record.startedAt,
    completedAt: record.completedAt,
    updatedAt: record.updatedAt,
    retainedUntil: record.retainedUntil,
    acknowledgedAt: record.acknowledgedAt,
    terminationRequestedAt: record.terminationRequestedAt,
    returnCode: record.returnCode,
    log: record.log,
    isActive: activeByScriptIdentity.get(record.scriptIdentity) === record.executionId,
    isRetained: retainedByScriptIdentity.get(record.scriptIdentity) === record.executionId,
    isAcknowledged: !!record.acknowledgedAt,
  };
}

function getSocketIo() {
  try {
    const wsBrowserTransport = require('./communications/wsBrowserTransport.js');
    return wsBrowserTransport.getIO();
  } catch (error) {
    logger.debug(`[scriptTestManager] Socket.io unavailable: ${error.message}`);
    return null;
  }
}

function getWsBrowserTransport() {
  try {
    return require('./communications/wsBrowserTransport.js');
  } catch (error) {
    logger.debug(`[scriptTestManager] wsBrowserTransport unavailable: ${error.message}`);
    return null;
  }
}

function emitEvent(eventType, executionId, payload) {
  // Use room-scoped emission for script test events to prevent cross-user data leakage
  const transport = getWsBrowserTransport();
  if (!transport) {
    return;
  }

  transport.emitScriptTestEvent(eventType, executionId, payload);
}

function removeExecutionFromIndexes(record) {
  if (!record) {
    return;
  }

  if (activeByScriptIdentity.get(record.scriptIdentity) === record.executionId) {
    activeByScriptIdentity.delete(record.scriptIdentity);
  }

  if (retainedByScriptIdentity.get(record.scriptIdentity) === record.executionId) {
    retainedByScriptIdentity.delete(record.scriptIdentity);
  }
}

function purgeExecution(executionId) {
  const record = testsByExecutionId.get(executionId);
  if (!record) {
    return false;
  }

  removeExecutionFromIndexes(record);
  testsByExecutionId.delete(executionId);
  return true;
}

function cleanupExpiredTests() {
  const now = Date.now();

  for (const [executionId, record] of testsByExecutionId.entries()) {
    if (!record.retainedUntil) {
      continue;
    }

    if (record.status === 'pending' || record.status === 'running' || record.status === 'terminating') {
      continue;
    }

    if (new Date(record.retainedUntil).getTime() <= now) {
      purgeExecution(executionId);
    }
  }
}

function hashScriptSource(scriptSource) {
  return crypto.createHash('sha256').update(scriptSource || '').digest('hex').slice(0, 16);
}

function buildScriptIdentity({ scriptName, scriptSource, sourceType }) {
  if (scriptName) {
    return `script:${scriptName}`;
  }

  return `${sourceType || 'editor'}:${hashScriptSource(scriptSource)}`;
}

function buildScriptLabel({ scriptName, sourceType }) {
  if (scriptName) {
    return scriptName;
  }

  return sourceType === 'saved' ? 'Saved Script' : 'Unsaved Editor Script';
}

function buildJobName({ scriptLabel, executionId }) {
  return `Script Test [${scriptLabel}] [${executionId}]`;
}

function getExecution(executionId) {
  cleanupExpiredTests();
  return cloneExecution(testsByExecutionId.get(executionId));
}

function getBlockingState(scriptIdentity) {
  cleanupExpiredTests();

  const activeExecutionId = activeByScriptIdentity.get(scriptIdentity);
  if (activeExecutionId) {
    const activeExecution = cloneExecution(testsByExecutionId.get(activeExecutionId));
    if (activeExecution) {
      return {
        type: 'active',
        execution: activeExecution,
      };
    }
    activeByScriptIdentity.delete(scriptIdentity);
  }

  const retainedExecutionId = retainedByScriptIdentity.get(scriptIdentity);
  if (retainedExecutionId) {
    const retainedExecution = testsByExecutionId.get(retainedExecutionId);
    if (!retainedExecution) {
      retainedByScriptIdentity.delete(scriptIdentity);
      return null;
    }

    if (!retainedExecution.acknowledgedAt) {
      return {
        type: 'retained',
        execution: cloneExecution(retainedExecution),
      };
    }
  }

  return null;
}

function createTest({ executionId, agentName, scriptName, scriptDescription, scriptParameters, scriptSource, sourceType, commandParams, requestedBy }) {
  cleanupExpiredTests();

  const scriptIdentity = buildScriptIdentity({ scriptName, scriptSource, sourceType });
  const blockingState = getBlockingState(scriptIdentity);
  if (blockingState) {
    return {
      ok: false,
      type: blockingState.type,
      execution: blockingState.execution,
      scriptIdentity,
    };
  }

  const previousRetainedExecutionId = retainedByScriptIdentity.get(scriptIdentity);
  if (previousRetainedExecutionId) {
    purgeExecution(previousRetainedExecutionId);
  }

  const scriptLabel = buildScriptLabel({ scriptName, sourceType });
  const record = {
    executionId,
    status: 'pending',
    agentName,
    scriptName: scriptName || null,
    scriptDescription: scriptDescription || '',
    scriptParameters: scriptParameters || '',
    scriptLabel,
    scriptIdentity,
    sourceType: sourceType || 'editor',
    commandParams: commandParams || '',
    requestedBy: requestedBy || 'unknown',
    jobName: buildJobName({ scriptLabel, executionId }),
    createdAt: getNowIso(),
    startedAt: null,
    completedAt: null,
    updatedAt: getNowIso(),
    retainedUntil: null,
    acknowledgedAt: null,
    terminationRequestedAt: null,
    returnCode: null,
    log: '',
  };

  testsByExecutionId.set(executionId, record);
  activeByScriptIdentity.set(scriptIdentity, executionId);

  emitEvent('scriptTestStatus', executionId, cloneExecution(record));

  return {
    ok: true,
    execution: cloneExecution(record),
  };
}

function markRunning(executionId) {
  cleanupExpiredTests();

  const record = testsByExecutionId.get(executionId);
  if (!record) {
    return null;
  }

  record.status = 'running';
  record.startedAt = record.startedAt || getNowIso();
  record.updatedAt = getNowIso();

  const serialized = cloneExecution(record);
  emitEvent('scriptTestStatus', executionId, serialized);
  return serialized;
}

function appendLog(executionId, logData) {
  cleanupExpiredTests();

  const record = testsByExecutionId.get(executionId);
  if (!record) {
    return null;
  }

  record.log += logData || '';
  record.updatedAt = getNowIso();

  // Trim log if it exceeds size limits to prevent unbounded memory growth
  trimLogIfNeeded(record);

  const serialized = cloneExecution(record);
  emitEvent('scriptTestLog', executionId, {
    executionId,
    // Only emit the new chunk to reduce network overhead
    // Client can accumulate or fetch full log via API if needed
    chunk: logData || '',
    status: serialized.status,
    isTruncated: !!record.isTruncated,
  });

  return serialized;
}

function trimLogIfNeeded(record) {
  if (!record) {
    return;
  }

  let isTruncated = false;

  // Check size limit
  if (record.log.length > MAX_LOG_BYTES) {
    // Keep only the last MAX_LOG_BYTES of data
    const excess = record.log.length - MAX_LOG_BYTES;
    record.log = record.log.slice(excess);
    isTruncated = true;
  }

  // Check line limit
  const lines = record.log.split('\n');
  if (lines.length > MAX_LOG_LINES) {
    // Keep only the last MAX_LOG_LINES
    const excess = lines.length - MAX_LOG_LINES;
    const droppedLines = lines.slice(0, excess).length;
    record.log = lines.slice(excess).join('\n');
    isTruncated = true;
  }

  record.isTruncated = isTruncated;
}

function requestTermination(executionId, requestedBy) {
  cleanupExpiredTests();

  const record = testsByExecutionId.get(executionId);
  if (!record) {
    return null;
  }

  record.terminationRequestedAt = getNowIso();
  record.updatedAt = getNowIso();
  if (record.status === 'pending' || record.status === 'running') {
    record.status = 'terminating';
  }
  if (requestedBy) {
    record.terminationRequestedBy = requestedBy;
  }

  const serialized = cloneExecution(record);
  emitEvent('scriptTestStatus', executionId, serialized);
  return serialized;
}

function completeTest(executionId, returnCode) {
  cleanupExpiredTests();

  const record = testsByExecutionId.get(executionId);
  if (!record) {
    return null;
  }

  const parsedReturnCode = returnCode === null || returnCode === undefined
    ? null
    : parseInt(returnCode, 10);

  record.returnCode = Number.isNaN(parsedReturnCode) ? null : parsedReturnCode;
  record.completedAt = getNowIso();
  record.updatedAt = getNowIso();
  record.retainedUntil = new Date(Date.now() + RETENTION_MS).toISOString();

  if (record.returnCode === 998 || record.returnCode === 999) {
    record.status = 'terminated';
  } else if (record.returnCode === 0) {
    record.status = 'completed';
  } else {
    record.status = 'failed';
  }

  if (activeByScriptIdentity.get(record.scriptIdentity) === executionId) {
    activeByScriptIdentity.delete(record.scriptIdentity);
  }
  retainedByScriptIdentity.set(record.scriptIdentity, executionId);

  const serialized = cloneExecution(record);
  emitEvent('scriptTestStatus', executionId, serialized);
  emitEvent('scriptTestEnded', executionId, serialized);
  return serialized;
}

function acknowledgeExecution(executionId) {
  cleanupExpiredTests();

  const record = testsByExecutionId.get(executionId);
  if (!record) {
    return null;
  }

  record.acknowledgedAt = getNowIso();
  record.updatedAt = getNowIso();

  const serialized = cloneExecution(record);
  emitEvent('scriptTestStatus', executionId, serialized);
  return serialized;
}

function discardExecution(executionId) {
  cleanupExpiredTests();

  const record = testsByExecutionId.get(executionId);
  if (!record) {
    return false;
  }

  if (activeByScriptIdentity.get(record.scriptIdentity) === executionId) {
    return false;
  }

  const serialized = cloneExecution(record);
  const removed = purgeExecution(executionId);
  if (removed) {
    emitEvent('scriptTestStatus', executionId, {
      executionId,
      status: 'discarded',
      scriptIdentity: serialized.scriptIdentity,
    });
  }

  return removed;
}

module.exports = {
  RETENTION_MS,
  appendLog,
  acknowledgeExecution,
  buildScriptIdentity,
  cleanupExpiredTests,
  completeTest,
  createTest,
  discardExecution,
  getBlockingState,
  getExecution,
  markRunning,
  requestTermination,
};