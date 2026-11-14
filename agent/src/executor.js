const fs = require('fs');
const { spawn } = require('child_process');
const { returnCodeFromExit } = require('../../shared/protocol');
const runtime = require('./runtime');
const { AGENT_STATUS } = require('./constants');
const { debug, DEBUG_LEVEL } = require('./log');
const { publishStatusUpdate, publishLogData } = require('./publish');
const {
  generateRandomFileName,
  getCurrentDateTimeFormatted,
  sanitizeUnixFilename,
  createEmptyFile,
  writeFileAndWait,
  addExecutePermission,
  deleteFile,
} = require('./files');

function getActiveJobKey(executionId, jobName) {
  return executionId || jobName;
}

function removeIntervalReference(intervalId) {
  runtime.activeIntervals = runtime.activeIntervals.filter(id => id !== intervalId);
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
  const jobRecord = runtime.activeJobs.get(executionId);
  if (!jobRecord) {
    debug(DEBUG_LEVEL.WARN, `No active execution found for termination [${executionId}]`);
    return false;
  }

  jobRecord.terminationRequested = true;
  return signalTrackedJob(jobRecord, 'SIGTERM');
}

function backupComplete(jobName, eta, manual, executionId = null, executionContext = null, completionReturnCode = null) {
  runtime.status = AGENT_STATUS.IDLE;
  publishLogData('', jobName, eta, completionReturnCode, undefined, manual, executionId, executionContext);
  debug(DEBUG_LEVEL.INFO, `Job completed in ${eta} seconds`);
}

function sanitizeCommandParams(commandParams) {
  return String(commandParams || '')
    .replace(/[\r\n]+/g, ' ')
    .replace(/[;&|`$()\\\"'<>]/g, '\\$&');
}

function executeBackupCommand(command, commandParams, jobName, callback, manual, executionId = null, triggerContext = null, contextEnvVars = {}, executionContext = null) {
  runtime.jobCount++;

  const sanitizedParams = sanitizeCommandParams(commandParams);
  const activeJobKey = getActiveJobKey(executionId, jobName);

  runtime.activeJobs.set(activeJobKey, {
    startTime: Date.now(),
    status: 'running',
    executionId,
    jobName,
    pid: null,
    childProcess: null,
    logStreamInterval: null,
    tempScriptPath: null,
    logfile: null,
    executionContext: executionContext || null,
    terminationRequested: false,
    returnCode: null,
  });

  runtime.status = AGENT_STATUS.RUNNING;
  publishStatusUpdate('running', `Job in progress [${jobName}]`, null, jobName, undefined, manual, executionId, 0, executionContext);

  const file = `${runtime.workingDir}/${generateRandomFileName()}.sh`;
  const logfile = `${runtime.workingDir}/${sanitizeUnixFilename(jobName)}_${getCurrentDateTimeFormatted()}.log`;
  createEmptyFile(logfile);
  writeFileAndWait(file, command);
  addExecutePermission(file);

  const jobRecord = runtime.activeJobs.get(activeJobKey);
  if (jobRecord) {
    jobRecord.tempScriptPath = file;
    jobRecord.logfile = logfile;
  }

  const runCommand = `. ${file} ${sanitizedParams} >> ${logfile}`;
  const start = new Date().getTime();
  debug(DEBUG_LEVEL.INFO, `Starting job: ${jobName}`);

  const spawnEnv = { ...process.env, ...contextEnvVars };
  if (Object.keys(contextEnvVars).length > 0) {
    debug(DEBUG_LEVEL.INFO, `[TRIGGER CONTEXT] Injecting ${Object.keys(contextEnvVars).length} environment variables into script`);
  }

  const childProcess = spawn('bash', ['-c', runCommand], {
    detached: true,
    stdio: 'ignore',
    env: spawnEnv,
  });
  childProcess.unref();

  if (jobRecord) {
    jobRecord.childProcess = childProcess;
    jobRecord.pid = childProcess.pid;
  }

  childProcess.on('error', (err) => {
    debug(DEBUG_LEVEL.ERROR, `Failed to spawn process for job [${jobName}]: ${err.message}`);
    if (jobRecord && jobRecord.logStreamInterval) {
      clearInterval(jobRecord.logStreamInterval);
      removeIntervalReference(jobRecord.logStreamInterval);
    }
    runtime.activeJobs.delete(activeJobKey);
    publishStatusUpdate('error', `Job failed to start: ${err.message}`, null, jobName, undefined, manual, executionId, 0, executionContext);
  });

  let lastOffset = 0;
  let childProcessExited = false;
  let localReturnCode = null;

  const logStreamInterval = setInterval(() => {
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
          localReturnCode !== 0 ? runtime.failJobCount++ : runtime.successJobCount++;
          runtime.activeJobs.delete(activeJobKey);
          runtime.status = AGENT_STATUS.IDLE;
          deleteFile(file);
          callback(jobName, (stop - start) / 1000, manual, executionId, executionContext, localReturnCode);
        }
      } catch (err) {
        debug(DEBUG_LEVEL.TRACE, `Logfile read error: ${err.message}`);
      }
    }
  }, 1000);

  runtime.activeIntervals.push(logStreamInterval);
  if (jobRecord) {
    jobRecord.logStreamInterval = logStreamInterval;
  }

  childProcess.on('exit', (code, signal) => {
    childProcessExited = true;
    localReturnCode = returnCodeFromExit(code, signal);
    const currentJobRecord = runtime.activeJobs.get(activeJobKey);
    if (currentJobRecord) {
      currentJobRecord.returnCode = localReturnCode;
    }
  });
}

function cleanupBackgroundTasks() {
  runtime.activeIntervals.forEach(intervalId => {
    try {
      clearInterval(intervalId);
    } catch (err) {
      debug(DEBUG_LEVEL.TRACE, `Error clearing interval: ${err.message}`);
    }
  });
  runtime.activeIntervals = [];

  runtime.activeJobs.forEach((job, jobName) => {
    debug(DEBUG_LEVEL.WARN, `Cleaning up active job on shutdown: ${jobName}`);
    signalTrackedJob(job, 'SIGTERM');
  });
  runtime.activeJobs.clear();
}

module.exports = {
  executeBackupCommand,
  backupComplete,
  terminateExecution,
  cleanupBackgroundTasks,
  sanitizeCommandParams,
};
