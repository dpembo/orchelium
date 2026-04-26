const status_topic = 'orchelium/agent/status';
const command_topic = 'orchelium/agent/command';
const EventEmitter = require('events');
const metricResultEmitter = new EventEmitter();

// Serialize DB log writes per key so concurrent log chunks cannot overwrite
// each other (for example, two first chunks both seeing NotFound).
const logWriteQueues = {};
const scriptTestManager = require('./scriptTestManager.js');

/**
 * Wait for a metric_result reply from an agent.
 * @param {string} jobName  - The correlation ID sent with the queryMetric command
 * @param {number} timeoutMs - Milliseconds before rejecting (default 30s)
 * @returns {Promise<{agent: string, result: Object}>}
 */
function waitForMetricResult(jobName, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      metricResultEmitter.removeAllListeners(`result:${jobName}`);
      reject(new Error(`Timeout waiting for metric result [${jobName}] after ${timeoutMs}ms`));
    }, timeoutMs);

    metricResultEmitter.once(`result:${jobName}`, (payload) => {
      clearTimeout(timer);
      resolve(payload);
    });
  });
}

async function processMessage(topic, message,protocol) {
    logger.debug(`Received message on Protocol: '${protocol}' with topic '${topic}': ${message.toString()}`);
    var obj = JSON.parse(message);
    var agentKnown = agents.getAgent(obj.name);
    debugMessage(topic, obj, message, agentKnown);
  
    if(agentKnown){
      if (obj.executionMode === 'test') {
        await handleScriptTestMessage(obj);
        return;
      }
      
      //Status Notification from Agent
      if (topic == "orchelium/agent/status" && obj.status == "notification") {
        logger.info("Received Notification From Agent:" + obj.name);
        try {
          logger.info(obj.data);
          var data = obj.data.split(".")[1];
          logger.info(data);
          var decodedString = Buffer.from(data, 'base64').toString('utf-8');
          logger.debug(decodedString);
          var dataObj = JSON.parse(decodedString);
          if(serverConfig.server.jobFailEnabled=="true")notifier.sendNotification(`Error from Agent: ${obj.name}`, `Error Notification From Agent: ${obj.name}`, obj.description + ": data [" + decodedString + "]", "ERROR", `/agentHistory.html?name=${obj.name}`);
        }
        catch (error) {
          if(serverConfig.server.jobFailEnabled=="true")notifier.sendNotification(`Error from Agent: ${obj.name}`, `Error Notification From Agent: ${obj.name}`, obj.description + ": data [" + decodedString + "]", "ERROR", `/agentHistory.html?name=${obj.name}`);
        }
      }
  
      //Known agent registering
      if (topic == "orchelium/agent/status" && obj.status == "register") {
        agents.updateAgentStatus(obj.name, "online", "Agent Back Online",null,null,null,message,protocol);
      }
  
      //known agent going Offline
      if (topic == "orchelium/agent/status" && obj.status == "offline") {
        agents.updateAgentStatus(obj.name, obj.status, obj.description,null,null,null,message,protocol);
      }
  
      //known agent running status
      if (topic == "orchelium/agent/status" && obj.status == "running") {
        logger.debug(">>>>>> RUNNING\n" + JSON.stringify(obj));
        logger.info(`Agent running: jobName=[${obj.jobName}], executionId=[${obj.executionId}], manual=[${obj.manual}]`);
        agents.updateAgentStatus(obj.name, obj.status, obj.description, null,obj.jobName, new Date(),message,protocol);
        
        // Check if this execution is already in the running queue to avoid duplicates
        const existingItem = running.getItems().find(item => 
          item.jobName === obj.jobName && item.executionId === obj.executionId
        );
        
        if (!existingItem) {
          logger.info(`Creating new running item for [${obj.jobName}] with executionId [${obj.executionId}] on agent [${obj.name}]`);
          var item = running.createItem(obj.jobName, obj.lastStatusReport, obj.manual, obj.executionId, obj.name);
          logger.info(`Created running item: jobName=[${item.jobName}], executionId=[${item.executionId}], agentName=[${item.agentName}]`);
          running.add(item);
        } else {
          logger.info(`Running item already exists for [${obj.jobName}] with executionId [${obj.executionId}], skipping duplicate add`);
        }
      }
  
      //Ping response received
      if (topic == "orchelium/agent/status" && obj.status == "pong") {
        logger.debug("PONG RECEIVED: " + obj.status);
        logger.debug("AGENT STATUS: " + agents.getAgent(obj.name).status);
        logger.debug("--------------------------------------");
        agentStats.set(obj.name,obj.data);
        logger.debug("--------------------------------------");
        const agentRunningCount = running.getRunningCountForAgent(obj.name);
        const agentConcurrencyLimit = agents.getConcurrency(obj.name);
        
        // Defensive check: log warning if we have running items but they lack agentName
        const allRunningItems = running.getItems();
        const itemsWithoutAgent = allRunningItems.filter(item => !item.agentName);
        if (itemsWithoutAgent.length > 0) {
          logger.warn(`[CONCURRENCY] WARNING: Found ${itemsWithoutAgent.length} running items without agentName - concurrency enforcement may be bypassed`);
        }
        
        agents.updateAgentStatus(obj.name, agentRunningCount > 0 ? "running" : "online", "Ping response returned", null, null, null, message, protocol);
  
      }

      // Metric query result received
      if (topic == "orchelium/agent/status" && obj.status == "metric_result") {
        logger.info(`[METRIC] Received metric_result from agent [${obj.name}] for job [${obj.jobName}]`);
        try {
          const result = JSON.parse(obj.data);
          metricResultEmitter.emit(`result:${obj.jobName}`, { agent: obj.name, result });
        } catch (parseErr) {
          logger.error(`[METRIC] Failed to parse metric_result data: ${parseErr.message}`);
          metricResultEmitter.emit(`result:${obj.jobName}`, { agent: obj.name, error: parseErr.message });
        }
      }
  
      //Log submission received
      if (obj.status == "log_submission") {
        logger.info("Received Log event");
        // Use executionId to build correct log key when multiple same-job runs are concurrent
        // If agent didn't send executionId, resolve it from the running item so log key lookups work correctly
        if (!obj.executionId) {
          const runningItm = running.getItemByName(obj.jobName);
          if (runningItm && runningItm.executionId) {
            obj.executionId = runningItm.executionId;
            logger.debug(`[LOG] Resolved executionId from running queue for [${obj.jobName}]: ${obj.executionId}`);
          }
        }
        //add log to db
        updateLogRecord(obj).catch(err => logger.error('Failed to update log record:', err.message));
      }
  
      //ETA Submission received
      if (obj.status == "eta_submission") {
        var runTime = obj.eta;
        // Use executionId to get the correct running item start time when multiple same-job runs are concurrent
        var runningItm = obj.executionId
          ? running.getItemByExecutionId(obj.executionId)
          : running.getItemByName(obj.jobName);
        // If agent didn't send executionId, resolve it from the running item so log key lookups work correctly
        if (!obj.executionId && runningItm && runningItm.executionId) {
          obj.executionId = runningItm.executionId;
        }
        var startTime = runningItm ? runningItm.startTime : null;
        logger.info("Received ETA event");
        //add log to db
        if (obj.returnCode !== null && obj.returnCode == "0") {
          //Success run
        }
        else {
          var body = "";
          body += "Job Name     : " + obj.jobName;
          body += "\n---------------------------------------------------------";
          body += "\nStart Time   : " + startTime;
          body += "\nIs Manual    : " + obj.manual;
          body += "\nAgent        : " + obj.name;
          body += "\nTime Running : " + obj.eta;
          body += "\nReturn Code  : " + obj.returnCode;

          // Only send notification for regular jobs - orchestration node failures are handled at orchestration.saveExecutionResult()
          const isOrchestrationNode = obj.jobName && obj.jobName.includes('Orchestration [');
          if(serverConfig.server.jobFailEnabled=="true" && !isOrchestrationNode) {
            const detailUrl = obj.executionId ? `/scheduleInfo.html?jobname=${encodeURIComponent(obj.jobName)}&executionId=${encodeURIComponent(obj.executionId)}` : "/history.html";
            notifier.sendNotification(obj.jobName + "- job failed", body, "WARNING", detailUrl);
          }
        }
        logger.debug("Adding History record for: " + JSON.stringify(obj));
        
        // Signal orchestration engine if this is an orchestration job
        if (obj.jobName && obj.jobName.includes('Orchestration [')) {
          try {
            logger.debug(`[ORCHESTRATION] Received eta_submission for orchestration job [${obj.jobName}]`);
            const orchestrationEngine = require('./orchestrationEngine.js');
            
            // Fetch the log from database using the same key structure as updateLogRecord
            let logOutput = '';
            try {
              const logKey = `${obj.name}_${obj.jobName}_${obj.executionId}_log`;
              logger.debug(`[ORCHESTRATION] Fetching log with key: [${logKey}]`);
              const logData = await db.getData(logKey);
              logOutput = logData || '';
              logger.debug(`[ORCHESTRATION] Fetched log (${logOutput.length} bytes) for job [${obj.jobName}]`);
              logger.debug(`[ORCHESTRATION] Log content first 100 chars: ${logOutput.substring(0, 100)}`);
              
              // Clear the log from the database to prevent mixing with future runs if same job ID is reused
              try {
                await db.deleteData(logKey);
                logger.debug(`[ORCHESTRATION] Cleared log data for key [${logKey}]`);
              } catch (deleteErr) {
                logger.debug(`[ORCHESTRATION] Could not clear log (non-critical): ${deleteErr.message}`);
              }
            } catch (logErr) {
              logger.debug(`[ORCHESTRATION] No log data found for job [${obj.jobName}]: ${logErr.message}`);
            }
            
            orchestrationEngine.signalScriptCompletion(obj.jobName, {
              exitCode: parseInt(obj.returnCode || 0),
              stdout: logOutput,
              stderr: ''
            });
            logger.debug(`[ORCHESTRATION] Signaled orchestration completion for job [${obj.jobName}]`);
          } catch (err) {
            logger.error(`[ORCHESTRATION] Failed to signal orchestration completion: ${err.message}`);
          }
        }
        
        updateStatusRecords(obj, startTime);
        
        // Remove from running queue by executionId if available, otherwise by job name
        logger.info(`Job completion for [${obj.jobName}]: executionId=[${obj.executionId}], returnCode=[${obj.returnCode}]`);
        if (obj.executionId) {
          logger.info(`Removing job [${obj.jobName}] from running queue by executionId [${obj.executionId}]`);
          running.removeItemByExecutionId(obj.executionId);
        } else {
          logger.info(`Removing job [${obj.jobName}] from running queue by job name (no executionId)`);
          running.removeItem(obj.jobName);
        }
        
        agents.updateAgentStatus(obj.name, "online", `Job [${obj.name}] completed`,null,null,null,message,protocol,null);
        
        // Emit schedule update to notify frontend that job completed (skip for orchestration jobs)
        if (!obj.jobName || !obj.jobName.includes('Orchestration [')) {
          emitScheduleUpdate(obj.jobName, obj.executionId);
        }
      }
    }
    else {
      //Register an unknown agent
      if (topic == "orchelium/agent/status" && obj.status == "register") {
        agents.addObjToAgentStatusDict(obj).catch(err => {
          logger.error(`Failed to register unknown agent [${obj.name}]:`, err.message);
        });
        webSocketBrowser.emitNotification('register', `${message.toString()}`);
      }
  
    }
  }

  async function handleScriptTestMessage(obj) {
    if (obj.status === 'running') {
      logger.info(`[SCRIPT-TEST] Execution [${obj.executionId}] is running on agent [${obj.name}]`);
      scriptTestManager.markRunning(obj.executionId);
      return;
    }

    if (obj.status === 'log_submission') {
      logger.info(`[SCRIPT-TEST] Received log update for execution [${obj.executionId}]`);
      scriptTestManager.appendLog(obj.executionId, obj.data || '');
      return;
    }

    if (obj.status === 'eta_submission') {
      logger.info(`[SCRIPT-TEST] Execution [${obj.executionId}] completed with return code [${obj.returnCode}]`);
      scriptTestManager.completeTest(obj.executionId, obj.returnCode);
      return;
    }

    if (obj.status === 'error') {
      const errorMessage = obj.data || obj.message || 'Agent reported a script test error before completion.';
      const returnCode = (typeof obj.returnCode === 'number' && obj.returnCode !== 0) ? obj.returnCode : 1;

      logger.error(`[SCRIPT-TEST] Execution [${obj.executionId}] failed on agent [${obj.name}]: ${errorMessage}`);
      scriptTestManager.appendLog(obj.executionId, `[ERROR] ${errorMessage}`);
      scriptTestManager.completeTest(obj.executionId, returnCode);
      return;
    }
    logger.debug(`[SCRIPT-TEST] Ignoring unsupported test status [${obj.status}] for execution [${obj.executionId}]`);
  }
  
  function debugMessage(topic, obj, message, agentKnown) {
    logger.debug("+------------------+");
    logger.debug("| AGENT MESSAGE    |");
    logger.debug("+------------------+");
    logger.debug("Topic   [" + topic + "]");
    logger.debug("Name    [" + obj.name + "]");
    logger.debug("Status  [" + obj.status + "]");
    logger.debug("Message [" + message + "]");
  
    if (!agentKnown) {
      logger.debug("Agent   [" + obj.name + "] is NEW!");
    }
  }




  //Update log record
  async function updateLogRecord(obj){
    const key = getDbKey(obj, "log");
    const previous = logWriteQueues[key] || Promise.resolve();

    const next = previous.then(async () => {
      const chunk = obj.data || '';
      let existingData;

      try {
        existingData = await db.simpleGetData(key);
      } catch (error) {
        if (error.message && error.message.includes('NotFoundError')) {
          existingData = '';
        } else {
          logger.error(`Unknown Issue searching DB for key [${key}]`);
          logger.error(error);
          throw error;
        }
      }

      const updatedData = `${existingData}${chunk}`;

      try {
        await db.simplePutData(key, updatedData);
        logger.debug(`Data updated successfully key [${key}] bytes [${updatedData.length}]`);
      } catch (error) {
        logger.error(`unable to add [${key}] to DB`);
        logger.error(error);
        throw error;
      }

      if (!obj.jobName || !obj.jobName.includes('Orchestration [')) {
        emitScheduleLogUpdate(obj.jobName, updatedData, obj.executionId);
      } else {
        emitOrchestrationNodeLogUpdate(obj.jobName, updatedData);
      }
    });

    // Keep chain alive after failures so future chunks are not blocked forever.
    logWriteQueues[key] = next.catch(() => {});
    return next;
  }

  function emitOrchestrationNodeLogUpdate(jobName, logData) {
    try {
      // jobName format: "Orchestration [jobId] Execution [executionId] Node [nodeId]"
      const match = jobName.match(/^Orchestration\s+\[(.+?)\]\s+Execution\s+\[(.+?)\]\s+Node\s+\[(.+?)\]/);
      if (!match) return;
      const jobId = match[1];
      const executionId = match[2];
      const nodeId = match[3];
      if (!executionId) return;
      const wsBrowserTransport = require('./communications/wsBrowserTransport.js');
      const io = wsBrowserTransport.getIO();
      if (io) {
        const eventName = `orchestrationNodeLog:${jobId}:${executionId}:${nodeId}`;
        io.emit(eventName, { nodeId, log: logData });
        logger.debug(`[emitOrchestrationNodeLogUpdate] Emitted ${eventName} (${logData.length} bytes)`);
      }
    } catch (error) {
      logger.debug(`[emitOrchestrationNodeLogUpdate] Error: ${error.message}`);
    }
  }

  function emitScheduleLogUpdate(jobName, logData, executionId) {
    try {
      var scheduleIndex = scheduler.getScheduleIndex(jobName);
      logger.debug(`[emitScheduleLogUpdate] jobName: ${jobName}, scheduleIndex: ${scheduleIndex}`);
      
      if (scheduleIndex !== undefined && scheduleIndex !== null && scheduleIndex >= 0) {
        // Use lazy loading to get the socket.io instance at runtime, avoiding initialization order issues
        var wsBrowserTransport = require('./communications/wsBrowserTransport.js');
        var io = wsBrowserTransport.getIO();
        
        if (io) {
          if (executionId) {
            // Scope to specific execution so concurrent same-job runs don't mix logs
            logger.debug(`[emitScheduleLogUpdate] Emitting scheduleLog:${scheduleIndex}:${executionId}`);
            io.emit(`scheduleLog:${scheduleIndex}:${executionId}`, { log: logData });
          } else {
            logger.debug(`[emitScheduleLogUpdate] Emitting scheduleLog:${scheduleIndex}`);
            io.emit(`scheduleLog:${scheduleIndex}`, { log: logData });
          }
        } else {
          logger.warn(`[emitScheduleLogUpdate] Socket.io instance not available yet`);
        }
      } else {
        logger.warn(`[emitScheduleLogUpdate] Invalid scheduleIndex: ${scheduleIndex} for jobName: ${jobName}`);
      }
    } catch (error) {
      logger.debug(`[emitScheduleLogUpdate] Error emitting schedule log update: ${error.message}`);
    }
  }

  async function emitScheduleUpdate(jobName, executionId) {
    try {
      var scheduleIndex = scheduler.getScheduleIndex(jobName);
      logger.debug(`[emitScheduleUpdate] jobName: ${jobName}, scheduleIndex: ${scheduleIndex}`);
      
      if (scheduleIndex !== undefined && scheduleIndex !== null && scheduleIndex >= 0) {
        // Gather schedule data to emit
        var schedule = scheduler.getSchedules(scheduleIndex);
        var agentData = agents.getAgent(schedule.agent);

        // When a specific execution is being viewed, use the running item's exact startTime
        // so the ETA progress bar reflects this execution's elapsed time, not the shared
        // agent jobStarted which is overwritten by whichever concurrent run sent the last status.
        if (executionId) {
          var runningItem = running.getItemByExecutionId(executionId);
          if (runningItem && runningItem.startTime) {
            agentData = Object.assign({}, agentData);
            agentData.jobStarted = runningItem.startTime;
          }
        }
        
        // Get stats and log from database
        var key1 = schedule.agent + "_" + schedule.jobName + "_" + "stats";
        // Use executionId-scoped log key when available (matches how logs are stored)
        var key2 = executionId
          ? schedule.agent + "_" + schedule.jobName + "_" + executionId + "_log"
          : schedule.agent + "_" + schedule.jobName + "_log";
        
        var stats = null;
        var log = null;
        try {
          stats = await db.getData(key1);
        } catch(err) {
          logger.warn(`[emitScheduleUpdate] Unable to find stats data for ${key1}`);
        }
        
        try {
          log = await db.getData(key2);
        } catch(err) {
          logger.warn(`[emitScheduleUpdate] Unable to find log data for ${key2}`);
        }
        
        // Get history data
        var histLastRun = hist.getLastRun(schedule.jobName);
        var histAvgRuntime = hist.getAverageRuntime(schedule.jobName);
        
        // Build response data matching scheduleInfo.ejs expectations
        var data = {
          agent: agentData,
          schedule: schedule,
          index: scheduleIndex,
          stats: stats,
          log: log,
          hist: {
            histLastRun: histLastRun,
            histAvgRuntime: histAvgRuntime,
            histAvgRuntimeSecs: dateTimeUtils.displaySecs(histAvgRuntime)
          }
        };
        
        // Emit the update
        var wsBrowserTransport = require('./communications/wsBrowserTransport.js');
        var io = wsBrowserTransport.getIO();
        
        if (io) {
          if (executionId) {
            // Emit scoped event for the specific execution's page
            logger.debug(`[emitScheduleUpdate] Emitting scheduleUpdate:${scheduleIndex}:${executionId}`);
            io.emit(`scheduleUpdate:${scheduleIndex}:${executionId}`, data);
          }
          // Also emit unscoped event for pages not tracking a specific execution
          logger.debug(`[emitScheduleUpdate] Emitting scheduleUpdate:${scheduleIndex}`);
          io.emit(`scheduleUpdate:${scheduleIndex}`, data);
        } else {
          logger.warn(`[emitScheduleUpdate] Socket.io instance not available yet`);
        }
      } else {
        logger.warn(`[emitScheduleUpdate] Invalid scheduleIndex: ${scheduleIndex} for jobName: ${jobName}`);
      }
    } catch (error) {
      logger.error(`[emitScheduleUpdate] Error emitting schedule update: ${error.message}`);
    }
  }

  async function updateStatusRecords(obj,startDate){
    logger.debug("Updating status records");
    var key = getDbKey(obj,"stats");
    var stats={};
    stats.current={};
    stats.previous={};
    if(startDate===undefined||startDate===null)startDate=obj.lastStatusReport;
    stats.current.lastRun = new Date(obj.lastStatusReport);
    stats.current.eta = (obj.eta)
    stats.previous.lastRun = new Date(obj.lastStatusReport);
    stats.previous.eta = (obj.eta)
    stats.etaRollingAvg = (obj.eta).toFixed(0);
    stats.current.returnCode = obj.returnCode;
    stats.previous.returnCode = obj.returnCode;
  
    
    //------------------

    var data;
    var resp;
    try{
      data = await db.simpleGetData(key);
    }
    catch (error){
      //If item doesn't exist add to DB
      if (error.message.includes('NotFoundError')){
        try {
            logger.debug(`No stats record found - creating with key [${key}]`);
            resp = await db.simplePutData(key,stats);
            logger.debug(`stats data created successfully for Key [${key}], Response [${resp}], Data \n${obj.data}`);
            await addHistoryRecord(obj,startDate);
        }
        catch (insertErr){
          logger.error(`Unknown Issue creating new stats entry for key [${key}] to DB`);
          logger.error(insertErr);
          throw insertErr;
        }
      }
      else{
        logger.error(`Unknown Issue searching DB for key [${key}]`);
        logger.error(error);
        throw insertErr;
      }
    }

    //Reord already exsits
    if(data!==undefined){
      logger.debug('Retrieved data:', data);
      logger.debug('Moving last stats data to previous');
      stats.previous.eta = data.current.eta;
      stats.previous.lastRun = data.current.lastRun;
      stats.previous.returnCode = data.current.returnCode;
      //Now calculate the moving average
      logger.debug(`Previous ETA: ${stats.previous.eta}`);
      logger.debug(`Current ETA: ${stats.current.eta}`);
      var average = ((parseFloat(stats.previous.eta) + parseFloat(stats.current.eta))/2).toFixed(0);
      logger.debug(`Average ETA: ${average}`);
      stats.etaRollingAvg = average;

      //Now updating
      try {
        logger.debug(`Updating stats record with key [${key}], data [${data}]`);
        resp = await db.simplePutData(key,stats);
        logger.debug(`stats data updated successfully for Key [${key}], Response [${resp}], Data \n${obj.data}`);
        await addHistoryRecord(obj,startDate);
      }
      catch (insertErr){
        logger.error(`Unknown Issue creating new stats entry for key [${key}] to DB`);
        logger.error(insertErr);
        throw insertErr;
      }
    }
  }
  
  async function addHistoryRecord(obj,startDate)
  {
    logger.info("Adding history record");
    logger.debug(obj);
    var key1 = getDbKey(obj,"stats");
    var key2 = getDbKey(obj,"log");
    var stats = null;
    var log = null;
    var returnCode = 0; // Default to success if stats not available
    
    // Extract executionId from message object or orchestration job name
    let executionId = obj.executionId || null;
    let orchestrationJobId = null;
    let orchestrationNodeId = null;
    
    // Check if this is an orchestration node
    const orchestrationMatch = obj.jobName.match(/^Orchestration\s+\[(.+?)\]\s+Execution\s+\[(.+?)\]\s+Node\s+\[(.+?)\]$/);
    if (orchestrationMatch) {
      orchestrationJobId = orchestrationMatch[1];
      executionId = orchestrationMatch[2];
      orchestrationNodeId = orchestrationMatch[3];
      
      logger.debug(`[ORCHESTRATION] Detected orchestration node - jobId: ${orchestrationJobId}, executionId: ${executionId}, nodeId: ${orchestrationNodeId}`);
      
      // Fetch log from ORCHESTRATION_EXECUTIONS instead of regular log database
      try {
        const orchestrationExecutions = await db.getData('ORCHESTRATION_EXECUTIONS');
        if (orchestrationExecutions[orchestrationJobId]) {
          const execution = orchestrationExecutions[orchestrationJobId].find(exec => exec.executionId === executionId);
          if (execution && execution.scriptOutputs && execution.scriptOutputs[orchestrationNodeId]) {
            const nodeOutput = execution.scriptOutputs[orchestrationNodeId];
            log = nodeOutput.stdout || "";
            // Use exit code from orchestration if available
            if (nodeOutput.exitCode !== undefined) {
              returnCode = nodeOutput.exitCode;
            }
            logger.debug(`[ORCHESTRATION] Successfully retrieved log for node ${orchestrationNodeId}`);
          }
        }
      } catch (err) {
        logger.debug(`[ORCHESTRATION] Unable to fetch log from ORCHESTRATION_EXECUTIONS: ${err.message}`);
      }
    }
    
    // If not orchestration or log not found via orchestration, try regular log database
    if (!log) {
      try {
        log = await db.simpleGetData(key2);
        log = log || "";
      } catch(err){
        logger.debug("Unable to find log data for key: " + key2);
        log = "";
      }
    }
    
    // If return code not set by orchestration, try to get from stats
    if (returnCode === 0) {
      try {
        stats = await db.simpleGetData(key1);
        returnCode = stats.current.returnCode;
      } catch(err){
        logger.debug("Unable to find stats data for key: " + key1);
      }
    }
    
    // Calculate actual runtime in seconds from startDate to now
    var runTime = Math.round((Date.now() - new Date(startDate).getTime()) / 1000);
    var histObj = hist.createHistoryItem(obj.jobName,startDate,returnCode,runTime,log,obj.manual,executionId,obj.rerunFrom);
    logger.debug("Adding History obj: " + JSON.stringify(histObj));
    hist.add(histObj);

    // Clean up execution-specific log from DB now that it's saved in history
    if (obj.executionId && !orchestrationMatch) {
      try {
        await db.deleteData(key2);
        logger.debug(`Cleaned up execution log key [${key2}]`);
      } catch (deleteErr) {
        logger.debug(`Could not clean up log key [${key2}] (non-critical): ${deleteErr.message}`);
      }
    }
  }

  //Get a db key
  function getDbKey(logEvent, type)
  {
    // Use executionId-scoped key for log entries so concurrent same-job runs don't collide
    if (type === 'log' && logEvent.executionId) {
      return `${logEvent.name}_${logEvent.jobName}_${logEvent.executionId}_${type}`;
    }
    return logEvent.name + "_" + logEvent.jobName + "_" + type;
  }


  module.exports = { updateLogRecord, processMessage, waitForMetricResult }