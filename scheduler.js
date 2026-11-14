const scheduleFile = "./data/schedules.json";
const db = require('./db.js');
const definitionStore = require('./definitionStore.js');
var schedules = [];
var ruleIntervals = new Map(); // Track setInterval handles for rule-based jobs

var mqttClient;
var mqttCommand_topic;

// In-progress execution cache for orchestrations being executed
var inProgressExecutions = {};

function getInProgressKey(jobId, executionId) {
  return `${jobId}:${executionId}`;
}

function saveInProgressExecution(jobId, executionId, execution) {
  const key = getInProgressKey(jobId, executionId);
  inProgressExecutions[key] = execution;
  logger.debug(`Cached in-progress execution [${key}]`);
}

function getInProgressExecution(jobId, executionId) {
  const key = getInProgressKey(jobId, executionId);
  return inProgressExecutions[key];
}

function removeInProgressExecution(jobId, executionId) {
  const key = getInProgressKey(jobId, executionId);
  delete inProgressExecutions[key];
  logger.debug(`Removed in-progress execution cache [${key}]`);
}

function updateInProgressExecution(jobId, executionId, executionLog) {
  const cachedExecution = getInProgressExecution(jobId, executionId);
  if (!cachedExecution || !executionLog) {
    return;
  }

  cachedExecution.visitedNodes = executionLog.visitedNodes;
  cachedExecution.currentNode = executionLog.currentNode;
  cachedExecution.status = executionLog.status;
  cachedExecution.finalStatus = executionLog.finalStatus;
  cachedExecution.endTime = executionLog.endTime;
  cachedExecution.errors = executionLog.errors;
  cachedExecution.scriptOutputs = executionLog.scriptOutputs;
  cachedExecution.conditionEvaluations = executionLog.conditionEvaluations;
  cachedExecution.nodeMetrics = executionLog.nodeMetrics;
  saveInProgressExecution(jobId, executionId, cachedExecution);
}

const fs = require('fs').promises;
const fsSync = require('fs');
const path = require('path');
const crypto = require('crypto');
const dateTimeUtils = require('./utils/dateTimeUtils.js');
const asyncUtils = require('./utils/asyncUtils.js');
const { handleError, AppError } = require('./utils/errorHandler.js');
const { resolveScriptPath } = require('./utils/scriptFilename.js');
const running = require('./running.js');
const triggerContext = require('./triggerContext.js');

function checkExecutePermissionCapability(directoryPath) {
  try {
    // Check if you have write permissions on the directory
    fsSync.accessSync(directoryPath, fsSync.constants.W_OK);

    // Create a temporary file in the directory
    const tempFileName = '.tmp_permission_test';
    const tempFilePath = path.join(directoryPath, tempFileName);
    fsSync.writeFileSync(tempFilePath, 'Temporary file content', 'utf8');

    // Try to set execute permission on the temporary file
    fsSync.chmodSync(tempFilePath, '755');

    // Clean up by removing the temporary file
    fsSync.unlinkSync(tempFilePath);

    logger.info('Confirmed capabilities to set execute permissions on files in execute directory.');
  } catch (err) {
    logger.error('Error:', err);
    logger.error('You do not have capabilities to set execute permissions on files in the directory.');
    process.exit(1);
  }
}
// Initialize scheduler
async function init() {
  try {
    checkExecutePermissionCapability("/tmp");
    await readSchedules();
    await scheduleJobs();
    logger.info("Scheduler initialized successfully");
  } catch (err) {
    logger.error("Error initializing scheduler:", err.message);
  }
}

function publishExecute(agent_id, command, commandParams, jobName, isManual, executionId, triggerContextParam = null, rerunFrom = null) {
  logger.info(`Executing Scheduled command on Agent [${agent_id}] with executionId [${executionId}]` + (rerunFrom ? ` [rerunFrom: ${rerunFrom}]` : ""));
  
  // Convert trigger context to environment variables for the script
  let contextEnvVars = {};
  if (triggerContextParam && triggerContext.isValidTriggerContext(triggerContextParam)) {
    contextEnvVars = triggerContext.contextToEnvVars(triggerContextParam);
    logger.debug(`[TRIGGER CONTEXT] Injecting ${Object.keys(contextEnvVars).length} environment variables for job [${jobName}]`);
  }
  
  agentComms.sendCommand(agent_id, mqttTransport.getCommandTopic(), command, commandParams, jobName, undefined, isManual, executionId, triggerContextParam, contextEnvVars, rerunFrom);
}

/**
 * Migrate legacy schedules.json into the active definitions backend.
 */
async function migrateLegacySchedulesJson() {
  try {
    if (!fsSync.existsSync(scheduleFile)) {
      logger.info("Legacy schedules JSON file not found - no migration needed");
      return false;
    }

    logger.info("Found legacy schedules JSON file, migrating into definitions backend...");
    
    // Read the JSON file
    const data = await fs.readFile(scheduleFile, 'utf8');
    const jsonData = JSON.parse(data);
    const scheduleCount = Array.isArray(jsonData) ? jsonData.length : 0;

    if (scheduleCount === 0) {
      logger.info("Schedules file is empty, deleting file");
      await fs.unlink(scheduleFile);
      return false;
    }

    await definitionStore.replaceJobs(jsonData);
    logger.info(`Successfully migrated ${scheduleCount} schedules from JSON file`);

    // Delete the JSON file after successful migration
    await fs.unlink(scheduleFile);
    logger.info("Legacy schedules JSON file deleted after successful migration");
    
    return true;
  } catch (err) {
    logger.error(`Error during legacy schedules migration: ${err.message}`);
    throw new AppError(`Failed to migrate legacy schedules: ${err.message}`, 500);
  }
}

// Read schedules from the active definitions backend
async function readSchedules() {
  try {
    logger.info("Loading Scheduler Data from definition store");
    
    const migrated = await migrateLegacySchedulesJson();

    const data = await definitionStore.listJobs();
    schedules = Array.isArray(data) ? data : [];
    logger.info(`Loaded ${schedules.length} schedules from ${definitionStore.getBackend()} backend${migrated ? ' (after legacy migration)' : ''}`);
  } catch (err) {
    logger.error('Error reading schedules:', err.message);
    schedules = [];
  }
}

// Write schedules to the active definitions backend and reschedule jobs
async function writeSchedules() {
  try {
    logger.debug("Writing Schedules to definition store");
    await definitionStore.replaceJobs(schedules);
    logger.debug('Schedules stored successfully');
    await scheduleJobs();
  } catch (err) {
    logger.error('Error writing schedules:', err.message);
    throw new AppError(`Failed to save schedules: ${err.message}`, 500);
  }
}

// Persist schedules WITHOUT rescheduling (for runtime state updates only)
async function persistSchedules() {
  try {
    logger.debug("Persisting Schedules to definition store (no reschedule)");
    await definitionStore.replaceJobs(schedules);
    logger.debug('Schedules persisted successfully');
  } catch (err) {
    logger.error('Error persisting schedules:', err.message);
  }
}

async function writeSingleSchedule(schedule) {
  try {
    logger.debug(`Persisting single schedule [${schedule.jobName}] to definition store`);
    await definitionStore.saveJob(schedule.jobName, schedule);
    logger.debug(`Schedule [${schedule.jobName}] stored successfully`);
    await scheduleJobs();
  } catch (err) {
    logger.error(`Error writing schedule [${schedule.jobName}]:`, err.message);
    throw new AppError(`Failed to save schedule [${schedule.jobName}]: ${err.message}`, 500);
  }
}

async function persistSingleSchedule(schedule) {
  try {
    logger.debug(`Persisting single schedule [${schedule.jobName}] to definition store (no reschedule)`);
    await definitionStore.saveJob(schedule.jobName, schedule);
    logger.debug(`Schedule [${schedule.jobName}] persisted successfully`);
  } catch (err) {
    logger.error(`Error persisting schedule [${schedule.jobName}]:`, err.message);
  }
}

async function clearSchedules() {
  schedules = [];
  await definitionStore.clearJobs();
  await scheduleJobs();
}

async function migrateSchedulesToFilesystem(options = {}) {
  const count = await definitionStore.migrateSchedulesFromDbToFilesystem(options);
  await readSchedules();
  await scheduleJobs();
  return count;
}


function deleteSchedule(jobName) {
  logger.info(`Deleting Schedule with job name [${jobName}]`);
  var index = -1;
  for (var i = 0; i < schedules.length; i++) {
    if (schedules[i].jobName == jobName) {
      index = i;
      break;
    }
  }
  logger.debug(`Found job at index [${index}]`);
  if (index != -1) {
    return deleteScheduleAtIndex(index);
  }
  return Promise.resolve();
}

async function deleteScheduleAtIndex(index) {
  try {
    logger.info(`Deleting Schedule at index [${index}]`);
    if (index > -1) {
      const deletedSchedule = schedules[index];
      var newArr = [];
      for (var i = 0; i < schedules.length; i++) {
        if (i != index) newArr.push(schedules[i]);
      }
      schedules = [];
      for (var i = 0; i < newArr.length; i++) {
        schedules.push(newArr[i]);
      }
      if (definitionStore.getBackend() === 'hybrid' && deletedSchedule && deletedSchedule.jobName) {
        await definitionStore.deleteJob(deletedSchedule.jobName);
        await scheduleJobs();
      } else {
        await writeSchedules();
      }
      logger.info(`Schedule at index [${index}] deleted successfully`);
    }
  } catch (err) {
    logger.error(`Error deleting schedule at index [${index}]:`, err.message);
    throw err;
  }
}

function isScheduleDisabled(schedule) {
  if (!schedule) return false;
  return schedule.disabled === true || schedule.disabled === 'true';
}

function isManualInvocation(isManual) {
  return isManual === true || isManual === 'manual';
}

function getScheduleIndex(jobName){
  for(var i=0;i<schedules.length;i++)
  {
      if(schedules[i].jobName==jobName)return i;
  }
  return -1;
}

function getSchedule(jobName){
    for(var i=0;i<schedules.length;i++)
    {
        if(schedules[i].jobName==jobName)
        {
            var schedule = schedules[i];
            schedule.disabled = isScheduleDisabled(schedule);
            if (schedule.triggerType === 'clock') {
              schedule.nextRunDate = getNextRunDate(schedules[i]);
            } else if (schedule.triggerType === 'rule') {
              schedule.nextRunDate = `Polls every ${schedule.rulePollIntervalMins || 15}m`;
            } else if (schedule.triggerType === 'webhook') {
              schedule.nextRunDate = 'Webhook Trigger';
            } else {
              schedule.nextRunDate = 'n/a';
            }
            return schedule;
        }
    }
    return null;
}

function getScheduleObject(jobName, colour, description, scheduleType, scheduleTime, dayOfWeek, dayInMonth, agentselect, agentcommand, commandparams, icon, scheduleMode, orchestrationId, triggerType, ruleMetric, ruleCondition, rulePollIntervalMins, ruleCooldownMins) {
    var schedule = {};
    schedule.jobName = jobName;
    schedule.description = description;
    schedule.triggerType = triggerType || 'clock';
    schedule.eta = 1;
    schedule.color = colour;
    schedule.icon = icon;
    schedule.scheduleMode = scheduleMode || 'classic';  // Default to classic for backwards compatibility

    if (schedule.triggerType === 'clock') {
      schedule.scheduleType = scheduleType;
      schedule.dayOfWeek = dayOfWeek;
      schedule.dayInMonth = dayInMonth;
      schedule.scheduleTime = scheduleTime;
      schedule.ruleMetric = null;
      schedule.ruleCondition = null;
    } else if (schedule.triggerType === 'rule') {
      schedule.scheduleType = null;
      schedule.dayOfWeek = null;
      schedule.dayInMonth = null;
      schedule.scheduleTime = null;
      schedule.ruleMetric = ruleMetric || null;
      schedule.ruleCondition = ruleCondition || null;
      
      // Parse and validate rule polling interval (clamp to minimum 1 minute)
      const parsedRulePollIntervalMins = parseInt(rulePollIntervalMins, 10);
      schedule.rulePollIntervalMins = Number.isFinite(parsedRulePollIntervalMins) ? Math.max(1, parsedRulePollIntervalMins) : 15;
      
      // Parse and validate rule cooldown (clamp to minimum 1 minute)
      const parsedRuleCooldownMins = parseInt(ruleCooldownMins, 10);
      schedule.ruleCooldownMins = Number.isFinite(parsedRuleCooldownMins) ? Math.max(1, parsedRuleCooldownMins) : 60;
    } else if (schedule.triggerType === 'webhook') {
      // Webhook trigger: no schedule or rule fields
      schedule.scheduleType = null;
      schedule.dayOfWeek = null;
      schedule.dayInMonth = null;
      schedule.scheduleTime = null;
      schedule.ruleMetric = null;
      schedule.ruleCondition = null;
    }

    if (schedule.scheduleMode === 'classic') {
      schedule.command = agentcommand;
      schedule.commandParams = commandparams;
      schedule.agent = agentselect;
    } else if (schedule.scheduleMode === 'orchestration') {
      schedule.orchestrationId = orchestrationId;
      // Clear classic mode fields
      schedule.command = null;
      schedule.commandParams = null;
      schedule.agent = null;
    }
    
    return schedule;
}

function upsertSchedule(index, jobName, colour, description, scheduleType, scheduleTime, dayOfWeek, dayInMonth, agentselect, agentcommand, commandparams, icon, scheduleMode, orchestrationId, triggerType, ruleMetric, ruleCondition, rulePollIntervalMins, ruleCooldownMins, disabled) {
  return _upsertScheduleAsync(index, jobName, colour, description, scheduleType, scheduleTime, dayOfWeek, dayInMonth, agentselect, agentcommand, commandparams, icon, scheduleMode, orchestrationId, triggerType, ruleMetric, ruleCondition, rulePollIntervalMins, ruleCooldownMins, disabled);
}

async function _upsertScheduleAsync(index, jobName, colour, description, scheduleType, scheduleTime, dayOfWeek, dayInMonth, agentselect, agentcommand, commandparams, icon, scheduleMode, orchestrationId, triggerType, ruleMetric, ruleCondition, rulePollIntervalMins, ruleCooldownMins, disabled) {
  try {
    logger.debug("Upserting Schedule");

    const replacing = !(index === undefined || index === null || index.length <= 0 || index == -1);
    const existing = replacing ? schedules[index] : null;

    var schedule = getScheduleObject(jobName, colour, description, scheduleType, scheduleTime, dayOfWeek, dayInMonth, agentselect, agentcommand, commandparams, icon, scheduleMode, orchestrationId, triggerType, ruleMetric, ruleCondition, rulePollIntervalMins, ruleCooldownMins);
    if (disabled === undefined || disabled === null || disabled === '') {
      schedule.disabled = isScheduleDisabled(existing);
    } else {
      schedule.disabled = disabled === true || disabled === 'true';
    }
    logger.info(JSON.stringify(schedule));
    schedule.lastUpdated = new Date().toISOString();

    if (index === undefined || index === null || index.length <= 0 || index == -1) {
      logger.info("Index is null - adding new schedule");
      if (schedules === undefined || schedules === null) schedules = [];
      schedules.push(schedule);
      logger.info("Number of schedules: " + schedules.length);
    } else {
      logger.info("Index defined: " + index);
      schedules[index] = schedule;
    }

    if (definitionStore.getBackend() === 'hybrid') {
      await writeSingleSchedule(schedule);
    } else {
      await writeSchedules();
    }
    logger.info(`Schedule [${jobName}] upserted successfully`);
  } catch (err) {
    logger.error(`Error upserting schedule [${jobName}]:`, err.message);
    throw err;
  }
}


/*function addSchedule(schedule) {
    //Add the new schedule to the array and save
    schedules.push(schedule);
    writeSchedules();
}*/

function getNextRunDate(schedule) {
    //logger.debug(schedule);
    const now = new Date();     
    let nextRunDate;
    if (schedule.scheduleType === 'daily') {
      const [hours, minutes] = schedule.scheduleTime.split(':');
      nextRunDate = new Date(now);
      nextRunDate.setHours(hours, minutes, 0, 0);
  
      if (nextRunDate <= now) {
        nextRunDate.setDate(nextRunDate.getDate() + 1);
      }
    } else if (schedule.scheduleType === 'weekly') {
      const [hours, minutes] = schedule.scheduleTime.split(':');
      nextRunDate = new Date(now);
      const dayOfWeek = parseInt(schedule.dayOfWeek);
  
      if (dayOfWeek === nextRunDate.getDay()) {
        nextRunDate.setHours(hours, minutes, 0, 0);
  
        if (nextRunDate <= now) {
          nextRunDate.setDate(nextRunDate.getDate() + 7);
        }
      } else {
        nextRunDate.setDate(now.getDate() + ((dayOfWeek + 7 - now.getDay()) % 7));
        nextRunDate.setHours(hours, minutes, 0, 0);
      }
    } else if (schedule.scheduleType === 'monthly') {
      const [hours, minutes] = schedule.scheduleTime.split(':');
      nextRunDate = new Date(now);
      const dayInMonth = parseInt(schedule.dayInMonth);
  
      if (dayInMonth >= 1 && dayInMonth <= 31) {
        nextRunDate.setDate(dayInMonth);
        nextRunDate.setHours(hours, minutes, 0, 0);
  
        if (nextRunDate <= now) {
          nextRunDate.setMonth(nextRunDate.getMonth() + 1);
        }
      } else {
        throw new Error('Invalid dayInMonth value. It should be between 1 and 31.');
      }
    } else {
      logger.debug('Invalid scheduleType. Supported values are "daily", "weekly", or "monthly".');
      return "n/a";
    }
    return nextRunDate;
  }

  
  function displayFormatDate(inDate, future, targetTimeZone, format,addDelta) {
    var formattedDate;
    if (targetTimeZone !== undefined) {
        // Convert to target timezone and apply the provided format or default to ISO format
        formattedDate = moment.tz(inDate, targetTimeZone).format(format || 'YYYY-MM-DD HH:mm:ss');
    } else {
        // Use the provided format or default to ISO without timezone adjustment
        formattedDate = format ? moment(inDate).format(format) : inDate.toISOString().split('.')[0];
    }
    
    var time = inDate.getTime();
    var nowDate = new Date().getTime();
    var delta;
    if (future === true) delta = time - nowDate;
    else delta = nowDate - time;
    delta = delta / 1000;
    const formattedDelta = dateTimeUtils.displaySecs(delta);

    if(addDelta!==undefined && addDelta == false ) return `${formattedDate}`;
    else return `${formattedDate} (${formattedDelta})`;
}

function getSchedules(index){
    if(index===undefined||index===null)
    {
        for(var i=0;i<schedules.length;i++){
            // Ensure scheduleMode defaults to 'classic' for backwards compatibility
            if (!schedules[i].scheduleMode) {
              schedules[i].scheduleMode = 'classic';
            }
            // Ensure triggerType defaults to 'clock' for legacy records
            if (!schedules[i].triggerType) {
              schedules[i].triggerType = (schedules[i].scheduleType === 'cputhreshold' || schedules[i].scheduleType === 'storagethreshold') ? 'legacy_threshold' : 'clock';
            }
            schedules[i].disabled = isScheduleDisabled(schedules[i]);
            if (schedules[i].triggerType === 'clock') {
              schedules[i].nextRunDate = displayFormatDate(new Date(getNextRunDate(schedules[i])),true,serverConfig.server.timezone,"YYYY-MM-DDTHH:mm:ss",true);
            } else if (schedules[i].triggerType === 'rule') {
              schedules[i].nextRunDate = `Polls every ${schedules[i].rulePollIntervalMins || 15}m`;
            } else if (schedules[i].triggerType === 'webhook') {
              schedules[i].nextRunDate = 'Webhook Trigger';
            } else {
              schedules[i].nextRunDate = 'n/a';
            }
             schedules[i].eta=hist.getAverageRuntime(schedules[i].jobName)/60;
            schedules[i].lastUpdated = displayFormatDate(new Date(schedules[i].lastUpdated),false,serverConfig.server.timezone,"YYYY-MM-DDTHH:mm:ss",false);

            //schedules[i].lastRun = displayFormatDate(new Date(schedules[i].lastRun),false); 


        }
        return schedules;
    }
    else
    {
        if(schedules[index]!==undefined && schedules[index]!==null){
          // Ensure scheduleMode defaults to 'classic' for backwards compatibility
          if (!schedules[index].scheduleMode) {
            schedules[index].scheduleMode = 'classic';
          }
          schedules[index].disabled = isScheduleDisabled(schedules[index]);
          if(schedules[index].scheduleType=="daily"||schedules[index].scheduleType=="weekly"||schedules[index].scheduleType=="monthly"){
            schedules[index].nextRunDate = displayFormatDate(new Date(getNextRunDate(schedules[index])),true); 
          }
          else{
            schedules[index].nextRunDate = "n/a";
          }
        //schedules[index].lastRun = displayFormatDate(new Date(schedules[index].lastRun),false); 
        return schedules[index];
        }
        else return {};
    } 
}

function runUpdateJob(agentId,inCommandParams) {
  logger.info(`Running Update job`);
  var jobName = "||INTERNAL||Update " + agentId;
  var description = "Update Agent"
  var agent = agents.getAgent(agentId);
  db.deleteData(agentId + "_" + jobName + "_log").catch(err => 
    logger.debug(`No log entry to delete for job [${jobName}]`)
  );

  if(agent.status!="online")
  {
      var message=`Upgrade failed for agent[${agentId}] - not in the correct state [${agent.status}]`;
      var fullMessage=message + "\n\nData:\n" + JSON.stringify(agent);
      logger.error(message);
      if(serverConfig.server.jobFailEnabled=="true")notifier.sendNotification(`Upgrade Failed [${jobName}]`,message,"ERROR",`/scheduler.html?jobname=${jobName}`);
      var obj = hist.createHistoryItem(jobName,new Date().toISOString(),-1,0,fullMessage,true);
      hist.add(obj);
      return "error";
  }

  logger.info(`Command [${inCommandParams}]`)
  publishExecute(agentId,inCommandParams,"",jobName,"update");
  return "ok";    
}


// Run the job function
async function runJob(jobName, isManual, inData, triggerContextParam = null, rerunFrom = null) {
  try {
    if (isManual === undefined) { isManual = false; }
    logger.info(`Running job: ${jobName}` + (rerunFrom ? ` [rerunFrom: ${rerunFrom}]` : ""));
    var schedItem = getSchedule(jobName);
    if (!schedItem) {
      throw new AppError(`Schedule item not found for job: ${jobName}`, 404);
    }

    logger.info(`Description: ${schedItem.description}`);
    logger.info(`Mode: ${isManual}`);

    if (isScheduleDisabled(schedItem) && !isManualInvocation(isManual)) {
      logger.info(`Job [${jobName}] is disabled - skipping automatic run`);
      return { status: 'skipped', executionId: null };
    }

    // Handle orchestration schedules
    if (schedItem.scheduleMode === 'orchestration') {
      logger.info(`Running orchestration job [${jobName}] with orchestration ID [${schedItem.orchestrationId}]`);
      
      try {
        // Fetch orchestration job to get icon and color
        let orchestrationIcon = 'schema';
        let orchestrationColor = '#000000';
        try {
          const orchestrationModule = require('./orchestration.js');
          const orchestrationJob = await orchestrationModule.getJob(schedItem.orchestrationId);
          orchestrationIcon = orchestrationJob.icon || 'schema';
          orchestrationColor = orchestrationJob.color || '#000000';
          logger.debug(`Fetched orchestration [${schedItem.orchestrationId}] icon=[${orchestrationIcon}] color=[${orchestrationColor}]`);
        } catch (fetchErr) {
          logger.warn(`Could not fetch orchestration [${schedItem.orchestrationId}] details: ${fetchErr.message}. Using defaults.`);
        }
        
        // Generate execution ID for this orchestration run
        const executionId = triggerContextParam && triggerContextParam.executionId ? triggerContextParam.executionId : crypto.randomBytes(8).toString('hex');
        logger.info(`Generated execution ID for orchestration [${schedItem.orchestrationId}]: ${executionId}`);
        
        // Add to running queue to track this orchestration execution
        const runningItem = running.createItem(jobName, new Date().toISOString(), isManual, executionId, null, schedItem.orchestrationId, orchestrationIcon, orchestrationColor);
        running.add(runningItem);
        logger.info(`Added orchestration to running queue: [${jobName}] with execution ID [${executionId}]`);
        
        // Create and cache a stub execution for in-progress tracking (before orchestration starts)
        // This allows the monitor page to query the execution before it completes
        const stubExecution = {
          executionId: executionId,
          jobId: schedItem.orchestrationId,
          orchestrationVersion: 1,
          startTime: new Date().toISOString(),
          endTime: null,
          status: 'running',
          currentNode: null,
          visitedNodes: [],
          scriptOutputs: {},
          conditionEvaluations: {},
          nodeMetrics: {},
          errors: [],
          finalStatus: null,
          manual: isManual,
          triggerContext: triggerContextParam || null,
          isStub: true,  // Mark as temporary stub execution
          rerunFrom: rerunFrom || null  // Track if this is a rerun
        };
        
        // Cache the stub in-memory so monitor page can find it before completion
        saveInProgressExecution(schedItem.orchestrationId, executionId, stubExecution);
        logger.info(`Created and cached in-progress execution stub for [${schedItem.orchestrationId}] with ID [${executionId}]`);
        
        // Create callback to update cache as nodes complete (same pattern as manual execution)
        const onNodeComplete = (executionLog) => {
          updateInProgressExecution(schedItem.orchestrationId, executionId, executionLog);
        };
        
        const orchestration = require('./orchestrationEngine.js');
        const executionLog = await orchestration.executeJob(schedItem.orchestrationId, isManual, executionId, onNodeComplete, triggerContextParam, rerunFrom);
        await orchestration.saveExecutionResult(executionLog);
        
        // If this was a re-run, mark the original execution as having been re-run
        if (rerunFrom) {
          const hist = require('./history.js');
          await hist.markAsRerun(rerunFrom);
        }
        
        // Remove from running queue after completion
        running.removeItemByExecutionId(executionId);
        logger.info(`Removed orchestration from running queue: [${jobName}] with execution ID [${executionId}]`);
        
        // Clean up the in-progress execution cache
        removeInProgressExecution(schedItem.orchestrationId, executionId);
        
        logger.info(`Orchestration [${schedItem.orchestrationId}] execution completed with status [${executionLog.finalStatus}]`);
        return { status: (executionLog.finalStatus === 'success' ? 'ok' : 'error'), executionId: executionLog.executionId || null };
      } catch (err) {
        logger.error(`Error executing orchestration [${schedItem.orchestrationId}]:`, err.message);
        if (serverConfig.server.jobFailEnabled == "true") {
          notifier.sendNotification(
            `Orchestration execution failed [${jobName}]`,
            `Failed to execute orchestration: ${err.message}`,
            "ERROR",
            `/orchestrationList.html`
          );
        }
        return { status: "error", executionId: null };
      }
    }

    // Handle classic schedules (script + agent)
    logger.info(`Agent: ${schedItem.agent}`);
    
    var agent = agents.getAgent(schedItem.agent);
    if (agent === undefined || agent === null) {
      var message = `Unable to execute job [${jobName}] agent[${schedItem.agent}] does not exist - please correct this job`;
      logger.error(message);
      if (serverConfig.server.jobFailEnabled == "true") {
        notifier.sendNotification(`Unable to execute job [${jobName}]`, message, "WARNING", `/scheduler.html?jobname=${jobName}`);
      }
      return { status: "error", executionId: null };
    }

    // Delete old log entry
    try {
      await db.deleteData(schedItem.agent + "_" + jobName + "_log");
    } catch (err) {
      logger.debug(`No log entry to delete for job [${jobName}]`);
    }

    if (agent.status === 'offline') {
      var message = `Unable to execute job [${jobName}] agent[${schedItem.agent}] is offline`;
      var fullMessage = message + "\n\nData:\n" + JSON.stringify(agent);
      logger.error(message);
      if (serverConfig.server.jobFailEnabled == "true") {
        notifier.sendNotification(`Unable to execute job [${jobName}]`, message, "WARNING", `/scheduler.html?jobname=${jobName}`);
      }
      var obj = hist.createHistoryItem(jobName, new Date().toISOString(), 9998, 0, fullMessage, isManual);
      hist.add(obj);
      return { status: "error", executionId: null };
    }

    const concurrencyLimit = agents.getConcurrency(schedItem.agent);
    const runningCount = running.getRunningCountForAgent(schedItem.agent);
    if (runningCount >= concurrencyLimit) {
      logger.warn(`[SCHEDULER] Job [${jobName}] skipped - agent [${schedItem.agent}] at concurrency limit [${runningCount}/${concurrencyLimit}]`);
      return { status: "error", executionId: null };
    }

    var command = schedItem.command;
    var commandParams = schedItem.commandParams;
    logger.info(`Command [${command}]`);

    // Generate execution ID for this job run (or use the one from trigger context if provided)
    const executionId = triggerContextParam && triggerContextParam.executionId ? triggerContextParam.executionId : crypto.randomBytes(8).toString('hex');
    logger.info(`Generated execution ID for job [${jobName}]: ${executionId}`);
    
    if (command !== undefined && command !== null && command.length > 0) {
      const scriptPath = resolveScriptPath(command);
      try {
        if (!scriptPath) {
          const outsideErr = new Error('Script path is outside the scripts directory');
          outsideErr.code = 'EOUTSIDE';
          throw outsideErr;
        }
        const data = await fs.readFile(scriptPath, 'utf8');
        logger.debug('Script file content loaded successfully');

        if (inData !== undefined && inData !== null) {
          commandParams += inData;
        }
        
        publishExecute(schedItem.agent, data, commandParams, jobName, isManual, executionId, triggerContextParam, rerunFrom);
      } catch (err) {
        const logMessage = (err && err.code === 'EOUTSIDE')
          ? `Script path for job [${jobName}] is outside the scripts directory`
          : (err && err.code === 'ENOENT')
            ? `Script cannot be found for job [${jobName}] at path [${scriptPath}]`
            : `Failed to read script for job [${jobName}] at path [${scriptPath}]: ${err.message}`;

        logger.error(logMessage);

        try {
          const histItem = hist.createHistoryItem(
            jobName,
            new Date().toISOString(),
            1,
            0,
            logMessage,
            isManual,
            executionId,
            rerunFrom
          );
          hist.add(histItem);
        } catch (historyErr) {
          logger.warn(`Unable to add history entry for missing script on job [${jobName}]: ${historyErr.message}`);
        }

        if (serverConfig.server.jobFailEnabled == "true") {
          notifier.sendNotification(`Error reading backup script for job [${jobName}]`, logMessage, "ERROR", `/scheduler.html?jobname=${jobName}`);
        }
        return { status: "error", executionId: null };
      }
    } else {
      publishExecute(schedItem.agent, commandParams, "", jobName, isManual, executionId, triggerContextParam, rerunFrom);
    }

    // Add to running queue to track this execution
    const runningItem = running.createItem(jobName, new Date().toISOString(), isManual, executionId, schedItem.agent);
    running.add(runningItem);
    logger.info(`Added job to running queue: [${jobName}] with execution ID [${executionId}]`);

    logger.info(`Returning from runJob with status=ok and executionId=${executionId}`);
    return { status: "ok", executionId: executionId };
  } catch (err) {
    logger.error(`Error in runJob [${jobName}]:`, err.message);
    throw err;
  }
}

// Manual execution of job
async function manualJobRun(index, jobName, rerunFrom = null) {
  try {
    logger.debug('Initiating Manual Run of Job With name [' + jobName + "] and Index [" + index + "]" + (rerunFrom ? ` [RerunFrom: ${rerunFrom}]` : ""));

    if (Array.isArray(jobName)) {
      logger.error("Error: jobName was an array");
      return { status: "error", executionId: null };
    }

    logger.debug("passed array check");

    if (jobName !== undefined && jobName !== null && typeof jobName !== 'string') {
      logger.error("Error in jobname sanitization");
      return { status: "error", executionId: null };
    }

    logger.debug("passed string check");

    if (index !== undefined && index !== null && index.length > 0) {
      jobName = getSchedules(index).jobName;
    }

    logger.debug('Requesting Manual Run of:' + jobName);
    logger.info(`Starting manual run of job: ${jobName}` + (rerunFrom ? ` (rerun from: ${rerunFrom})` : ""));
    const result = await runJob(jobName, "manual", null, null, rerunFrom);
    logger.info(`Manual run completed with result: ${JSON.stringify(result)}`);
    return result;
  } catch (err) {
    logger.error(`Error in manualJobRun:`, err.message);
    throw err;
  }
}


// Schedule jobs based on the configurations in schedules.json
/*

*    *    *    *    *    *
┬    ┬    ┬    ┬    ┬    ┬
│    │    │    │    │    │
│    │    │    │    │    └ day of week (0 - 7) (0 or 7 is Sun)
│    │    │    │    └───── month (1 - 12)
│    │    │    └────────── day of month (1 - 31)
│    │    └─────────────── hour (0 - 23)
│    └──────────────────── minute (0 - 59)
└───────────────────────── second (0 - 59, OPTIONAL)
*/
function createScheduleCron(second, minute, hour, dayOfMonth, month, dayOfweek) {
  return second.toString() + " " + minute.toString() + " " + hour.toString() + " " + dayOfMonth.toString() + " " + month.toString() + " " + dayOfweek.toString();
}

/**
 * Evaluate a rule condition: compare actual metric value against threshold.
 */
function evaluateRuleCondition(actual, operator, threshold) {
  const a = parseFloat(actual);
  const t = parseFloat(threshold);
  if (isNaN(a) || isNaN(t)) return false;
  switch (operator) {
    case '>':  return a > t;
    case '>=': return a >= t;
    case '<':  return a < t;
    case '<=': return a <= t;
    case '==': return a === t;
    case '!=': return a !== t;
    default:
      logger.warn(`[RULE] Unknown operator [${operator}]`);
      return false;
  }
}

/**
 * Poll a rule-based job: query the metric, evaluate the condition, fire if met.
 * Only calls runJob() on a trigger — no history entry is created on non-triggering polls.
 */
async function pollRuleJob(schedItemRef) {
  // Re-fetch the live schedule record to pick up updated ruleLastTriggered
  const schedItem = getSchedule(schedItemRef.jobName) || schedItemRef;
  const { jobName, ruleMetric, ruleCondition, ruleCooldownMins } = schedItem;

  if (!ruleMetric || !ruleCondition) {
    logger.warn(`[RULE] Job [${jobName}] is missing ruleMetric or ruleCondition - skipping poll`);
    return;
  }

  const { agent: ruleAgent, type, path: rulePath, pattern } = ruleMetric;
  const { operator, threshold } = ruleCondition;

  // Skip if agent is offline or not found
  const agent = agents.getAgent(ruleAgent);
  if (!agent || agent.status === 'offline') {
    logger.debug(`[RULE] Job [${jobName}] poll skipped - agent [${ruleAgent}] is ${agent ? agent.status : 'not found'}`);
    return;
  }

  // Skip metric poll if agent is at concurrency limit (avoid stacking rule triggers)
  const ruleConcurrencyLimit = agents.getConcurrency(ruleAgent);
  const ruleRunningCount = running.getRunningCountForAgent(ruleAgent);
  if (ruleRunningCount >= ruleConcurrencyLimit) {
    logger.debug(`[RULE] Job [${jobName}] poll skipped - agent [${ruleAgent}] at concurrency limit [${ruleRunningCount}/${ruleConcurrencyLimit}]`);
    return;
  }

  try {
    const crypto = require('crypto');
    const correlationId = `RuleCheck_${jobName}_${crypto.randomBytes(4).toString('hex')}`;
    const metricConfig = { type };
    if (rulePath) metricConfig.path = rulePath;
    if (pattern) metricConfig.pattern = pattern;

    const agentMsgProcessor = require('./agentMessageProcessor.js');
    const waitPromise = agentMsgProcessor.waitForMetricResult(correlationId, 30000);
    agentComms.sendCommand(ruleAgent, mqttTransport.getCommandTopic(), 'queryMetric', JSON.stringify(metricConfig), correlationId, undefined, false, null);

    const payload = await waitPromise;

    if (payload.error) {
      logger.warn(`[RULE] Job [${jobName}] metric query transport error: ${payload.error}`);
      return;
    }

    const metricValue = payload.result ? payload.result.value : null;
    if (metricValue === null || metricValue === undefined) {
      logger.warn(`[RULE] Job [${jobName}] metric returned no value (agent error: ${payload.result && payload.result.error})`);
      return;
    }

    const conditionMet = evaluateRuleCondition(metricValue, operator, threshold);
    logger.debug(`[RULE] Job [${jobName}] polled: value=${metricValue}, condition: ${metricValue} ${operator} ${threshold} → ${conditionMet ? 'MET' : 'not met'}`);

    if (!conditionMet) {
      return; // No history entry, no action
    }

    // Check cooldown
    const cooldownMs = (ruleCooldownMins || 60) * 60 * 1000;
    const lastTriggered = schedItem.ruleLastTriggered ? new Date(schedItem.ruleLastTriggered).getTime() : 0;
    const now = Date.now();
    if (lastTriggered && (now - lastTriggered) < cooldownMs) {
      const remainingMins = Math.ceil((cooldownMs - (now - lastTriggered)) / 60000);
      logger.debug(`[RULE] Job [${jobName}] condition MET but in cooldown - ${remainingMins}m remaining before next trigger`);
      return;
    }

    // FIRE: condition met and not in cooldown
    logger.info(`[RULE] Job [${jobName}] TRIGGERED: ${metricValue} ${operator} ${threshold}. Firing action.`);

    // Persist ruleLastTriggered before firing to prevent concurrent re-triggers
    // Use persistSchedules (not writeSchedules) to avoid resetting all rule polling intervals
    const schedIndex = getScheduleIndex(jobName);
    if (schedIndex !== -1) {
      schedules[schedIndex].ruleLastTriggered = new Date().toISOString();
      if (definitionStore.getBackend() === 'hybrid') {
        persistSingleSchedule(schedules[schedIndex]).catch(err => logger.warn(`[RULE] Failed to persist ruleLastTriggered for [${jobName}]: ${err.message}`));
      } else {
        persistSchedules().catch(err => logger.warn(`[RULE] Failed to persist ruleLastTriggered for [${jobName}]: ${err.message}`));
      }
    }

    // Create trigger context for the job execution
    const executionId = crypto.randomBytes(8).toString('hex');
    const ruleId = `rule_${jobName}_${Date.now()}`;
    const metricResultWithContext = {
      type,
      agent: ruleAgent,
      path: rulePath,
      pattern,
      value: metricValue,
      previousValue: null
    };
    
    const ruleTriggerContext = triggerContext.createRuleTriggerContext(
      ruleId,
      jobName,
      metricResultWithContext,
      ruleCondition,
      executionId
    );

    runJob(jobName, 'rule', null, ruleTriggerContext).catch(err =>
      logger.error(`[RULE] Error executing triggered job [${jobName}]: ${err.message}`)
    );

  } catch (err) {
    logger.warn(`[RULE] Job [${jobName}] poll error: ${err.message}`);
  }
}

async function scheduleJobs() {
  try {
    logger.debug("Scheduling jobs - clearing existing schedules");
    
    // Clear existing rule polling intervals
    ruleIntervals.forEach((handle, name) => {
      clearInterval(handle);
      logger.debug(`Cleared rule interval for job [${name}]`);
    });
    ruleIntervals.clear();

    if (nodeschedule.scheduledJobs !== undefined && nodeschedule.scheduledJobs !== null) {
      var jobList = nodeschedule.scheduledJobs;
      for (jobName in jobList) {
        var EachJobObject = nodeschedule.scheduledJobs[jobName];
        logger.debug(`Cancelling Schedule Item [${jobName}]`);
        EachJobObject.cancel();
      }
    }

    // Now for each job, set up the appropriate trigger
    schedules.forEach((schedItem) => {
      const { jobName, scheduleType, scheduleTime, dayOfWeek, dayInMonth } = schedItem;

      if (isScheduleDisabled(schedItem)) {
        logger.info(`Job [${jobName}] is disabled - it will not run until enabled`);
        return;
      }

      // Resolve effective triggerType — default legacy records to 'clock'
      const effectiveTriggerType = schedItem.triggerType || (
        (scheduleType === 'daily' || scheduleType === 'weekly' || scheduleType === 'monthly') ? 'clock' : 'legacy_threshold'
      );

      if (effectiveTriggerType === 'clock') {
        var scheduleCron = "";
        var sHour = parseInt(scheduleTime.split(':')[0]);
        var sMin = parseInt(scheduleTime.split(':')[1]);

        switch (scheduleType) {
          case "daily":
            scheduleCron = createScheduleCron(0, sMin, sHour, "*", "*", "*");
            break;
          case "weekly":
            scheduleCron = createScheduleCron(0, sMin, sHour, "*", "*", dayOfWeek);
            break;
          case "monthly":
            scheduleCron = createScheduleCron(0, sMin, sHour, dayInMonth, "*", "*");
            break;
          default:
            logger.warn(`[SCHEDULER] Job [${jobName}] has triggerType 'clock' but unrecognised scheduleType [${scheduleType}] - skipping`);
            return;
        }

        logger.debug(`Scheduling clock job [${jobName}] with cron [${scheduleCron}]`);
        nodeschedule.scheduleJob(scheduleCron, function () {
          runJob(jobName, "schedule").catch(err =>
            logger.error(`Error in scheduled job [${jobName}]:`, err.message)
          );
        });

      } else if (effectiveTriggerType === 'rule') {
        const pollMs = (schedItem.rulePollIntervalMins || 15) * 60 * 1000;
        logger.info(`[RULE] Setting up polling for job [${jobName}] every ${schedItem.rulePollIntervalMins || 15} mins`);
        const handle = setInterval(() => {
          pollRuleJob(schedItem).catch(err =>
            logger.error(`[RULE] Unhandled error in rule poll for [${jobName}]: ${err.message}`)
          );
        }, pollMs);
        ruleIntervals.set(jobName, handle);

        // Initial poll shortly after startup (allow connections to settle)
        setTimeout(() => {
          pollRuleJob(schedItem).catch(err =>
            logger.error(`[RULE] Unhandled error in initial rule poll for [${jobName}]: ${err.message}`)
          );
        }, 15000);
      
      } else if (effectiveTriggerType === 'webhook') {
            logger.info(`Job [${jobName}] is a webhook trigger - no schedule setup required`);
      } else {
        // legacy_threshold — these used the old thresholdJobs system, now deprecated
        logger.warn(`[DEPRECATED] Job [${jobName}] uses deprecated trigger type [${scheduleType}]. Please recreate it as a Rule-based job in the Jobs screen.`);
      }
    });

    logger.info(`Scheduled ${schedules.length} jobs successfully`);
} catch (err) {
  logger.error("Error scheduling jobs:", err.message);
  throw new AppError(`Failed to schedule jobs: ${err.message}`, 500);
}
} 

function isSameDate(date1, date2) {
  return date1.getUTCFullYear() === date2.getUTCFullYear() &&
  date1.getUTCMonth() === date2.getUTCMonth() &&
  date1.getUTCDate() === date2.getUTCDate();
}

function filterJobsForToday(jobs) {
  var today = new Date();
  return filterJobsWithDate(jobs,today);
}

function filterJobsWithDate(jobs,inDate) {
  filterDate = new Date(moment.tz(inDate, serverConfig.server.timezone));
  const currentDayOfWeek = filterDate.getDay(); // Sunday is 0, Saturday is 6
  const currentDayOfMonth = filterDate.getDate(); // Gets today's date in the month
  
  return jobs.filter(job => {
    const { scheduleType, dayOfWeek, dayInMonth } = job;

    if (isScheduleDisabled(job)) {
      return false;
    }

    // Daily jobs always run
    if (scheduleType === 'daily') {
      return true;
    }
    
    // Weekly jobs should run if today is the scheduled day
    if (scheduleType === 'weekly' && parseInt(dayOfWeek) === currentDayOfWeek) {
      return true;
    }
    
    // Monthly jobs should run if today is the scheduled day of the month
    if (scheduleType === 'monthly' && parseInt(dayInMonth) === currentDayOfMonth) {
      return true;
    }

    // Otherwise, the job does not run today
    return false;
  });
}


async function setScheduleDisabled(jobName, disabled) {
  const index = getScheduleIndex(jobName);
  if (index < 0) {
    throw new AppError(`Schedule item not found for job: ${jobName}`, 404);
  }

  schedules[index].disabled = disabled === true || disabled === 'true';
  schedules[index].lastUpdated = new Date().toISOString();
  delete schedules[index].nextRunDate;

  if (definitionStore.getBackend() === 'hybrid') {
    await writeSingleSchedule(schedules[index]);
  } else {
    await writeSchedules();
  }

  logger.info(`Job [${jobName}] ${schedules[index].disabled ? 'disabled' : 'enabled'}`);
  return schedules[index];
}

function getTodaysScheduleCount(){
  var filtered = filterJobsForToday(schedules);
  return filtered.length;
}

function getLast7DaysScheduleCount(){
  var filtered = filterJobsForToday(schedules);
  

  var today = new Date();
  var tminus1 = new Date().setDate(today.getDate()-1);
  var tminus2 = new Date().setDate(today.getDate()-2);
  var tminus3 = new Date().setDate(today.getDate()-3);
  var tminus4 = new Date().setDate(today.getDate()-4);
  var tminus5 = new Date().setDate(today.getDate()-5);
  var tminus6 = new Date().setDate(today.getDate()-6);

  var todayCount = filterJobsWithDate(schedules,today).length;
  var tminus1Count = filterJobsWithDate(schedules,tminus1).length;
  var tminus2Count = filterJobsWithDate(schedules,tminus2).length;
  var tminus3Count = filterJobsWithDate(schedules,tminus3).length;
  var tminus4Count = filterJobsWithDate(schedules,tminus4).length;
  var tminus5Count = filterJobsWithDate(schedules,tminus5).length;
  var tminus6Count = filterJobsWithDate(schedules,tminus6).length;

  var array=[tminus6Count,tminus5Count,tminus4Count,tminus3Count,tminus2Count,tminus1Count,todayCount];
  return array;
}

module.exports = { init, getSchedule, getSchedules, getScheduleIndex, upsertSchedule, setScheduleDisabled, deleteSchedule,deleteScheduleAtIndex, clearSchedules, migrateSchedulesToFilesystem, manualJobRun, runJob, getNextRunDate, runUpdateJob, getTodaysScheduleCount,getLast7DaysScheduleCount, getInProgressExecution, saveInProgressExecution, removeInProgressExecution, updateInProgressExecution };