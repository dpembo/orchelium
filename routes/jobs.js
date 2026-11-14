module.exports = function register(app) {
app.get('/rest/running', User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_VIEW), asyncHandler(async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html?not+authenticated');
  }
  logger.debug('Fetching all running jobs');
  
  const runningJobs = running.getItems();
  res.json({ 
    success: true, 
    count: runningJobs.length,
    jobs: runningJobs 
  });
}));

app.delete('/rest/running/:index', User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_EDIT), asyncHandler(async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html?not+authenticated');
  }
  const index = parseInt(req.params.index);
  logger.info(`Deleting running job at index: ${index}`);
  
  try {
    running.removeItemByIndex(index);
    res.json({ success: true, message: `Running job at index ${index} removed` });
  } catch (err) {
    logger.error(`Failed to delete running job at index ${index}: ${err.message}`);
    res.status(400).json({ success: false, message: `Error deleting running job: ${err.message}` });
  }
}));

app.delete('/rest/running/byName/:jobName', User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_EDIT), asyncHandler(async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html?not+authenticated');
  }
  const jobName = req.params.jobName;
  logger.info(`Deleting running job: ${jobName}`);
  
  try {
    running.removeItem(jobName);
    res.json({ success: true, message: `Running job [${jobName}] removed` });
  } catch (err) {
    logger.error(`Failed to delete running job [${jobName}]: ${err.message}`);
    res.status(400).json({ success: false, message: `Error deleting running job: ${err.message}` });
  }
}));

app.delete('/rest/running/byExecutionId/:executionId', User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_EDIT), asyncHandler(async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html?not+authenticated');
  }
  const executionId = req.params.executionId;
  logger.info(`Deleting running job with executionId: ${executionId}`);
  
  try {
    running.removeItemByExecutionId(executionId);
    res.json({ success: true, message: `Running job with executionId [${executionId}] removed` });
  } catch (err) {
    logger.error(`Failed to delete running job with executionId [${executionId}]: ${err.message}`);
    res.status(400).json({ success: false, message: `Error deleting running job: ${err.message}` });
  }
}));

app.delete('/rest/running', User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_EDIT), asyncHandler(async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html?not+authenticated');
  }
  logger.info('Deleting all running jobs');
  
  try {
    const result = await running.deleteAll();
    res.json({ success: true, message: `All running jobs deleted`, deletedCount: result.deletedCount });
  } catch (err) {
    logger.error(`Failed to delete all running jobs: ${err.message}`);
    res.status(400).json({ success: false, message: `Error deleting running jobs: ${err.message}` });
  }
}));

app.get('/about.html',User.isAuthenticated, async (req, res) => {

  const user = req.session.user;
  if (!user) return res.redirect('/register.html');

  res.render('about.ejs', { 
    user: User,
    version: version,
    csrf: req.csrfToken(),
   });
  
});

app.get('/runList/data',User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_VIEW), asyncHandler(async (req, res) => {
  var format = "json"
  if(req.query.format !==undefined && req.query.format!=null)format = req.query.format;

  var runningList = running.getItemsUsingTZ();
  var schedules = scheduler.getSchedules();
  
  logger.debug(`[RunList] Retrieved ${runningList.length} running items from cache`);
  runningList.forEach((item, idx) => {
    logger.debug(`  [${idx}] jobName=${item.jobName}, executionId=${item.executionId}, orchestrationId=${item.orchestrationId}`);
  });
  
  // Load orchestration definitions for icon/color lookup
  let orchestrations = {};
  try {
    const orchestrationModule = require('../orchestration.js');
    const allOrchestrations = await orchestrationModule.getAllJobs();
    orchestrations = allOrchestrations;
  } catch (err) {
    logger.debug(`Could not load orchestrations for icon/color lookup: ${err.message}`);
  }

  for(var x=0;x<runningList.length;x++)
  {
    var workJob = runningList[x].jobName;
    
    // If this is an orchestration, fetch icon/color from orchestration definition
    if (runningList[x].orchestrationId && orchestrations[runningList[x].orchestrationId]) {
      const orchestrationJob = orchestrations[runningList[x].orchestrationId];
      const currentVersionData = orchestrationJob.versions[orchestrationJob.versions.length - 1];
      runningList[x].icon = orchestrationJob.icon || 'schema';
      runningList[x].color = orchestrationJob.color || '#000000';
    } else {
      // For regular jobs, look up from schedules
      for(var y=0;y<schedules.length;y++){
        if(workJob == schedules[y].jobName){
          runningList[x].icon=schedules[y].icon;
          runningList[x].color=schedules[y].color;
          break;
        }
      }
    }
  }

  logger.debug(`[RunList] After enrichment: ${runningList.length} items`);
  runningList.forEach((item, idx) => {
    logger.debug(`  [${idx}] jobName=${item.jobName}, executionId=${item.executionId}, orchId=${item.orchestrationId}, icon=${item.icon}`);
  });

  if(format.toUpperCase()=="HTML"){
    // Log orchestration running items for debugging
    const orchestrationItems = runningList.filter(item => item.orchestrationId);
    if (orchestrationItems.length > 0) {
      logger.debug(`[RunList] Rendering ${orchestrationItems.length} orchestration items:`);
      orchestrationItems.forEach(item => {
        logger.debug(`  - Job: ${item.jobName}, OrchId: ${item.orchestrationId}, ExecId: ${item.executionId}`);
      });
    }
    res.render('runningListData',{
      runningList: runningList,
      schedules: schedules,
      csrf: req.csrfToken(),
    });
  }
  else
  {
    var data={}
    data.running=runningList;
    data.schedules=schedules;
    res.send(data)
  }
}));

app.get('/historyList/data',User.isAuthenticated, User.requirePermission(PERMISSIONS.HISTORY_VIEW), async (req, res) => {

  var format = "json"
  if(req.query.format !==undefined && req.query.format!=null)format = req.query.format;

  logger.info(`Format is ${format}`);
  var historyList = await hist.getItemsGroupedByOrchestration();
  var runningList = running.getItems();
  var schedules = scheduler.getSchedules();

  /*logger.debug(historyList);
  logger.debug("----");
  logger.debug(runningList);
  logger.debug("----");
  logger.debug(schedules);
  logger.debug("----");*/
  
  // Apply icons and colors from schedules
  function applyScheduleStyle(item) {
    var workJob = item.jobName;
    for(var y=0;y<schedules.length;y++){
      if(workJob == schedules[y].jobName){
        item.icon=schedules[y].icon;
        item.color=schedules[y].color;
        item.description=schedules[y].description;
        break;
      }
    }
    if(!item.icon ||  item.icon.trim()===''){
      item.icon = item.isOrchestration ? "hub" : "remove_circle";
      item.color = item.isOrchestration ? "#2196F3" : "#AAAAAA";
    }
  }
  
  // Pre-fetch orchestration success percentages to avoid N+1 queries
  // Get all execution history once and compute percentages in-memory
  let orchestrationSuccessMap = {};
  try {
    const allExecutions = await db.getData('ORCHESTRATION_EXECUTIONS');
    if (allExecutions) {
      for (const jobId in allExecutions) {
        const executions = allExecutions[jobId] || [];
        if (executions.length > 0) {
          const successCount = executions.filter(e => e.finalStatus === 'success').length;
          orchestrationSuccessMap[jobId] = Math.round((successCount / executions.length) * 100);
        } else {
          orchestrationSuccessMap[jobId] = '-';
        }
      }
    }
  } catch (err) {
    logger.warn(`Error pre-fetching orchestration execution history: ${err.message}`);
  }
  
  for(var x=0;x<historyList.length;x++)
  {
    applyScheduleStyle(historyList[x]);
    
    // Also apply styles to child nodes if this is an orchestration item
    if (historyList[x].children) {
      for (var c = 0; c < historyList[x].children.length; c++) {
        applyScheduleStyle(historyList[x].children[c]);
      }
      
      // Calculate total runtime from children for orchestration items
      var totalRunTime = 0;
      historyList[x].children.forEach(function(child) {
        if (child.runTime) totalRunTime += parseInt(child.runTime) || 0;
      });
      historyList[x].runTime = totalRunTime;
    }
    
    // Set default runTime if not present (for sorting)
    if (historyList[x].runTime === undefined) {
      historyList[x].runTime = 0;
    }
    
    // Use pre-computed success percentage (calculated once before loop)
    if (historyList[x].isOrchestration) {
      historyList[x].successPercentage = orchestrationSuccessMap[historyList[x].jobId] || '-';
    }
  }

  var refresh = req.query["refresh"];
  if (refresh === undefined) refresh = 0;
  const sort = req.query.sort || 'jobName'; // Default sorting by jobName
  const order = req.query.order || 'asc'; // Default sorting order is ascending

  logger.info("GetHistoryListData: " + sort + " " + order);
  let sortedData;

  // Note: getItemsGroupedByOrchestration already returns data sorted by date (newest first)
  // Adjusting sort logic for grouped data
  if (sort === 'jobName') {
    sortedData = [...historyList].sort((a, b) => {
      if (order === 'asc') {
        return a.jobName.localeCompare(b.jobName);
      } else {
        return b.jobName.localeCompare(a.jobName);
      }
    });
  } else if (sort === 'runDate') {
    sortedData = [...historyList].sort((a, b) => {
      const dateA = new Date(a.runDate);
      const dateB = new Date(b.runDate);
      return order === 'asc' ? dateA - dateB : dateB - dateA;
    });
  } else if (sort === 'runTime') {
    sortedData = [...historyList].sort((a, b) => {
      return order === 'asc' ? a.runTime - b.runTime : b.runTime - a.runTime;
    });
  } else if (sort === 'status') {
    sortedData = [...historyList].sort((a, b) => {
      return order === 'asc' ? a.returnCode - b.returnCode : b.returnCode - a.returnCode;
    });
  } else {
    // Default sort already by newest first
    sortedData = [...historyList];
  }
 
  //var index=req.query.index;
  //var redir=req.query.redir;

  if(format.toUpperCase()=="HTML"){
    res.render('historyListData',{
      historyList: sortedData,
      schedules: schedules,
      sort: sort,
      order: order,  
      refresh: refresh,
      csrf: req.csrfToken(),
    });
  }
  else
  {
    var data={}
    data.history=sortedData;
    data.running=runningList;
    data.schedules=schedules;
    res.send(data)
  }
});

app.delete('/rest/history/clear', User.isAuthenticated, User.requirePermission(PERMISSIONS.HISTORY_RESUBMIT), asyncHandler(async (req, res) => {
  logger.info('Clearing all history items');
  try {
    await hist.clearHistory();
    res.json({ 
      success: true, 
      message: 'History cleared successfully' 
    });
  } catch (err) {
    logger.error('Error clearing history: ' + err.message);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to clear history: ' + err.message 
    });
  }
}));

app.get('/history.html',User.isAuthenticated, User.requirePermission(PERMISSIONS.HISTORY_VIEW), (req, res) => {

  var refresh = req.query["refresh"];
  if (refresh === undefined) refresh = 0;
  const sort = req.query.sort || 'jobName'; // Default sorting by jobName
  const order = req.query.order || 'asc'; // Default sorting order is ascending
  
  res.render('history',{
    refresh: refresh,
    sort:sort,
    order:order,
    csrf: req.csrfToken(),
  });
});

app.post('/delete-schedule.html', validateCsrf, User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_DELETE), (req, res) => {
  logger.warn("Deleting Schedule*******************");
  var jobName = req.body.jobName;
  var redir = safeRedirect(req.body.redir, '/scheduleList.html');
  logger.info("Deleting Schedule with jobName: " + jobName);
  scheduler.deleteSchedule(jobName);
  logger.debug("schedule deleted");
  const separator = redir.includes('?') ? '&' : '?';
  res.redirect(redir + separator + 'message=' + encodeURIComponent('Deleted schedule: ' + jobName));
});

app.get('/scheduler.html',User.isAuthenticated, User.requireAnyPermission([PERMISSIONS.JOBS_CREATE, PERMISSIONS.JOBS_EDIT]), async (req, res) => {
  logger.info("Scheduler.html");
  var index=req.query.index;
  var copyIndex=req.query.copyIndex;
  var internal=req.query.internal;
  var jobname=req.query.jobname;
  var redir=safeRedirect(req.query.redir, '');
  var time=req.query.time;
  var day=req.query.day;
  var type=req.query.type;
  var copy = false;
  if(copyIndex!==undefined && copyIndex!==null){
    index = copyIndex;
    copy = true;    
  }
 
  if(index===undefined || index === null )index = scheduler.getScheduleIndex(jobname);
  var scheduleItem = scheduler.getSchedules(index);
 
  if(copy){
    scheduleItem = JSON.parse(JSON.stringify(scheduleItem));
    scheduleItem.index=undefined;
    index = undefined;
    scheduleItem.jobName = "Copy of " + scheduleItem.jobName;
  };
  //refresh the scripts list
  var scriptsMeta = refreshScripts();
  var scripts = [];
  var scriptsDesc =[];

  for(var x=0;x<scriptsMeta.length;x++){
    scripts.push(scriptsMeta[x].data.filename);
    scriptsDesc.push(scriptsMeta[x].data.metaDescription);
  }

  //Get the schedule
  let orchestrations = {};
  try {
    orchestrations = await orchestration.getAllJobs() || {};
  } catch (err) {
    logger.debug('Unable to fetch orchestrations for scheduler:', err.message);
  }

  res.render('scheduler',{
    scripts: scripts,
    scriptsDesc: scriptsDesc,
    agents: agents.getDict(),
    schedule: scheduleItem,
    copy:copy,
    index:index,
    csrf: req.csrfToken(),  
    redir:redir,
    type:type,
    day:day,
    time:time,
    icons:serverConfig.job_icons,
    internal: internal,
    orchestrations: orchestrations
  });
});

app.get('/scheduleInfo/data/name',User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_VIEW), async(req, res) => {
  const user = req.session.user;
  
  if (!user) {
   
    return res.redirect('/register.html?not+authenticated');
  }
  
  const jobname=req.query.jobname;
  const executionId = req.query.executionId;  // Optional: look up specific execution
  var index = scheduler.getScheduleIndex(jobname);
  var data = await getSchedulerData(index, executionId);
  res.setHeader("Content-Type","Application/JSON");
  res.send(data);
});

app.get('/scheduleInfo/data/:index',User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_VIEW), async(req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html?not+authenticated');
  }

  const index = req.params.index;
  const executionId = req.query.executionId;  // Optional: look up specific execution
  
  var data = await getSchedulerData(index, executionId);
  
  res.setHeader("Content-Type","Application/JSON");
  res.send(data);
});

app.get('/scheduleInfo.html',User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_VIEW), async(req, res) => {
  var index=req.query.index;
  var redir=safeRedirect(req.query.redir, '');
  var jobname=req.query.jobname;

  if(jobname!==undefined && jobname !==null && jobname.length>0){
    //Find the index from the name
    logger.debug("Getting Sechedule for: " + jobname);
    index = scheduler.getScheduleIndex(jobname);
    logger.debug("Found job at index: " + index);
  }

  var refresh = req.query["refresh"];
  if (refresh === undefined) refresh = 0;
  var schedule = scheduler.getSchedules(index);

  //return logEvent.name + "_" + logEvent.jobName + "_" + type;
  var key1 = schedule.agent + "_" + schedule.jobName + "_" + "stats";
  var key2 = schedule.agent + "_" + schedule.jobName + "_" + "log";

  var stats = null;
  var log = null;
  try {
    stats = await db.simpleGetData(key1);
    log = await db.simpleGetData(key2);
  }
  catch(err){
     logger.warn("Unable to find stats/log data");
     logger.warn(JSON.stringify(err));
  }

  if(stats!=null){
    stats.current.etaDisplay = dateTimeUtils.displaySecs(stats.current.eta);
    stats.etaRollingAvgDisplay = dateTimeUtils.displaySecs(stats.etaRollingAvg);
  }
  else{
    if(stats !== undefined && stats!==null && (stats.etaRollingAvg == undefined || stats.etaRollingAvg ==null)) stats.etaRollingAvg=0;
  }

  res.render('scheduleInfo',{
    //scripts: scripts,
    agent: agents.getAgent(schedule.agent),
    schedule: schedule,
    index:index,
    redir:redir,
    stats:stats,
    csrf: req.csrfToken(),
    log:log,
    refresh,refresh,
    hist:hist,
  });  
});

app.post('/scheduler.html', validateCsrf, User.isAuthenticated, User.requireAnyPermission([PERMISSIONS.JOBS_CREATE, PERMISSIONS.JOBS_EDIT]), asyncHandler(async (req, res) => {
  
  let { jobName, colour, description, scheduleType, scheduleTime, dayOfWeek,
    dayInMonth, agentselect, agentcommand, commandparams, index, redir, icon, scheduleMode, orchestrationId,
    // Rule trigger fields
    triggerType,
    ruleMetricAgent, ruleMetricType, ruleMetricPath, ruleMetricPattern,
    ruleConditionOperator, ruleConditionThreshold,
    rulePollIntervalMins, ruleCooldownMins,
    disabled
  } = req.body;
    jobName = sanitizeHtml(jobName);
    colour = sanitizeHtml(colour);

    // Normalize and validate scheduleMode to allow-list
    scheduleMode = scheduleMode === 'orchestration' ? 'orchestration' : 'classic';

    // Normalize and validate triggerType to allow-list
    const allowedTriggerTypes = ['rule', 'webhook'];
    triggerType = allowedTriggerTypes.includes(triggerType) ? triggerType : 'clock';

    // Sanitize orchestrationId
    if (orchestrationId) {
      orchestrationId = sanitizeHtml(orchestrationId);
    }

    // Build rule config objects when triggerType is 'rule'
    let ruleMetric = null;
    let ruleCondition = null;
    if (triggerType === 'rule') {
      if (!ruleMetricAgent || !ruleMetricType || !ruleConditionOperator || ruleConditionThreshold === undefined) {
        return res.status(400).send('Error: Agent, metric type, operator, and threshold are required for rule-based jobs');
      }
      const allowedMetricTypes = ['cpu', 'mount_usage', 'dir_size', 'file_size', 'file_count', 'file_age'];
      const allowedOperators = ['>', '>=', '<', '<=', '==', '!='];
      if (!allowedMetricTypes.includes(ruleMetricType)) {
        return res.status(400).send('Error: Invalid metric type');
      }
      if (!allowedOperators.includes(ruleConditionOperator)) {
        return res.status(400).send('Error: Invalid condition operator');
      }
      ruleMetric = {
        agent: sanitizeHtml(ruleMetricAgent),
        type: ruleMetricType,
        path: ruleMetricPath ? sanitizeHtml(ruleMetricPath) : null,
        pattern: ruleMetricPattern ? sanitizeHtml(ruleMetricPattern) : null,
      };
      
      // Validate and parse ruleConditionThreshold
      const parsedThreshold = parseFloat(ruleConditionThreshold);
      if (isNaN(parsedThreshold) || !isFinite(parsedThreshold)) {
        return res.status(400).json({ 
          success: false, 
          message: 'Error: Threshold must be a valid finite number' 
        });
      }
      ruleCondition = {
        operator: ruleConditionOperator,
        threshold: parsedThreshold,
      };
      
      // Validate and parse rulePollIntervalMins
      const pollInterval = parseInt(rulePollIntervalMins, 10);
      if (isNaN(pollInterval) || pollInterval < 1) {
        return res.status(400).json({ 
          success: false, 
          message: 'Error: Poll interval must be a positive integer (minimum 1 minute)' 
        });
      }
      rulePollIntervalMins = pollInterval;
      
      // Validate and parse ruleCooldownMins
      const cooldown = parseInt(ruleCooldownMins, 10);
      if (isNaN(cooldown) || cooldown < 1) {
        return res.status(400).json({ 
          success: false, 
          message: 'Error: Cooldown must be a positive integer (minimum 1 minute)' 
        });
      }
      ruleCooldownMins = cooldown;
      
      // Rule jobs have no clock schedule
      scheduleType = null;
      scheduleTime = null;
    }

    // Validate based on schedule mode
    if (scheduleMode === 'orchestration') {
      if (!orchestrationId || orchestrationId.length === 0) {
        return res.status(400).send('Error: Orchestration ID is required for orchestration schedules');
      }
      else{
        const job = await orchestration.getJob(orchestrationId);
        icon = job.icon;
        colour = job.color;
      }
    } else {
      // Classic mode validation
      if (!agentselect || agentselect.length === 0) {
        return res.status(400).send('Error: Agent is required for classic schedules');
      }
      if (agentcommand != null && agentcommand !== '' && (typeof agentcommand !== 'string' || !resolveScriptPath(agentcommand))) {
        return res.status(400).send('Error: Script path is outside the scripts directory');
      }
    }

    const hasDisabledField = Object.prototype.hasOwnProperty.call(req.body, 'disabled');
    const disabledFlag = hasDisabledField ? (disabled === true || disabled === 'true') : undefined;

    scheduler.upsertSchedule(index, jobName, colour, description, scheduleType, scheduleTime, dayOfWeek,
      dayInMonth, agentselect, agentcommand, commandparams, icon, scheduleMode, orchestrationId,
      triggerType, ruleMetric, ruleCondition, rulePollIntervalMins, ruleCooldownMins, disabledFlag);

  if(redir===undefined || redir === null || redir.length<=0)redir="/scheduleList.html";
  res.redirect(safeRedirect(redir, '/scheduleList.html'));
}));

app.get('/scheduleList.html',User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_VIEW), asyncHandler(async (req, res) => {
  //console.time('getSchedules');
  const schedules = scheduler.getSchedules();
  //console.timeEnd('getSchedules');
  //console.time('getHitstItems');
  const items = hist.getItemsUsingTZ();
  //console.timeEnd('getHitstItems');

  const lastRuns = {};
  for (let i = items.length - 1; i >= 0; i--) {
      const item = items[i];
      if (!lastRuns[item.jobName]) { // Only set if not already found (first from end = last run)
          lastRuns[item.jobName] = item;
      }
  }

  // Pre-fetch orchestration executions once so orchestration schedule rows can
  // use latest execution state for row highlighting.
  let orchestrationExecutions = {};
  try {
    orchestrationExecutions = await db.getData('ORCHESTRATION_EXECUTIONS');
  } catch (err) {
    orchestrationExecutions = {};
  }

  // Enhance schedules with precomputed lastRun
  const processedSchedules = await Promise.all(schedules.map(async (schedule) => {
      let successPercentage;
      let returnCode = lastRuns[schedule.jobName] ? lastRuns[schedule.jobName].returnCode : '0';

      if (schedule.scheduleMode === 'orchestration' && schedule.orchestrationId) {
        successPercentage = await hist.getOrchestrationSuccessPercentage(schedule.orchestrationId);

        const executions = orchestrationExecutions[schedule.orchestrationId] || [];
        if (executions.length > 0) {
          const latestExecution = executions[executions.length - 1];
          returnCode = latestExecution.finalStatus === 'success' ? '0' : '1';
        }
      } else {
        successPercentage = hist.getSuccessPercentage(schedule.jobName);
      }

      return {
        ...schedule,
        returnCode: returnCode,
        successPercentage: successPercentage
      };
  }));

  //console.time('render');
  res.render('scheduleList', {
    schedules: processedSchedules,
    hist:hist,
    csrf: req.csrfToken(), 
  });
  //console.timeEnd('render');
}));

app.get('/scheduleListCalendar.html',User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_VIEW), (req, res) => {
  const schedules = scheduler.getSchedules();
  viewType = req.query.viewType;
  inDate = req.query.inDate;
  if(viewType === undefined || viewType === null)viewType="weekly";
  res.render('scheduleListCalendar', {
    schedules: schedules,
    inViewType: viewType,
    inDate: inDate,
    csrf: req.csrfToken(),
  });
});

app.post('/runSchedule.html', validateCsrf, User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_EDIT), asyncHandler(async (req, res) => {
  var index=req.body.index;
  var jobname=req.body.jobname;
  var redir=safeRedirect(req.body.redir, '');
  var rerunFrom=req.body.rerunFrom;  // NEW: Track which failed execution prompted this re-run
  logger.info(`Running Schedule - Index: ${index ?? 'undefined'}, Jobname: ${jobname ?? 'undefined'}, RerunFrom: ${rerunFrom ?? 'undefined'}`);
  
  if(!redir) redir="/scheduleInfo.html?index=" + encodeURIComponent(index || '') + "&refresh=3";
  
  let errorMessage = null;
  
  // Get the schedule - either by index or by jobname
  const schedules = scheduler.getSchedules();
  let schedule = null;
  
  if (index !== undefined && index !== null) {
    schedule = schedules[index];
  }
  
  // If not found by index, look up by jobname
  if (!schedule && jobname) {
    schedule = scheduler.getSchedule(jobname);
  }
  
  // Handle orchestration mode
  if (schedule && schedule.scheduleMode === 'orchestration') {
    try {
      const orchestrationId = schedule.orchestrationId;
      if (!orchestrationId) {
        errorMessage = "Orchestration not configured for this schedule";
      } else {
        // Generate execution ID immediately (same as orchestrationEngine does)
        const crypto = require('crypto');
        const executionId = crypto.randomBytes(8).toString('hex');
        
        // Create a stub execution record (NOT saved to permanent history yet)
        // This is cached in-memory so monitor can find it immediately
        const stubExecution = {
          jobId: orchestrationId,
          executionId: executionId,
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
          manual: true,
          isStub: true,  // Mark as temporary stub execution
          rerunFrom: rerunFrom || null  // Track if this is a rerun
        };
        
        // Cache the stub in-memory (not in permanent history)
        saveInProgressExecution(orchestrationId, executionId, stubExecution);
        logger.info(`Created in-progress execution for [${orchestrationId}] with ID [${executionId}]`);
        
        // Get the orchestration job to get its name for the running queue
        let orchestrationName = orchestrationId; // fallback to orchestrationId
        try {
          const job = await orchestration.getJob(orchestrationId);
          if (job && job.name) {
            orchestrationName = job.name;
          }
        } catch (err) {
          logger.warn(`Could not get orchestration name for [${orchestrationId}]: ${err.message}`);
        }
        
        // Add to running queue so it shows in running list with link to monitor
        // Note: Orchestrations span multiple agents, so we pass null for agentName (concurrency not enforced per-agent)
        const runningItem = running.createItem(orchestrationName, new Date().toISOString(), 'manual', executionId, null);
        runningItem.orchestrationId = orchestrationId;
        running.add(runningItem);
        logger.info(`Added orchestration to running queue: [${orchestrationName}] with execution [${executionId}]`);
        
        // Create callback to update cache as nodes complete
        const onNodeComplete = (executionLog) => {
          updateInProgressExecution(orchestrationId, executionId, executionLog);
        };
        
        // Start orchestration execution asynchronously (fire-and-forget)
        // Pass executionId so engine uses the same ID for websocket events
        // Pass onNodeComplete callback to update cache as nodes complete
        // Don't await - redirect immediately so user sees monitor view right away
        orchestration.executeJob(orchestrationId, true, executionId, onNodeComplete, null, rerunFrom).then(async (executionLog) => {
          updateInProgressExecution(orchestrationId, executionId, executionLog);
          // Execute in background, save results when complete
          try {
            await orchestration.saveExecutionResult(executionLog);
            // If this was a re-run, mark the original execution as having been re-run
            if (rerunFrom) {
              await hist.markAsRerun(rerunFrom);
            }
          } catch (saveErr) {
            logger.error(`Failed to save execution result for orchestration [${orchestrationId}]: ${saveErr.message}`);
          }
          // Clear the stub from cache - actual execution is now in history
          clearInProgressExecution(orchestrationId, executionId);
          // Remove from running queue by executionId for consistency with REST endpoint
          running.removeItemByExecutionId(executionId);
          logger.info(`Orchestration [${orchestrationId}] execution completed with status: ${executionLog.finalStatus}`);
        }).catch((err) => {
          logger.error(`Background orchestration execution failed: ${err.message}`);
          // Try to retrieve execution log from cache to save to history
          const inProgressExecution = getInProgressExecution(orchestrationId, executionId);
          if (inProgressExecution) {
            // Mark as failed if not already marked
            if (!inProgressExecution.status || inProgressExecution.status === 'running') {
              inProgressExecution.status = 'failed';
              inProgressExecution.finalStatus = 'error';
            }
            inProgressExecution.endTime = new Date().toISOString();
            if (!inProgressExecution.errors) {
              inProgressExecution.errors = [];
            }
            inProgressExecution.errors.push({ message: err.message });
            // Save to history even on error
            orchestration.saveExecutionResult(inProgressExecution).catch((saveErr) => {
              logger.error(`Failed to save failed execution result for orchestration [${orchestrationId}]: ${saveErr.message}`);
            });
          }
          // Clear the stub on error too
          clearInProgressExecution(orchestrationId, executionId);
          // Remove from running queue on error by executionId
          running.removeItemByExecutionId(executionId);
        });
        
        // Redirect to monitor view immediately with the execution ID
        redir = `/orchestration/monitor.html?jobId=${encodeURIComponent(orchestrationId)}&executionId=${encodeURIComponent(executionId)}`;
      }
    } catch (err) {
      logger.error(`Error starting orchestration: ${err.message}`);
      errorMessage = `Failed to start orchestration: ${err.message}`;
    }
  } else {
    // Classic mode: Check if agent is reachable and under concurrency limit
    if (schedule && schedule.scheduleMode !== 'orchestration') {
      const agent = agents.getAgent(schedule.agent);
      
      if (agent) {
        if (agent.status === 'offline') {
          errorMessage = `Agent '${schedule.agent}' is offline - cannot execute job`;
        } else {
          const concurrencyLimit = agents.getConcurrency(schedule.agent);
          const runningCount = running.getRunningCountForAgent(schedule.agent);
          if (runningCount >= concurrencyLimit) {
            errorMessage = `Agent '${schedule.agent}' is at concurrency limit (${runningCount}/${concurrencyLimit})`;
          }
        }
      } else {
        errorMessage = `Agent '${schedule.agent}' does not exist`;
      }
    }
    
    // Only attempt to run if no pre-check errors
    if (!errorMessage) {
      logger.info(`About to call manualJobRun with index=${index}, jobname=${jobname}, rerunFrom=${rerunFrom ?? 'undefined'}`);
      var result = await scheduler.manualJobRun(index, jobname, rerunFrom);
      logger.info(`Job execution result: ${JSON.stringify(result)}`)
      if(result && result.status !== "ok") {
        errorMessage = "Job execution failed";
        logger.error(`Classic job execution failed - result: ${JSON.stringify(result)}`);
      } else if (result && result.executionId) {
        // If this is a re-run, mark the original execution as having been re-run
        if (rerunFrom) {
          await hist.markAsRerun(rerunFrom);
        }
        logger.info(`Adding executionId to URL: ${result.executionId}`);
        // Add executionId to redirect URL for classic jobs
        redir += (redir.includes('?') ? '&' : '?') + 'executionId=' + encodeURIComponent(result.executionId);
        logger.info(`Final redirect URL: ${redir}`);
      } else {
        logger.warn(`No executionId in result: ${JSON.stringify(result)}`);
      }
    }
  }
  
  if(errorMessage) {
    logger.info(`Job failed: ${errorMessage}`);
    const separator = redir.includes('?') ? '&' : '?';
    const encodedMsg = encodeURIComponent(errorMessage);
    redir += separator + "message=" + encodedMsg;
  }
  logger.info(`Final redir URL before redirect: ${redir}`);
  res.redirect(safeRedirect(redir, '/scheduleList.html'));
}));

app.get('/edit/:index',User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_EDIT), (req, res) => {
  const index = req.params.index;
  const schedules = scheduler.getSchedules();
  const schedule = schedules[index];

  res.render('scheduler', { 
    schedules, 
    index,
    scripts: scripts,
    agents: agents.getDict(), 
    csrf: req.csrfToken(),
  });
});

app.post('/edit/:index', validateCsrf, User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_EDIT), (req, res) => {
  const index = req.params.index;
  const schedules = scheduler.getSchedules();
  const schedule = schedules[index];

  schedule.jobName = req.body.jobName;
  schedule.scheduleType = req.body.scheduleType;
  schedule.scheduleTime = req.body.scheduleTime;

  //writeSchedules(schedules);

  res.redirect('/schedules');
});

app.get('/rest/jobs',User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_VIEW), (req, res) => {
  //populateBackupConfig();
  res.header('Content-type', 'application/json');
  res.send(JSON.stringify(config, null, "  "));
});

app.get('/',User.isAuthenticated, async (request, response) => {

  var historyList = hist.getItemsUsingTZ();
  var runningList = running.getItemsUsingTZ();
  var agentsDict = agents.getDict();
  var chartData = await hist.getChartDataSet(7);

  var username = request.session.user.username;
  if(username===undefined || username===null)username="User";

  var runningCount = runningList.length;
  
  // Calculate success/fail from chart data (includes both regular jobs and orchestrations)
  var successCount = chartData.success.reduce((a, b) => a + b, 0);
  var failCount = chartData.fail.reduce((a, b) => a + b, 0);

  // Last 5 executions: job history (minus orchestration node items) plus parent orchestration executions
  var recentRuns = [];
  var tz = serverConfig.server.timezone;
  const runTrigger = (manual, ctx) => {
    const t = ctx && ctx.type;
    if (manual === 'webhook' || t === 'webhook') return 'Webhook';
    if (manual === 'rule' || t === 'rule') return 'Rule';
    if (manual === true || manual === 'true' || manual === 'manual') return 'Manual';
    return 'Schedule';
  };
  historyList.forEach(item => {
    if (item.jobName && /^Orchestration\s+\[.+?\]\s+Execution\s+\[.+?\]\s+Node\s+\[.+?\]/.test(item.jobName)) return;
    recentRuns.push({
      name: item.jobName,
      ts: moment.tz(item.runDate, 'YYYY-MM-DD HH:mm:ss.SSS', tz).valueOf(),
      status: item.returnCode == 0 ? 'success' : 'fail',
      runtime: parseFloat(item.runTime) || 0,
      trigger: runTrigger(item.manual, item.triggerContext),
      type: 'Script'
    });
  });
  try {
    var orchExecs = await db.getData('ORCHESTRATION_EXECUTIONS').catch(() => ({}));
    for (const jobId in (orchExecs || {})) {
      (orchExecs[jobId] || []).forEach(ex => {
        if (!ex.startTime) return;
        var st = new Date(ex.startTime).getTime();
        var rt = ex.endTime ? (new Date(ex.endTime) - st) / 1000 : 0;
        recentRuns.push({
          name: ex.jobName || ex.orchestrationName || jobId,
          jobId: jobId,
          ts: st,
          status: ex.finalStatus === 'success' ? 'success' : (ex.finalStatus === 'failure' || ex.finalStatus === 'error') ? 'fail' : 'running',
          runtime: rt,
          trigger: runTrigger(ex.manual, ex.triggerContext),
          type: 'Orchestration'
        });
      });
    }
  } catch (e) {
    logger.warn(`Unable to load orchestration executions for dashboard: ${e.message}`);
  }
  recentRuns.sort((a, b) => b.ts - a.ts);
  recentRuns = recentRuns.slice(0, 4).map(r => ({
    ...r,
    when: moment.tz(r.ts, tz).format('MMM D, HH:mm:ss')
  }));

  for (const r of recentRuns) {
    if (r.type !== 'Orchestration' || !r.jobId) continue;
    try {
      const job = await orchestration.getJob(r.jobId);
      if (job && job.name) r.name = job.name;
    } catch (err) {
      logger.warn(`Could not get orchestration name for [${r.jobId}]: ${err.message}`);
    }
  }

  response.render('index', {

    recentRuns: recentRuns,

    subject: 'Orchelium',
    name: 'Control',
    runningCount: runningCount,
    successCount, successCount,
    failCount, failCount,
    chartData: chartData,
    agents,agentsDict,
    username:username,
    mqttConnected: mqttTransport.isMQTTConnected(),
    todayJobs: scheduler.getTodaysScheduleCount(),
    scheduleHistory: scheduler.getLast7DaysScheduleCount(),
    todayJobsRun: await hist.getTodaysRun()
  });
});

app.get('/index.html',User.isAuthenticated, (request, response) => {
  // response.render('index', {
  //   subject: 'EJS template engine',
  //   name: 'Control',
  // });
  response.redirect("/");
});

app.post('/rest/jobs/executions/:executionId/markAsRerun', User.isAuthenticated, User.requirePermission(PERMISSIONS.HISTORY_RESUBMIT), asyncHandler(async (req, res) => {
  const { executionId } = req.params;
  
  logger.info(`Marking classic job execution [${executionId}] as re-run`);
  
  try {
    // Get history and mark the execution as re-run
    const hist = require('../history.js');
    const success = await hist.markAsRerunDirect(executionId);
    
    if (success) {
      res.json({ success: true });
    } else {
      res.status(404).json({ success: false, message: `Execution [${executionId}] not found` });
    }
  } catch (err) {
    logger.error(`Error marking job as re-run: ${err.message}`);
    res.status(500).json({ success: false, message: err.message });
  }
}));

app.get('/rest/schedules', User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_VIEW), asyncHandler(async (req, res) => {
  const schedules = scheduler.getSchedules();
  res.json(schedules);
}));

app.get('/rest/schedules/:jobName', User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_VIEW), asyncHandler(async (req, res) => {
  const { jobName } = req.params;
  const schedule = scheduler.getSchedule(jobName);
  
  if (!schedule) {
    return res.status(404).json({ 
      success: false, 
      message: `Schedule [${jobName}] not found` 
    });
  }
  
  res.json(schedule);
}));

app.post('/rest/schedules/:jobName/disabled', validateCsrf, User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_EDIT), asyncHandler(async (req, res) => {
  const { jobName } = req.params;
  const disabled = req.body && (req.body.disabled === true || req.body.disabled === 'true');
  const schedule = await scheduler.setScheduleDisabled(jobName, disabled);
  res.json({
    success: true,
    jobName: schedule.jobName,
    disabled: schedule.disabled === true
  });
}));

app.delete('/rest/schedules/:jobName', User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_DELETE), asyncHandler(async (req, res) => {
  const { jobName } = req.params;
  
  try {
    await scheduler.deleteSchedule(jobName);
    res.json({ success: true, message: `Schedule [${jobName}] deleted` });
  } catch (err) {
    logger.error(`Error deleting schedule [${jobName}]: ${err.message}`);
    res.status(400).json({ 
      success: false, 
      message: `Failed to delete schedule: ${err.message}` 
    });
  }
}));

app.delete('/rest/schedules', User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_DELETE), asyncHandler(async (req, res) => {
  try {
    await scheduler.clearSchedules();
    res.json({ success: true, message: 'All schedules deleted' });
  } catch (err) {
    logger.error(`Error deleting all schedules: ${err.message}`);
    res.status(400).json({ 
      success: false, 
      message: `Failed to delete schedules: ${err.message}` 
    });
  }
}));

app.get('/rest/definitions/status', User.isAuthenticated, User.requireAnyPermission([PERMISSIONS.JOBS_VIEW, PERMISSIONS.ORCHESTRATIONS_VIEW]), asyncHandler(async (_req, res) => {
  res.json({ success: true, status: definitionStore.getStatus() });
}));

app.post('/rest/definitions/reload', User.isAuthenticated, User.requireAnyPermission([PERMISSIONS.JOBS_EDIT, PERMISSIONS.ORCHESTRATIONS_EDIT]), asyncHandler(async (_req, res) => {
  await definitionStore.reload();
  await scheduler.init();
  await orchestration.init();
  res.json({ success: true, message: 'Definitions reloaded', status: definitionStore.getStatus() });
}));

app.post('/rest/definitions/migrate-db-to-fs', User.isAuthenticated, User.requireAnyPermission([PERMISSIONS.JOBS_EDIT, PERMISSIONS.ORCHESTRATIONS_EDIT]), asyncHandler(async (req, res) => {
  if (!['fs', 'hybrid'].includes(definitionStore.getBackend())) {
    return res.status(400).json({
      success: false,
      message: `Definition backend must be fs or hybrid (current: ${definitionStore.getBackend()})`
    });
  }

  const deleteSource = req.body && (req.body.deleteSource === true || req.body.deleteSource === 'true');
  const migratedSchedules = await scheduler.migrateSchedulesToFilesystem({ deleteSource });
  const migratedOrchestrations = await orchestration.migrateToFilesystem({ deleteSource });

  res.json({
    success: true,
    message: 'Definition migration complete',
    migrated: {
      schedules: migratedSchedules,
      orchestrations: migratedOrchestrations
    },
    deleteSource
  });
}));

app.get('/rest/history', User.isAuthenticated, User.requirePermission(PERMISSIONS.HISTORY_VIEW), asyncHandler(async (req, res) => {
  const items = await hist.getItemsUsingTZ();
  res.json(items);
}));

app.get('/rest/history/:index', User.isAuthenticated, User.requirePermission(PERMISSIONS.HISTORY_VIEW), asyncHandler(async (req, res) => {
  const items = await hist.getItemsUsingTZ();
  const index = parseInt(req.params.index);
  if (index < 0 || index >= items.length) {
    return res.status(404).json({ success: false, message: 'History item not found' });
  }
  res.json(items[index]);
}));

app.post('/rest/history', User.isAuthenticated, User.requirePermission(PERMISSIONS.HISTORY_VIEW), asyncHandler(async (req, res) => {
  const item = req.body;
  hist.add(item);
  res.status(201).json({ success: true, item });
}));

app.put('/rest/history/:index', User.isAuthenticated, User.requirePermission(PERMISSIONS.HISTORY_RESUBMIT), asyncHandler(async (req, res) => {
  const items = await hist.getItemsUsingTZ();
  const index = parseInt(req.params.index);
  if (index < 0 || index >= items.length) {
    return res.status(404).json({ success: false, message: 'History item not found' });
  }
  items[index] = req.body;
  await hist.updateDb();
  res.json({ success: true, item: items[index] });
}));

app.delete('/rest/history/:index', User.isAuthenticated, User.requirePermission(PERMISSIONS.HISTORY_RESUBMIT), asyncHandler(async (req, res) => {
  let items = await hist.getItemsUsingTZ();
  const index = parseInt(req.params.index);
  if (index < 0 || index >= items.length) {
    return res.status(404).json({ success: false, message: 'History item not found' });
  }
  items.splice(index, 1);
  await hist.updateDb();
  res.json({ success: true });
}));
};
