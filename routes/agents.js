module.exports = function register(app) {
app.get('/rest/agent/:id', User.isAuthenticated, User.requirePermission(PERMISSIONS.AGENTS_VIEW), (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  const id = req.params.id;
  logger.info("Getting Agent with ID: [" + id + "]");
  var agent = agents.getAgent(id);
  //var agent = agents.searchAgent(id);
  //console.log("Agent is: " + agent);
  res.setHeader("Content-Type","Application/JSON");
  res.send(agent);  
});

app.get('/rest/agent/:id/ping', User.isAuthenticated, User.requirePermission(PERMISSIONS.AGENTS_VIEW), (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  const id = req.params.id;
  logger.info("Pinging Agent with ID: " + id);
  //notificationData.deleteItem(index);
  pingIndividualRuntime(id);
  res.setHeader("Content-Type","Application/JSON");
  res.send('{"status":"ok"}');  
});

app.get('/rest/agents/install-command', User.isAuthenticated, User.requirePermission(PERMISSIONS.AGENTS_CREATE), async (req, res) => {
  const host = req.get('host');
  if ((req.protocol !== 'http' && req.protocol !== 'https') || !host) {
    return res.status(400).json({ success: false, message: 'Unable to build the install command' });
  }
  const agentAuth = require('../communications/agentAuth');
  let token;
  try {
    token = await agentAuth.issueInstallToken();
  } catch (err) {
    logger.error(`Failed to create an agent install token: ${err.message}`);
    return res.status(500).json({ success: false, message: 'Unable to build the install command' });
  }
  const command = buildAgentInstallCommand(serverConfig, `${req.protocol}://${host}`, token);
  res.setHeader('Cache-Control', 'no-store');
  res.json({ command, token });
});

app.get('/rest/agents', User.isAuthenticated, User.requirePermission(PERMISSIONS.AGENTS_VIEW), (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  logger.info("Getting all agents");
  const agentDict = agents.getDict();
  res.setHeader("Content-Type","Application/JSON");
  res.send(agentDict);  
});

app.post('/rest/agent/:id/metric', User.isAuthenticated, User.requirePermission(PERMISSIONS.AGENTS_VIEW), asyncHandler(async (req, res) => {
  const { id } = req.params;
  const { type, path: targetPath, pattern } = req.body;

  if (!type) {
    return res.status(400).json({ success: false, message: 'Missing required field: type' });
  }

  const agent = agents.getAgent(id);
  if (!agent) {
    return res.status(404).json({ success: false, message: `Agent [${id}] not found` });
  }
  if (agent.status === 'offline') {
    return res.status(503).json({ success: false, message: `Agent [${id}] is currently offline` });
  }

  const crypto = require('crypto');
  const jobName = `MetricQuery_${id}_${crypto.randomBytes(6).toString('hex')}`;
  const metricConfig = { type };
  if (targetPath) metricConfig.path = targetPath;
  if (pattern) metricConfig.pattern = pattern;

  logger.info(`[METRIC] Sending queryMetric [${type}] to agent [${id}] with jobName [${jobName}]`);

  const agentMsgProcessor = require('../agentMessageProcessor.js');
  const waitPromise = agentMsgProcessor.waitForMetricResult(jobName, 30000);

  agentComms.sendCommand(id, mqttTransport.getCommandTopic(), 'queryMetric', JSON.stringify(metricConfig), jobName, undefined, false, null);

  try {
    const payload = await waitPromise;
    
    // Check if the agent returned an error (e.g., JSON parse failure)
    if (payload.error) {
      logger.error(`[METRIC] Agent [${payload.agent}] returned error for [${jobName}]: ${payload.error}`);
      return res.status(400).json({ success: false, message: `Agent error: ${payload.error}` });
    }
    
    logger.info(`[METRIC] Got result for [${jobName}]: ${JSON.stringify(payload.result)}`);
    res.json({ success: true, agent: payload.agent, jobName, result: payload.result });
  } catch (err) {
    logger.error(`[METRIC] Query timed out or failed for [${jobName}]: ${err.message}`);
    res.status(408).json({ success: false, message: err.message });
  }
}));

app.get('/metric-test.html', User.isAuthenticated, asyncHandler(async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/login.html');
  }
  const agentList = agents.getDict();
  res.render('metrictest', {
    version,
    user,
    agents: agentList,
    csrf: req.csrfToken(),
  });
}));

app.get('/agentHistory.html',User.isAuthenticated, User.requirePermission(PERMISSIONS.AGENTS_VIEW), async (request, response) => {
  var agentname = request.query.name;
  var startDate = request.query.startDate;
  var dateNow = new Date();
  var agent = agents.getAgent(agentname);
  if(agent===undefined)agent={};

  var agentData;
  agentData=agentStats.get(agentname);
  if(agentData===undefined)agentData={};
  else agentData=JSON.parse(agentData);
  

  response.render('agentHistory', {
    agentname: agentname,
    timezone: serverConfig.server.timezone,
    serverTime: dateNow,
    agent: agent,
    agentData: agentData  
  });
});

app.get('/rest/agentdetail',User.isAuthenticated, User.requirePermission(PERMISSIONS.AGENTS_VIEW), async (request, response) => {
  
  var agentname = request.query.agentname;
  logger.info("Getting agent detail for Agent [" + agentname + "]")
  var startDate = request.query.startDate;
  var endDate = request.query.endDate;
  logger.info("Start Date [" + startDate + "]")
  logger.info("End Date   [" + endDate + "]")

  var agent = agents.getAgent(agentname);
  if(agent===undefined)agent={};
  var history = {};
  //var item = await agentHistory.getStatus(agentname);
  var item = await agentHistory.getDateFilteredStatus(agentname,startDate,endDate);
  //debug.info("Items are: " + item);
  
  var totalDuration = 0;
  for (let i = 0; i < item.length; i++) {
    //debug.debug("adding: " + item[i].duration);
    totalDuration+=item[i].duration;
  }

  for (let i = 0; i < item.length; i++) {
    //debug.debug("adding: " + item[i].duration);
    var pct = (item[i].duration / totalDuration) * 100.00;
    //pct = Math.round(pct);
    pct=mathUtils.roundToDecimalPlaces(pct,5);
    item[i].percent=pct;
  }
  var totalPct=0;
  for (let i = 0; i < item.length; i++) {
    totalPct += item[i].percent;
  }

  var data={};
  data.filter={};
  data.filter.startDate=startDate;
  data.filter.endDate=endDate;
  data.totalDuration=totalDuration;
  data.totalPct=totalPct;
  data.history=item;
  response.json(data);
});

app.get('/rest/agentquery',User.isAuthenticated, User.requirePermission(PERMISSIONS.AGENTS_VIEW), (request, response) => {
  var agentname = request.query.name;
  var agent = agents.getAgent(agentname);
  response.header('Content-type', 'application/json');
  if(agent===undefined)agent={};
  agent.found="false";
  response.send(JSON.stringify(agent,null,2));
});

app.get('/agentEdit.html',User.isAuthenticated, User.requirePermission(PERMISSIONS.AGENTS_EDIT), (request, response) => {
  var msg = request.query.agentId;
  var agentObj=agents.getAgent(msg);
  var scriptsMeta = refreshScripts();
  var scripts = [];
  for (var i = 0; i < scriptsMeta.length; i++) {
    scripts.push(scriptsMeta[i].data.filename);
  }
  var tokenNotice = request.session.tokenNotice || '';
  delete request.session.tokenNotice;
  var shownToken = null;
  if (request.session.shownAgentToken && request.session.shownAgentToken.name === msg) {
    shownToken = request.session.shownAgentToken;
    delete request.session.shownAgentToken;
  }
  response.render('agentEdit', {
    subject: 'Agent Edit',
    name: 'Control/AgentEdit',
    agent: agentObj,
    scripts: scripts,
    csrf: request.csrfToken(),
    hasCredential: require('../communications/agentAuth').hasCredential(msg),
    tokenNotice: tokenNotice,
    shownToken: shownToken,
  });
});

app.get('/agentregister.html',User.isAuthenticated, User.requirePermission(PERMISSIONS.AGENTS_CREATE), (request, response) => {
  var msg = request.query.message;
  var agentObj=JSON.parse(msg);
  response.render('agentregister', {
    subject: 'EJS template engine',
    name: 'Control/AgentRegister',
    agent: agentObj,
    //scripts: scripts,
    csrf: request.csrfToken(),
  });
});

app.get('/agentstatus.html',User.isAuthenticated, User.requirePermission(PERMISSIONS.AGENTS_VIEW),async (request, response) => {

    var agentdict = agents.getDict();
    var history = {};
    
    for (const [key, value] of Object.entries(agentdict)) {
      var item = await agentHistory.getStatus(key);
      //console.log(">>>>>>>>>>>>>>>>>>>>>>");
      //console.log(JSON.stringify(key));
      //console.log(JSON.stringify(value));
      //console.log(">>>> AGENT DATE: " + item.date);
      value.lastStatusReport = dateTimeUtils.displayFormatDate(new Date(value.lastStatusReport),false,serverConfig.server.timezone,"YYYY-MM-DDTHH:mm:ss",false);
            

      history[key]={};
      history[key].list=item;
    }

    //console.log(history);

    
    
    const tokenNotice = request.session.tokenNotice || '';
    delete request.session.tokenNotice;
    const agentToast = request.session.agentToast || '';
    delete request.session.agentToast;
    const agentAuth = require('../communications/agentAuth');
    const tokenAgents = {};
    for (const agentName of Object.keys(agentdict)) {
      if (agentAuth.hasCredential(agentName)) tokenAgents[agentName] = true;
    }

    response.render('agentstatus', {
    subject: 'EJS template engine',
    name: 'Control/AgentStatus',
    logs: logsStr,
    agents: agentdict,
    history: history,
    serverVers: minSupportedVersion,
    tokenNotice: tokenNotice,
    agentToast: agentToast,
    tokenAgents: tokenAgents,
  });
});

app.post('/agent-submit.html', validateCsrf, User.isAuthenticated, User.requirePermission(PERMISSIONS.JOBS_CREATE), async (req, res) => {
  //Get the form data
  
  var name = req.body.agentname;
  var command = req.body.agentcommand;
  var description = req.body.agentdescription;
  var imageurl = req.body.imageurl;

  agents.registerAgent(name,description,command,imageurl,undefined,description);
  webSocketServer.promotePending(name);
  const agentAuth = require('../communications/agentAuth');
  const agent = agents.getAgent(name);
  if (agentAuth.hasCredential(name)) {
    req.session.agentToast = 'Agent ' + name + ' has been approved and is now available';
    return res.redirect('/agentstatus.html');
  }
  if (agentAuth.supportsAgentToken(agent)) {
    try {
      const token = await agentAuth.issue(name);
      const delivered = agentComms.sendEnrollment(name, token);
      req.session.shownAgentToken = { name, token, delivered: Boolean(delivered), rotationPending: false };
      return res.redirect('/agentEdit.html?agentId=' + encodeURIComponent(name));
    } catch (err) {
      logger.error(`Failed to issue connection token for [${name}]: ${err.message}`);
      req.session.tokenNotice = 'The agent was approved, but the connection token could not be created.';
    }
  } else {
    req.session.tokenNotice = 'This agent does not support connection tokens yet. Update it, then issue a token from the agent edit page. It will not be accepted until that token is in place.';
  }
  res.redirect("/agentstatus.html");

});

app.post('/agent-token.html', validateCsrf, User.isAuthenticated, User.requirePermission(PERMISSIONS.AGENTS_EDIT), async (req, res) => {
  const name = req.body.agentname;
  const action = req.body.tokenAction;
  const agentAuth = require('../communications/agentAuth');
  const agent = agents.getAgent(name);
  if (!agent) {
    return res.status(404).send('Agent not found');
  }

  const editUrl = '/agentEdit.html?agentId=' + encodeURIComponent(name);
  try {
    if (action === 'rotate') {
      if (!agentAuth.hasCredential(name)) {
        req.session.tokenNotice = 'Issue a connection token before rotating it.';
        return res.redirect(editUrl);
      }
      if (!agentAuth.supportsAgentToken(agent)) {
        req.session.tokenNotice = 'Update the agent before rotating its connection token.';
        return res.redirect(editUrl);
      }
      const reachable = agent.commsType === 'mqtt'
        ? serverConfig.mqtt.enabled == 'true' && mqttTransport.isMQTTConnected()
        : webSocketServer.hasOpenConnection(name);
      if (!reachable) {
        req.session.tokenNotice = 'The agent must be connected before its token can be rotated.';
        return res.redirect(editUrl);
      }
      const sealed = await agentAuth.beginRotation(name);
      agentComms.sendCommand(name, null, 'rotateToken', sealed, 'token-rotation', agent.commsType, false, null);
      req.session.tokenNotice = 'Rotation sent. The current token stays valid until the agent confirms the new one.';
      return res.redirect(editUrl);
    }

    if (action === 'reveal') {
      const revealed = agentAuth.reveal(name);
      if (!revealed) {
        req.session.tokenNotice = 'This agent has no connection token.';
      } else {
        req.session.shownAgentToken = {
          name,
          token: revealed.token,
          delivered: false,
          rotationPending: revealed.rotationPending,
        };
      }
      return res.redirect(editUrl);
    }

    const token = await agentAuth.issue(name);
    const delivered = agentAuth.supportsAgentToken(agent) ? Boolean(agentComms.sendEnrollment(name, token)) : false;
    req.session.shownAgentToken = { name, token, delivered, rotationPending: false };
    return res.redirect(editUrl);
  } catch (err) {
    logger.error(`Agent token action failed for [${name}]: ${err.message}`);
    if (action === 'rotate') {
      try { await agentAuth.cancelRotation(name); } catch (cancelErr) { logger.warn(cancelErr.message); }
    }
    req.session.tokenNotice = 'The connection token could not be updated.';
    return res.redirect(editUrl);
  }
});

app.post('/agentEdit.html', validateCsrf, User.isAuthenticated, User.requirePermission(PERMISSIONS.AGENTS_EDIT), async (req, res) => {
  try {
    //Get the form data
    var name = req.body.agentname;
    var description = req.body.agentdescription;
    var imageurl = req.body.imageurl;
    var concurrency = parseInt(req.body.concurrency, 10);
    if (isNaN(concurrency) || concurrency < 1) concurrency = 3;
    var agentObj = agents.getAgent(name);
    agentObj.display = description;
    agentObj.concurrency = concurrency;
    await agents.addObjToAgentStatusDict(agentObj);

    //agents.registerAgent(name,description,command,imageurl,undefined,description);
    res.redirect("/agentstatus.html");
  } catch (err) {
    logger.error(`Failed to update agent [${req.body.agentname}]:`, err.message);
    res.status(500).send('Error updating agent');
  }
});

app.get('/rest/updateAgent/:agentId/isRunning', express.json(), User.isAuthenticated, User.requirePermission(PERMISSIONS.AGENTS_VIEW), (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  const agentId = req.params.agentId;
  logger.debug(`Getting Job Status update for agent [${agentId}]`);

  var runningList = running.searchItemWithName("||INTERNAL||Update " + agentId);
  logger.debug(JSON.stringify(runningList));
  var response={};

  if(runningList!=null){
    response.status=true;
    response.item=runningList;
  }
  else {
    response.status=false;
    response.item=runningList
  }

  res.set('Content-Type', 'application/json'); // Set the content-type header
  res.send(response);
});

app.get('/rest/updateAgent/:agentId/status', express.json(), User.isAuthenticated, User.requirePermission(PERMISSIONS.AGENTS_VIEW), (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  const agentId = req.params.agentId;
  var agent = agents.getAgent(agentId);

  var searchTerm = "||INTERNAL||Update " + agentId;
  var runningList = running.searchItemWithName(searchTerm);
  var response = {};
  response.status = "ERROR";
  response.message = "";
  response.agent = agent;
  if(runningList!=null){
    //Job Is running
    response.status="RUNNING";
    response.item = runningList;
  }
  else {
    //Job is not running - Whats status of last completed item  
    response.status="COMPLETED";
    logger.debug(`Getting Job Status update for agent [${agentId}]`);
    var historyItem = hist.searchItemWithName("||INTERNAL||Update " + agentId);
    response.item=historyItem;
    if(historyItem!=null){
      if(response.item.returnCode!=0){
        response.status="ERROR"
        message=response.item.log;  
      }
    }
    else{
      response.status="UNKOWN";
      response.item = undefined;
      response.message = "No running, or historical log of uprade";
    }

    if(response.status=="COMPLETED"){
      //Now see if the agent is actually connected because once the job is finished, the agent goes into a restart

      var agentStatus = agent.status;
      var agentReportDate = agent.lastStatusReport;
      
      const ONLINE  = "online";
      const OFFLINE = "offline";
      const RUNNING = "running";
      
      if(agent.status == OFFLINE){
        response.status="WAITING_AGENT_CONNECTION";
        response.message=`Agent in status [${agent.status}]. Waiting for connection`;
      }

    }
  }
  

  res.set('Content-Type', 'application/json'); // Set the content-type header
  res.send(response);
});

app.post('/rest/updateAgent/:agentId', express.json(), User.isAuthenticated, User.requirePermission(PERMISSIONS.AGENTS_UPDATE), (req, res) => {
  
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  const agentId = req.params.agentId;
  const jsonData = req.body;
  
  try{

    //Issue a job execution to run the update script
    logger.debug(`Recevied [${JSON.stringify(jsonData)}]`);
    logger.debug(`starting update for agent [${agentId}] with command [${jsonData.command}]`);
    //scheduler.runUpdateJob(agentId,jsonData);
    res.set('Content-Type', 'application/json'); // Set the content-type header
    scheduler.runUpdateJob(agentId,jsonData.command); 
    res.send({"status":"ok"}); // Send the JSON response
  }
  catch(err){
    logger.error(err);
    res.status(500).json({ status: "error", message: "Internal Server Error [" + err.name +"]" + err.message});
  }
});
};
