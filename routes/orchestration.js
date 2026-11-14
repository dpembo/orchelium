module.exports = function register(app) {
app.get('/rest/orchestration/jobs', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_VIEW), asyncHandler(async (req, res) => {
  const jobs = await orchestration.getAllJobs();
  res.json(jobs);
}));

app.get('/rest/orchestration/jobs/:jobId', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_VIEW), asyncHandler(async (req, res) => {
  const job = await orchestration.getJob(req.params.jobId);
  res.json(job);
}));

app.post('/rest/orchestration/jobs', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_CREATE), asyncHandler(async (req, res) => {
  const { jobId, name, description, nodes, edges, icon, color } = req.body;
  
  if (!jobId || !name) {
    return res.status(400).json({ error: 'Job ID and name are required' });
  }
  
  const job = await orchestration.saveJob(jobId, {
    name,
    description,
    nodes,
    edges,
    icon,
    color
  });
  
  res.json({ success: true, job });
}));

app.get('/rest/orchestration/jobs/:jobId/export', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_EXPORT), asyncHandler(async (req, res) => {
  const jobId = req.params.jobId;
  const job = await orchestration.getJob(jobId);

  const zipBuffer = await importExportManager.createOrchestrationExportBuffer({
    jobId,
    orchestration: job
  });

  const safeName = importExportManager.toSafeFilenamePart(job.name || jobId, 'orchestration');
  const timestamp = importExportManager.getTimestampForFilename();
  const exportFilename = `orchestration-${safeName}-${timestamp}.zip`;

  res.setHeader('Content-Type', 'application/zip');
  res.setHeader('Content-Disposition', `attachment; filename="${exportFilename}"`);
  res.send(zipBuffer);
}));

app.post('/rest/orchestration/jobs/import', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_IMPORT), upload.single('importFile'), asyncHandler(async (req, res) => {
  if (!req.file || !req.file.buffer) {
    throw new AppError('Missing import file upload', 400);
  }

  let parsedImport;
  try {
    parsedImport = await importExportManager.parseOrchestrationImportBuffer(req.file.buffer);
  } catch (error) {
    throw new AppError(`Orchestration import validation failed: ${error.message}`, 400);
  }

  const existingJobs = await orchestration.getAllJobs();
  const identity = importExportManager.resolveUniqueOrchestrationIdentity({
    desiredName: parsedImport.orchestration.name,
    desiredJobId: parsedImport.orchestration.jobId,
    existingJobs
  });

  const savePayload = {
    name: identity.name,
    description: parsedImport.orchestration.description,
    icon: parsedImport.orchestration.icon,
    color: parsedImport.orchestration.color,
    nodes: parsedImport.orchestration.nodes,
    edges: parsedImport.orchestration.edges
  };

  await orchestration.saveJob(identity.jobId, savePayload);
  logger.info(`Orchestration imported successfully as [${identity.jobId}]`);

  res.json({
    success: true,
    importedType: 'orchestration',
    original: {
      jobId: parsedImport.orchestration.jobId || null,
      name: parsedImport.orchestration.name
    },
    imported: {
      jobId: identity.jobId,
      name: identity.name
    },
    renamed: identity.renamed,
    warnings: identity.renamed ? ['Orchestration identity already existed and was auto-renamed.'] : []
  });
}));

app.put('/rest/orchestration/jobs/:jobId', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_EDIT), asyncHandler(async (req, res) => {
  const { name, description, nodes, edges, icon, color } = req.body;
  const { jobId } = req.params;

  const job = await orchestration.saveJob(jobId, {
    name,
    description,
    nodes,
    edges,
    icon,
    color
  });
  
  res.json({ success: true, job });
}));

app.delete('/rest/orchestration/jobs/:jobId', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_DELETE), asyncHandler(async (req, res) => {
  const { jobId } = req.params;
  
  await orchestration.deleteJob(jobId);
  res.json({ success: true, message: `Orchestration job [${jobId}] deleted` });
}));

app.post('/rest/orchestration/jobs/:jobId/executions/:executionId/markAsRerun', User.isAuthenticated, User.requirePermission(PERMISSIONS.HISTORY_RESUBMIT), asyncHandler(async (req, res) => {
  const { jobId, executionId } = req.params;
  
  logger.info(`Marking orchestration execution [${jobId}] [${executionId}] as re-run`);
  
  try {
    const executions = await db.getData('ORCHESTRATION_EXECUTIONS').catch(() => ({}));
    
    if (!executions[jobId]) {
      return res.status(404).json({ success: false, message: `Orchestration [${jobId}] not found` });
    }
    
    const execution = executions[jobId].find(e => e.executionId === executionId);
    if (!execution) {
      return res.status(404).json({ success: false, message: `Execution [${executionId}] not found` });
    }
    
    // Mark as re-run with current timestamp
    execution.wasRerunAt = new Date().toISOString();
    logger.info(`Marked execution [${executionId}] with wasRerunAt [${execution.wasRerunAt}]`);
    
    // Save back to database
    await db.putData('ORCHESTRATION_EXECUTIONS', executions);
    
    res.json({ success: true, wasRerunAt: execution.wasRerunAt });
  } catch (err) {
    logger.error(`Error marking orchestration as re-run: ${err.message}`);
    res.status(500).json({ success: false, message: err.message });
  }
}));

app.post('/rest/orchestration/jobs/:jobId/execute', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_VIEW), asyncHandler(async (req, res) => {
  const { jobId } = req.params;
  const { rerunFrom } = req.body || {};  
  
  logger.info(`Executing orchestration job [${jobId}]` + (rerunFrom ? ` [rerunFrom: ${rerunFrom}]` : ""));
  
  // Generate execution ID immediately (same as orchestrationEngine does)
  const crypto = require('crypto');
  const executionId = crypto.randomBytes(8).toString('hex');
  
  // Get the orchestration job to get its name for the running queue
  let orchestrationName = jobId; // fallback to jobId
  try {
    const job = await orchestration.getJob(jobId);
    if (job && job.name) {
      orchestrationName = job.name;
    }
  } catch (err) {
    logger.warn(`Could not get orchestration name for [${jobId}]: ${err.message}`);
  }
  
  // Create a stub execution record (NOT saved to permanent history yet)
  // This is cached in-memory so monitor can find it immediately
  const stubExecution = {
    jobId: jobId,
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
    rerunFrom: rerunFrom || null  // NEW: Track if this is a rerun
  };
  
  // Cache the stub in-memory (not in permanent history)
  saveInProgressExecution(jobId, executionId, stubExecution);
  logger.info(`Created in-progress execution for [${jobId}] with ID [${executionId}]`);
  
  // Add to running queue so it shows in running list with link to monitor
  // Note: Orchestrations span multiple agents, so we pass null for agentName (concurrency not enforced per-agent)
  const runningItem = running.createItem(orchestrationName, new Date().toISOString(), 'manual', executionId, null);
  runningItem.orchestrationId = jobId;
  running.add(runningItem);
  logger.info(`Added orchestration to running queue: [${orchestrationName}] with execution [${executionId}]`);
  
  // Create callback to update cache as nodes complete
  const onNodeComplete = (executionLog) => {
    updateInProgressExecution(jobId, executionId, executionLog);
  };
  
  // Start orchestration execution asynchronously (fire-and-forget)
  // Pass executionId so engine uses the same ID for websocket events
  // Pass onNodeComplete callback to update cache as nodes complete
  // Don't await - return immediately so client redirects right away
  orchestration.executeJob(jobId, true, executionId, onNodeComplete, null, rerunFrom).then(async (executionLog) => {
    updateInProgressExecution(jobId, executionId, executionLog);
    // Execute in background, save results when complete
    try {
      await orchestration.saveExecutionResult(executionLog);
      // If this was a re-run, mark the original execution as having been re-run
      if (rerunFrom) {
        await hist.markAsRerun(rerunFrom);
      }
    } catch (saveErr) {
      logger.error(`Failed to save execution result for orchestration [${jobId}]: ${saveErr.message}`);
    }
    // Clear the stub from cache - actual execution is now in history
    clearInProgressExecution(jobId, executionId);
    // Remove from running queue
    running.removeItemByExecutionId(executionId);
    logger.info(`Orchestration [${jobId}] execution completed with status: ${executionLog.finalStatus}`);
  }).catch((err) => {
    logger.error(`Background orchestration execution failed: ${err.message}`);
    // Try to retrieve execution log from cache to save to history
    const inProgressExecution = getInProgressExecution(jobId, executionId);
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
        logger.error(`Failed to save failed execution result for orchestration [${jobId}]: ${saveErr.message}`);
      });
    }
    // Clear the stub on error too
    clearInProgressExecution(jobId, executionId);
    // Remove from running queue on error as well
    running.removeItemByExecutionId(executionId);
  });
  
  // Return immediately with executionId
  res.json({ 
    success: true, 
    executionId: executionId
  });
}));

app.get('/rest/orchestration/jobs/:jobId/executions', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_VIEW), asyncHandler(async (req, res) => {
  const { jobId } = req.params;
  
  const executions = await orchestration.getExecutionHistory(jobId);
  res.json(executions);
}));

app.get('/rest/orchestration/scripts', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_VIEW), asyncHandler(async (req, res) => {
  const scripts = await orchestration.getAvailableScripts();
  res.json(scripts);
}));

app.get('/rest/orchestration/plugins', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_VIEW), asyncHandler(async (req, res) => {
  const plugins = await orchestration.getAvailablePlugins();
  res.json(plugins);
}));

app.get('/rest/orchestration/agents', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_VIEW), asyncHandler(async (req, res) => {
  const agentDict = agents.getDict();
  const agentList = Object.entries(agentDict).map(([name, agent]) => ({
    id: name,
    name: name,
    description: agent.description || '',
    status: agent.status || 'offline',
    address: agent.address || '',
    version: agent.version || ''
  }));
  res.json(agentList);
}));

app.get('/rest/orchestration/jobs/:jobId/metadata', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_VIEW), asyncHandler(async (req, res) => {
  const job = await orchestration.getJob(req.params.jobId);
  res.json({
    id: job.id,
    name: job.name,
    description: job.description,
    icon: job.icon || 'schema',
    color: job.color || '#000000',
    type: job.type,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    currentVersion: job.currentVersion,
    totalVersions: job.totalVersions
  });
}));

app.get('/rest/orchestration/jobs/:jobId/versions/:version', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_VIEW), asyncHandler(async (req, res) => {
  const job = await orchestration.getJobVersion(req.params.jobId, req.params.version);
  res.json(job);
}));

app.get('/orchestrationList.html', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_VIEW), asyncHandler(async (req, res) => {
  res.render('orchestrationList', { csrfToken: req.csrfToken() });
}));

app.get('/orchestrationBuilder.html', User.isAuthenticated, User.requireAnyPermission([PERMISSIONS.ORCHESTRATIONS_CREATE, PERMISSIONS.ORCHESTRATIONS_EDIT]), asyncHandler(async (req, res) => {
  const jobId = req.query.id; // undefined for new jobs, or specific ID for editing
  var color = "#FF9800";
  var icon = "workspaces";
  var jobName = '';
  var jobDescription = '';

  if (jobId !== undefined && jobId !== '') {
    try {
      const job = await orchestration.getJob(jobId);
      if (job) {
        color = job.color || color;
        icon = job.icon || icon;
        jobName = job.name || '';
        jobDescription = job.description || '';
      }
    } catch (e) {
      logger.warn(`Could not load orchestration [${jobId}] for builder: ${e.message}`);
    }
  }

  // Drawflow is now the primary orchestration builder UI
  res.render('orchestrationBuilderDrawflow', {
    csrfToken: req.csrfToken(),
    icons: serverConfig.job_icons,
    jobId: jobId || '',
    jobName: jobName,
    jobDescription: jobDescription,
    color: color,
    icon: icon
  });
}));

app.get('/orchestrationBuilderDrawflow.html', User.isAuthenticated, User.requireAnyPermission([PERMISSIONS.ORCHESTRATIONS_CREATE, PERMISSIONS.ORCHESTRATIONS_EDIT]), asyncHandler(async (req, res) => {
  const q = req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : '';
  res.redirect(302, '/orchestrationBuilder.html' + q);
}));

app.get('/orchestration/monitor.html', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_VIEW), asyncHandler(async (req, res) => {
  const jobId = req.query.jobId;
  let executionIndex = req.query.executionIndex || 'latest';
  const executionId = req.query.executionId;

  if (!jobId) {
    return res.status(400).send('Job ID is required');
  }

  // Drawflow is now the primary orchestration monitor UI
  res.render('orchestrationMonitorDrawflow', {
    csrfToken: req.csrfToken(),
    jobId: jobId,
    executionIndex: executionIndex,
    executionId: executionId
  });
}));

app.get('/orchestrationMonitorDrawflow.html', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_VIEW), asyncHandler(async (req, res) => {
  const q = req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : '';
  res.redirect(302, '/orchestration/monitor.html' + q);
}));

app.get('/orchestration/execution/details', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_VIEW), asyncHandler(async (req, res) => {
  const orchestrationMonitor = require('../orchestrationMonitor.js');
  const jobId = req.query.jobId;
  let executionIndex = req.query.executionIndex || 'latest';
  const executionId = req.query.executionId;
  
  if (!jobId) {
    return res.status(400).json({ error: 'Job ID is required' });
  }
  
  // Check if this is an in-progress execution in the cache
  if (executionId) {
    const cachedExecution = getInProgressExecution(jobId, executionId);
    if (cachedExecution) {
      logger.debug(`Found in-progress execution [${executionId}] in cache, building details`);
      
      // Build details from cached stub
      try {
        const orchestrationMonitor = require('../orchestrationMonitor.js');
        const jobDef = await orchestrationMonitor.getJobDefinitionVersion(jobId, 'current');
        
        if (!jobDef) {
          return res.status(404).json({ error: `Orchestration job [${jobId}] not found` });
        }

        // Fetch live logs from database for in-progress execute nodes
        // on every request so selected-node detail panels can stay current.
        const agents = require('../agents.js');
        for (const node of jobDef.nodes) {
          if (node.type === 'execute' && node.data && node.data.agent) {
            const agentId = node.data.agent;
            const agent = agents.getAgent(agentId);
            if (agent) {
              // Node is executing or recently completed, fetch latest logs from database
              const jobName = `Orchestration [${jobId}] Execution [${executionId}] Node [${node.id}]`;
              const logKey = `${agent.name}_${jobName}_${executionId}_log`;
              try {
                // Use simpleGetData to match agent's log storage mechanism
                const logContent = await db.simpleGetData(logKey);
                if (logContent) {
                  // Initialize the scriptOutputs entry if not present
                  if (!cachedExecution.scriptOutputs[node.id]) {
                    cachedExecution.scriptOutputs[node.id] = {
                      script: node.data.script || '',
                      parameters: node.data.parameters || '',
                      agent: agentId,
                      status: 'in-progress'
                    };
                  }
                  // Always update stdout with the latest persisted log snapshot.
                  cachedExecution.scriptOutputs[node.id].stdout = logContent;
                  logger.info(`[Orchestration Monitor] Fetched live logs for node [${node.id}] - ${logContent.length} bytes from key [${logKey}]`);
                }
              } catch (logErr) {
                // Log file doesn't exist yet or error reading - that's fine
                logger.debug(`[Orchestration Monitor] No logs yet for node [${node.id}]: ${logErr.message}`);
              }
            }
          }
        }
        
        // Format nodes with cached execution data (including fetched logs)
        const formattedNodes = orchestrationMonitor.formatNodeDetails(jobDef.nodes, cachedExecution);
        const formattedEdges = orchestrationMonitor.formatEdgeDetails(jobDef.edges, cachedExecution, cachedExecution.visitedNodes || []);
        
        return res.json({
          jobId: jobDef.id,
          jobName: jobDef.name,
          description: jobDef.description || '',
          orchestrationVersion: jobDef.version || 1,
          execution: {
            executionId: cachedExecution.executionId,
            startTime: cachedExecution.startTime,
            endTime: cachedExecution.endTime,
            status: cachedExecution.status,
            finalStatus: cachedExecution.finalStatus || 'unknown',
            mode: orchestrationMonitor.getExecutionMode(cachedExecution),
            triggerContext: cachedExecution.triggerContext || null,
            triggerSummary: orchestrationMonitor.buildExecutionTriggerSummary(cachedExecution),
            duration: cachedExecution.endTime ? 
              new Date(cachedExecution.endTime) - new Date(cachedExecution.startTime) : null,
            nodeMetrics: cachedExecution.nodeMetrics || {}
          },
          nodes: formattedNodes,
          edges: formattedEdges,
          nodeScriptOutputs: cachedExecution.scriptOutputs || {},
          conditionEvaluations: cachedExecution.conditionEvaluations || {},
          errors: cachedExecution.errors || [],
          visitedNodes: cachedExecution.visitedNodes || [],
          executionIndex: 'in-progress',
          isInProgress: true
        });
      } catch (err) {
        logger.error(`Error building details for cached execution: ${err.message}`);
        // Fall through to normal execution history lookup
      }
    }
  }
  
  // Normal execution history lookup (for completed executions)
  // If executionId is provided, resolve it to an index
  if (executionId && executionIndex === 'latest') {
    logger.info(`[MONITOR] Resolving executionId [${executionId}] to index for job [${jobId}]`);
    const index = await orchestrationEngine.getExecutionIndexById(jobId, executionId);
    if (index >= 0) {
      logger.info(`[MONITOR] Resolved executionId [${executionId}] to index [${index}]`);
      executionIndex = index;
    } else {
      logger.warn(`[MONITOR] Failed to resolve executionId [${executionId}] - using latest instead`);
    }
  }
  
  const details = await orchestrationMonitor.getExecutionDetails(jobId, executionIndex);
  res.json(details);
}));

app.get('/orchestration/node/output', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_VIEW), asyncHandler(async (req, res) => {
  const orchestrationMonitor = require('../orchestrationMonitor.js');
  const jobId = req.query.jobId;
  const nodeId = req.query.nodeId;
  let executionIndex = req.query.executionIndex || 'latest';
  const executionId = req.query.executionId;
  
  if (!jobId || !nodeId) {
    return res.status(400).json({ error: 'Job ID and Node ID are required' });
  }
  
  // If executionId is provided, resolve it to an index
  if (executionId && executionIndex === 'latest') {
    const index = await orchestrationEngine.getExecutionIndexById(jobId, executionId);
    if (index >= 0) {
      executionIndex = index;
    }
  }
  
  const output = await orchestrationMonitor.getNodeOutput(jobId, nodeId, executionIndex);
  res.json(output);
}));

app.get('/api/schedules/by-orchestration/:orchestrationId', User.isAuthenticated, User.requirePermission(PERMISSIONS.ORCHESTRATIONS_VIEW), asyncHandler(async (req, res) => {
  const orchestrationId = req.params.orchestrationId;
  
  if (!orchestrationId) {
    return res.status(400).json({ error: 'Orchestration ID is required' });
  }
  
  // Get all schedules
  const allSchedules = scheduler.getSchedules();
  let matchingSchedules = [];
  
  try {
    const schedules = Array.isArray(allSchedules) ? allSchedules : [];
    
    matchingSchedules = schedules.filter(schedule => 
      schedule.scheduleMode === 'orchestration' && 
      schedule.orchestrationId === orchestrationId
    );
  } catch (err) {
    logger.warn('Error fetching schedules for orchestration:', err.message);
    matchingSchedules = [];
  }
  
  res.json({
    success: true,
    schedules: matchingSchedules
  });
}));
};
