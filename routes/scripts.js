module.exports = function register(app) {
app.post('/saveScript', express.json(), validateCsrf, User.isAuthenticated,
  User.requireAnyPermission([PERMISSIONS.SCRIPTS_CREATE, PERMISSIONS.SCRIPTS_EDIT]),
  asyncHandler(async (req, res) => {
  const body = req.body && typeof req.body === 'object' ? req.body : {};
  const scriptName = normalizeScriptFilename(body.scriptName);
  if (!scriptName) {
    throw new AppError('Invalid script filename', 400);
  }
  if (typeof body.script !== 'string') {
    throw new AppError('Script content is required', 400);
  }

  const filePath = getValidatedScriptPath(scriptName);

  try {
    await asyncUtils.writeFileAsync(filePath, body.script);
    logger.info(`File ${scriptName} saved successfully`);
    res.json({ status: 'success', message: 'File saved successfully.', scriptName });
  } catch (error) {
    logger.error(`Error saving script ${scriptName}:`, error.message);
    throw new AppError(`Failed to save script: ${error.message}`, 500);
  }
}));

app.get('/rest/script/:script', User.isAuthenticated, User.requirePermission(PERMISSIONS.SCRIPTS_VIEW), (req, res) => {
  const scriptFilename = req.params.script;
  const mode = req.query.mode;
  let scriptPath;
  try {
    scriptPath = getValidatedScriptPath(scriptFilename);
  } catch (err) {
    return res.status(400).send('Invalid script filename');
  }

  fs.readFile(scriptPath, 'utf8', (err, data) => {
    if (err) {
      return res.status(500).send('Error reading script file');
    }

    if (mode === 'meta') {
      return res.json(parseScriptParameterBlock(data));
    }

    // The editor loads this response into the buffer. Return the file unchanged.
    res.set('Content-Type', 'text/plain; charset=utf-8');
    res.send(data);
  });
});

app.get('/rest/script/:scriptName/export', User.isAuthenticated, User.requirePermission(PERMISSIONS.SCRIPTS_EXPORT), asyncHandler(async (req, res) => {
  const scriptName = req.params.scriptName;
  const scriptPath = getValidatedScriptPath(scriptName);

  let scriptContent;
  try {
    scriptContent = await asyncUtils.readFileAsync(scriptPath, 'utf8');
  } catch (error) {
    if (error && error.code === 'ENOENT') {
      throw new AppError('Script not found', 404);
    }
    throw new AppError(`Unable to read script [${scriptName}]`, 500);
  }

  const zipBuffer = await importExportManager.createScriptExportBuffer({
    scriptName,
    scriptContent
  });

  const safeName = importExportManager.toSafeFilenamePart(scriptName.replace(/\.sh$/i, ''), 'script');
  const timestamp = importExportManager.getTimestampForFilename();
  const exportFilename = `script-${safeName}-${timestamp}.zip`;

  res.setHeader('Content-Type', 'application/zip');
  res.setHeader('Content-Disposition', `attachment; filename="${exportFilename}"`);
  res.send(zipBuffer);
}));

app.post('/rest/script/import', User.isAuthenticated, User.requirePermission(PERMISSIONS.SCRIPTS_IMPORT), upload.single('importFile'), asyncHandler(async (req, res) => {
  if (!req.file || !req.file.buffer) {
    throw new AppError('Missing import file upload', 400);
  }

  let parsedImport;
  try {
    parsedImport = await importExportManager.parseScriptImportBuffer(req.file.buffer);
  } catch (error) {
    throw new AppError(`Script import validation failed: ${error.message}`, 400);
  }

  const existingScripts = refreshScripts().map((item) => item && item.data ? item.data.filename : null).filter(Boolean);
  const resolvedName = importExportManager.resolveUniqueScriptFilename(parsedImport.scriptName, existingScripts);
  const destinationPath = getValidatedScriptPath(resolvedName.finalName);
  await asyncUtils.writeFileAsync(destinationPath, parsedImport.scriptContent);

  logger.info(`Script imported successfully as [${resolvedName.finalName}]`);
  res.json({
    success: true,
    importedType: 'script',
    originalName: parsedImport.scriptName,
    scriptName: resolvedName.finalName,
    renamed: resolvedName.renamed,
    warnings: resolvedName.renamed ? ['Script name already existed and was auto-renamed.'] : []
  });
}));

app.get('/rest/script-test/:executionId', User.isAuthenticated, User.requirePermission(PERMISSIONS.SCRIPTS_TEST), asyncHandler(async (req, res) => {
  const execution = scriptTestManager.getExecution(req.params.executionId);
  if (!execution) {
    res.status(404).json({ success: false, error: 'Test execution not found or expired' });
    return;
  }

  res.json({ success: true, execution });
}));

app.post('/rest/script-test/state', User.isAuthenticated, User.requirePermission(PERMISSIONS.SCRIPTS_TEST), asyncHandler(async (req, res) => {
  scriptTestManager.cleanupExpiredTests();

  const scriptName = req.body.scriptName || null;
  const sourceType = req.body.sourceType === 'saved' ? 'saved' : 'editor';
  const scriptSource = typeof req.body.scriptContent === 'string' ? req.body.scriptContent : '';

  if (sourceType === 'saved' && !scriptName) {
    res.status(400).json({ success: false, error: 'Saved script lookup requires a script name' });
    return;
  }

  if (sourceType !== 'saved' && !scriptName && (!scriptSource || scriptSource.trim().length === 0)) {
    res.status(400).json({ success: false, error: 'Editor lookup requires script content or a script name' });
    return;
  }

  const scriptIdentity = scriptTestManager.buildScriptIdentity({
    scriptName,
    scriptSource,
    sourceType,
  });
  const blockingState = scriptTestManager.getBlockingState(scriptIdentity);

  res.json({
    success: true,
    scriptIdentity,
    state: blockingState,
  });
}));

app.post('/rest/script-test/start', User.isAuthenticated, User.requirePermission(PERMISSIONS.SCRIPTS_TEST), asyncHandler(async (req, res) => {
  scriptTestManager.cleanupExpiredTests();

  const agentName = req.body.agentName;
  const scriptName = req.body.scriptName || null;
  let scriptDescription = typeof req.body.scriptDescription === 'string' ? req.body.scriptDescription : '';
  const sourceType = req.body.sourceType === 'saved' ? 'saved' : 'editor';
  const commandParams = String(req.body.commandParams || '');
  let scriptSource = typeof req.body.scriptContent === 'string' ? req.body.scriptContent : '';

  if (!agentName) {
    res.status(400).json({ success: false, error: 'Agent is required' });
    return;
  }

  const agent = agents.getAgent(agentName);
  if (!agent) {
    res.status(404).json({ success: false, error: `Agent [${agentName}] not found` });
    return;
  }

  if (agent.status === 'offline') {
    res.status(409).json({ success: false, error: `Agent [${agentName}] is offline` });
    return;
  }

  if (sourceType === 'saved') {
    if (!scriptName) {
      res.status(400).json({ success: false, error: 'Saved script tests require a script name' });
      return;
    }

    scriptSource = await readSavedScript(scriptName);
  } else if (!scriptSource || scriptSource.trim().length === 0) {
    res.status(400).json({ success: false, error: 'Editor tests require script content' });
    return;
  }

  const parameterBlock = parseScriptParameterBlock(scriptSource);
  scriptDescription = scriptDescription || parameterBlock.metaDescription;
  const scriptParameters = parameterBlock.metaParameters;

  const crypto = require('crypto');
  const executionId = crypto.randomBytes(8).toString('hex');
  const createResult = scriptTestManager.createTest({
    executionId,
    agentName,
    scriptName,
    scriptDescription,
    scriptParameters,
    scriptSource,
    sourceType,
    commandParams,
    requestedBy: getScriptTestRequestedBy(req),
  });

  if (!createResult.ok) {
    res.status(409).json({
      success: false,
      code: createResult.type === 'active' ? 'ACTIVE_TEST_EXISTS' : 'UNSEEN_RETAINED_RESULT',
      execution: createResult.execution,
      scriptIdentity: createResult.scriptIdentity,
    });
    return;
  }

  const execution = createResult.execution;
  agentComms.sendCommand(
    agentName,
    'execute/scriptTest',
    scriptSource,
    commandParams,
    execution.jobName,
    undefined,
    true,
    execution.executionId,
    null,
    {},
    null,
    {
      executionMode: 'test',
      scriptName: execution.scriptName,
      scriptIdentity: execution.scriptIdentity,
      sourceType: execution.sourceType,
      scriptLabel: execution.scriptLabel,
    }
  );

  res.json({ success: true, execution });
}));

app.post('/rest/script-test/:executionId/acknowledge', User.isAuthenticated, User.requirePermission(PERMISSIONS.SCRIPTS_TEST), asyncHandler(async (req, res) => {
  const execution = scriptTestManager.acknowledgeExecution(req.params.executionId);
  if (!execution) {
    res.status(404).json({ success: false, error: 'Test execution not found or expired' });
    return;
  }

  res.json({ success: true, execution });
}));

app.post('/rest/script-test/:executionId/discard', User.isAuthenticated, User.requirePermission(PERMISSIONS.SCRIPTS_TEST), asyncHandler(async (req, res) => {
  const discarded = scriptTestManager.discardExecution(req.params.executionId);
  if (!discarded) {
    res.status(409).json({ success: false, error: 'Unable to discard active or missing test execution' });
    return;
  }

  res.json({ success: true });
}));

app.post('/rest/script-test/:executionId/terminate', User.isAuthenticated, User.requirePermission(PERMISSIONS.SCRIPTS_TEST), asyncHandler(async (req, res) => {
  const execution = scriptTestManager.getExecution(req.params.executionId);
  if (!execution) {
    res.status(404).json({ success: false, error: 'Test execution not found or expired' });
    return;
  }

  if (!execution.isActive) {
    res.status(409).json({ success: false, error: 'Test execution is no longer active' });
    return;
  }

  scriptTestManager.requestTermination(req.params.executionId, getScriptTestRequestedBy(req));
  agentComms.sendCommand(
    execution.agentName,
    'execute/terminateScriptTest',
    'terminateExecution',
    req.params.executionId,
    execution.jobName,
    undefined,
    true,
    req.params.executionId,
    null,
    {},
    null,
    {
      executionMode: 'test',
      scriptName: execution.scriptName,
      scriptIdentity: execution.scriptIdentity,
      sourceType: execution.sourceType,
      scriptLabel: execution.scriptLabel,
    }
  );

  res.json({ success: true, execution: scriptTestManager.getExecution(req.params.executionId) });
}));

app.get('/rest/jobs-for-script/:scriptName', User.isAuthenticated, User.requirePermission(PERMISSIONS.SCRIPTS_VIEW), asyncHandler(async (req, res) => {
  try {
    const scriptName = req.params.scriptName;
    if (!SCRIPT_FILENAME_REGEX.test(scriptName)) {
      return res.status(400).json({ success: false, error: 'Invalid script filename' });
    }

    const usageEntries = await getScriptUsageEntries(scriptName);

    res.json({
      success: true,
      script: scriptName,
      jobs: usageEntries,
      count: usageEntries.length
    });
  } catch (err) {
    logger.error('Error fetching jobs for script:', err.message);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch jobs for script: ' + err.message
    });
  }
}));

app.delete('/rest/script/:scriptName', User.isAuthenticated, User.requirePermission(PERMISSIONS.SCRIPTS_DELETE), asyncHandler(async (req, res) => {
  const scriptName = req.params.scriptName;
  const csrfToken = req.headers['x-csrf-token'];

  if (!csrfToken || csrfToken !== req.session.csrfToken) {
    return res.status(403).json({ success: false, error: 'CSRF token validation failed' });
  }

  if (!SCRIPT_FILENAME_REGEX.test(scriptName)) {
    return res.status(400).json({ success: false, error: 'Invalid script filename' });
  }

  const usageEntries = await getScriptUsageEntries(scriptName);
  if (usageEntries.length > 0) {
    return res.status(409).json({
      success: false,
      error: 'Script is currently used by jobs or orchestrations',
      jobs: usageEntries,
      count: usageEntries.length
    });
  }

  const scriptPath = getValidatedScriptPath(scriptName);

  try {
    await fs.promises.unlink(scriptPath);
    logger.info(`Script deleted successfully: ${scriptName}`);
    return res.json({ success: true, script: scriptName });
  } catch (err) {
    if (err && err.code === 'ENOENT') {
      return res.status(404).json({ success: false, error: 'Script not found' });
    }
    logger.error(`Error deleting script [${scriptName}]: ${err.message}`);
    return res.status(500).json({ success: false, error: 'Failed to delete script' });
  }
}));

app.get('/scriptEditor.html',User.isAuthenticated, User.requireAnyPermission([PERMISSIONS.SCRIPTS_CREATE, PERMISSIONS.SCRIPTS_EDIT]), (req, res) => {
  //refresh the scripts list
  var scriptsMeta = refreshScripts();
  var scriptToEdit=req.query.index;

  var templateData = undefined;
  if(serverConfig.templates.enabled=="true")templateData=templates;
  res.render('scripteditor',{
    scripts: scriptsMeta,
    agents: agents.getDict(),
    scriptTestAgentsBase64: buildScriptTestAgentsBase64(),
    templates: templateData,
    scriptToEdit: scriptToEdit,
    csrf: req.csrfToken(),
  });
});

app.get('/scriptList.html', User.isAuthenticated, User.requirePermission(PERMISSIONS.SCRIPTS_VIEW), asyncHandler(async (req, res) => {
  var scriptsMeta = refreshScripts();

  res.render('scriptList', {
    agents: agents.getDict(),
    scriptTestAgentsBase64: buildScriptTestAgentsBase64(),
    scripts: scriptsMeta,
    csrf: req.csrfToken(),
  });
}));
};
