module.exports = function register(app) {
app.post('/settings.html', validateCsrf, User.isAuthenticated, User.requirePermission(PERMISSIONS.SETTINGS_ACCESS), async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html?not+authenticated');
  }

  logger.debug(`[SETTINGS_POST] Received form submission with ${Object.keys(req.body).length} fields`);
  logger.debug(`[SETTINGS_POST] Request body keys: ${Object.keys(req.body).join(', ')}`);

  var { protocol, serverHostname, serverPort, timezone, notificationType,loglevel,
    websocketEnabled,websocketServer,websocketPort,
    mqttEnabled,mqttServer,mqttPort,mqttUsername,mqttPassword,
    smtpEnabled, smtpServer, smtpPort, smtpSecure, smtpUsername, smtpPassword,smtpEmailFrom,smtpEmailTo,
    webHookUrl,iconslist,
    minDisconnectDurationForNotification,
    templateEnabled,templateServer,
    connectionEnabled,loginSuccessEnabled,loginFailEnabled,jobFailEnabled,
    pluginRegistryUrl,pluginCacheLocation,validationLocation,rejectedLocation,iconsLocation,installsLocation    
  } = req.body;

  logger.debug(`[SETTINGS_POST] iconslist extracted from req.body: "${iconslist}" (type: ${typeof iconslist})`);

  //Template
  if(templateEnabled=="on")templateEnabled="true";
  else templateEnabled="false";
  serverConfig.templates.enabled = templateEnabled;
  serverConfig.templates.repositoryUrl = templateServer;

  //Plugin Manager settings
  if (!serverConfig.pluginRegistry) serverConfig.pluginRegistry = {};
  if (pluginRegistryUrl) serverConfig.pluginRegistry.url = pluginRegistryUrl;
  if (pluginCacheLocation) serverConfig.pluginRegistry.plugins = pluginCacheLocation;
  if (validationLocation) serverConfig.pluginRegistry.validation = validationLocation;
  if (rejectedLocation) serverConfig.pluginRegistry.rejected = rejectedLocation;
  if (iconsLocation) serverConfig.pluginRegistry.icons = iconsLocation;
  if (installsLocation) serverConfig.pluginRegistry.installs = installsLocation;

  // Allow clearing the token by saving an empty string
  // if(serverConfig.pluginRegistry.githubToken !== undefined && serverConfig.pluginRegistry.githubToken !== null) {
  //   serverConfig.pluginRegistry.githubToken = pluginGithubToken || '';
  // }

  //websocket Settings
  if(websocketEnabled=="on")websocketEnabled="true";
  else websocketEnabled="false";
  serverConfig.websocket_server.enabled = websocketEnabled;
  serverConfig.websocket_server.port = websocketPort;
  serverConfig.websocket_server.server = websocketServer;

  //MQTT Settings
  if(mqttEnabled=="on")mqttEnabled="true";
  else mqttEnabled="false";
  serverConfig.mqtt.enabled = mqttEnabled;
  serverConfig.mqtt.server = mqttServer;
  serverConfig.mqtt.port = mqttPort;
  if(mqttUsername!=null && mqttUsername.length>0){
    serverConfig.mqtt.username = mqttUsername;
  }
  const nextMqttPassword = replaceStoredSecret(mqttPassword);
  if (nextMqttPassword !== null) {
    serverConfig.mqtt.password = nextMqttPassword;
  }

  //STMP Settings
  if(smtpEnabled=="on")smtpEnabled="true";
  else smtpEnabled="false";
  serverConfig.smtp.enabled = smtpEnabled;
  serverConfig.smtp.host = smtpServer;
  serverConfig.smtp.port = smtpPort;
  if(smtpSecure=="on")smtpSecure="true";
  else smtpSecure="false";
  serverConfig.smtp.secure = smtpSecure;
  serverConfig.smtp.username = smtpUsername;
  const nextSmtpPassword = replaceStoredSecret(smtpPassword);
  if (nextSmtpPassword !== null) {
    serverConfig.smtp.password = nextSmtpPassword;
  }
  serverConfig.smtp.emailFrom = smtpEmailFrom;
  serverConfig.smtp.emailTo = smtpEmailTo;

  //Webhook settings
  serverConfig.webhook.url = webHookUrl;

  //Server Settings
  serverConfig.server.loglevel = parseInt(loglevel);
  if(serverConfig.server.loglevel == 0)serverConfig.server.debug="false";
  else serverConfig.server.debug="true";
  serverConfig.server.clearData = "false";
  serverConfig.server.protocol = protocol;
  serverConfig.server.hostname = serverHostname;
  serverConfig.server.timezone = timezone;
  serverConfig.server.notificationType = notificationType;
  serverConfig.server.port = parseInt(serverPort);
  serverConfig.server.minDisconnectDurationForNotification = parseInt(minDisconnectDurationForNotification);

  serverConfig.server.connectionEnabled = connectionEnabled === "on" ? "true" : "false";
  serverConfig.server.loginSuccessEnabled = loginSuccessEnabled === "on" ? "true" : "false";
  serverConfig.server.loginFailEnabled = loginFailEnabled === "on" ? "true" : "false";
  serverConfig.server.jobFailEnabled = jobFailEnabled === "on" ? "true" : "false";
  

  //icons list - filter out empty strings and validate
  logger.debug(`[ICONS_SAVE] Raw iconslist value: "${iconslist}" (type: ${typeof iconslist}, length: ${iconslist ? iconslist.length : 'undefined'})`);
  
  if (typeof iconslist !== 'string' || iconslist.trim() === '') {
    logger.warn(`[ICONS_SAVE] ATTEMPT TO SAVE EMPTY ICONS - iconslist is empty or null`);
    logger.warn(`[ICONS_SAVE] Request body keys: ${Object.keys(req.body).join(', ')}`);
    iconslist = '';
  }
  
  const iconsList = iconslist
    .split(',')
    .map(icon => icon.trim())
    .filter(icon => icon.length > 0);
  
  logger.debug(`[ICONS_SAVE] After processing - found ${iconsList.length} valid icons: ${JSON.stringify(iconsList)}`);
  
  // If no valid icons provided, keep the existing ones or use defaults
  if (iconsList.length === 0) {
    logger.warn(`[ICONS_SAVE] NO VALID ICONS PROVIDED - keeping existing configuration`);
    logger.warn(`[ICONS_SAVE] Current serverConfig.job_icons: ${JSON.stringify(serverConfig.job_icons)}`);
    if (!serverConfig.job_icons || serverConfig.job_icons.length === 0) {
      logger.warn(`[ICONS_SAVE] No existing icons found, restoring defaults`);
      // Fallback to default icons if none exist
      serverConfig.job_icons = [
        "work","cloud","save","storage","dns","layers","schema","delete","hub","insert_drive_file",
        "folder","folder_shared","folder_delete","folder_special","folder_zip","drive_folder_upload",
        "topic","rule_folder","sd_card","archive","library_music","video_library","video_camera_back",
        "photo_library","audio_file","password","account_box","build","restart_alt","start","stop_circle",
        "settings","security","safety_check","sports_esports","dangerous","error","favorite","send",
        "search","feed","shopping_cart","room_service","email","desktop_windows","camera_alt","laptop",
        "power_settings_new","router","schedule","flag","grade","image","key","receipt","money"
      ];
      logger.warn(`[ICONS_SAVE] Defaults restored: ${serverConfig.job_icons.length} icons`);
    }
  } else {
    logger.info(`[ICONS_SAVE] Successfully saved ${iconsList.length} valid icons`);
    serverConfig.job_icons = iconsList;
  }

  confighandler.saveServerConfig();

  return res.redirect('/settings.html?message="Saved+Settings.+Please+restart+the+container+for+the+values+to+take+effect');
  //res.render('settings.ejs', { serverConfig, user });
});

app.get('/settings.html',User.isAuthenticated, User.requirePermission(PERMISSIONS.SETTINGS_ACCESS), async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  const shownEncryptionKey = req.session.shownEncryptionKey || '';
  delete req.session.shownEncryptionKey;
  res.render('settings.ejs', {
    serverConfig,
    user,
    csrf: req.csrfToken(),
    encryptionKeyIsDefault: passman.usesDefaultEncryptionKey(),
    encryptionKeyFromEnv: Boolean(process.env.ORCHELIUM_ENCRYPTION_KEY),
    shownEncryptionKey,
    storedSecretMask: STORED_SECRET_MASK,
  });
});

app.post('/settings/encryption-key', validateCsrf, User.isAuthenticated, User.requirePermission(PERMISSIONS.SETTINGS_ACCESS), asyncHandler(async (req, res) => {
  const { rotateHubEncryptionKey } = require('../utils/rotateEncryptionKey');
  const agentAuth = require('../communications/agentAuth');
  const submitted = typeof req.body.newEncryptionKey === 'string' ? req.body.newEncryptionKey.trim() : '';
  if (!submitted || submitted === STORED_SECRET_MASK) {
    return res.redirect('/settings.html?message=' + encodeURIComponent('Enter a new encryption key, or generate one.') + '#secrets');
  }
  try {
    const result = await rotateHubEncryptionKey({
      dataDir: path.join(appRoot, 'data'),
      configPath: path.join(appRoot, 'data', 'server-config.json'),
      serverConfig,
      newSecret: req.body.newEncryptionKey,
      readCredentials: async () => agentAuth.currentCredentialStore(),
      writeCredentials: async (next) => {
        await agentAuth.replaceCredentialStore(next);
      },
    });
    req.session.shownEncryptionKey = result.secret;
    if (!passman.usesDefaultEncryptionKey()) {
      const items = notificationData.getItems();
      for (let i = items.length - 1; i >= 0; i--) {
        if (items[i] && items[i].title === ENCRYPTION_KEY_NOTICE_TITLE) {
          await notificationData.deleteItem(i);
        }
      }
    }
    logger.info('Hub encryption key changed and stored secrets were re-encrypted');
    return res.redirect('/settings.html?message=' + encodeURIComponent('Encryption key changed. Copy the new key if an agent still uses the hub key for enrollment.') + '#secrets');
  } catch (err) {
    logger.error('Encryption key change failed: ' + err.message);
    return res.redirect('/settings.html?message=' + encodeURIComponent(err.message) + '#secrets');
  }
}));

app.get('/api/backup/items', User.isAuthenticated, User.requirePermission(PERMISSIONS.SETTINGS_ACCESS), (req, res) => {
  try {
    const items = backupManager.getBackupItems();
    res.json(items);
  } catch (err) {
    logger.error('Error getting backup items:', err);
    res.status(500).json({ error: 'Failed to get backup items' });
  }
});

app.post('/api/backup/create', User.isAuthenticated, User.requirePermission(PERMISSIONS.SETTINGS_ACCESS), asyncHandler(async (req, res) => {
  try {
    const options = req.body;
    logger.info('Creating backup with options:', options);

    // Create backup
    const backupBuffer = await backupManager.createBackup(options);

    // Generate filename with timestamp
    const timestamp = new Date().toISOString().split('T')[0].replace(/-/g, '');
    const filename = `orchelium-backup-${timestamp}.zip`;

    // Send file to client
    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Length', backupBuffer.length);
    res.send(backupBuffer);

    logger.info(`Backup downloaded: ${filename}`);
  } catch (err) {
    logger.error('Backup creation error:', err);
    res.status(err.statusCode || 500).json({ error: err.message || 'Backup failed' });
  }
}));

app.post('/api/backup/restore', User.isAuthenticated, User.requirePermission(PERMISSIONS.SETTINGS_ACCESS), upload.single('backupFile'), asyncHandler(async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No backup file provided' });
    }

    logger.info('Starting backup restore from uploaded file');

    // Restore backup from upload
    const results = await backupManager.restoreBackup(req.file.buffer);

    logger.info('Backup restore completed:', results);
    res.json({
      success: true,
      itemsRestored: results.itemsRestored,
      warnings: results.warnings,
      recommendations: results.recommendations,
    });
  } catch (err) {
    logger.error('Backup restore error:', err);
    res.status(err.statusCode || 500).json({ error: err.message || 'Restore failed' });
  }
}));

app.get('/rest/settings/connection-enabled', User.isAuthenticated, User.requirePermission(PERMISSIONS.AGENTS_VIEW), (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  logger.info("Getting connection enabled setting");
  const connectionEnabled = serverConfig.server.connectionEnabled === 'true';
  res.setHeader("Content-Type","Application/JSON");
  res.send({ connectionEnabled: connectionEnabled });  
});

app.get('/rest/mqtt/reconnect', User.isAuthenticated, User.requirePermission(PERMISSIONS.SETTINGS_ACCESS), async (req, res) => {
  logger.info("Resetting MQTT connection")
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }

  var resp = null;
  try{
    logger.debug("Initiating new MQTT Connection");
    resp = await mqttTransport.startMqttConnectionProcess(true);
    logger.warn("Response is: " + resp);
    logger.debug("Initialized");
    if(resp!==null &&resp=="ok") res.sendStatus(200);
    else res.sendStatus(500);
  }
  catch (error){
    logger.error("Error occurred connecting to MQTT",error);
    res.sendStatus(500); 
  }
});

app.get('/rest/templates', User.isAuthenticated, User.requirePermission(PERMISSIONS.SETTINGS_ACCESS), async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  var templateRepository = {};
  templateRepository.status="unknown";
  templateRepository.date=new Date();
  templateRepository.templates = templates;
  templateRepository.status="Retrieved Templates from cache";
  templateRepository.date=new Date();
  res.set('Content-Type', 'application/json'); // Set the content-type header
  res.send(templateRepository);
});

app.get('/rest/templates/refresh', User.isAuthenticated, User.requirePermission(PERMISSIONS.SETTINGS_ACCESS), async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  var templateRepository = {};
  templateRepository.status="unknown";

  if(serverConfig.templates && serverConfig.templates.enabled=="true"){
    (async () => {
      templates = new TemplateRepository(serverConfig.templates.repositoryUrl);
      await templates.init();
    })();
    templateRepository.status="refreshed" 
    templateRepository.date=new Date();
  }
  else{
    templateRepository.status="Template Scripts Disabled";
    templateRepository.templates = null;
    logger.info("Template Scripts Disabled");
  }
  res.set('Content-Type', 'application/json'); // Set the content-type header
  res.send(templateRepository);

});
};
