module.exports = function register(app) {
app.get('/login.html', asyncHandler(async (req, res) => {
  const redirect = safeRedirect(req.query.redirect, '');
  logger.debug("Getting login page");
  if (serverConfig.server.hostname == "UNDEFINED") {
    serverConfig.server.hostname = req.hostname;
  }

  const userCount = await User.getUserCount();
  if (userCount > 0) {
    logger.debug("A user is registered");
    res.render('login', {
      version: version,
      redirect: redirect,
      csrf: req.csrfToken(),
    });
  } else {
    logger.warn("no user exists - switching to first time register");
    res.redirect('/register.html');
  }
}));

app.get('/logout.html', (req, res) => {
  req.session.destroy(() => {
    res.redirect('/login.html');
  });
});

app.get('/register.html', asyncHandler(async (req, res) => {
  const userCount = await User.getUserCount();
  logger.debug("User Count:" + userCount);
  if (userCount <= 0) {
    res.render('register', { csrf: req.csrfToken() });
  } else {
    logger.warn("Register accessed when user already exists");
    res.redirect('/?message=Register+is+disabled');
  }
}));

app.post('/register.html', validateCsrf, asyncHandler(async (req, res) => {
  if (await User.getUserCount() <= 0) {
    const user = await User.createUser(req.body.username, req.body.email, req.body.password);
    res.redirect('/login.html?message=User+Created.+Please+authenticate+with+your+credentials');
  } else {
    throw new AppError('User registration is disabled', 400);
  }
}));

app.get('/forgot.html', (req, res) => {
  if(serverConfig.server.hostname=="UNDEFINED")serverConfig.server.hostname = req.hostname;
  res.render('forgot', { csrf: req.csrfToken() });
});

app.post('/forgot.html', validateCsrf, asyncHandler(async (req, res) => {
  const token = await User.generateResetToken(req.body.username);
  logger.info(`Reset token generated for user: ${req.body.username}`);
  const message = "Please+check+your+email+for+a+one+time+password+reset+link+to+continue";
  res.redirect('/login.html?message=' + message);
}));

app.get('/reset/:token/:user', asyncHandler(async (req, res) => {
  const tokenValid = await User.isResetTokenValid(req.params.user, req.params.token);
  if (tokenValid !== null) {
    res.render('reset', { token: req.params.token, user: req.params.user, csrf: req.csrfToken() });
  } else {
    res.redirect('/forgot.html?message=Your+one+time+reset+token+has+already+been+used+or+timed+out.+Please+reset+your+passsword+again');
  }
}));

app.post('/reset/:token/:user', validateCsrf, asyncHandler(async (req, res) => {
  const tokenValid = await User.isResetTokenValid(req.params.user, req.params.token);
  if (tokenValid !== null) {
    const resetSuccessful = await User.resetPassword(req.params.user, req.params.token, req.body.password);
    if (resetSuccessful) {
      logger.info(`Password reset successful for user: ${req.params.user}`);
      res.redirect('/login.html?message=Your+password+has+been+reset.+Please+login+with+your+new+credentials');
    } else {
      res.redirect(`/reset/${req.params.token}/${req.params.user}?message=Unable+to+reset+the+password`);
    }
  } else {
    res.redirect('/forgot.html?message=Your+one+time+reset+token+has+already+been+used+or+timed+out.+Please+reset+your+passsword+again');
  }
}));

app.get('/invite/:token/:username', asyncHandler(async (req, res) => {
  if(serverConfig.server.hostname === 'UNDEFINED') serverConfig.server.hostname = req.hostname;
  const valid = await User.isInviteTokenValid(req.params.username, req.params.token);
  if (valid) {
    res.render('invite', { token: req.params.token, username: req.params.username, csrf: req.csrfToken() });
  } else {
    res.redirect('/login.html?message=Invite+link+has+expired+or+already+been+used');
  }
}));

app.post('/invite/:token/:username', validateCsrf, asyncHandler(async (req, res) => {
  const { password, password2 } = req.body;
  const inviteUrl = new URL(`/invite/${req.params.token}/${req.params.username}`, `${req.protocol}://${req.get('host')}`);

  if (typeof password !== 'string' || password.length < 8) {
    inviteUrl.searchParams.append('message', 'Password must be at least 8 characters long');
    return res.redirect(inviteUrl.toString());
  }

  if (password2 !== undefined && password !== password2) {
    inviteUrl.searchParams.append('message', 'Passwords do not match');
    return res.redirect(inviteUrl.toString());
  }

  const accepted = await User.acceptInvite(req.params.username, req.params.token, password);
  if (accepted) {
    logger.info(`Invite accepted for user: ${req.params.username}`);
    res.redirect('/login.html?message=Account+activated.+Please+login+with+your+new+credentials');
  } else {
    inviteUrl.searchParams.append('message', 'Invite link has expired or already been used');
    res.redirect(inviteUrl.toString());
  }
}));

app.post('/login.html', validateCsrf, asyncHandler(async (req, res) => {
  const { username, password } = req.body;
  const redirect = safeRedirect(req.body.redirect, '');
  const ipAddress = req.headers['x-forwarded-for'] || req.ip;
  logger.info(`Logging in user ${username} from IP: ${ipAddress}`);

  const user = await User.getUserByUsername(username.toLowerCase());
  if (!user) {
    logger.warn(`Login attempt for non-existent user: ${username}`);
    const loginUrl = new URL('/login.html', `${req.protocol}://${req.get('host')}`);
    loginUrl.searchParams.append('message', 'Invalid username or password');
    if (redirect) loginUrl.searchParams.append('redirect', redirect);
    return res.redirect(loginUrl.toString());
  }

  if (!user.isActive) {
    logger.warn(`Login attempt for inactive account: ${username}`);
    const loginUrl = new URL('/login.html', `${req.protocol}://${req.get('host')}`);
    if (!user.password) {
      loginUrl.searchParams.append('message', 'Account is not active yet. Please complete your invite setup from the email link.');
    } else {
      loginUrl.searchParams.append('message', 'Account has been deactivated');
    }
    return res.redirect(loginUrl.toString());
  }

  if (!user.password) {
    logger.warn(`Login blocked for user without password hash: ${username}`);
    const loginUrl = new URL('/login.html', `${req.protocol}://${req.get('host')}`);
    loginUrl.searchParams.append('message', 'Invalid username or password');
    if (redirect) loginUrl.searchParams.append('redirect', redirect);
    return res.redirect(loginUrl.toString());
  }

  const passwordMatch = await bcrypt.compare(password, user.password);
  if (!passwordMatch) {
    logger.warn(`Failed login attempt for user: ${username} from IP: ${ipAddress}`);
    if (serverConfig.server.loginFailEnabled == "true") {
      notifier.sendNotification("Authentication Failure",
        `User Login for account ${username} from IP: ${ipAddress} failed. If this was unexpected, please change the password immediately`,
        "WARNING");
    }
    const loginUrl = new URL('/login.html', `${req.protocol}://${req.get('host')}`);
    loginUrl.searchParams.append('message', 'Invalid username or password');
    if (redirect) loginUrl.searchParams.append('redirect', redirect);
    return res.redirect(loginUrl.toString());
  }

  logger.debug("passwords match - authentication successful");
  user.lastLogin = Date.now();
  await User.updateUser(user);
  req.session.user = user;

  if (serverConfig.server.loginSuccessEnabled == "true") {
    notifier.sendNotification("UserAuthentication",
      `User Logged in for account ${username} from IP: ${ipAddress}`,
      "INFORMATION");
  }

  // Determine redirect location
  if (serverConfig.websocket_server.port && serverConfig.websocket_server.port !== null && serverConfig.websocket_server.port.length > 0) {
    if (redirect && redirect !== null) {
      logger.debug(`Found redirect in url: ${redirect}`);
      res.redirect(redirect);
    } else {
      res.redirect('/');
    }
  } else {
    // MQTT Not connected - go to initial setup
    res.redirect('/initial-setup-welcome.html');
  }
}));

app.get('/initial-setup.html', User.isAuthenticated, asyncHandler(async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html?not+authenticated');
  }

  res.render('initialsetup1', {
    version: version,
    serverConfig: serverConfig,
    csrf: req.csrfToken(),
  });
}));

app.get('/initial-setup-mqtt.html',User.isAuthenticated, async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html?not+authenticated');
  }

  const fullHost = req.get('host'); // or req.headers.host
  const hostName = fullHost.split(':')[0]; // This strips the port if it exists

  res.render('initialsetupMQTT',{
    version: version,
    serverConfig: serverConfig,
    hostName: hostName,
    csrf: req.csrfToken(),
  }); 
});

app.get('/initial-setup-welcome.html',User.isAuthenticated, async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html?not+authenticated');
  }

  const fullHost = req.get('host'); // or req.headers.host
  const hostName = fullHost.split(':')[0]; // This strips the port if it exists

  res.render('initialsetupWelcome',{
    version: version,
    serverConfig: serverConfig,
    hostName: hostName,
    csrf: req.csrfToken(),
  }); 
});

app.get('/initial-setup-complete.html',User.isAuthenticated, async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html?not+authenticated');
  }

  res.render('initialsetupComplete',{
    version: version,
    serverConfig: serverConfig,
    csrf: req.csrfToken(),
  }); 
});

app.get('/initial-setup-server.html',User.isAuthenticated, async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html?not+authenticated');
  }

  const fullHost = req.get('host'); // or req.headers.host
  const hostName = fullHost.split(':')[0]; // This strips the port if it exists

  res.render('initialsetupServer',{
    version: version,
    serverConfig: serverConfig,
    hostName: hostName,
    csrf: req.csrfToken(),
  }); 
});

app.post('/initial-setup-server.html', validateCsrf, User.isAuthenticated, async (req, res) => {
  logger.debug("POST for initial-setup-server.html");
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html?not+authenticated');
  }

  var { wsServer,webserverPort, wsPort,timezone, websocket, mqtt} = req.body;
  const websocketEnabled = websocket === "on" ? "true" : "false";
  const mqttEnabled = mqtt === "on" ? "true" : "false";
  if (websocketEnabled === "false" && mqttEnabled === "false") {
    return res.redirect('/initial-setup-server.html?message=' + encodeURIComponent('Enable WebSocket or MQTT so agents have a way to connect.'));
  }
  serverConfig.websocket_server.server = wsServer;
  serverConfig.server.hostname = wsServer;
  serverConfig.server.port = webserverPort;
  serverConfig.websocket_server.port = wsPort;
  serverConfig.websocket_server.enabled = websocketEnabled;
  serverConfig.mqtt.enabled = mqttEnabled;
  
  serverConfig.server.timezone = timezone;

  confighandler.saveServerConfig();
  if (typeof webSocketServer !== 'undefined' && webSocketServer.applyWebSocketSetting) {
    await webSocketServer.applyWebSocketSetting();
  }
  if (mqttEnabled === "true") return res.redirect('/initial-setup-mqtt.html');
  if (typeof mqttTransport !== 'undefined' && mqttTransport.startMqttConnectionProcess) {
    await mqttTransport.startMqttConnectionProcess(true);
  }
  return res.redirect('/initial-setup-complete.html');
});

app.post('/initial-setup1.html', validateCsrf, User.isAuthenticated, async (req, res) => {
  logger.debug("POST for initial-setup1.html");
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html?not+authenticated');
  }

  logger.warn(`[ICONS_RESET] INITIAL SETUP 1 CALLED - This will reset job_icons to defaults`);
  logger.warn(`[ICONS_RESET] Previous icons: ${JSON.stringify(serverConfig.job_icons)}`);

  var { websocket,mqtt} = req.body;
  const websocketEnabled = websocket === "on" ? "true" : "false";
  const mqttEnabled = mqtt === "on" ? "true" : "false";
  if (websocketEnabled === "false" && mqttEnabled === "false") {
    return res.redirect('/initial-setup1.html?message=' + encodeURIComponent('Enable WebSocket or MQTT so agents have a way to connect.'));
  }

  serverConfig.websocket_server.enabled = websocketEnabled;
  serverConfig.mqtt.enabled = mqttEnabled;

  serverConfig.job_icons=[
    "work","cloud","save","storage","dns","layers","schema","delete","hub","insert_drive_file",
    "folder","folder_shared","folder_delete","folder_special","folder_zip","drive_folder_upload",
    "topic","rule_folder","sd_card","archive","library_music","video_library","video_camera_back",
    "photo_library","audio_file","password","account_box","build","restart_alt","start","stop_circle",
    "settings","security","safety_check","sports_esports","dangerous","error","favorite","send",
    "search","feed","shopping_cart","room_service","email","desktop_windows","camera_alt","laptop",
    "power_settings_new","router","schedule","flag","grade","image","key","receipt","money"
  ]

  logger.warn(`[ICONS_RESET] New icons set: ${serverConfig.job_icons.length} icons`);
  logger.debug(`websocket ${websocket}, MQTT ${mqtt}`)
  confighandler.saveServerConfig();
  
  // if(mqttEnabled=="true")return res.redirect('/initial-setup-mqtt.html');
  // else return res.redirect('/initial-setup-complete.html');
  return res.redirect('/initial-setup-complete.html');
});

app.post('/initial-setupMQTT.html', validateCsrf, User.isAuthenticated, async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html?not+authenticated');
  }

    var { mqttServer,mqttPort,mqttUsername,mqttPassword} = req.body;
    serverConfig.mqtt.server=mqttServer;
    serverConfig.mqtt.port=mqttPort;
    serverConfig.mqtt.username=mqttUsername;
    serverConfig.mqtt.password=mqttPassword;
    logger.debug(`MQTT Server ${mqttServer}, Port ${mqttPort}`)
    confighandler.saveServerConfig();
    
    try {
      logger.debug("Conneting to MQTT Server");
      await mqttTransport.connectToMQTTServer();
      logger.debug("Returned ok");
      // Connection successful, perform actions here
      return res.redirect('/initial-setup-complete.html');
    } 
    catch (error) {
      // Handle connection error here
      logger.warn("Unable to connect to MQTT server");
      logger.warn(error);
      return res.redirect('/initial-setupMQTT.html?message=Please+Check+Your+MQTT+Server+and+Settings');  
    }
});

app.get('/profile.html',User.isAuthenticated, async (req, res) => {
  const user = req.session.user;
  if (!user) return res.redirect('/register.html');
  res.render('profile.ejs', { user, csrf: req.csrfToken() });
});
};
