module.exports = function register(app) {
app.get('/socket.io/socket.io.js', (req, res) => {
  res.sendFile(appRoot + '/node_modules/socket.io/client-dist/socket.io.js');
});

app.get('/rest/debug/logs', User.isAuthenticated, User.requireSuperAdmin(), async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  const logMessages = loglines.map(log => log.data.join(' ')); // Extract log messages
  res.json(logMessages); // Send log messages as JSON
});

app.get('/rest/debug/on', User.isAuthenticated, User.requireSuperAdmin(), async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }

  try {
    logger.level="trace";
    const statusJsonStr = JSON.stringify({"logger_level":logger.level}); 
    res.set('Content-Type', 'application/json'); // Set the content-type header
    res.send(statusJsonStr); // Send the JSON response
  } catch (error) {
    console.error("Error in API:", error);
    res.status(500).json({ status: "error", message: "Internal Server Error" });
  }
});

app.get('/rest/debug/off', User.isAuthenticated, User.requireSuperAdmin(), async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }

  try {
    logger.level="warn";
    const statusJsonStr = JSON.stringify({"logger_level":logger.level}); 
    res.set('Content-Type', 'application/json'); // Set the content-type header
    res.send(statusJsonStr); // Send the JSON response
  } catch (error) {
    console.error("Error in API:", error);
    res.status(500).json({ status: "error", message: "Internal Server Error" });
  }
});

app.get('/rest/debug', User.isAuthenticated, User.requireSuperAdmin(), async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  
  //build debug Object
  var debug={};
  debug.settings={};
  debug.settings.REPOSITORY_URL= REPOSITORY_URL;
  debug.settings.serverConfig = serverConfig;
  debug.connections={};
  debug.connections.mqtt={};
  debug.connections.mqtt.transport = mqttTransport;
  debug.connections.ws={};
  debug.connections.ws.transport= webSocketServer;
  debug.connections.ws.wsClients=wsClients;
  debug.logger={};
  debug.logger.level = logger.level;

  debug.history={};

//  debug.user={}
//  debug.user.info={};
//  debug.user.userCount=await User.getUserCount();
//  debug.user.info.username = user

  try{
    var debugStr = maskPasswords(JSON.stringify(debug));
    res.set('Content-Type', 'application/json'); // Set the content-type header
    res.send(debugStr); // Send the JSON response
  }
  catch(err){
    res.status(500).json({ status: "error", message: "Internal Server Error [" + err.name +"]" + err.message});
  }
});
};
