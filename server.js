debug = require("./debug.js");
log4js = require("log4js");
moment = require('moment-timezone');
sanitizeHtml = require('sanitize-html');

var loglines=[];

// Manually format log messages
function formatLog(loggingEvent) {
  const timestamp = new Date(loggingEvent.startTime).toISOString(); // ISO timestamp
  const level = loggingEvent.level.levelStr; // Log level (DEBUG, INFO, etc.)
  const loggerName = loggingEvent.categoryName; // Logger name
  const message = loggingEvent.data.join(' '); // Log message

  return `[${timestamp}] [${level}] ${loggerName} - ${message}`;
}

// Configure log4js
log4js.configure({
  appenders: {
    // Custom appender to store logs in memory
    memory: {
      layout: { type: 'pattern', pattern: '[%d{yyyy-MM-ddThh:mm:ss.SSS}] [%p] %c - %m' }, // Apply pattern to memory output
      type: {
        configure: () => (loggingEvent) => {
          //const formattedMessage = formatLog(loggingEvent); // Manually format log event
          loglines.push(loggingEvent); // Store formatted message
          if (loglines.length > 250) loglines.shift(); // Limit to last 100 logs
        }
        
      }
    },
    console: {
      type: 'console',
      layout: { type: 'pattern', pattern: '[%[%d{yyyy-MM-ddThh:mm:ss.SSS}] [%f{1} %l] %m%]' } // Apply pattern to console output
    }
  },
  categories: {
    default: { appenders: ['memory', 'console'], level: 'debug',enableCallStack: true }
  }
});

logger = log4js.getLogger("OrcheliumServer");
logger.level="info";

//debug.setLogLevel(3);
confighandler = require("./configuration.js");

var express = require('express');
var session = require('express-session');
var cookieParser = require('cookie-parser');
var lusca = require('lusca');
var querystring = require('querystring');
var https = require('https');
var bodyParser = require('body-parser');
const multer = require('multer');

const User = require('./models/user');
const { asyncHandler, errorHandlerMiddleware, AppError } = require('./utils/errorHandler.js');
const asyncUtils = require('./utils/asyncUtils.js');
dateTimeUtils = require('./utils/dateTimeUtils.js');
mathUtils = require('./mathUtils.js');
passman = require('./passman.js');
smtpPass = "";
const versionInfo = require('./version.js');
const version = versionInfo.getVersion();
const minSupportedVersion = versionInfo.getMinimumAgentSupportedVersion();
const bcrypt = require('bcrypt');
//User.initializeDB('./data/user.db');

const TemplateRepository = require('./TemplateRepository');
var templates;
let REPOSITORY_URL = 'https://orchelium.com/template-repository/';

var path = require('path');
fs = require('fs');

nodemailer = require('nodemailer');
notifier = require ("./notify.js");
const backupManager = require('./backupManager.js');

serverConfig = confighandler.initServerConfig({});
serverConfig = confighandler.loadConfigJson("./data/server-config.json");
serverConfig = confighandler.processServerConfig(serverConfig);

hist = require("./history.js");
notificationData = require("./notificationData.js");

running=require("./running.js");
agentHistory = require ("./agentHistory.js");
db = require("./db.js");
nodeschedule = require('node-schedule');
scheduler = require ("./scheduler.js");
orchestration = require("./orchestration.js");
orchestrationEngine = require("./orchestrationEngine.js");
scriptTestManager = require('./scriptTestManager.js');
//const moment = require('moment-timezone');

agentComms = require ("./communications/agentCommunication.js");
mqttTransport = require ("./communications/mqttTransport.js");
mqttTransport.init();

webSocketBrowser = require('./communications/wsBrowserTransport.js');
webSocketServer = require("./communications/wsServerTransport.js");
webSocketServer.init();


agentStats = new Map();
wsClients = new Map();

// Helper function to find a WebSocket client by key prefix (startsWith search)
function findClientByPrefix(prefix) {
  for (const [connId, client] of wsClients.entries()) {
    if (connId.startsWith(prefix)) {
      logger.debug(`Found client connection ID starting with [${prefix}]: [${connId}]`);
      return client;
    }
  }
  logger.debug(`No client connection ID found starting with [${prefix}]`);
  return undefined;
}

const pingInterval = 60;
const connTimeoutInterval = 180;
const failedPingOffline = 3;

recordHolder = {};
const DEFAULT_MQTT_SERVER = 'localhost';
const DEFAULT_MQTT_SERVER_PORT = '1883';
const DEFAULT_WEBSOCKER_SERVER_PORT='49981"'





//------------------------------------------------------------------
// Initialize Debug
//------------------------------------------------------------------
//if(serverConfig.server.debug=="true"){debug.on();}
if(serverConfig.server.debug=="true"){
  if(serverConfig.server.loglevel){
    switch(serverConfig.server.loglevel){
      case 0:
        logger.level="off";
        break;
      case 1:
        logger.level="error";
        break;        
      case 2:
        logger.level="warn";
        break;
      case 3:
        logger.level="info";
        break;        
      case 4:
        logger.level="debug";
        break;        
      case 5:
        logger.level="trace";
        break;        
    }
    //debug.setLogLevel(serverConfig.server.loglevel);
  }
}
else{
  logger.level="warn";
}
// Initialize database and agents asynchronously
(async () => {
  try {
    if(serverConfig.server.clearData=="true"){
      logger.warn("!!!!!!!!!!!!!!!!!!!!!!!!!");
      logger.warn("!! CLEARING DATA STORE !!");
      logger.warn("!!!!!!!!!!!!!!!!!!!!!!!!!");
      await db.clearData();
      logger.warn("Data store cleared successfully.");
    }

    //------------------------------------------------------------------

    db.initializeDB('./data/data.db');
    User.init('./data/user.db',logger,notifier);
    agentHistory.initializeDB("./data/agentHistory.db");

    //------------------------------------------------------------------

    mqttDisconnectNotificiationSent=false;

    const execSync = require('child_process').execSync;
    const { config } = require("process");

    agents = require("./agents.js");
    await agents.init();
    logger.info('Agents initialized successfully');
  } catch (error) {
    logger.error('Error during initialization:', error.message);
    process.exit(1);
  }

  // Initialize webhooks data
  try {
    const webhooksData = require("./webhooksData.js");
    await webhooksData.init();
    logger.info('Webhooks data initialized successfully');
  } catch (error) {
    logger.error('Error initializing webhooks data:', error.message);
    // Continue - webhooks are optional
  }
})();

const port = serverConfig.server.port;


//------------------------------------------------------------------

function maskPasswords(jsonStr) {
  try {
      let jsonObj = JSON.parse(jsonStr);

      function maskObject(obj) {
          for (let key in obj) {
              if (typeof obj[key] === 'object' && obj[key] !== null) {
                  // Recursively mask nested objects
                  maskObject(obj[key]);
              } else if (typeof obj[key] === 'string' && key.toLowerCase().includes("password")) {
                  // Mask the password field
                  obj[key] = "********";
              }
          }
      }

      if (Array.isArray(jsonObj)) {
          // Handle case when the root is an array of objects
          jsonObj.forEach(item => maskObject(item));
      } else {
          // Handle case when the root is a single object
          maskObject(jsonObj);
      }

      return JSON.stringify(jsonObj, null, 2); // Return the modified JSON string
  } catch (error) {
      console.error("Invalid JSON input:", error);
      return null;
  }
}

function extractScriptMetadata(fileName) {
  try {
    const fullPath = path.join("./scripts", fileName);

    // --- Validate file exists ---
    if (!fs.existsSync(fullPath)) {
      return {
        success: false,
        error: `File not found: ${fileName}`
      };
    }

    // --- Get file metadata ---
    const stats = fs.statSync(fullPath);
    const lastUpdated = stats.mtime;

    // --- Read file content ---
    const scriptContent = fs.readFileSync(fullPath, "utf8");
    const lines = scriptContent.split("\n");

    let inBlock = false;
    let description = null;
    let parameters = [];

    for (const rawLine of lines) {
      const line = rawLine.trim();

      if (line === "#start-params") {
        inBlock = true;
        continue;
      }
      if (line === "#end-params") {
        inBlock = false;
        break;
      }

      if (inBlock && line.startsWith("#")) {
        const content = line.substring(1).trim(); // remove leading '#'

        if (!description) {
          // First line = description (strip HTML)
          description = content.replace(/<[^>]*>/g, "");
        } else {
          // Remaining lines = parameters (keep HTML)
          parameters.push(content);
        }
      }
    }

    // --- If no metadata block found ---
    if (!description) {
      return {
        success: true,
        error: `No metadata block (#start-params ... #end-params) found in ${fileName}`,
        data: {
          filename: fileName,
          lastUpdated,
          metaDescription: "",
          metaParameters: ""
        }
      };
    }

    return {
      success: true,
      error: null,
      data: {
        filename: fileName,
        lastUpdated,
        metaDescription: description,
        metaParameters: parameters.join("\n")
      }
    };

  } catch (err) {
    return {
      success: false,
      error: `Failed to parse ${fileName}: ${err.message}`
    };
  }
}

// function extractFirstLineFromFile(fileName) {
//   try {
//     //console.log("Extracting desc from: " + fileName);
//     const scriptContent = fs.readFileSync("./scripts/" + fileName, 'utf8');
//     const lines = scriptContent.split('\n');
//     let capture = false;

//     for (const line of lines) {
//       if (line.trim() === '#start-params') {
//         capture = true;
//       } else if (line.trim() === '#end-params') {
//         capture = false;
//       } else if (capture && line.trim().startsWith('#')) {
//         // Remove the leading '#' and return the first line after the start marker
//         //console.log("Found: " + line);
//         return " - " + line.trim().substring(1).replace(/<[^>]*>/g, '');
//       }
//     }

//     // Return an empty string if the start marker is not found
//     return '';
//   } catch (error) {
//     console.error('Error reading file:', error);
//     return '';
//   }
// }

function refreshScripts() {
  const scriptsDir = path.join(__dirname, "scripts");

  // If the directory doesn't exist, create it and return empty metadata
  if (!fs.existsSync(scriptsDir)) {
    fs.mkdirSync(scriptsDir, { recursive: true });
    return [];
  }

  const scriptsMeta = [];

  fs.readdirSync(scriptsDir).forEach(file => {
    scriptsMeta.push(extractScriptMetadata(file));
  });

  return scriptsMeta;
}

function parseScriptDescriptionFromContent(scriptContent) {
  try {
    const lines = String(scriptContent || '').split('\n');
    let inBlock = false;
    let description = null;

    for (const rawLine of lines) {
      const line = rawLine.trim();

      if (line === '#start-params') {
        inBlock = true;
        continue;
      }
      if (line === '#end-params') {
        break;
      }

      if (inBlock && line.startsWith('#')) {
        description = line.substring(1).trim().replace(/<[^>]*>/g, '');
        break;
      }
    }

    return description || '';
  } catch (error) {
    return '';
  }
}

// Regex allows characters that /saveScript preserves: alphanumeric, underscore, dot, hyphen
// Also requires .sh suffix to ensure it's a shell script
// Path traversal is protected by startsWith(baseDir) check
const SCRIPT_FILENAME_REGEX = /^[a-zA-Z0-9_.-]+\.sh$/;

function getValidatedScriptPath(scriptFilename) {
  if (!scriptFilename || !SCRIPT_FILENAME_REGEX.test(scriptFilename)) {
    throw new AppError('Invalid script filename', 400);
  }

  const baseDir = path.join(__dirname, 'scripts');
  const scriptPath = path.join(baseDir, scriptFilename);
  if (!scriptPath.startsWith(baseDir)) {
    throw new AppError('Invalid script filename', 400);
  }

  return scriptPath;
}

async function readSavedScript(scriptFilename) {
  const scriptPath = getValidatedScriptPath(scriptFilename);
  try {
    return await asyncUtils.readFileAsync(scriptPath, 'utf8');
  } catch (error) {
    logger.error(`Unable to read script [${scriptFilename}]: ${error.message}`);
    throw new AppError(`Unable to read script [${scriptFilename}]`, 404);
  }
}

function getScriptTestRequestedBy(req) {
  if (req.user && req.user.username) {
    return req.user.username;
  }

  if (req.user && req.user.email) {
    return req.user.email;
  }

  return 'unknown';
}

function buildScriptTestAgentsJson() {
  return JSON.stringify(Object.values(agents.getDict() || {}).map((agent) => ({
    name: agent.name,
    status: agent.status,
    commsType: agent.commsType,
  })));
}

function buildScriptTestAgentsBase64() {
  return Buffer.from(buildScriptTestAgentsJson(), 'utf8').toString('base64');
}

//------------------------------------------------------------------


function getDbKey(logEvent,type)
{
  return logEvent.name + "_" + logEvent.jobName + "_" + type;
}

 
// //------------------------------------------------------------------
// /**
//  * Executes a shell command
//  * @param {} cmd 
//  * @returns 
//  */
// function execShellCommand(cmd) {
//   logger.info("Executing Command");
//   try {
//     code = execSync(cmd).toString();
//   }
//   catch (error) {
//     code = 1;
//     logger.error("Errored executing command",error);
//   }
//   return code;
// }

// //------------------------------------------------------------------
// /**
//  * Calculates a time differente beween start time and now
//  * @param {} startTime 
//  * @returns 
//  */
// function getRuntime(startTime) {
//   var now = new Date();
//   var stYYYY = startTime.substr(0, 4);
//   var stMM = startTime.substr(5, 2) - 1;
//   var stDD = startTime.substr(8, 2);
//   var stHour = startTime.substr(11, 2);
//   var stMin = startTime.substr(14, 2);
//   var stSec = startTime.substr(17, 2);

//   var startDate = new Date(stYYYY, stMM, stDD, stHour, stMin, stSec);
//   secs = Math.round((now.getTime() - startDate.getTime()) / 1000);
//   return secs;
// }

// function calcRunPercent(runtime, etaRuntime) {
//   runtime = parseInt(runtime);
//   etaRuntime = parseInt(etaRuntime);
//   var pct = (runtime / parseInt(etaRuntime)) * 100;
//   pct = Math.round(pct);
//   return pct;
// }

// /**
//  * Function to sort alphabetically an array of objects by some specific key.
//  * 
//  * @param {String} property Key of the object to sort.
//  */
// function dynamicSort(property) {
//   var sortOrder = 1;

//   if (property[0] === "-") {
//     sortOrder = -1;
//     property = property.substr(1);
//   }

//   return function (a, b) {
//     if (sortOrder == -1) {
//       return b[property].localeCompare(a[property]);
//     } else {
//       return a[property].localeCompare(b[property]);
//     }
//   }
// }

async function getSchedulerData(index, executionId = null)
{
  logger.info("Getting Schedule Info with index: " + index + (executionId ? ` and executionId: ${executionId}` : ''));
  //var redir=req.query.redir;
  //var refresh = req.query["refresh"];
  //if (refresh === undefined) refresh = 0;
  var schedule = scheduler.getSchedules(index);

  var data={};

  // var scriptsMeta = refreshScripts();
  // var scripts = [];
  // var scriptsDesc =[];

  // for(var x=0;x<scriptsMeta.length;x++){
  //   scripts.push(scriptsMeta[x].data.filename);
  //   scriptsDesc.push(scriptsMeta[x].data.metaDescription);
  // }

  // data.scripts = scripts;
  data.agent=agents.getAgent(schedule.agent);
  
  // Check if agent exists, provide helpful error info
  if (!data.agent) {
    logger.warn(`Agent '${schedule.agent}' not found for schedule '${schedule.jobName}'`);
    data.agent = {
      name: schedule.agent,
      status: 'offline',
      errorMessage: `Agent '${schedule.agent}' is not registered or offline`
    };
  }

  // Pre-format jobStarted with server timezone (same approach as history.getLastRun via getItemsUsingTZ)
  if (data.agent.jobStarted) {
    data.agent.jobStartedDisplay = dateTimeUtils.displayFormatDate(new Date(data.agent.jobStarted), false, serverConfig.server.timezone, 'YYYY-MM-DDTHH:mm:ss.SSS', false);
  }

  // When a specific execution is being viewed, use the running item's exact startTime so the
  // ETA progress bar reflects this execution's elapsed time, not the shared agent jobStarted
  // which can be overwritten by whichever concurrent run sent the most recent status message.
  if (executionId) {
    const runningItem = running.getItemByExecutionId(executionId);
    if (runningItem && runningItem.startTime) {
      // Clone agent to avoid mutating shared state
      data.agent = Object.assign({}, data.agent);
      data.agent.jobStarted = runningItem.startTime;
      data.agent.jobStartedDisplay = dateTimeUtils.displayFormatDate(new Date(runningItem.startTime), false, serverConfig.server.timezone, 'YYYY-MM-DDTHH:mm:ss.SSS', false);
    }
  }
  
  data.schedule = schedule;
  data.index = index;
  data.hist = {};

  // If executionId provided, look up specific historical execution
  if (executionId) {
    logger.info(`Looking up execution [${executionId}] for job [${schedule.jobName}]`);
    const allHistory = hist.getItems();
    const historyItem = allHistory.find(item => item.jobName === schedule.jobName && item.executionId === executionId);
    
    if (historyItem) {
      logger.info(`Found historical execution [${executionId}]`);
      data.hist.histLastRun = historyItem;
      data.log = historyItem.log;
      data.stats = null;  // No live stats for historical lookups
      data.executionMode = 'historical';
    } else {
      // Check if it's a currently running execution with this ID
      const runningItem = running.getItems().find(item => item.jobName === schedule.jobName && item.executionId === executionId);
      if (runningItem) {
        logger.info(`Found active execution [${executionId}] - treating as current run`);
        // Treat as current/active execution, get live stats
        data.executionMode = 'current_active';
      } else {
        logger.warn(`Execution [${executionId}] not found for job [${schedule.jobName}]`);
        data.executionMode = 'historical_not_found';
      }
    }
  } else {
    // Current mode - get live stats
    data.executionMode = 'current';
  }
  
  // If not historical, get current live stats
  if (data.executionMode !== 'historical' && data.executionMode !== 'historical_not_found') {
    //return logEvent.name + "_" + logEvent.jobName + "_" + type;
    var key1 = schedule.agent + "_" + schedule.jobName + "_" + "stats";
    // Use executionId-scoped log key when available (matches agentMessageProcessor.js getDbKey)
    var key2 = executionId
      ? schedule.agent + "_" + schedule.jobName + "_" + executionId + "_log"
      : schedule.agent + "_" + schedule.jobName + "_log";

    var stats = null;
    var log = null;
    try {
      stats = await db.simpleGetData(key1);
    }
    catch(err){
      logger.warn("Unable to find stats data");
      logger.warn(JSON.stringify(err));
    }

    try {
      log = await db.simpleGetData(key2);
    }
    catch(err){
      logger.warn("Unable to find log data");
      logger.warn(JSON.stringify(err));
    }  
    
    logger.debug("Stats:\n" + stats);
    logger.debug("Log  :\n" + log);
    if(stats!=null){
      stats.current.etaDisplay = dateTimeUtils.displaySecs(stats.current.eta);
      stats.etaRollingAvgDisplay = dateTimeUtils.displaySecs(stats.etaRollingAvg);
    }
    else{
      if(stats !== undefined && stats!==null && (stats.etaRollingAvg == undefined || stats.etaRollingAvg ==null)) stats.etaRollingAvg=0;
    }

    data.stats = stats;
    data.log = log;
    data.hist.histLastRun = hist.getLastRun(schedule.jobName);
  }
  
  data.hist.histAvgRuntime = hist.getAverageRuntime(schedule.jobName);
  data.hist.histAvgRuntimeSecs = dateTimeUtils.displaySecs(hist.getAverageRuntime(schedule.jobName));
  return data;
}
//______________________________________________________________________________________________________

/**
 * Initialise Express
 */
var app = express();
//app.use(express.static('public'));
app.use(express.static(path.join(__dirname, 'public')));
app.use('/monaco-editor', express.static('node_modules/monaco-editor'));
app.use('/material-icons', express.static('node_modules/material-icons'));
app.use('/material-design-icons', express.static('node_modules/material-design-icons-iconfont/dist'));
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(cookieParser());
//app.use(session({ secret: "dave's session secret", resave:false, saveUninitialized: false }));
app.use(session({
  secret: "dave's session secret", 
  resave: false, 
  saveUninitialized: false, 
  /*cookie: { secure: true, httpOnly: true } */
}));

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

// Generate CSRF tokens for all requests but don't validate
app.use((req, res, next) => {
  // Generate a CSRF token if not already present
  if (!req.csrfToken) {
    const crypto = require('crypto');
    req.csrfToken = () => {
      if (!req.session.csrfToken) {
        req.session.csrfToken = crypto.randomBytes(32).toString('hex');
      }
      return req.session.csrfToken;
    };
  }
  next();
});

// CSRF validation middleware for form-based authenticated routes
const validateCsrf = (req, res, next) => {
  // Skip validation for public routes
  const publicRoutes = ['/login.html', '/register.html', '/forgot.html', '/saveScript'];
  if (publicRoutes.includes(req.path) || req.path.startsWith('/reset')) {
    logger.debug(`[CSRF] Skipping validation for public route: ${req.path}`);
    return next();
  }
  
  // Skip validation for REST APIs
  if (req.path.startsWith('/rest')) {
    logger.debug(`[CSRF] Skipping validation for REST API: ${req.method} ${req.path}`);
    return next();
  }

  // Get token from request body or headers
  const tokenFromRequest = req.body._csrf || req.headers['x-csrf-token'];
  const tokenFromSession = req.session.csrfToken;

  logger.debug(`[CSRF] Validating token for ${req.method} ${req.path}`);
  logger.debug(`[CSRF] Token from request: ${tokenFromRequest ? 'present' : 'missing'}`);
  logger.debug(`[CSRF] Token from session: ${tokenFromSession ? 'present' : 'missing'}`);

  if (!tokenFromSession || !tokenFromRequest || tokenFromSession !== tokenFromRequest) {
    logger.warn(`[CSRF] CSRF token validation FAILED for ${req.method} ${req.path}`);
    return res.status(403).json({ error: 'CSRF token validation failed' });
  }

  logger.debug(`[CSRF] CSRF token validation PASSED for ${req.method} ${req.path}`);
  next();
};

// ============================================================================
// SENSITIVE API TOKEN - For protecting critical data endpoints (/rest/data/*)
// ============================================================================
// Generate a token for sensitive data APIs on first startup
const crypto = require('crypto');
//const { getJobVersion } = require("./orchestration.js");
let sensitiveDataToken = null;
 
function initializeSensitiveDataToken() {
  if (!sensitiveDataToken) {
    sensitiveDataToken = crypto.randomBytes(32).toString('hex');
    console.log('\n');
    console.log('='.repeat(80));
    console.log('SENSITIVE DATA API TOKEN (save this in a safe place):');
    console.log(`Token: ${sensitiveDataToken}`);
    console.log('Use as header: X-Data-Token: <token>');
    console.log('Or as URL param: ?dataToken=<token>');
    console.log('='.repeat(80));
    console.log('\n');
  }
  return sensitiveDataToken;
}

// Middleware to validate sensitive data API token
const validateSensitiveDataToken = (req, res, next) => {
  const token = initializeSensitiveDataToken();
  const providedToken = req.headers['x-data-token'] || req.headers['x-sensitive-token'] || req.query.dataToken;
  
  if (!providedToken || providedToken !== token) {
    logger.warn(`[SENSITIVE-API] Unauthorized access attempt to ${req.method} ${req.path} - invalid or missing token`);
    return res.status(403).json({ success: false, message: 'Invalid or missing sensitive data token. Check server console for token.' });
  }
  
  logger.debug(`[SENSITIVE-API] Authorized access to ${req.method} ${req.path}`);
  next();
};


// Routes
// Login route: Redirect to registration if no user is registered
app.get('/login.html', asyncHandler(async (req, res) => {
  const redirect = req.query.redirect;
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

// Logout route
app.get('/logout.html', (req, res) => {
  req.session.destroy(() => {
    res.redirect('/login.html');
  });
});


app.post('/saveScript', express.json(), asyncHandler(async (req, res) => {
  // Sanitize filename: keep only alphanumeric, underscore, dot, hyphen
  let scriptName = req.body.scriptName.replace(/[^a-zA-Z0-9_.-]/g, '');
  
  // Enforce .sh suffix if not present
  if (!scriptName.endsWith('.sh')) {
    scriptName = scriptName + '.sh';
  }

  // Path traversal protection
  const baseDir = path.join(__dirname, 'scripts');
  const filePath = path.join(baseDir, scriptName);
  if (!filePath.startsWith(baseDir)) {
    throw new AppError('Invalid script filename', 400);
  }

  const scriptContent = req.body.script;

  try {
    await asyncUtils.writeFileAsync(filePath, scriptContent);
    logger.info(`File ${scriptName} saved successfully`);
    res.json({ status: 'success', message: 'File saved successfully.', scriptName });
  } catch (error) {
    logger.error(`Error saving script ${scriptName}:`, error.message);
    throw new AppError(`Failed to save script: ${error.message}`, 500);
  }
}));

// Register route: displays registration form if allowed and not already registered
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

// Forgot password route: displays forgot password form
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

// Reset password route: displays reset password form
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


app.post('/login.html', validateCsrf, asyncHandler(async (req, res) => {
  const { username, password, redirect } = req.body;
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

//Initial setup
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



//Initial setup MQTT
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


//Initial setup Welcome
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

//Initial setup Complete
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

//Initial setup
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

  var { wsServer,webserverPort, wsPort,timezone} = req.body;
  //var mqttEnabled = serverConfig.mqtt.enabled;
  serverConfig.websocket_server.server = wsServer;
  serverConfig.server.hostname = wsServer;
  serverConfig.server.port = webserverPort;
  serverConfig.websocket_server.port = wsPort;
  
  serverConfig.server.timezone = timezone;

  confighandler.saveServerConfig();
  
  //if(mqttEnabled=="true")return res.redirect('/initial-setup-mqtt.html');
  return res.redirect('/initial-setup-complete.html');
  //if(mqttEnabled=="false")res.redirect('/initial-setup.html?message=One+of+WebSocket+or+MQTT+must+be+checked');
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
  var mqttEnabled = "false";

  if(mqtt && mqtt=="on")mqttEnabled="true";

  serverConfig.websocket_server.enabled = "true";
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

app.post('/settings.html', validateCsrf, User.isAuthenticated, async (req, res) => {
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
    connectionEnabled,loginSuccessEnabled,loginFailEnabled,jobFailEnabled
    
  } = req.body;

  logger.debug(`[SETTINGS_POST] iconslist extracted from req.body: "${iconslist}" (type: ${typeof iconslist})`);

  //Template
  if(templateEnabled=="on")templateEnabled="true";
  else templateEnabled="false";
  serverConfig.templates.enabled = templateEnabled;
  serverConfig.templates.repositoryUrl = templateServer;

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
    serverConfig.mqtt.password = mqttPassword;
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
  serverConfig.smtp.password = smtpPassword;
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
  
  if (!iconslist || iconslist.trim() === '') {
    logger.warn(`[ICONS_SAVE] ATTEMPT TO SAVE EMPTY ICONS - iconslist is empty or null`);
    logger.warn(`[ICONS_SAVE] Request body keys: ${Object.keys(req.body).join(', ')}`);
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

app.get('/settings.html',User.isAuthenticated, async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  res.render('settings.ejs', { serverConfig, user, csrf: req.csrfToken() });
});

// Backup Management Routes
// Configure multer for in-memory file uploads
const upload = multer({ 
  storage: multer.memoryStorage(),
  limits: { fileSize: 500 * 1024 * 1024 } // 500MB max
});

app.get('/api/backup/items', User.isAuthenticated, (req, res) => {
  try {
    const items = backupManager.getBackupItems();
    res.json(items);
  } catch (err) {
    logger.error('Error getting backup items:', err);
    res.status(500).json({ error: 'Failed to get backup items' });
  }
});

app.post('/api/backup/create', User.isAuthenticated, asyncHandler(async (req, res) => {
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

app.post('/api/backup/restore', User.isAuthenticated, upload.single('backupFile'), asyncHandler(async (req, res) => {
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

// ============================================================================
// WEBHOOK JOB TRIGGER API
// ============================================================================
/**
 * POST /api/webhook/trigger
 * Trigger a job via webhook with custom JSON payload
 * 
 * Authentication: Webhook API key (via ?key= parameter or X-Webhook-Key header)
 * Body: Arbitrary JSON payload to pass to the job/orchestration
 * 
 * Example:
 *   curl -X POST "http://server:8082/api/webhook/trigger/backup-job?key=<uuid>" \
 *        -H "Content-Type: application/json" \
 *        -d '{"status": "warning", "message": "Storage at 85% capacity"}'
 */
app.post('/api/webhook/trigger/:jobName', asyncHandler(async (req, res) => {
  try {
    const { jobName } = req.params;
    const webhookManager = require('./webhookManager.js');
    const webhooksData = require('./webhooksData.js');
    const triggerContextModule = require('./triggerContext.js');
    
    // Extract webhook API key from request
    const providedKey = webhookManager.extractWebhookKeyFromRequest(req);
    
    if (!providedKey) {
      logger.warn(`[WEBHOOK] Trigger attempt on job [${jobName}] without API key`);
      return res.status(401).json({ error: 'Webhook API key required (via ?key= or X-Webhook-Key header)' });
    }
    
    if (!webhookManager.isValidWebhookKey(providedKey)) {
      logger.warn(`[WEBHOOK] Trigger attempt on job [${jobName}] with invalid key format`);
      return res.status(401).json({ error: 'Invalid webhook API key format' });
    }
    
    // Validate webhook key against database
    let validatedWebhook = null;
    try {
      validatedWebhook = await webhooksData.validateWebhookKey(jobName, providedKey);
      if (!validatedWebhook) {
        logger.warn(`[WEBHOOK] Webhook key not found for job [${jobName}]`);
        return res.status(401).json({ error: 'Invalid webhook API key for this job' });
      }
    } catch (validateErr) {
      logger.warn(`[WEBHOOK] Webhook key validation failed for job [${jobName}]: ${validateErr.message}`);
      return res.status(401).json({ error: 'Webhook key validation failed' });
    }
    
    const payload = req.body || {};
    
    if (!webhookManager.isValidWebhookPayload(payload)) {
      logger.warn(`[WEBHOOK] Invalid payload for job [${jobName}]`);
      return res.status(400).json({ error: 'Webhook payload must be a JSON object' });
    }
    
    logger.info(`[WEBHOOK] Triggering job [${jobName}] from webhook [${validatedWebhook.id}]`);
    
    // Create webhook trigger context
    const crypto = require('crypto');
    const executionId = crypto.randomBytes(8).toString('hex');
    
    const webhookTriggerContext = await triggerContextModule.createWebhookTriggerContext(
      validatedWebhook.id,
      jobName,
      payload,
      executionId
    );
    webhookTriggerContext.webhookName = validatedWebhook.name || '';
    
    // Record the trigger event
    try {
      await webhooksData.recordWebhookTrigger(jobName, validatedWebhook.id);
    } catch (recordErr) {
      logger.warn(`[WEBHOOK] Could not record trigger event: ${recordErr.message}`);
      // Continue anyway - webhook still triggers
    }
    
    // Trigger the job asynchronously (fire & forget) - don't await it
    const scheduler = require('./scheduler.js');
    scheduler.runJob(jobName, 'webhook', null, webhookTriggerContext).catch(err => {
      logger.error(`[WEBHOOK] Job [${jobName}] execution failed: ${err.message}`);
    });
    
    // Return immediately with 202 Accepted + executionId before job completes
    logger.info(`[WEBHOOK] Job [${jobName}] queued for execution with executionId [${executionId}]`);
    return res.status(202).json({
      success: true,
      jobName,
      executionId,
      message: `Job triggered via webhook and queued for execution`,
      statusUrl: `/rest/orchestration/executions/${executionId}`
    });
    
  } catch (err) {
    logger.error(`[WEBHOOK] Error processing webhook trigger: ${err.message}`);
    res.status(500).json({ error: 'Webhook processing error: ' + err.message });
  }
}));

// ============================================================================
// WEBHOOK MANAGEMENT API (Authenticated)
// ============================================================================

/**
 * GET /rest/webhooks/:jobName
 * Get all webhooks for a job
 */
app.get('/rest/webhooks/:jobName', User.isAuthenticated, asyncHandler(async (req, res) => {
  try {
    const { jobName } = req.params;
    const webhooksData = require('./webhooksData.js');
    
    const webhooks = await webhooksData.getWebhooksForJob(jobName);
    
    // Return webhooks without exposing API keys
    const safeWebhooks = webhooks.map(w => ({
      id: w.id,
      name: w.name,
      description: w.description,
      isActive: w.isActive,
      createdAt: w.createdAt,
      lastTriggeredAt: w.lastTriggeredAt,
      triggerCount: w.triggerCount,
      keyRotatedAt: w.keyRotatedAt
    }));
    
    res.json(safeWebhooks);
  } catch (err) {
    logger.error(`[WEBHOOK API] Error fetching webhooks for [${req.params.jobName}]:`, err);
    res.status(500).json({ error: err.message });
  }
}));

/**
 * POST /rest/webhooks/:jobName
 * Create a new webhook for a job
 */
app.post('/rest/webhooks/:jobName', User.isAuthenticated, express.json(), asyncHandler(async (req, res) => {
  try {
    const { jobName } = req.params;
    const { name, description } = req.body;
    const webhooksData = require('./webhooksData.js');
    
    if (!name || typeof name !== 'string' || name.trim().length === 0) {
      return res.status(400).json({ error: 'Webhook name is required' });
    }
    
    const webhook = await webhooksData.createWebhook(jobName, name.trim(), description || '');
    
    logger.info(`[WEBHOOK API] Webhook created for job [${jobName}] by user`);
    
    res.status(201).json({
      success: true,
      webhook: {
        id: webhook.id,
        name: webhook.name,
        description: webhook.description,
        apiKey: webhook.apiKey,  // Only return key on creation
        isActive: webhook.isActive,
        createdAt: webhook.createdAt
      }
    });
  } catch (err) {
    logger.error(`[WEBHOOK API] Error creating webhook for [${req.params.jobName}]:`, err);
    res.status(500).json({ error: err.message });
  }
}));

/**
 * PUT /rest/webhooks/:jobName/:webhookId
 * Update webhook (name, description, enable/disable)
 */
app.put('/rest/webhooks/:jobName/:webhookId', User.isAuthenticated, express.json(), asyncHandler(async (req, res) => {
  try {
    const { jobName, webhookId } = req.params;
    const { name, description, isActive } = req.body;
    const webhooksData = require('./webhooksData.js');
    
    const updates = {};
    if (name !== undefined) updates.name = name;
    if (description !== undefined) updates.description = description;
    if (isActive !== undefined) updates.isActive = isActive;
    
    const webhook = await webhooksData.updateWebhook(jobName, webhookId, updates);
    
    logger.info(`[WEBHOOK API] Webhook updated [${jobName}:${webhookId}] by user`);
    
    res.json({
      success: true,
      webhook: {
        id: webhook.id,
        name: webhook.name,
        description: webhook.description,
        isActive: webhook.isActive,
        createdAt: webhook.createdAt
      }
    });
  } catch (err) {
    logger.error(`[WEBHOOK API] Error updating webhook [${req.params.jobName}:${req.params.webhookId}]:`, err);
    res.status(500).json({ error: err.message });
  }
}));

/**
 * POST /rest/webhooks/:jobName/:webhookId/rotate-key
 * Rotate webhook API key
 */
app.post('/rest/webhooks/:jobName/:webhookId/rotate-key', User.isAuthenticated, express.json(), asyncHandler(async (req, res) => {
  try {
    const { jobName, webhookId } = req.params;
    const { oldKey } = req.body;
    const webhooksData = require('./webhooksData.js');
    
    // For authenticated users, oldKey verification is optional
    // Since user is already in admin panel, additional verification not needed
    const webhook = await webhooksData.rotateWebhookKey(jobName, webhookId, oldKey);
    
    logger.info(`[WEBHOOK API] Webhook key rotated [${jobName}:${webhookId}] by user`);
    
    res.json({
      success: true,
      webhook: {
        id: webhook.id,
        name: webhook.name,
        apiKey: webhook.apiKey,  // Return new key
        keyRotatedAt: webhook.keyRotatedAt
      }
    });
  } catch (err) {
    logger.error(`[WEBHOOK API] Error rotating webhook key [${req.params.jobName}:${req.params.webhookId}]:`, err);
    res.status(500).json({ error: err.message });
  }
}));

/**
 * DELETE /rest/webhooks/:jobName/:webhookId
 * Delete a webhook
 */
app.delete('/rest/webhooks/:jobName/:webhookId', User.isAuthenticated, asyncHandler(async (req, res) => {
  try {
    const { jobName, webhookId } = req.params;
    const webhooksData = require('./webhooksData.js');
    
    await webhooksData.deleteWebhook(jobName, webhookId);
    
    logger.info(`[WEBHOOK API] Webhook deleted [${jobName}:${webhookId}] by user`);
    
    res.json({
      success: true,
      message: `Webhook deleted successfully`
    });
  } catch (err) {
    logger.error(`[WEBHOOK API] Error deleting webhook [${req.params.jobName}:${req.params.webhookId}]:`, err);
    res.status(500).json({ error: err.message });
  }
}));

/**
 * GET /rest/webhooks-all
 * Get all webhooks across all jobs
 */
app.get('/rest/webhooks-all', User.isAuthenticated, asyncHandler(async (req, res) => {
  try {
    const webhooksData = require('./webhooksData.js');
    const webhooks = await webhooksData.getAllWebhooks();
    
    // Return without exposing API keys
    const safeWebhooks = webhooks.map(w => ({
      id: w.id,
      name: w.name,
      description: w.description,
      jobName: w.jobName,
      isActive: w.isActive,
      createdAt: w.createdAt,
      lastTriggeredAt: w.lastTriggeredAt,
      triggerCount: w.triggerCount,
      keyRotatedAt: w.keyRotatedAt
    }));
    
    res.json(safeWebhooks);
  } catch (err) {
    logger.error(`[WEBHOOK API] Error fetching all webhooks:`, err);
    res.status(500).json({ error: err.message });
  }
}));

/**
 * GET /rest/webhooks/stats
 * Get webhook statistics
 */
app.get('/rest/webhooks-stats', User.isAuthenticated, asyncHandler(async (req, res) => {
  try {
    const webhooksData = require('./webhooksData.js');
    const stats = await webhooksData.getWebhookStats();
    res.json(stats);
  } catch (err) {
    logger.error(`[WEBHOOK API] Error getting webhook stats:`, err);
    res.status(500).json({ error: err.message });
  }
}));

app.delete('/rest/notifications', User.isAuthenticated, (req, res) => {
  logger.info("Delete all notifications");
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  logger.info("Deleting all Notifications");
  var resp ="";
  try{
    resp = notificationData.deleteAll();  
  }
  catch(err){
    res.sendStatus(503);
  }
  logger.debug("all notifications deleted");
  res.sendStatus(200);
});

app.delete('/rest/notifications/:index', User.isAuthenticated, (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  const index = req.params.index;
  logger.info("Deleting Notification at index: " + index);
  var resp ="";
  try{
    resp = notificationData.deleteItem(index);  
  }
  catch(err){
    res.sendStatus(503);
  }
  logger.debug("notification at index [" + index + "] deleted");
  res.sendStatus(200);
});

// ================================================================
// RUNNING JOBS REST API - CRUD ENDPOINTS
// ================================================================

/**
 * Get all running jobs
 */
app.get('/rest/running', User.isAuthenticated, asyncHandler(async (req, res) => {
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

/**
 * Delete a running job by index
 */
app.delete('/rest/running/:index', User.isAuthenticated, asyncHandler(async (req, res) => {
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

/**
 * Delete a running job by job name
 */
app.delete('/rest/running/byName/:jobName', User.isAuthenticated, asyncHandler(async (req, res) => {
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

/**
 * Delete a running job by execution ID
 */
app.delete('/rest/running/byExecutionId/:executionId', User.isAuthenticated, asyncHandler(async (req, res) => {
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

app.delete('/rest/running', User.isAuthenticated, asyncHandler(async (req, res) => {
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

app.get('/rest/agent/:id', User.isAuthenticated, (req, res) => {
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

app.get('/rest/agent/:id/ping', User.isAuthenticated, (req, res) => {
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

// app.get('/rest/agent/:id/ping', User.isAuthenticated, (req, res) => {
//   const user = req.session.user;
//   if (!user) {
//     return res.redirect('/register.html');
//   }
//   const id = req.params.id;
//   logger.info("Pinging Agent with ID: " + id);
//   //notificationData.deleteItem(index);
//   pingIndividualRuntime(id);
//   res.setHeader("Content-Type","Application/JSON");
//   res.send('{"status":"ok"}');  
// });

/**
 * POST /rest/agent/:id/metric
 * Send a queryMetric command to an agent and wait for the result.
 * Body: { type, path?, pattern? }
 * Supported types: cpu, mount_usage, dir_size, file_size, file_count, file_age
 * NOTE: This is a temporary test endpoint - remove once Rules Engine is built.
 */
app.post('/rest/agent/:id/metric', User.isAuthenticated, asyncHandler(async (req, res) => {
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

  const agentMsgProcessor = require('./agentMessageProcessor.js');
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

/**
 * GET /metric-test.html  — Temporary test page for queryMetric exploration.
 * NOTE: Remove this page once the Rules Engine is built.
 */
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

app.get('/rest/notifications', User.isAuthenticated, (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  var notifyObj = notificationData.getItems();
  res.setHeader("Content-Type","Application/JSON");
  res.send(notifyObj);
  //res.sendStatus(200);
});

app.get('/rest/notifications/count', User.isAuthenticated, (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  var notifyCount = notificationData.getCount();
  res.setHeader("Content-Type","Application/JSON");
  res.send(`{"count":${notifyCount}}`);
  //res.sendStatus(200);
});

app.get('/rest/notifty/test', User.isAuthenticated, (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  notifier.sendNotification("Test Notification","This is a test notification from Orchelium Server.","INFORMATION","/settings.html");
  res.sendStatus(200);
});

app.get('/rest/mqtt/reconnect', User.isAuthenticated, async (req, res) => {
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

app.get('/profile.html',User.isAuthenticated, async (req, res) => {
  const user = req.session.user;
  if (!user) return res.redirect('/register.html');
  res.render('profile.ejs', { user, csrf: req.csrfToken() });
});

app.get('/about.html',User.isAuthenticated, async (req, res) => {

  const user = req.session.user;
  if (!user) return res.redirect('/register.html');

  res.render('about.ejs', { 
    user: User,
    version: version,
    csrf: req.csrfToken(),
   });
  
});

app.get('/runList/data',User.isAuthenticated, asyncHandler(async (req, res) => {
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
    const orchestrationModule = require('./orchestration.js');
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


app.get('/historyList/data',User.isAuthenticated, async (req, res) => {

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

app.delete('/rest/history/clear', User.isAuthenticated, asyncHandler(async (req, res) => {
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

app.get('/history.html',User.isAuthenticated, (req, res) => {

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

app.get('/rest/script/:script', User.isAuthenticated, (req, res) => {
  const scriptFilename = req.params.script;
  const mode = req.query.mode;
  const scriptPath = `./scripts/${scriptFilename}`;

  const baseDir = path.join(__dirname, 'scripts'); // Absolute base directory
  const testPath = path.join(baseDir, scriptFilename);

  // Check if the resolved path is within the base directory
  if (!testPath.startsWith(baseDir)) {
    return res.status(400).send('Invalid script filename');
  }

  // Validate filename format to match /saveScript behavior: alphanumeric, underscore, dot, hyphen, and .sh suffix
  if (!scriptFilename.match(/^[a-zA-Z0-9_.-]+\.sh$/)) {
    return res.status(400).send('Invalid script filename');
  }

  // Read the content of the script file
  fs.readFile(scriptPath, 'utf8', (err, data) => {
    if (err) {
      return res.status(500).send('Error reading script file');
    }

    let beforeParams = '';
    let htmlContent = '';
    let afterParams = '';
    let capturingBefore = true;
    let capturingHtml = false;
    let capturingAfter = false;

    const lines = data.split('\n');
    for (const line of lines) {
      if (line.trim() === '#start-params') {
        capturingBefore = false;
        capturingHtml = true;
        beforeParams += line + '\n'; // Add the marker to beforeParams
        continue;
      } else if (line.trim() === '#end-params') {
        capturingHtml = false;
        capturingAfter = true;
        htmlContent += line + '\n'; // Add the marker to htmlContent
        continue;
      }

      if (capturingBefore) {
        beforeParams += line + '\n';
      } else if (capturingHtml) {
        htmlContent += line + '\n';
      } else if (capturingAfter) {
        afterParams += line + '\n';
      }
    }

    console.log("Before #start-params:", beforeParams);
    console.log("Between #start-params and #end-params (before sanitization):", htmlContent);
    console.log("After #end-params:", afterParams);

    // Sanitize only the HTML content between markers
    let sanitizedHtml = sanitizeHtml(htmlContent, {
      allowedTags: ['b', 'i', 'p', 'br'], // Only allow safe tags
      disallowedTagsMode: 'discard', // Remove disallowed tags completely
      allowedAttributes: {}, // No attributes allowed
      selfClosing: ['br'], // Ensure self-closing tags like <br> are handled correctly
      textFilter: function(text) {
        return text; // No additional text filtering needed
      },
      // Explicitly disallow dangerous tags
      disallowedTags: ['script', 'style', 'iframe', 'link', 'meta', 'object', 'embed', 'base', 'form', 'input'],
    });

    console.log("Sanitized HTML output:", sanitizedHtml);

    // Combine sanitized HTML with the rest of the script, including the markers
    let finalContent = beforeParams + sanitizedHtml + afterParams;

    // Send the combined content
    res.send(finalContent);
  });
});

app.get('/rest/script-test/:executionId', User.isAuthenticated, asyncHandler(async (req, res) => {
  const execution = scriptTestManager.getExecution(req.params.executionId);
  if (!execution) {
    res.status(404).json({ success: false, error: 'Test execution not found or expired' });
    return;
  }

  res.json({ success: true, execution });
}));

app.post('/rest/script-test/state', User.isAuthenticated, asyncHandler(async (req, res) => {
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

app.post('/rest/script-test/start', User.isAuthenticated, asyncHandler(async (req, res) => {
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
    scriptDescription = scriptDescription || parseScriptDescriptionFromContent(scriptSource);
  } else if (!scriptSource || scriptSource.trim().length === 0) {
    res.status(400).json({ success: false, error: 'Editor tests require script content' });
    return;
  } else {
    scriptDescription = scriptDescription || parseScriptDescriptionFromContent(scriptSource);
  }

  const executionId = crypto.randomBytes(8).toString('hex');
  const createResult = scriptTestManager.createTest({
    executionId,
    agentName,
    scriptName,
    scriptDescription,
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

app.post('/rest/script-test/:executionId/acknowledge', User.isAuthenticated, asyncHandler(async (req, res) => {
  const execution = scriptTestManager.acknowledgeExecution(req.params.executionId);
  if (!execution) {
    res.status(404).json({ success: false, error: 'Test execution not found or expired' });
    return;
  }

  res.json({ success: true, execution });
}));

app.post('/rest/script-test/:executionId/discard', User.isAuthenticated, asyncHandler(async (req, res) => {
  const discarded = scriptTestManager.discardExecution(req.params.executionId);
  if (!discarded) {
    res.status(409).json({ success: false, error: 'Unable to discard active or missing test execution' });
    return;
  }

  res.json({ success: true });
}));

app.post('/rest/script-test/:executionId/terminate', User.isAuthenticated, asyncHandler(async (req, res) => {
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

async function getScriptUsageEntries(scriptName) {
  const allSchedules = scheduler.getSchedules();

  const jobsUsingScript = allSchedules.filter(schedule => {
    return schedule.scheduleMode === 'classic' && schedule.command === scriptName;
  });

  const formattedJobs = jobsUsingScript.map(job => ({
    jobName: job.jobName,
    description: job.description || '',
    type: 'Single Script',
    nextRunDate: job.nextRunDate || 'n/a',
    triggerType: job.triggerType || 'clock'
  }));

  try {
    const allOrchestrations = await orchestration.getAllJobs();

    Object.values(allOrchestrations).forEach(orchJob => {
      const currentVersion = orchJob.versions[orchJob.versions.length - 1];
      if (!currentVersion || !currentVersion.nodes) {
        return;
      }

      const usesScript = currentVersion.nodes.some(node => node.data && node.data.script === scriptName);
      if (usesScript) {
        formattedJobs.push({
          jobName: orchJob.name,
          description: orchJob.description || '',
          type: 'Orchestration',
          nextRunDate: 'Manual/Scheduled',
          triggerType: 'orchestration'
        });
      }
    });
  } catch (err) {
    logger.warn('Could not search orchestrations for script usage:', err.message);
  }

  return formattedJobs;
}

app.get('/rest/jobs-for-script/:scriptName', User.isAuthenticated, asyncHandler(async (req, res) => {
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

app.delete('/rest/script/:scriptName', User.isAuthenticated, asyncHandler(async (req, res) => {
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

app.get('/delete-schedule.html',User.isAuthenticated, (req, res) => {
  logger.warn("Deleting Schedule*******************");
  var jobName=req.query.jobName;
  var redir=req.query.redir;
  logger.info("Deleting Schedule with jobName: " + jobName);
  scheduler.deleteSchedule(jobName);
  logger.debug("schedule deleted");
  if(redir!==undefined && redir!==null) {
    res.redirect(redir + (redir.includes('?') ? '&' : '?') + "message=Deleted+schedule: " + jobName);
  } else {
    res.redirect("/scheduleList.html?message=Deleted+schedule: " + jobName);
  }
});


app.get('/scriptEditor.html',User.isAuthenticated, (req, res) => {
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

app.get('/scheduler.html',User.isAuthenticated, async (req, res) => {
  logger.info("Scheduler.html");
  var index=req.query.index;
  var copyIndex=req.query.copyIndex;
  var internal=req.query.internal;
  var jobname=req.query.jobname;
  var redir=req.query.redir;
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
    orchestrations = await db.getData('ORCHESTRATION_JOBS') || {};
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


app.get('/scheduleInfo/data/name',User.isAuthenticated, async(req, res) => {
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

app.get('/scheduleInfo/data/:index',User.isAuthenticated, async(req, res) => {
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


app.get('/scheduleInfo.html',User.isAuthenticated, async(req, res) => {
  var index=req.query.index;
  var redir=req.query.redir;
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



app.post('/scheduler.html', validateCsrf, User.isAuthenticated, asyncHandler(async (req, res) => {
  
  let { jobName, colour, description, scheduleType, scheduleTime, dayOfWeek,
    dayInMonth, agentselect, agentcommand, commandparams, index, redir, icon, scheduleMode, orchestrationId,
    // Rule trigger fields
    triggerType,
    ruleMetricAgent, ruleMetricType, ruleMetricPath, ruleMetricPattern,
    ruleConditionOperator, ruleConditionThreshold,
    rulePollIntervalMins, ruleCooldownMins
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
    }

    scheduler.upsertSchedule(index, jobName, colour, description, scheduleType, scheduleTime, dayOfWeek,
      dayInMonth, agentselect, agentcommand, commandparams, icon, scheduleMode, orchestrationId,
      triggerType, ruleMetric, ruleCondition, rulePollIntervalMins, ruleCooldownMins);

  if(redir===undefined || redir === null || redir.length<=0)redir="/scheduleList.htm";
  res.redirect(redir);
}));

app.get('/scheduleList.html',User.isAuthenticated, asyncHandler(async (req, res) => {
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

app.get('/scheduleListCalendar.html',User.isAuthenticated, (req, res) => {
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

app.get('/runSchedule.html',User.isAuthenticated, asyncHandler(async (req, res) => {
  var index=req.query.index;
  var jobname=req.query.jobname;
  var redir=req.query.redir;
  var rerunFrom=req.query.rerunFrom;  // NEW: Track which failed execution prompted this re-run
  logger.info(`Running Schedule - Index: ${index ?? 'undefined'}, Jobname: ${jobname ?? 'undefined'}, RerunFrom: ${rerunFrom ?? 'undefined'}`);
  
  if(redir===undefined || redir === null || redir.length<=0)redir="/scheduleInfo.html?index=" + index + "&refresh=3";
  
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
  res.redirect(redir);
}));

app.get('/edit/:index',User.isAuthenticated, (req, res) => {
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

app.post('/edit/:index', validateCsrf, User.isAuthenticated, (req, res) => {
  const index = req.params.index;
  const schedules = scheduler.getSchedules();
  const schedule = schedules[index];

  schedule.jobName = req.body.jobName;
  schedule.scheduleType = req.body.scheduleType;
  schedule.scheduleTime = req.body.scheduleTime;

  //writeSchedules(schedules);

  res.redirect('/schedules');
});

app.get('/rest/jobs',User.isAuthenticated, (req, res) => {
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

  //scheduler.getTodaysScheduleCount();
  

  response.render('index', {

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




app.get('/agentHistory.html',User.isAuthenticated, async (request, response) => {
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


app.get('/rest/servertime',User.isAuthenticated, async (request, response) => {
  var dateTime = new Date();
  var serverTime={};
  serverTime.dateTime = dateTime;
  response.json(serverTime);
});


app.get('/rest/agentdetail',User.isAuthenticated, async (request, response) => {
  
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


app.get('/rest/agentquery',User.isAuthenticated, (request, response) => {
  var agentname = request.query.name;
  var agent = agents.getAgent(agentname);
  response.header('Content-type', 'application/json');
  if(agent===undefined)agent={};
  agent.found="false";
  response.send(JSON.stringify(agent,null,2));
});

app.get('/agentEdit.html',User.isAuthenticated, (request, response) => {
  var msg = request.query.agentId;
  var agentObj=agents.getAgent(msg);
  response.render('agentEdit', {
    subject: 'Agent Edit',
    name: 'Control/AgentEdit',
    agent: agentObj,
    //scripts: scripts,
    csrf: request.csrfToken(),
  });
});

app.get('/agentregister.html',User.isAuthenticated, (request, response) => {
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

app.get('/agentstatus.html',User.isAuthenticated,async (request, response) => {

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

    
    
    response.render('agentstatus', {
    subject: 'EJS template engine',
    name: 'Control/AgentStatus',
    logs: logsStr,
    agents: agentdict,
    history: history,
    MQTT_ENABLED: serverConfig.mqtt.enabled,
    MQTT_SERVER_PORT: serverConfig.mqtt.port,
    MQTT_SERVER: serverConfig.mqtt.server,
    WS_ENABLED: serverConfig.websocket_server.enabled,
    WS_SERVER: serverConfig.websocket_server.server,
    WS_PORT: serverConfig.websocket_server.port,
    serverVers: minSupportedVersion
    //agentHistory:agentHistory,
  });
});


app.get('/rest/eta',User.isAuthenticated, (request, response) => {
  //populateBackupConfig();
  response.header('Content-type', 'application/json');
  response.send(JSON.stringify(config,null,2));
});

var logsStr = "";


app.post('/agent-submit.html', validateCsrf, User.isAuthenticated, (req, res) => {
  //Get the form data
  
  var name = req.body.agentname;
  var command = req.body.agentcommand;
  var description = req.body.agentdescription;
  var imageurl = req.body.imageurl;

  agents.registerAgent(name,description,command,imageurl,undefined,description);
  res.redirect("/agentstatus.html");

});

app.post('/agentEdit.html', validateCsrf, User.isAuthenticated, async (req, res) => {
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

app.get('/socket.io/socket.io.js', (req, res) => {
  res.sendFile(__dirname + '/node_modules/socket.io/client-dist/socket.io.js');
});


app.get('/rest/debug/logs', User.isAuthenticated, async (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  const logMessages = loglines.map(log => log.data.join(' ')); // Extract log messages
  res.json(logMessages); // Send log messages as JSON
});


app.get('/rest/debug/on', User.isAuthenticated, async (req, res) => {
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


app.get('/rest/debug/off', User.isAuthenticated, async (req, res) => {
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

app.get('/rest/updateAgent/:agentId/isRunning', express.json(), User.isAuthenticated, (req, res) => {
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

app.get('/rest/updateAgent/:agentId/status', express.json(), User.isAuthenticated, (req, res) => {
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


app.post('/rest/updateAgent/:agentId', express.json(), User.isAuthenticated, (req, res) => {
  
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

app.get('/rest/templates', User.isAuthenticated, async (req, res) => {
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

app.get('/rest/templates/refresh', User.isAuthenticated, async (req, res) => {
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

app.get('/rest/debug', User.isAuthenticated, async (req, res) => {
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


app.use((req, res, next) => {
  res.on('finish', () => {
    if (!res.headersSent) {
      clearInterval(pingRuntimeCheck);
      clearInterval(offlineCheck)
      clearInterval(reestablishConn);
    }
  });
  next();
});

//______________________________________________________________________________________________________

const pingRuntimeCheck = setInterval(pingRuntimes, pingInterval * 1000); // 60 seconds * 1000 milliseconds
const offlineCheck     = setInterval(markOffline, (pingInterval * failedPingOffline+1) * 1000);
//const reestablishConn  = setInterval(mqttTransport.startMqttConnectionProcess,30*1000);


function pingIndividualRuntime(inAgentName){
  //var agent = agents.getAgent(inAgentName)
  logger.info("[pingIndividualRuntime] Pinging runtime [" + inAgentName + "]");  
  agentComms.pingAgent(undefined,inAgentName);
}

function pingRuntimes()
{

  agentComms.pingAllAgents(agents.getDict());
}

function markOffline()
{
  var agentStatusDict = agents.getDict();
  logger.info("Running offline check for runtimes");
  for (const [key, value] of Object.entries(agentStatusDict)) {
   //if(value.status!="register" && value.status !="offline")
   //{
      logger.debug("Offline check for runtime [" + key + "]");    
      var now = new Date();
      var lastUpd = value.lastStatusReport;
      
      //debug.debug(`NOW   [${now.toISOString()}]`);
      //debug.debug(`LAST  [${lastUpd}]`);
      var lastDte = new Date(lastUpd);
      var diff = Math.floor((now.getTime() - lastDte.getTime()) /1000 /60);
      logger.debug(`DIFF  [${diff}mins]`);
      if(diff >= 1)
      {
        logger.warn(`Marking Agent [${key} as Offline`);
        agents.updateAgentStatus(key,"offline","",null,null,null,"");
        client = findClientByPrefix(key);
        var connId;
        if(client!==undefined){
          try{ 
            connId = client.connectionId;
            client.close();
          } catch (err) {
            logger.debug("Unable to close client",err);
          }
          logger.info(`Evicting dead client connection [${connId}]`);
          try{ 
            wsClients.delete(connId); 
          } catch (err) {
            logger.debug("Unable to remove: " + connId,err)
          }

        }
        //webSocketServer.forceCloseRemove(key);
      }
    //}
  }
}

// ============================================================================
// ORCHESTRATION JOBS - REST API ENDPOINTS
// ============================================================================

/**
 * Get list of all orchestration jobs
 */
app.get('/rest/orchestration/jobs', User.isAuthenticated, asyncHandler(async (req, res) => {
  const jobs = await orchestration.getAllJobs();
  res.json(jobs);
}));

/**
 * Get a specific orchestration job
 */
app.get('/rest/orchestration/jobs/:jobId', User.isAuthenticated, asyncHandler(async (req, res) => {
  const job = await orchestration.getJob(req.params.jobId);
  res.json(job);
}));

/**
 * Create or update an orchestration job
 */
app.post('/rest/orchestration/jobs', User.isAuthenticated, asyncHandler(async (req, res) => {
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

/**
 * Update an existing orchestration job
 */
app.put('/rest/orchestration/jobs/:jobId', User.isAuthenticated, asyncHandler(async (req, res) => {
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

/**
 * Delete an orchestration job
 */
app.delete('/rest/orchestration/jobs/:jobId', User.isAuthenticated, asyncHandler(async (req, res) => {
  const { jobId } = req.params;
  
  await orchestration.deleteJob(jobId);
  res.json({ success: true, message: `Orchestration job [${jobId}] deleted` });
}));

/**
 * Mark a classic job execution as having been re-run
 */
app.post('/rest/jobs/executions/:executionId/markAsRerun', User.isAuthenticated, asyncHandler(async (req, res) => {
  const { executionId } = req.params;
  
  logger.info(`Marking classic job execution [${executionId}] as re-run`);
  
  try {
    // Get history and mark the execution as re-run
    const hist = require('./history.js');
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

/**
 * Mark an orchestration execution as having been re-run
 */
app.post('/rest/orchestration/jobs/:jobId/executions/:executionId/markAsRerun', User.isAuthenticated, asyncHandler(async (req, res) => {
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

/**
 * Execute an orchestration job
 */
app.post('/rest/orchestration/jobs/:jobId/execute', User.isAuthenticated, asyncHandler(async (req, res) => {
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

/**
 * Get execution history for a job
 */
app.get('/rest/orchestration/jobs/:jobId/executions', User.isAuthenticated, asyncHandler(async (req, res) => {
  const { jobId } = req.params;
  
  const executions = await orchestration.getExecutionHistory(jobId);
  res.json(executions);
}));

/**
 * Get list of available scripts for the palette
 */
app.get('/rest/orchestration/scripts', User.isAuthenticated, asyncHandler(async (req, res) => {
  const scripts = await orchestration.getAvailableScripts();
  res.json(scripts);
}));

/**
 * Get list of available agents for orchestration execution
 */
app.get('/rest/orchestration/agents', User.isAuthenticated, asyncHandler(async (req, res) => {
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

/**
 * Get orchestration metadata (name, description, icon, color) without full definition
 */
app.get('/rest/orchestration/jobs/:jobId/metadata', User.isAuthenticated, asyncHandler(async (req, res) => {
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

/**
 * Get a specific version of an orchestration job
 */
app.get('/rest/orchestration/jobs/:jobId/versions/:version', User.isAuthenticated, asyncHandler(async (req, res) => {
  const job = await orchestration.getJobVersion(req.params.jobId, req.params.version);
  res.json(job);
}));

/**
 * Get all schedules
 */
app.get('/rest/schedules', User.isAuthenticated, asyncHandler(async (req, res) => {
  const schedules = scheduler.getSchedules();
  res.json(schedules);
}));

/**
 * Get a specific schedule by job name
 */
app.get('/rest/schedules/:jobName', User.isAuthenticated, asyncHandler(async (req, res) => {
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

/**
 * Delete a specific schedule by job name
 */
app.delete('/rest/schedules/:jobName', User.isAuthenticated, asyncHandler(async (req, res) => {
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

/**
 * Delete all schedules
 */
app.delete('/rest/schedules', User.isAuthenticated, asyncHandler(async (req, res) => {
  try {
    // Clear all schedules by writing an empty array
    await db.putData('SCHEDULES_CONFIG', []);
    scheduler.init(); // Reinitialize scheduler with empty data
    res.json({ success: true, message: 'All schedules deleted' });
  } catch (err) {
    logger.error(`Error deleting all schedules: ${err.message}`);
    res.status(400).json({ 
      success: false, 
      message: `Failed to delete schedules: ${err.message}` 
    });
  }
}));

// =========================
// NOTIFICATIONS REST API - ADDITIONAL ENDPOINTS
// =========================

/**
 * Create a new notification
 */
app.post('/rest/notifications', User.isAuthenticated, asyncHandler(async (req, res) => {
  const { type, title, description, url } = req.body;
  const runDate = new Date().toISOString();
  const item = notificationData.createNotificationItem(runDate, type, title, description, url);
  notificationData.add(item);
  res.status(201).json({ success: true, item });
}));

/**
 * Update a notification by index
 */
app.put('/rest/notifications/:index', User.isAuthenticated, asyncHandler(async (req, res) => {
  const index = parseInt(req.params.index);
  const { type, title, description, url } = req.body;
  let items = notificationData.getItems();
  if (index < 0 || index >= items.length) {
    return res.status(404).json({ success: false, message: 'Notification not found' });
  }
  items[index] = notificationData.createNotificationItem(new Date().toISOString(), type, title, description, url);
  await notificationData.updateDbPromise();
  res.json({ success: true, item: items[index] });
}));

// =========================
// JOB HISTORY REST API - CRUD ENDPOINTS
// =========================

/**
 * Get all job history items
 */
app.get('/rest/history', User.isAuthenticated, asyncHandler(async (req, res) => {
  const items = await hist.getItemsUsingTZ();
  res.json(items);
}));

/**
 * Get a single job history item by index
 */
app.get('/rest/history/:index', User.isAuthenticated, asyncHandler(async (req, res) => {
  const items = await hist.getItemsUsingTZ();
  const index = parseInt(req.params.index);
  if (index < 0 || index >= items.length) {
    return res.status(404).json({ success: false, message: 'History item not found' });
  }
  res.json(items[index]);
}));

/**
 * Create a new job history item
 */
app.post('/rest/history', User.isAuthenticated, asyncHandler(async (req, res) => {
  const item = req.body;
  hist.add(item);
  res.status(201).json({ success: true, item });
}));

/**
 * Update a job history item by index
 */
app.put('/rest/history/:index', User.isAuthenticated, asyncHandler(async (req, res) => {
  const items = await hist.getItemsUsingTZ();
  const index = parseInt(req.params.index);
  if (index < 0 || index >= items.length) {
    return res.status(404).json({ success: false, message: 'History item not found' });
  }
  items[index] = req.body;
  await hist.updateDb();
  res.json({ success: true, item: items[index] });
}));

/**
 * Delete a job history item by index
 */
app.delete('/rest/history/:index', User.isAuthenticated, asyncHandler(async (req, res) => {
  let items = await hist.getItemsUsingTZ();
  const index = parseInt(req.params.index);
  if (index < 0 || index >= items.length) {
    return res.status(404).json({ success: false, message: 'History item not found' });
  }
  items.splice(index, 1);
  await hist.updateDb();
  res.json({ success: true });
}));

// =========================
// USER REST API - CRUD ENDPOINTS
// =========================

/**
 * List all users
 */
app.get('/rest/users', User.isAuthenticated, asyncHandler(async (req, res) => {
  try {
    let users = [];
    for await (const [key, value] of User.db.iterator()) {
      users.push({ username: value.username, email: value.email });
    }
    res.json(users);
  } catch (err) {
    logger.error(`Error listing users: ${err.message}`);
    res.status(500).json({ success: false, message: 'Error listing users' });
  }
}));

/**
 * Get a user by username
 */
app.get('/rest/users/:username', User.isAuthenticated, asyncHandler(async (req, res) => {
  const user = await User.getUserByUsername(req.params.username);
  if (!user) return res.status(404).json({ success: false, message: 'User not found' });
  res.json({ username: user.username, email: user.email });
}));

/**
 * Create a new user
 */
app.post('/rest/users', User.isAuthenticated, asyncHandler(async (req, res) => {
  const { username, email, password } = req.body;
  try {
    await User.createUser(username, email, password);
    res.status(201).json({ success: true, message: 'User created' });
  } catch (err) {
    logger.error(`Error creating user [${username}]: ${err.message}`);
    res.status(400).json({ success: false, message: `Error creating user: ${err.message}` });
  }
}));

/**
 * Update a user's email or password
 */
app.put('/rest/users/:username', User.isAuthenticated, asyncHandler(async (req, res) => {
  const { email, password } = req.body;
  try {
    const user = await User.getUserByUsername(req.params.username);
    if (!user) return res.status(404).json({ success: false, message: 'User not found' });
    if (email) user.email = email;
    if (password) await User.updatePassword(req.params.username, password);
    else await User.updateUser(req.params.username, user);
    res.json({ success: true });
  } catch (err) {
    logger.error(`Error updating user [${req.params.username}]: ${err.message}`);
    res.status(400).json({ success: false, message: `Error updating user: ${err.message}` });
  }
}));

/**
 * Delete a user
 */
app.delete('/rest/users/:username', User.isAuthenticated, asyncHandler(async (req, res) => {
  try {
    await User.deleteUser(req.params.username);
    res.json({ success: true });
  } catch (err) {
    logger.error(`Error deleting user [${req.params.username}]: ${err.message}`);
    res.status(400).json({ success: false, message: `Error deleting user: ${err.message}` });
  }
}));

// =========================
// GENERIC KEY-VALUE DATA REST API
// =========================

/**
 * Get value by key from custom data store
 * REQUIRES: User authentication + Sensitive data token
 */
app.get('/rest/data/:key', User.isAuthenticated, validateSensitiveDataToken, asyncHandler(async (req, res) => {
  try {
    const value = await db.getData(req.params.key);
    res.json({ key: req.params.key, value });
  } catch (err) {
    res.status(404).json({ success: false, message: 'Key not found' });
  }
}));

/**
 * Create or update value by key in custom data store
 * REQUIRES: User authentication + Sensitive data token
 */
app.put('/rest/data/:key', User.isAuthenticated, validateSensitiveDataToken, asyncHandler(async (req, res) => {
  try {
    await db.putData(req.params.key, req.body.value);
    res.json({ success: true });
  } catch (err) {
    logger.error(`Error storing data [${req.params.key}]: ${err.message}`);
    res.status(400).json({ success: false, message: `Error storing data: ${err.message}` });
  }
}));

/**
 * Delete value by key from custom data store
 * REQUIRES: User authentication + Sensitive data token
 */
app.delete('/rest/data/:key', User.isAuthenticated, validateSensitiveDataToken, asyncHandler(async (req, res) => {
  try {
    await db.deleteData(req.params.key);
    res.json({ success: true });
  } catch (err) {
    logger.error(`Error deleting data [${req.params.key}]: ${err.message}`);
    res.status(400).json({ success: false, message: `Error deleting data: ${err.message}` });
  }
}));

//New scriptsList screen
app.get('/scriptList.html', User.isAuthenticated, asyncHandler(async (req, res) => {
  var scriptsMeta = refreshScripts();

  res.render('scriptList', {
    agents: agents.getDict(),
    scriptTestAgentsBase64: buildScriptTestAgentsBase64(),
    scripts: scriptsMeta,
    csrf: req.csrfToken(),
  });
}));

/**
 * Orchestration UI pages
 */
app.get('/orchestrationList.html', User.isAuthenticated, asyncHandler(async (req, res) => {
  res.render('orchestrationList', { csrfToken: req.csrfToken() });
}));

app.get('/orchestrationBuilder.html', User.isAuthenticated, asyncHandler(async (req, res) => {
  const jobId = req.query.id; // undefined for new jobs, or specific ID for editing
  var color = "#FF9800";
  var icon = "workspaces";

  if(jobId !== undefined){
    const job = await orchestration.getJob(jobId);
    color = job.color;
    icon = job.icon;
  }

  res.render('orchestrationBuilder', { 
    csrfToken: req.csrfToken(),
    icons:serverConfig.job_icons,
    jobId: jobId || '',
    color: color,
    icon: icon
  });
}));

// In-memory cache for in-progress orchestration executions
// These are temporary stub records that get replaced when execution completes
const inProgressExecutions = {};  // key: `${jobId}:${executionId}`, value: execution stub

// Helper to generate a unique cache key for in-progress executions
function getInProgressKey(jobId, executionId) {
  return `${jobId}:${executionId}`;
}

// Helper to get in-progress execution if it exists
function getInProgressExecution(jobId, executionId) {
  const key = getInProgressKey(jobId, executionId);
  let cachedExecution = inProgressExecutions[key];
  
  // If not found in server's cache, check scheduler's cache (for webhook-triggered orchestrations)
  if (!cachedExecution && scheduler) {
    cachedExecution = scheduler.getInProgressExecution(jobId, executionId);
    logger.debug(`In-progress execution [${key}] found in scheduler cache`);
  }
  
  return cachedExecution;
}

// Helper to save in-progress execution
function saveInProgressExecution(jobId, executionId, execution) {
  const key = getInProgressKey(jobId, executionId);
  inProgressExecutions[key] = execution;
  logger.debug(`Cached in-progress execution [${key}]`);
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

// Helper to clear in-progress execution
function clearInProgressExecution(jobId, executionId) {
  const key = getInProgressKey(jobId, executionId);
  delete inProgressExecutions[key];
  logger.debug(`Cleared in-progress execution cache [${key}]`);
}

/**
 * Orchestration Monitor/Detail Routes
 */
app.get('/orchestration/monitor.html', User.isAuthenticated, asyncHandler(async (req, res) => {
  const jobId = req.query.jobId;
  let executionIndex = req.query.executionIndex || 'latest';
  const executionId = req.query.executionId;
  
  if (!jobId) {
    return res.status(400).send('Job ID is required');
  }
  
  // If executionId is provided, we'll let the client handle it
  // The JavaScript will read it from URL params and pass it to the API
  res.render('orchestrationMonitor', { 
    csrfToken: req.csrfToken(),
    jobId: jobId,
    executionIndex: executionIndex,
    executionId: executionId  // Pass executionId to template so it can be used in initial load
  });
}));

app.get('/orchestration/execution/details', User.isAuthenticated, asyncHandler(async (req, res) => {
  const orchestrationMonitor = require('./orchestrationMonitor.js');
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
        const orchestrationMonitor = require('./orchestrationMonitor.js');
        const jobDef = await orchestrationMonitor.getJobDefinitionVersion(jobId, 'current');
        
        if (!jobDef) {
          return res.status(404).json({ error: `Orchestration job [${jobId}] not found` });
        }

        // Fetch live logs from database for in-progress execute nodes
        // on every request so selected-node detail panels can stay current.
        const agents = require('./agents.js');
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
            startTime: cachedExecution.startTime,
            endTime: cachedExecution.endTime,
            status: cachedExecution.status,
            finalStatus: cachedExecution.finalStatus || 'unknown',
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

app.get('/orchestration/node/output', User.isAuthenticated, asyncHandler(async (req, res) => {
  const orchestrationMonitor = require('./orchestrationMonitor.js');
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

app.get('/api/schedules/by-orchestration/:orchestrationId', User.isAuthenticated, asyncHandler(async (req, res) => {
  const orchestrationId = req.params.orchestrationId;
  
  if (!orchestrationId) {
    return res.status(400).json({ error: 'Orchestration ID is required' });
  }
  
  // Get all schedules
  const allSchedules = await scheduler.getSchedules(-1); // Get all (passing -1 might be placeholder, let's use direct access)
  let matchingSchedules = [];
  
  try {
    // Access schedules from database
    const schedulesDb = require('./db.js');
    const schedules = await schedulesDb.getData('SCHEDULES_CONFIG').catch(() => []);
    
    if (Array.isArray(schedules)) {
      matchingSchedules = schedules.filter(schedule => 
        schedule.scheduleMode === 'orchestration' && 
        schedule.orchestrationId === orchestrationId
      );
    }
  } catch (err) {
    logger.warn('Error fetching schedules for orchestration:', err.message);
    matchingSchedules = [];
  }
  
  res.json({
    success: true,
    schedules: matchingSchedules
  });
}));

// ============================================================================
// ERROR HANDLING MIDDLEWARE (must be registered last)
// ============================================================================
app.use(errorHandlerMiddleware);

// ============================================================================
// SERVER RESTART NOTIFICATION
// ============================================================================
/**
 * Notify all agents that the server has restarted
 * This allows agents to reset their connection backoff and reconnect immediately
 */
async function notifyAgentsOfRestart() {
  try {
    const agentDict = agents.getDict();
    const http = require('http');
    
    if (!agentDict || Object.keys(agentDict).length === 0) {
      logger.info('No agents registered, skipping restart notifications');
      return;
    }
    
    const notificationPromises = [];
    const AGENT_HTTP_PORT = 49991;
    const REQUEST_TIMEOUT = 3000; // 3 seconds timeout per request
    
    for (const [agentName, agent] of Object.entries(agentDict)) {
      // Skip if agent doesn't have an address
      if (!agent.address) {
        logger.debug(`Agent [${agentName}] has no address, skipping restart notification`);
        continue;
      }
      
      const promise = new Promise((resolve) => {
        const postData = JSON.stringify({
          timestamp: new Date().toISOString()
        });
        
        const options = {
          hostname: agent.address,
          port: AGENT_HTTP_PORT,
          path: '/api/server-restart',
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(postData)
          },
          timeout: REQUEST_TIMEOUT
        };
        
        const req = http.request(options, (res) => {
          let data = '';
          res.on('data', (chunk) => {
            data += chunk;
          });
          res.on('end', () => {
            if (res.statusCode === 200) {
              logger.info(`Server restart notification sent to agent [${agentName}] at [${agent.address}:${AGENT_HTTP_PORT}]`);
            } else {
              logger.warn(`Server restart notification to agent [${agentName}] returned status code ${res.statusCode}`);
            }
            resolve();
          });
        });
        
        req.on('timeout', () => {
          req.destroy();
          logger.warn(`Server restart notification to agent [${agentName}] at [${agent.address}:${AGENT_HTTP_PORT}] timed out`);
          resolve();
        });
        
        req.on('error', (error) => {
          logger.warn(`Failed to notify agent [${agentName}] at [${agent.address}:${AGENT_HTTP_PORT}] of server restart: ${error.message}`);
          resolve();
        });
        
        req.write(postData);
        req.end();
      });
      
      notificationPromises.push(promise);
    }
    
    // Wait for all notifications to complete (or timeout)
    if (notificationPromises.length > 0) {
      await Promise.all(notificationPromises);
      logger.info(`Sent restart notifications to ${notificationPromises.length} agent(s)`);
    }
  } catch (error) {
    logger.error(`Error notifying agents of server restart: ${error.message}`);
  }
}

//______________________________________________________________________________________________________

var server = app.listen(port, async function () {
  var host = server.address().address
  var port = server.address().port
  debug.startBanner(version);
  logger.info("\x1b[32mListening on port: " + port);
  //if(debug.enabled())debug.warn("DEBUG ENABLED");
  logger.info("\x1b[32m----------------------------------------------------------------------------------------------\x1b[0m\n");
  
  try {
    passman.checkKey();

    await scheduler.init();
    logger.info('Scheduler initialized successfully');
    
    await orchestration.init();
    logger.info('Orchestration module initialized successfully');
    
    // Migrate to versioned format if needed
    try {
      const migratedCount = await orchestration.migrateToVersionedFormat();
      if (migratedCount > 0) {
        logger.info(`Migrated ${migratedCount} orchestrations to versioned format`);
      }
    } catch (err) {
      logger.error('Error during orchestration versioning migration:', err.message);
    }
    
    hist.init();
    //debug.Info("initiatilizing notification data");
    await notificationData.init();
    logger.debug('Initiatilizing notification data completed.');
    
    await running.init();
    
    // Notify all agents that the server has restarted
    await notifyAgentsOfRestart();
  } catch (error) {
    logger.error('Error during server startup initialization:', error.message);
  }
})
  

//Initialize the Browser Comms websocket
webSocketBrowser.init(server);

//Init the templates
if(serverConfig.templates && serverConfig.templates.enabled=="true"){
  (async () => {
    templates = new TemplateRepository(serverConfig.templates.repositoryUrl);
    await templates.init();
  })();
}
else{
  logger.info("Template Scripts Disabled");
}