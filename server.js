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
const { marked } = require('marked');
var session = require('express-session');
var cookieParser = require('cookie-parser');
var lusca = require('lusca');
var querystring = require('querystring');
var https = require('https');
var bodyParser = require('body-parser');
const multer = require('multer');

const User = require('./models/user');
const PERMISSIONS = User.PERMISSIONS;
const { asyncHandler, errorHandlerMiddleware, AppError } = require('./utils/errorHandler.js');
const { buildAgentInstallCommand } = require('./utils/agentInstallCommand.js');
const { SCRIPT_FILENAME_REGEX, normalizeScriptFilename, resolveScriptPath } = require('./utils/scriptFilename.js');
const { loadOrCreateSessionSecret } = require('./utils/sessionSecret.js');
const { safeRedirect } = require('./utils/safeRedirect.js');
const {
  configHasEncryptedSecrets,
  credentialsHaveEncryptedSecrets,
  loadOrCreateEncryptionKey,
} = require('./utils/encryptionKey.js');
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
const importExportManager = require('./utils/importExportManager.js');
const pluginManager = require('./pluginManager.js');

serverConfig = confighandler.initServerConfig({});
serverConfig = confighandler.loadConfigJson("./data/server-config.json");
// Passwords stay encrypted until the hub encryption key is loaded below.

hist = require("./history.js");
notificationData = require("./notificationData.js");

running=require("./running.js");
agentHistory = require ("./agentHistory.js");
db = require("./db.js");
definitionStore = require('./definitionStore.js');
nodeschedule = require('node-schedule');
scheduler = require ("./scheduler.js");
orchestration = require("./orchestration.js");
orchestrationEngine = require("./orchestrationEngine.js");
scriptTestManager = require('./scriptTestManager.js');
agentMessageProcessor = require('./agentMessageProcessor.js');
//const moment = require('moment-timezone');

agentComms = require ("./communications/agentCommunication.js");
mqttTransport = require ("./communications/mqttTransport.js");

webSocketBrowser = require('./communications/wsBrowserTransport.js');
webSocketServer = require("./communications/wsServerTransport.js");
webSocketServer.init();

let encryptionKeyState = null;
const ENCRYPTION_KEY_NOTICE_TITLE = 'Default encryption key in use';


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

function getDefinitionStoreConfig() {
  const defs = (serverConfig.server && serverConfig.server.definitions) || {};
  return {
    backend: process.env.DEFINITIONS_BACKEND || defs.backend || 'fs',
    jobsDir: path.resolve(__dirname, defs.jobsDir || './data/jobs'),
    orchestrationsDir: path.resolve(__dirname, defs.orchestrationsDir || './data/orchestrations'),
    stateDir: path.resolve(__dirname, defs.stateDir || './data/.state'),
    watcherDebounceMs: defs.watcherDebounceMs || 500,
    reconciliationIntervalSeconds: defs.reconciliationIntervalSeconds || 60
  };
}

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
// Initialize encryption key, database, and agents before accepting traffic.
const hubBootPromise = (async () => {
  try {
    db.initializeDB('./data/data.db');

    if (serverConfig.server && serverConfig.server.clearData == 'true') {
      logger.warn('!!!!!!!!!!!!!!!!!!!!!!!!!');
      logger.warn('!! CLEARING DATA STORE !!');
      logger.warn('!!!!!!!!!!!!!!!!!!!!!!!!!');
      await db.clearData();
      logger.warn('Data store cleared successfully.');
    }

    let hasEncryptedData = configHasEncryptedSecrets(serverConfig);
    try {
      const credentials = await db.getData('AGENT_CREDENTIALS');
      if (credentialsHaveEncryptedSecrets(credentials)) hasEncryptedData = true;
    } catch (err) {
      // Missing credentials record is normal on a new install.
    }

    encryptionKeyState = loadOrCreateEncryptionKey(path.join(__dirname, 'data'), {
      hasEncryptedData,
      envKey: process.env.ORCHELIUM_ENCRYPTION_KEY,
    });
    passman.setEncryptionSecret(encryptionKeyState.secret);
    passman.enforceEncryptionKeyPolicy();
    if (encryptionKeyState.source === 'generated') {
      logger.info('Created a new hub encryption key in data/encryption-key');
    } else if (encryptionKeyState.source === 'legacy') {
      logger.warn('Existing encrypted secrets found. Stored data/encryption-key as CHANGEIT until it is changed in Settings.');
    } else if (encryptionKeyState.source === 'env') {
      logger.info('Hub encryption key loaded from ORCHELIUM_ENCRYPTION_KEY');
    } else {
      logger.info('Loaded the stored hub encryption key');
    }

    serverConfig = confighandler.processServerConfig(serverConfig);
    mqttTransport.init();

    User.init('./data/user.db', logger, notifier);
    agentHistory.initializeDB('./data/agentHistory.db');

    mqttDisconnectNotificiationSent = false;

    agents = require('./agents.js');
    await agents.init();
    logger.info('Agents initialized successfully');
    const agentAuth = require('./communications/agentAuth');
    await agentAuth.init();
    logger.info('Agent credentials initialized');
  } catch (error) {
    logger.error('Error during initialization:', error.message);
    process.exit(1);
  }

  try {
    const webhooksData = require('./webhooksData.js');
    await webhooksData.init();
    logger.info('Webhooks data initialized successfully');
  } catch (error) {
    logger.error('Error initializing webhooks data:', error.message);
  }

  return encryptionKeyState;
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

function parseScriptParameterBlock(scriptContent) {
  const lines = String(scriptContent || '').split('\n');
  let inBlock = false;
  let description = null;
  const parameters = [];
  const documentationLines = [];

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
      const content = line.substring(1).trim();
      documentationLines.push(content);
      if (!description) {
        description = content.replace(/<[^>]*>/g, '');
      } else {
        parameters.push(content);
      }
    }
  }

  return {
    metaDescription: description || '',
    metaParameters: parameters.join('\n'),
    documentationHtml: documentationLines.join('\n'),
  };
}

function parseScriptDescriptionFromContent(scriptContent) {
  try {
    return parseScriptParameterBlock(scriptContent).metaDescription;
  } catch (error) {
    return '';
  }
}

// Script filenames are alphanumeric, underscore, dot, or hyphen, and end in .sh.
// Path traversal is protected by startsWith(baseDir) check.

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

const isProduction = process.env.NODE_ENV === 'production';
if (process.env.ORCHELIUM_TRUST_PROXY === 'true') {
  app.set('trust proxy', 1);
}

logger.info(`Running in ${isProduction ? 'production' : 'development'} mode`);
logger.info(`Set NODE_ENV to 'production' for production mode`);
logger.info(`Set ORCHELIUM_TRUST_PROXY to 'true' if running behind a reverse proxy (e.g., Nginx) to ensure secure cookies`);


//app.use(express.static('public'));
app.use(express.static(path.join(__dirname, 'public')));
app.use('/plugin-icons',express.static('plugins/.cache/icons'));
app.use('/monaco-editor', express.static('node_modules/monaco-editor'));
app.use('/material-icons', express.static('node_modules/material-icons'));
app.use('/material-design-icons', express.static('node_modules/material-design-icons-iconfont/dist'));
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(cookieParser());
const sessionSecretState = loadOrCreateSessionSecret(path.join(__dirname, 'data'));
if (sessionSecretState.created) {
  logger.info('Created a new session secret. Existing browser sessions must sign in again.');
} else {
  logger.info('Loaded the stored session secret.');
}
app.use(session({
  secret: sessionSecretState.secret,
  resave: false,
  saveUninitialized: false,
  cookie: {
    secure: isProduction,
    httpOnly: true,
    sameSite: 'strict'
  }
}));
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

// Serve docs assets and markdown safely (malformed markdown should not crash the server).
const docsRoot = path.join(__dirname, 'docs');
global.User = User;
global.asyncHandler = asyncHandler;
global.path = path;
global.marked = marked;
global.docsRoot = docsRoot;
require('./routes/docs')(app);

// Static docs assets (images/files) are served after markdown route handling.
app.use('/docs', User.isAuthenticated, express.static(docsRoot));

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

// Make currentUser available in every EJS template (populated by isAuthenticated)
app.use((req, res, next) => {
  res.locals.currentUser = req.session.user || null;
  res.locals.hasPerm = (permission) => User.hasPermission(res.locals.currentUser, permission);
  next();
});

// CSRF validation middleware for form-based authenticated routes
const validateCsrf = (req, res, next) => {
  // Skip validation for public routes
  const publicRoutes = ['/login.html', '/register.html', '/forgot.html'];
  if (publicRoutes.includes(req.path) || req.path.startsWith('/reset') || req.path.startsWith('/invite')) {
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
  }
  return sensitiveDataToken;
}

// Middleware to validate sensitive data API token
const validateSensitiveDataToken = (req, res, next) => {
  const token = initializeSensitiveDataToken();
  const providedToken = req.headers['x-data-token'] || req.headers['x-sensitive-token'] || req.query.dataToken;
  
  if (!providedToken || providedToken !== token) {
    logger.warn(`[SENSITIVE-API] Unauthorized access attempt to ${req.method} ${req.path} - invalid or missing token`);
    return res.status(403).json({ success: false, message: 'Invalid or missing sensitive data token' });
  }
  
  logger.debug(`[SENSITIVE-API] Authorized access to ${req.method} ${req.path}`);
  next();
};


// Routes
// Login route: Redirect to registration if no user is registered


// Logout route


// Register route: displays registration form if allowed and not already registered


// Forgot password route: displays forgot password form


// Reset password route: displays reset password form


// Invite acceptance route — like reset, but activates an invited (pending) user


//Initial setup


//Initial setup MQTT


//Initial setup Welcome


//Initial setup Complete


//Initial setup


const STORED_SECRET_MASK = '********';

function replaceStoredSecret(submitted) {
  if (typeof submitted !== 'string' || submitted.length === 0 || submitted === STORED_SECRET_MASK) {
    return null;
  }
  return submitted;
}


// Backup Management Routes
// Configure multer for in-memory file uploads
const upload = multer({ 
  storage: multer.memoryStorage(),
  limits: { fileSize: 500 * 1024 * 1024 } // 500MB max
});


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


// ============================================================================
// WEBHOOK MANAGEMENT API (Authenticated)
// ============================================================================

/**
 * GET /rest/webhooks/:jobName
 * Get all webhooks for a job
 */


/**
 * POST /rest/webhooks/:jobName
 * Create a new webhook for a job
 */


/**
 * PUT /rest/webhooks/:jobName/:webhookId
 * Update webhook (name, description, enable/disable)
 */


/**
 * POST /rest/webhooks/:jobName/:webhookId/rotate-key
 * Rotate webhook API key
 */


/**
 * DELETE /rest/webhooks/:jobName/:webhookId
 * Delete a webhook
 */


/**
 * GET /rest/webhooks-all
 * Get all webhooks across all jobs
 */


/**
 * GET /rest/webhooks/stats
 * Get webhook statistics
 */


// ================================================================
// RUNNING JOBS REST API - CRUD ENDPOINTS
// ================================================================

/**
 * Get all running jobs
 */


/**
 * Delete a running job by index
 */


/**
 * Delete a running job by job name
 */


/**
 * Delete a running job by execution ID
 */


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


/**
 * GET /metric-test.html  — Temporary test page for queryMetric exploration.
 * NOTE: Remove this page once the Rules Engine is built.
 */


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


var logsStr = "";


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
const staleReconcileSummaryCheck = setInterval(logStaleReconcileSummary, 60 * 60 * 1000);
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

function logStaleReconcileSummary()
{
  try {
    const reconciledCount = agentMessageProcessor.getAndResetStaleReconcileCount();
    logger.info(`[AGENT] Stale running queue reconciliations in last hour: ${reconciledCount}`);
  }
  catch(err) {
    logger.warn(`[AGENT] Unable to emit stale reconciliation summary: ${err.message}`);
  }
}

// ============================================================================
// ORCHESTRATION JOBS - REST API ENDPOINTS
// ============================================================================

/**
 * Get list of all orchestration jobs
 */


/**
 * Get a specific orchestration job
 */


/**
 * Create or update an orchestration job
 */


/**
 * Export a single orchestration job
 */


/**
 * Update an existing orchestration job
 */


/**
 * Delete an orchestration job
 */


/**
 * Mark a classic job execution as having been re-run
 */


/**
 * Mark an orchestration execution as having been re-run
 */


/**
 * Execute an orchestration job
 */


/**
 * Get execution history for a job
 */


/**
 * Get list of available scripts for the palette
 */


/**
 * Get list of available plugins for orchestration
 */


/**
 * Get list of available agents for orchestration execution
 */


/**
 * Get orchestration metadata (name, description, icon, color) without full definition
 */


/**
 * Get a specific version of an orchestration job
 */


/**
 * Get all schedules
 */


/**
 * Get a specific schedule by job name
 */


/**
 * Enable or disable a schedule without removing it.
 * A disabled job stays on the list and calendar and runs on the next schedule once enabled.
 */


/**
 * Delete a specific schedule by job name
 */


/**
 * Delete all schedules
 */


/**
 * Definition store status (backend, counts, invalid files)
 */


/**
 * Force reload of filesystem-backed definitions
 */


/**
 * Migrate definitions from DB keys into filesystem assets.
 */


// =========================
// NOTIFICATIONS REST API - ADDITIONAL ENDPOINTS
// =========================

/**
 * Create a new notification
 */


/**
 * Update a notification by index
 */


// =========================
// JOB HISTORY REST API - CRUD ENDPOINTS
// =========================

/**
 * Get all job history items
 */


/**
 * Get a single job history item by index
 */


/**
 * Create a new job history item
 */


/**
 * Update a job history item by index
 */


/**
 * Delete a job history item by index
 */


// =========================
// PLUGIN MANAGER PAGE
// =========================


// =========================
// PLUGIN MANAGER REST API
// =========================

// GET /rest/plugins/registry — fetch remote registry merged with local install status


// POST /rest/plugins/install/:name — download and install a plugin from the registry


// DELETE /rest/plugins/:name — uninstall a plugin


// =========================
// USER MANAGEMENT PAGE
// =========================


// =========================
// USER REST API
// =========================

// List all users (sanitised — no passwords)


// Get single user (sanitised)


// Invite a new user (creates pending account + sends email)


// Resend invite


// Update user permissions


// Activate a user


// Deactivate a user


// Admin-initiated password reset (sends reset email)


// Delete a user (cannot delete super admin)


// =========================
// GENERIC KEY-VALUE DATA REST API
// =========================

/**
 * Get value by key from custom data store
 * REQUIRES: User authentication + Sensitive data token
 */


/**
 * Create or update value by key in custom data store
 * REQUIRES: User authentication + Sensitive data token
 */


/**
 * Delete value by key from custom data store
 * REQUIRES: User authentication + Sensitive data token
 */


//New scriptsList screen


/**
 * Orchestration UI pages
 */


// Legacy Drawflow URL — redirect to canonical builder path


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


// Legacy Drawflow monitor URL — redirect to canonical monitor path


// ============================================================================
global.AppError = AppError;
global.ENCRYPTION_KEY_NOTICE_TITLE = ENCRYPTION_KEY_NOTICE_TITLE;
global.PERMISSIONS = PERMISSIONS;
global.REPOSITORY_URL = REPOSITORY_URL;
global.SCRIPT_FILENAME_REGEX = SCRIPT_FILENAME_REGEX;
global.STORED_SECRET_MASK = STORED_SECRET_MASK;
global.TemplateRepository = TemplateRepository;
global.User = User;
global.asyncHandler = asyncHandler;
global.asyncUtils = asyncUtils;
global.backupManager = backupManager;
global.bcrypt = bcrypt;
global.buildAgentInstallCommand = buildAgentInstallCommand;
global.buildScriptTestAgentsBase64 = buildScriptTestAgentsBase64;
global.clearInProgressExecution = clearInProgressExecution;
global.confighandler = confighandler;
global.docsRoot = docsRoot;
global.express = express;
global.getInProgressExecution = getInProgressExecution;
global.getSchedulerData = getSchedulerData;
global.getScriptTestRequestedBy = getScriptTestRequestedBy;
global.getScriptUsageEntries = getScriptUsageEntries;
global.getValidatedScriptPath = getValidatedScriptPath;
global.importExportManager = importExportManager;
global.loglines = loglines;
global.logsStr = logsStr;
global.marked = marked;
global.maskPasswords = maskPasswords;
global.minSupportedVersion = minSupportedVersion;
global.normalizeScriptFilename = normalizeScriptFilename;
global.parseScriptDescriptionFromContent = parseScriptDescriptionFromContent;
global.parseScriptParameterBlock = parseScriptParameterBlock;
global.path = path;
global.pingIndividualRuntime = pingIndividualRuntime;
global.pluginManager = pluginManager;
global.readSavedScript = readSavedScript;
global.refreshScripts = refreshScripts;
global.replaceStoredSecret = replaceStoredSecret;
global.resolveScriptPath = resolveScriptPath;
global.safeRedirect = safeRedirect;
global.saveInProgressExecution = saveInProgressExecution;
global.appRoot = __dirname;
Object.defineProperty(global, 'templates', {
  configurable: true,
  enumerable: true,
  get() { return templates; },
  set(value) { templates = value; },
});
global.updateInProgressExecution = updateInProgressExecution;
global.upload = upload;
global.validateCsrf = validateCsrf;
global.validateSensitiveDataToken = validateSensitiveDataToken;
global.version = version;
require('./routes/auth')(app);
require('./routes/pages')(app);
require('./routes/jobs')(app);
require('./routes/scripts')(app);
require('./routes/agents')(app);
require('./routes/settings')(app);
require('./routes/webhooks')(app);
require('./routes/notifications')(app);
require('./routes/orchestration')(app);
require('./routes/plugins')(app);
require('./routes/users')(app);
require('./routes/data')(app);
require('./routes/debug')(app);

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

var server;

(async () => {
  let keyState;
  try {
    keyState = await hubBootPromise;
    passman.checkKey();
  } catch (error) {
    logger.error('Error during hub boot:', error && error.message ? error.message : error);
    process.exit(1);
  }

  server = app.listen(port, async function () {
    var host = server.address().address
    var port = server.address().port
    debug.startBanner(version);
    logger.info("\x1b[32mListening on port: " + port);
    //if(debug.enabled())debug.warn("DEBUG ENABLED");
    logger.info("\x1b[32m----------------------------------------------------------------------------------------------\x1b[0m\n");

    try {
      await definitionStore.init(getDefinitionStoreConfig());
      logger.info(`Definition store initialized using backend [${definitionStore.getBackend()}]`);

      // Run idempotent user schema migration before accepting traffic
      try {
        await User.migrateUsers();
      } catch (err) {
        logger.error('User migration failed on startup — aborting:', err.message);
        process.exit(1);
      }

      await scheduler.init();
      logger.info('Scheduler initialized successfully');

      await orchestration.init();
      logger.info('Orchestration module initialized successfully');

      try {
        await require('./pluginRegistry.js').init();
        logger.info('Plugin registry initialized successfully');
      } catch (pluginInitErr) {
        logger.warn(`Plugin registry initialization failed (non-fatal): ${pluginInitErr.message}`);
      }
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

      if (keyState && keyState.recommendChange) {
        const existing = notificationData.getItems().some((item) => item && item.title === ENCRYPTION_KEY_NOTICE_TITLE);
        if (!existing) {
          notifier.sendNotification(
            ENCRYPTION_KEY_NOTICE_TITLE,
            'Stored secrets are still encrypted with the default key CHANGEIT. Open Settings and change the encryption key when you can.',
            'WARNING',
            '/settings.html'
          );
        }
      }

      await running.init();

      // Notify all agents that the server has restarted
      await notifyAgentsOfRestart();
    } catch (error) {
      logger.error('Error during server startup initialization:', error.message);
    }
  });

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
})();