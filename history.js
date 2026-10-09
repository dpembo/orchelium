
const dateTimeUtils = require('./utils/dateTimeUtils.js');
const orchestration = require('./orchestration.js');
const definitionStore = require('./definitionStore.js');

const MAX_HISTORY_ITEMS = 150;
const DBKEY = "JOB_HISTORY";
var historyItems = [];

// Note: logger, db, and serverConfig are injected as globals from server.js

/** Initialize  */
function init() {
    getData();

}

async function getData() {
    try {
        logger.debug("Getting history item: " + DBKEY);
        var obj = await db.getData(DBKEY);
        if (obj !== undefined && obj !== null) historyItems = obj;
        //logger.debug("HISTORY: \n " + JSON.stringify(obj));
    }
    catch (err) {
        // NotFoundError is expected on first startup - don't log as warning
        if (err.message && err.message.includes('NotFoundError')) {
            logger.debug("No history items found on startup (expected on first run)");
        } else {
            logger.warn("Unable to find history data:", err.message);
        }
        //logger.warn(JSON.stringify(err));
    }
}

/** Add a new item */
function add(item) {
    logger.info("Adding item to history [" + item.jobName + "] [" + item.lastRun +"] to history queue sized: " + historyItems.length);
    //logger.info(JSON.stringify(item));
    historyItems.push(item);
    if (historyItems.length > MAX_HISTORY_ITEMS) {
        historyItems.shift();
    }
    logger.info("Added history item - new list " + historyItems.length);
    updateDb();
}

/** Mark an execution as re-run by searching only classic job history */
function markAsRerunDirect(executionId) {
    if (!executionId) {
        logger.warn(`markAsRerunDirect called with no executionId`);
        return false;
    }
    
    logger.info(`Directly marking classic job execution with executionId [${executionId}] as re-run`);
    
    // Search classic job history
    for (let i = historyItems.length - 1; i >= 0; i--) {
        const item = historyItems[i];
        
        if (item.executionId === executionId) {
            item.wasRerunAt = new Date().toISOString();
            logger.info(`Marked classic job history item at index [${i}] (${item.jobName}) with executionId [${executionId}] as re-run at [${item.wasRerunAt}]`);
            updateDb();
            return true;
        }
    }
    
    logger.warn(`Could not find classic job history item with executionId [${executionId}] to mark as rerun`);
    return false;
}

/** Mark an existing execution as having been re-run */
async function markAsRerun(executionId) {
    if (!executionId) {
        logger.warn(`markAsRerun called with no executionId`);
        return false;
    }
    
    logger.info(`Marking execution with executionId [${executionId}] as having been re-run`);
    
    // First, search classic job history
    for (let i = historyItems.length - 1; i >= 0; i--) {
        const item = historyItems[i];
        
        if (item.executionId === executionId) {
            item.wasRerunAt = new Date().toISOString();
            logger.info(`Marked classic job history item at index [${i}] (${item.jobName}) with executionId [${executionId}] as re-run at [${item.wasRerunAt}]`);
            updateDb();
            return true;
        }
    }
    
    // Search orchestration executions
    try {
        const db = require('./db.js');
        const orchestrationExecutions = await db.getData('ORCHESTRATION_EXECUTIONS').catch(() => ({}));
        
        logger.debug(`Searching ${Object.keys(orchestrationExecutions).length} orchestration jobs for executionId [${executionId}]`);
        
        // Search all orchestration jobs
        for (const jobId in orchestrationExecutions) {
            const executions = orchestrationExecutions[jobId];
            if (!Array.isArray(executions)) continue;
            
            // Search backwards through executions
            for (let i = executions.length - 1; i >= 0; i--) {
                const exec = executions[i];
                if (exec.executionId === executionId) {
                    exec.wasRerunAt = new Date().toISOString();
                    logger.info(`Marked orchestration execution [${jobId}] with executionId [${executionId}] as re-run at [${exec.wasRerunAt}]`);
                    await db.putData('ORCHESTRATION_EXECUTIONS', orchestrationExecutions);
                    return true;
                }
            }
        }
        
        logger.warn(`Could not find execution (classic or orchestration) with executionId [${executionId}] to mark as rerun`);
        return false;
    } catch (err) {
        logger.error(`Error searching orchestration executions: ${err.message}`);
        logger.warn(`Could not find history item with executionId [${executionId}] to mark as rerun`);
        return false;
    }
}

async function updateDb() {
    logger.info("Updating History Records");
    try {
        await db.putData(DBKEY, historyItems);
        logger.debug(`History Data items updated successfully`);
    } catch (err) {
        logger.error(`unable to update history items [${DBKEY}] to DB`, err);
    }
}

function searchItemWithName(searchTerm)
{
    if (Array.isArray(searchTerm)) {
        searchTerm = searchTerm[0];
    }

    if (typeof searchTerm !== 'string') {
        logger.error('Invalid searchTerm parameter');
        return null;
    }
    logger.debug("Searching History item with Partial Job Name [" + searchTerm + "]");
    logger.debug("Number of History items:" + historyItems.length);
    for(var searchIndex=historyItems.length-1;searchIndex>=0;searchIndex--){

        logger.debug(`On SearchIndex [${searchIndex}]`);
        logger.debug(`Checking if ${historyItems[searchIndex].jobName} matches the searchTerm ${searchTerm}`);
        if(historyItems[searchIndex].jobName.indexOf(searchTerm)>=0)return historyItems[searchIndex];
    }
    return null;
}

function createHistoryItem(jobName, runDate, returnCode, runTime, log, isManual, executionId = null, rerunFrom = null, nodeAlias = null) {
    logger.debug("Creating history item [" + jobName + "]");
    if(isManual===undefined)isManual=false;
    var item = {};
    item.jobName = jobName;
    item.runDate = runDate;
    item.returnCode = returnCode;
    item.runTime = runTime;
    item.log = log;
    item.manual = isManual;
    if (executionId) {
      item.executionId = executionId;  // Orchestration execution ID for grouping
    }
    if (rerunFrom) {
      item.rerunFrom = rerunFrom;  // Track when this job was re-run from a previous failed execution
    }
    if (nodeAlias) {
      item.nodeAlias = nodeAlias;  // Human-readable alias for workflow context display
    }
    logger.debug("History Item:\n" + JSON.stringify(item));
    return item;
}

function getItemsUsingTZ() {
    var items = getItems();
    var itemsStr = JSON.stringify(items);

    var adjustedItems = JSON.parse(itemsStr);

    for(var i=0;i<adjustedItems.length;i++){
        adjustedItems[i].runDate = dateTimeUtils.displayFormatDate(new Date(adjustedItems[i].runDate),false,serverConfig.server.timezone,'YYYY-MM-DD HH:mm:ss.SSS',false);
    }
    return adjustedItems;

}

function getItems() {
    //logger.info("Getting history items " + histmoryItems.length);
    //console.log(JSON.stringify(historyItems));
    //runDate
    return historyItems.slice();

}

function getItem(index) {
    //logger.info("Getting history item [" + index + "]");
    return historyItems[index];
}

async function getChartDataSet(numberOfDays) {

    logger.debug("---- GETTING CHART DATA SET ----");
    // Calculate today's date and seven days ago
    var today = new Date();
    today = new Date(dateTimeUtils.applyTz(today,serverConfig.server.timezone));
    var sevenDaysAgo = new Date();
    sevenDaysAgo.setDate(today.getDate() - numberOfDays);

    // Initialize arrays to store results
    const lastSevenDays = [];
    const runTimeSumPerDay = {};
    const successPerDay = {};
    const failPerDay = {};

    // Generate date strings for the last seven days
    for (let i = 0; i < numberOfDays; i++) {
        const currentDate = new Date(today);
        currentDate.setDate(today.getDate() - i);
        lastSevenDays.unshift(currentDate.toISOString().slice(0, 10));
    }

    // Initialize runTimeSumPerDay with zeros for each day
    lastSevenDays.forEach(date => {
        runTimeSumPerDay[date] = 0;
        successPerDay[date] = 0;
        failPerDay[date] = 0;
    });

    // Iterate through regular job history, EXCLUDING orchestration node items
    getItemsUsingTZ().forEach(item => {
        // Skip orchestration node items - we'll count only the parent execution instead
        // Pattern: "Orchestration [jobId] Execution [executionId] Node [nodeId]"
        if (item.jobName && item.jobName.match(/^Orchestration\s+\[.+?\]\s+Execution\s+\[.+?\]\s+Node\s+\[.+?\]/)) {
            return; // Skip node items, we count parent execution instead
        }

        // Parse runDate and runTime
        var runDate = new Date(item.runDate);
        const runTime = parseInt(item.runTime);
        //logger.debug(`runDate: ${runDate}`);
        //logger.debug(`runTime: ${runTime}`);

        var success = 0;
        var fail = 0;
        if(item.returnCode==0){
            success=1
        }
        else {
            fail=1;
        }

        // Check if runDate is within the last 7 days
        //logger.debug(`7daysAgo: ${sevenDaysAgo}`);
        //logger.debug(`today   : ${today}`);
        if (runDate >= sevenDaysAgo && runDate <= today) {
            //const formattedDate = runDate.toISOString().slice(0, 10); // Convert to string format "YYYY-MM-DD"
            const formattedDate = moment.tz(runDate, serverConfig.server.timezone).format('YYYY-MM-DD');
            runTimeSumPerDay[formattedDate] += runTime;
            successPerDay[formattedDate] += success;
            failPerDay[formattedDate] += fail;
        }
    });

    // Include parent orchestration execution results (top-level only, not individual nodes)
    try {
        const db = require('./db.js');
        const allOrchExecutions = await db.getData('ORCHESTRATION_EXECUTIONS').catch(() => ({}));
        
        if (allOrchExecutions && typeof allOrchExecutions === 'object') {
            for (const jobId in allOrchExecutions) {
                const executions = allOrchExecutions[jobId] || [];
                executions.forEach(execution => {
                    if (execution.startTime) {
                        const execDate = new Date(execution.startTime);
                        if (execDate >= sevenDaysAgo && execDate <= today) {
                            const formattedDate = moment.tz(execDate, serverConfig.server.timezone).format('YYYY-MM-DD');
                            
                            // Add runtime in seconds (from parent execution, not individual nodes)
                            if (execution.startTime && execution.endTime) {
                                const duration = (new Date(execution.endTime) - new Date(execution.startTime)) / 1000;
                                runTimeSumPerDay[formattedDate] += duration;
                            }
                            
                            // Count ONLY the parent execution result, not individual node results
                            // This ensures each orchestration job execution counts as one success or failure
                            if (execution.finalStatus === 'success') {
                                successPerDay[formattedDate]++;
                            } else if (execution.finalStatus === 'failure' || execution.finalStatus === 'error') {
                                failPerDay[formattedDate]++;
                            }
                        }
                    }
                });
            }
        }
    } catch (err) {
        logger.debug(`Unable to include orchestration data in chart: ${err.message}`);
        // Continue with regular history data if orchestration data can't be fetched
    }

    // Convert runTimeSumPerDay object into an array of sums
    const runTimeSumArray = lastSevenDays.map(date => runTimeSumPerDay[date]);
    const successSumArray = lastSevenDays.map(date => successPerDay[date]);
    const failSumArray    = lastSevenDays.map(date => failPerDay[date]);

    var data = {};
    data.labels = lastSevenDays;
    data.runtime = runTimeSumArray;
    data.success = successSumArray;
    data.fail = failSumArray;
    return data;
}

function getAverageRuntime(inJobName)
{   
    var foundCount=0;
    var total = 0;
    for(var i=0;i<historyItems.length;i++)
    {
        //logger.debug(`Matching ${historyItems[i].jobName} with ${inJobName}`);
        if(historyItems[i].jobName==inJobName && historyItems[i].returnCode==0){
            total += historyItems[i].runTime;
            foundCount++;
            //logger.debug("Matched " + historyItems[i].runTime);
        }
    }
    //logger.debug("Found: " + foundCount);
    //logger.debug("total: " + total);
    var avg = 1800 //default to 30 mins if unknown;
    
    if (foundCount>0){
        avg = parseFloat(total) / parseFloat(foundCount);
        avg = Math.round(avg);
        //logger.debug("avg is:" + avg)
    }
    return avg;
}

function getLastRun(inJobName){
    logger.debug("Getting Last Run for Job: " + inJobName);
    var items = getItemsUsingTZ();
    for (var i = items.length - 1; i >= 0; i--) {
        if(items[i].jobName==inJobName){
            //logger.debug("Found item at [" + i+"]: [" + JSON.stringify(historyItems[i]) + "]")

            return items[i];
        }
    }
    return null;
}


function getSuccessPercentage(inJobName)
{
    var items=0;
    var successTotal = 0;
    for(var i=0;i<historyItems.length;i++)
    {
        if(historyItems[i].jobName==inJobName){
            items++
            if(historyItems[i].returnCode==0)successTotal++;
        }
    }
    var pct = (successTotal/items)*100;
    pct = Math.round(pct);
    return pct;
}

async function getOrchestrationSuccessPercentage(jobId)
{
    try {
        // Get execution history for this orchestration job
        const executions = await orchestration.getExecutionHistory(jobId);
        
        if (!executions || executions.length === 0) {
            return '-';
        }
        
        var successCount = 0;
        for(var i = 0; i < executions.length; i++) {
            if(executions[i].finalStatus === 'success') {
                successCount++;
            }
        }
        
        var pct = (successCount / executions.length) * 100;
        pct = Math.round(pct);
        return pct;
    } catch (err) {
        logger.warn(`Error calculating orchestration success percentage for ${jobId}: ${err.message}`);
        return '-';
    }
}

// The manual field holds true or a trigger string ('manual', 'webhook', 'rule', 'schedule')
function isUnscheduled(manual, triggerType) {
    return manual === true || manual === 'true' || manual === 'manual' || manual === 'webhook' || triggerType === 'webhook';
}

async function getTodaysRun(){
    var todayStr = moment.tz(new Date(), serverConfig.server.timezone).format('YYYY-MM-DD');

    var count=0;
    var schedCount=0;
    var manualCount=0;
    var fail=0;
    var schedFail=0;
    var manualFail=0;
    var items = getItemsUsingTZ();
    for (var i = items.length - 1; i >= 0; i--) {
        // Skip orchestration node items - we only count regular jobs and orchestration parent executions
        // Pattern: "Orchestration [jobId] Execution [executionId] Node [nodeId]"
        if (items[i].jobName && items[i].jobName.match(/^Orchestration\s+\[.+?\]\s+Execution\s+\[.+?\]\s+Node\s+\[.+?\]/)) {
            continue; // Skip orchestration nodes
        }

        var runDate = items[i].runDate;
        var runDateStr = runDate.substring(0, 10);

        if(runDateStr==todayStr){
            if(items[i].returnCode==0)
            {
                count++
                if(isUnscheduled(items[i].manual))manualCount++
                else schedCount++;
            }
            else{
                fail++;
                if(isUnscheduled(items[i].manual))manualFail++
                else schedFail++;
            }
        }
    }

    // Also include today's orchestration parent execution results
    try {
        const allOrchExecutions = await db.getData('ORCHESTRATION_EXECUTIONS').catch(() => ({}));
        if (allOrchExecutions && typeof allOrchExecutions === 'object') {
            for (const jobId in allOrchExecutions) {
                const executions = allOrchExecutions[jobId] || [];
                executions.forEach(execution => {
                    if (execution.startTime) {
                        const execDate = new Date(execution.startTime);
                        const execDateStr = moment.tz(execDate, serverConfig.server.timezone).format('YYYY-MM-DD');
                        
                        // Count this execution if it ran today
                        if (execDateStr === todayStr) {
                            if (execution.finalStatus === 'success') {
                                count++;
                                if (isUnscheduled(execution.manual, execution.triggerContext && execution.triggerContext.type)) manualCount++;
                                else schedCount++;
                            } else if (execution.finalStatus === 'failure' || execution.finalStatus === 'error') {
                                fail++;
                                if (isUnscheduled(execution.manual, execution.triggerContext && execution.triggerContext.type)) manualFail++;
                                else schedFail++;
                            }
                        }
                    }
                });
            }
        }
    } catch (err) {
        logger.debug(`Unable to include today's orchestration data in run count: ${err.message}`);
    }

    var result = {};
    result.success=count;
    result.manualCount=manualCount;
    result.scheduledCount=schedCount;
    result.fail=fail;
    result.scheduledFail=schedFail;
    result.manualFail=manualFail;
    return result;
}

/**
 * Group orchestration node executions under their parent orchestration job
 * Returns a mixed array of regular history items and grouped orchestration items
 * Fetches orchestration names from the database for display
 */
async function getItemsGroupedByOrchestration() {
    const items = getItemsUsingTZ();
    const grouped = [];
    const orchestrationMap = new Map(); // Map of "${jobId}#${executionId}" -> {parent, nodes}
    const regularItems = [];

    // Fetch all orchestrations to get their names, descriptions, icons, and colors
    let orchestrationNames = {};
    let orchestrationDescriptions = {};
    let orchestrationIcons = {};
    let orchestrationColors = {};
    // nodeId -> { type, label, description } per job (from latest definition)
    let orchestrationNodeMeta = {};
    try {
        const allOrchestrations = await definitionStore.listOrchestrations();
        if (allOrchestrations) {
            for (const [jobId, jobData] of Object.entries(allOrchestrations)) {
                orchestrationNames[jobId] = jobData.name || `Orchestration [${jobId}]`;
                orchestrationDescriptions[jobId] = jobData.description || '';
                orchestrationIcons[jobId] = jobData.icon || 'schema';
                orchestrationColors[jobId] = jobData.color || '#000000';

                const nodeMap = {};
                let nodes = jobData.nodes || [];
                if ((!nodes || !nodes.length) && Array.isArray(jobData.versions) && jobData.versions.length) {
                    const ver = jobData.versions[jobData.versions.length - 1];
                    nodes = (ver && ver.nodes) || [];
                }
                (nodes || []).forEach(function (n) {
                    if (!n || !n.id) return;
                    nodeMap[n.id] = {
                        type: n.type || '',
                        label: n.label || n.name || n.alias || '',
                        description: n.description || '',
                        script: (n.config && (n.config.scriptName || n.config.script)) || n.scriptName || '',
                        plugin: (n.config && (n.config.pluginId || n.config.plugin)) || n.pluginId || ''
                    };
                });
                orchestrationNodeMeta[jobId] = nodeMap;
            }
        }
    } catch (err) {
        logger.debug('Unable to fetch orchestration names: ' + err.message);
        // Continue without names if database fetch fails
    }

    // Separate orchestration nodes from regular items
    for (const item of items) {
        // Pattern: "Orchestration [jobId] Execution [executionId] Node [nodeId]"
        const orchestrationMatch = item.jobName.match(/^Orchestration \[([^\]]+)\] Execution \[([^\]]+)\] Node \[([^\]]+)\]$/);
        
        if (orchestrationMatch) {
            const jobId = orchestrationMatch[1];
            const executionId = orchestrationMatch[2];
            const nodeId = orchestrationMatch[3];
            
            // Use composite key to distinguish multiple executions of the same job
            const mapKey = `${jobId}#${executionId}`;
            
            if (!orchestrationMap.has(mapKey)) {
                // Get the orchestration display name, description, icon, and color
                const displayName = orchestrationNames[jobId] || `Orchestration [${jobId}]`;
                const displayDesc = orchestrationDescriptions[jobId] || '';
                const displayIcon = orchestrationIcons[jobId] || 'schema';
                const displayColor = orchestrationColors[jobId] || '#000000';
                
                orchestrationMap.set(mapKey, {
                    parent: {
                        jobName: displayName,
                        description: displayDesc,
                        jobId: jobId,
                        executionId: item.executionId,
                        runDate: item.runDate,
                        isOrchestration: true,
                        icon: displayIcon,
                        color: displayColor,
                        children: []
                    },
                    nodeMap: new Map()
                });
            }
            
            const orchData = orchestrationMap.get(mapKey);
            // Enrich child with node type / label from current definition (for history pills)
            const meta = (orchestrationNodeMeta[jobId] || {})[nodeId] || {};
            item.nodeId = nodeId;
            if (meta.type) item.nodeType = meta.type;
            if (meta.label && !item.nodeAlias) item.nodeAlias = meta.label;
            if (meta.label) item.nodeLabel = meta.label;
            if (meta.description) item.nodeDescription = meta.description;
            if (meta.script) item.nodeScript = meta.script;
            if (meta.plugin) item.nodePlugin = meta.plugin;
            orchData.nodeMap.set(nodeId, item);
        } else {
            regularItems.push(item);
        }
    }

    // Merge and sort: newest first
    // Orchestration items should group by latest execution date
    // Get orchestration executions to use finalStatus for parent status
    let orchestrationExecutions = {};
    try {
      orchestrationExecutions = await db.getData('ORCHESTRATION_EXECUTIONS');
    } catch (err) {
      logger.debug('Unable to fetch orchestration executions for grouping: ' + err.message);
    }

    const orchestrationItems = Array.from(orchestrationMap.values()).map(data => {
        const nodeItems = Array.from(data.nodeMap.values());
        data.parent.children = nodeItems;
        
        // Update parent runDate to be the latest among its children
        if (nodeItems.length > 0) {
            const latestNode = nodeItems.reduce((latest, current) => 
                new Date(current.runDate) > new Date(latest.runDate) ? current : latest
            );
            data.parent.runDate = latestNode.runDate;
            
            // Use finalStatus from ORCHESTRATION_EXECUTIONS if available
            const jobId = data.parent.jobId;
            const executionId = data.parent.executionId;
            let returnCode = 0; // default to success
            let manual = false; // default to scheduled
            
            if (orchestrationExecutions[jobId]) {
              // Find the execution with matching executionId
              const execution = orchestrationExecutions[jobId].find(exec => exec.executionId === executionId);
              if (execution && execution.finalStatus) {
                // Use finalStatus: 0 for success, 1 for failure/error
                returnCode = (execution.finalStatus === 'success') ? 0 : 1;
                // Use manual flag from execution
                manual = execution.manual || false;
                // Copy wasRerunAt flag from execution if present
                if (execution.wasRerunAt) {
                  data.parent.wasRerunAt = execution.wasRerunAt;
                }
              } else {
                // Fallback: calculate from children if execution not found
                returnCode = nodeItems.some(n => n.returnCode !== 0) ? 1 : 0;
              }
            } else {
              // Fallback: calculate from children if executions not available
              returnCode = nodeItems.some(n => n.returnCode !== 0) ? 1 : 0;
            }
            
            data.parent.returnCode = returnCode;
            data.parent.manual = manual;
        }
        
        return data.parent;
    });

    // Interleave orchestration and regular items, sorted by date (newest first)
    const allItems = [...regularItems, ...orchestrationItems];
    allItems.sort((a, b) => {
        const dateA = new Date(a.runDate);
        const dateB = new Date(b.runDate);
        return dateB - dateA; // Newest first
    });

    return allItems;
}

/**
 * Clear all history items from the database
 * @returns {Promise<void>}
 */
async function clearHistory() {
    try {
        logger.info("Clearing all history items");
        historyItems = [];
        await db.putData(DBKEY, historyItems);
        logger.info("History cleared successfully");
    } catch (err) {
        logger.error("Error clearing history: " + err.message);
        throw err;
    }
}

module.exports = { init, add, getItems, getItemsUsingTZ, getItem, searchItemWithName, createHistoryItem, markAsRerun, markAsRerunDirect, getChartDataSet, getAverageRuntime, getLastRun, getSuccessPercentage, getOrchestrationSuccessPercentage, getTodaysRun, getItemsGroupedByOrchestration, clearHistory };
