const { initServerConfig } = require("../configuration");
const { publishMessage } = require("./mqttTransport");
const { TOPICS, applyExecutionContext } = require("../shared/protocol");
const agentAuth = require("./agentAuth");

const command_topic = TOPICS.command;


function getCurrentStack(skipFrames = 1) {
    const error = new Error();
    const stack = error.stack || '';

    // Split the stack trace into lines
    const stackLines = stack.split('\n');

    // Remove the first `skipFrames` stack frames (usually the call to getCurrentStack)
    return stackLines.slice(skipFrames + 1).join('\n');
}


function sendCommand(agent_id, topic, command, commandParams, jobName, commsType, manual, executionId, triggerContext = null, contextEnvVars = {}, rerunFrom = null, executionMetadata = null){

    if(manual===undefined||manual===null)manual=false;

    if(commsType===undefined||commsType===null){
        value = agents.getAgent(agent_id);
        if(value!=undefined && value!==null)commsType = value.commsType;
    }
    const message={
        name: agent_id,
        topic: command_topic,
        manual: manual,
        commsType: commsType,
        command: command,
        commandParams: commandParams,
        jobName: jobName,
        executionId: executionId || null,  // Include execution ID in message
        triggerContext: triggerContext || null,  // Include trigger context for orchestrations/scripts
        contextEnvVars: contextEnvVars || {},  // Environment variables for script injection
        rerunFrom: rerunFrom || null  // Track if this is a rerun of a failed job
    }

    if (executionMetadata && typeof executionMetadata === 'object') {
        applyExecutionContext(message, executionMetadata);
    }

    var token = agentAuth.signCommand(message, 1);

    if(commsType=="mqtt" && serverConfig.mqtt.enabled=="true" && isTransportProtocolConnected("mqtt",false))mqttTransport.publishMessage(command_topic,token);
    else if(commsType=="websocket")webSocketServer.publishMessage(agent_id,command_topic,token);
    else logger.warn("Transport mechanism disconnected - unable to publish command");
}

function pingAgent(commsType, agent_id){
    logger.info(`Pinging runtime [${agent_id}]`);  
    //logger.debug(`Stack: ` + getCurrentStack(1));
    if(commsType===undefined||commsType===null||commsType.length==0){
        logger.debug("No Comms Type provided");
        var agent = agents.getAgent(agent_id);  
        commsType = agent.commsType;
    }

    logger.info(`Pinging runtime [${agent_id}] with CommsType [${commsType}]`);  

    //Send a publish message
    const message={
        name: agent_id,
        topic: command_topic,
        command: "ping"
    }
    var token = agentAuth.signCommand(message, 1);
    
    //logger.debug("serverConfig.mqtt.enabled: " + serverConfig.mqtt.enabled);
    //logger.debug("Is connected: " + isTransportProtocolConnected("mqtt",false));
    if(commsType=="mqtt" && serverConfig.mqtt.enabled=="true" && isTransportProtocolConnected("mqtt",false))mqttTransport.publishMessage(command_topic,token);
    else if(commsType=="websocket")webSocketServer.publishMessage(agent_id,command_topic,token);
    else logger.warn("Skipping ping of agents as transport mechanism is not connected");
}

function pingAllAgents(agentStatusDict){
    for (const [key, value] of Object.entries(agentStatusDict)) {
        if(value.status!="register")
        {
            logger.debug("AGENT: " + JSON.stringify(value));
            commsType = value.commsType;
            if(commsType===undefined||commsType===null)commsType="mqtt";

            //if(isTransportProtocolConnected("mqtt",true)){
                pingAgent(commsType, key);
            //}
            //else{
            //    logger.warn("Skipping ping of agents as transport mechanism is not connected");
            //}
        }
    }
}

function sendEnrollment(agentId, token) {
    const message = {
        name: agentId,
        topic: command_topic,
        command: 'enroll',
        commandParams: token,
    };
    // The agent does not have the token yet, so this message stays on the hub key.
    const jwtToken = passman.createJWTToken(message, 1);
    return webSocketServer.sendDirect(agentId, jwtToken);
}

function isTransportProtocolConnected(type,attemptReconnect){
    var connected=true;
    if(type=="mqtt" && serverConfig.mqtt.enabled=="true" && !mqttTransport.isMQTTConnected()){
        logger.warn("MQTT transport mechanism is not connected");
        if(attemptReconnect==true){
            logger.info("Attempting MQTT Reconnection Process");  
            mqttTransport.startMqttConnectionProcess(true);
        }
        connected = false;
    }
    return connected;
}

module.exports = {pingAgent,pingAllAgents,sendCommand,sendEnrollment}