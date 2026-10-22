var mqtt = require('mqtt');
var messProcessor = require('../agentMessageProcessor.js');
//var debug = undefined;
  
//const brokerUrl = 'mqtt://' + MQTT_SERVER + ':' + MQTT_SERVER_PORT;
const status_topic = 'orchelium/agent/status';
const command_topic = 'orchelium/agent/command';
var client;
let mqttReconnectTimeout;
var connectionTimeoutCallback;
var mqttConnected = false;



/*********************************
 * MQTT Handling for node status *
 *********************************/
async function startMqttConnectionProcess(reset){
  
    if(serverConfig.mqtt.enabled=="true")
    {
      logger.info("Initiating MQTT Connection");
      logger.debug(`reset = ${reset}`); 
      if(reset!==undefined && reset ==true)
      {
          return await connectToMqttServerOnStartup();  
      }
      else{
        logger.debug(`isMQTTSet() = ${confighandler.isMQTTSet()}`);
        logger.debug(`isMQTTConnected() = ${isMQTTConnected()}`);

        if(serverConfig.mqtt.enabled=="true" &&confighandler.isMQTTSet()  && !isMQTTConnected()){
          logger.warn("Connection is not operational - restarting");
          return await connectToMqttServerOnStartup();
        }
        else {
          logger.info("MQTT Server is already connected");
        }
      }
    }
    else
    {
      logger.info("MQTT is not enabled");
    }
  }
  
  async function connectToMqttServerOnStartup()
  {
    logger.info("Starting connection to MqttServer (on startup)");
    try {
      logger.debug(`client = ${client}`);
      if(client!=undefined && client!=null){
        logger.info("MQTT Connection still active, closing down");
        client.end();
        client=null;
      }
      logger.debug("Connecting to MQTT Server");
      
      await connectToMQTTServer();
      logger.debug("Returned ok");
      // Connection successful, perform actions here
      return "ok";
    } 
    catch (error) {
      // Handle connection error here
      logger.warn("Unable to connect to MQTT server");
      logger.warn(error);
      return JSON.stringify(error);
    }
  }

  function getClient(){
    return client;
  }

  function getCommandTopic(){
    return command_topic;
  }

  function getStatusTopic(){
    return status_topic;
  }

 
  function isMQTTConnected(){
    if(client && client.connected)return true;
    else return false;
  }
  
  
  function connectToMQTTServerWithTimeout(brokerUrl, clientOptions, timeout) {
    logger.info("Connecting to MQTT Server; Broker URL: " + brokerUrl);
    return new Promise((resolve, reject) => {
        client = mqtt.connect(brokerUrl, clientOptions);

        // MQTT client connect event
        client.on('connect', () => {
          mqttConnected = true;
          clearTimeout(connectionTimeoutCallback); // Clear the timeout if the connection succeeds
          resolve(client); // Resolve the promise with the client object
        });

        // MQTT client error event
        client.on('error', (error) => {
          clearTimeout(connectionTimeoutCallback); // Clear the timeout on error
          mqttConnected = false;
          client.end(); // Ensure the client is closed
          console.error('Failed to connect to MQTT broker:', error);
          reject(error); // Reject the promise with the error
        });

        // Implement the timeout
        connectionTimeoutCallback = setTimeout(() => {
          if(mqttConnected==false){
            console.error('Connection to MQTT broker timed out');
            client.end(); // Close the client if the timeout occurs
            reject(new Error('Connection timeout'));
          }
          else{
            console.info('Connection to MQTT broker ok');
            resolve(client); // Resolve the promise with the client object
          }
        }, timeout);
    });
}

  function connectToMQTTServer(){
    const connectionTimeout = 10000; // 10 seconds timeout (adjust as needed)
    let connected = false;
    var brokerUrl = 'mqtt://' + serverConfig.mqtt.server + ':' + serverConfig.mqtt.port;

    var clientOptions={};
    clientOptions.clientId="Orchelium_Server"
  
    if(confighandler.isMqttUserPresent()){
      logger.debug("Setting MQTT Auth credentials");
      clientOptions.username = serverConfig.mqtt.username;
      clientOptions.password = serverConfig.mqtt.password;
    }

    connectToMQTTServerWithTimeout(brokerUrl, clientOptions, connectionTimeout)
    .then(client => {
        logger.info('Successfully connected to MQTT broker');
        // Continue with your logic here
        mqttDisconnectNotificiationSent = false;
        logger.debug("subscribing to: " + status_topic);
        client.subscribe(status_topic);
        // Handle incoming messages
        client.on('message', (topic, message) => {
           messProcessor.processMessage(topic, message,"mqtt");
        });
    })
    .catch(error => {
        logger.warn("Unable to connect to MQTT Broker: " + JSON.stringify(error));
        //Send disconnect notification too
        if(mqttDisconnectNotificiationSent==false){
          if(serverConfig.server.connectionEnabled=="true")notifier.sendNotification("Orchelium - MQTT Disconnect",`Orchelium Server [${serverConfig.server.hostname}] Disconnected from MQTT Broker [${serverConfig.mqtt.server}] with error [${JSON.stringify(error)}]`,"WARNING","/agentstatus.html");
          mqttDisconnectNotificiationSent=true;
        }
        // Handle the error (e.g., retry the connection, alert the user, etc.)

    });
  }

  function connectToMQTTServerOrig() {
    const connectionTimeout = 10000; // 10 seconds timeout (adjust as needed)
    let connected = false;
    var brokerUrl = 'mqtt://' + serverConfig.mqtt.server + ':' + serverConfig.mqtt.port;
    logger.info("Connecting to MQTT Server; Broker URL: " + brokerUrl);
  
    var clientOptions={};
    clientOptions.clientId="Orchelium_Server"
  
    if(confighandler.isMqttUserPresent()){
      logger.debug("Setting MQTT Auth credentials");
      clientOptions.username = serverConfig.mqtt.username;
      clientOptions.password = serverConfig.mqtt.password;
    }
  
  
    return new Promise((resolve, reject) => {
      logger.info("Connecting to MQTT Server: " + brokerUrl);
      client = mqtt.connect(brokerUrl, clientOptions);
  
      // MQTT client connect event
      client.on('connect', () => {
        logger.info('Connected to MQTT broker: ' + brokerUrl + ". Subscribing to: " + status_topic);
        client.subscribe(status_topic); // Subscribe to the topic
        connected = true;
        mqttDisconnectNotificiationSent = false;
        clearTimeout(mqttReconnectTimeout);
        resolve(); // Resolve the promise indicating successful connection
      });
  
      // MQTT client error event
      client.on('error', (error) => {
        logger.error('MQTT Client Error occurred', error);
        clearTimeout(connectionTimeout);
        if (client && ! client.connected) {
          logger.error('Disconnected or failed to connect to MQTT broker:', error);
          
          //Send disconnect notification too
          if(mqttDisconnectNotificiationSent==false){
            if(serverConfig.server.connectionEnabled=="true")notifier.sendNotification("Orchelium - MQTT Disconnect",`Orchelium Server [${serverConfig.server.hostname}] Disconnected from MQTT Broker [${serverConfig.mqtt.server}] with error [${JSON.stringify(error)}]`,"WARNING","/agentstatus.html");
            mqttDisconnectNotificiationSent=true;
          }
          //client = undefined;
          client.end(true);
          client = undefined;
          //setTimeout(startMqttConnectionProcess,10000);
          reject(error); // Reject the promise with the error
        }
        else{
          logger.error('Unexpected MQTT Connection Error:', error);
          if(client && client.connected){
            client.end(true);
          }
          client = undefined;
          //setTimeout(startMqttConnectionProcess,10000);
          reject(error);
        }
      });
  
      // MQTT client close event
      client.on('close', () => {
        logger.debug("MQTT Connection Close initiated");
        logger.debug("connected = " + connected);
        if (!connected) {
          logger.warn('Connection to MQTT broker closed before establishing a connection');
          reject(new Error('Connection closed before successful connection'));
        }
      });
  
      // MQTT client disconnect event
      client.on('disconnect', () => {
        logger.debug("MQTT Disconnect Occurred");
        if(serverConfig.server.connectionEnabled=="true")notifier.sendNotification("Orchelium - MQTT Disconnect",`Orchelium Server [${serverConfig.server.hostname}] Disconnected from MQTT Broker [${serverConfig.mqtt.server}]`,"WARNING","/agentstatus.html");
        if (!connected) {
          logger.warn('Disconnected from MQTT broker before establishing a connection');
          reject(new Error('Disconnected before successful connection'));
        }
        else{
          reject(new Error('Disconnected from MQTT Server'));
        }
      });
  
      // MQTT message event
    client.on('message', (topic, message) =>  {
      messProcessor.processMessage(topic, message,"mqtt");
    });
  
      // Set a timeout to handle connection timeout
      mqttReconnectTimeout = setTimeout(() => {
        logger.debug("Checking for MQTT Connection timeout");

        if (client) {
        //if (client && !client.connected) {
          client.end(true); // Close the client if not connected within the timeout
          client=null;
        }
        logger.error('Failed to establish MQTT connection.');
        setTimeout(startMqttConnectionProcess,10000);
        //Set a timer to try agagin
        reject(new Error('Connection timeout'));
      }, connectionTimeout);
    });
  }
  
  

  function publishPing(agent_id){
    const message={
      name: agent_id,
      command: "ping"
    }
  
    var token = passman.createJWTToken(message,1);  
    publishMessage(command_topic,token);
  }
  
  function publishMessage(topic, message) {
    logger.debug(`Publishing to MQTT: Topic [${topic}], Message: [${JSON.stringify(message)}]`);
    if(client&& client.connected){
        client.publish(topic, JSON.stringify(message), {
        qos: 0,
        retain: false,
      });
    }
  }

  
  // Gracefully close the MQTT client on process termination
  process.on('SIGINT', () => {
    if(client&&client.connecte)client.end();
    process.exit();
  });
  
  function init(){
    startMqttConnectionProcess(true);
  }

  module.exports = { init, connectToMQTTServer, getCommandTopic, getStatusTopic, getClient, isMQTTConnected, startMqttConnectionProcess, publishPing, publishMessage }
