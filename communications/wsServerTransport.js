const WebSocket = require('ws');
const url = require('url');

var messProcessor = require('../agentMessageProcessor.js');
const notifier = require('../notify.js');

// Configuration. Command and status topic names come from shared/protocol
// and are chosen by the caller (agentCommunication) when it publishes.
const PORT = serverConfig.websocket_server.port;

// State management
const wsClients = new Map(); // connectionId -> WebSocket
const agentToConnection = new Map(); // agentId -> connectionId
const agentStatus = new Map(); // agentId -> { disconnectTime, timer }

// Notification functions
async function sendNotification(agentId, error) {
    if(serverConfig.server.connectionEnabled!="true")return;
    await notifier.sendNotification(
        `Agent [${agentId}] Disconnect`,
        `Agent [${agentId}] disconnected from Orchelium Server [${serverConfig.server.hostname}] Error [${JSON.stringify(error)}]`,
        "WARNING",
        "/agentstatus.html"
    );
}

async function sendInfoNotificationMessage(agentId, duration) {
    logger.warn("Sending reconnect");
    if(serverConfig.server.connectionEnabled!="true")return;
    await notifier.sendNotification(
        `Agent [${agentId}] Reconnect`,
        `Agent [${agentId}] reconnected to Orchelium Server [${serverConfig.server.hostname}] after ${Math.round(duration / 1000)} seconds`,
        "INFORMATION",
        "/agentstatus.html"
    );
}

const server = new WebSocket.Server({ port: PORT });
logger.info(`WebSocket server started on ws://localhost:${PORT}`);

async function init() {
    logger.info("WebSocket Server Initializing");
    server.on('connection', async (ws, request) => {
        const queryParams = url.parse(request.url, true).query;
        const connectionId = queryParams.name || 'anonymous';
        const agentId = connectionId.split('~')[0];

        // Capture agent's remote address for server restart notifications
        const remoteAddress = request.socket.remoteAddress || request.connection.remoteAddress || 'unknown';
        const agentHost = remoteAddress.includes('::ffff:') ? remoteAddress.substring(7) : remoteAddress;
        
        // Handle existing connection for this agent
        if (agentToConnection.has(agentId)) {
            const oldConnectionId = agentToConnection.get(agentId);
            const oldClient = wsClients.get(oldConnectionId);
            if (oldClient) {
                oldClient.close();
                wsClients.delete(oldConnectionId);
            }
        }

        // Add new connection
        wsClients.set(connectionId, ws);
        agentToConnection.set(agentId, connectionId);

        // Handle reconnection logic
        if (agentStatus.has(agentId)) {
            const status = agentStatus.get(agentId);
            

            if(serverConfig.server.minDisconnectDurationForNotification!==undefined && serverConfig.server.minDisconnectDurationForNotification>0){
                if (status.disconnectTime) {
                    const duration = Date.now() - status.disconnectTime;
                    //logger.warn(`Comparing: ${duration} vs ${serverConfig.server.minDisconnectDurationForNotification * 1000}`);
                    if (duration > (serverConfig.server.minDisconnectDurationForNotification * 1000)) {
                        await sendInfoNotificationMessage(agentId, duration);
                    }
                    if (status.timer) {
                        clearTimeout(status.timer);
                    }
                }
            }
            else {
                var duration;
                if(status.disconnectTime) duration = Date.now() - status.disconnectTime;
                else duration ="-1";
                await sendInfoNotificationMessage(agentId, duration);
            }
        }

        // Update status and store agent address
        agentStatus.set(agentId, { disconnectTime: null, timer: null });
        agents.updateAgentStatus(agentId, "online", "", null, null, null, "");
        agents.updateAgentAddress(agentId, agentHost);
        logger.info(`New client connected Agent: ${agentId}, connection ${connectionId}, host: ${agentHost}, Total clients: ${wsClients.size}`);

        ws.on('message', (message) => {
            try {
                const parsedMessage = JSON.parse(message);
                const agentId = parsedMessage.name;
                const receivedConnectionId = parsedMessage.connectionId;
                logger.debug(`Received message from Agent: ${agentId} on connection: ${connectionId} from: ${receivedConnectionId} Message: ${message}`);
                messProcessor.processMessage(parsedMessage.topic, message, "websocket");
            } catch (err) {
                logger.error('Failed to parse message:', err);
            }
        });

        ws.on('close', async () => {
            logger.warn(`WebSocket Client disconnected - removing [${connectionId}]`);
            wsClients.delete(connectionId);
            if (agentToConnection.get(agentId) === connectionId) {
                agentToConnection.delete(agentId);
            }

            const disconnectTime = Date.now();
            const timer = setTimeout(async () => {
                const currentStatus = agentStatus.get(agentId);
                if (currentStatus && currentStatus.disconnectTime === disconnectTime) {
                    await sendNotification(agentId, { error: "Agent Disconnect" });
                }
            }, serverConfig.server.minDisconnectDurationForNotification * 1000); 

            agentStatus.set(agentId, { disconnectTime, timer });
            agents.updateAgentStatus(agentId, "offline", "", null, null, null, "");
        });

        ws.on('error', async (error) => {
            wsClients.delete(connectionId);
            if (agentToConnection.get(agentId) === connectionId) {
                agentToConnection.delete(agentId);
            }
            logger.error(`WebSocket error - removing [${connectionId}] :`, error);

            const disconnectTime = Date.now();
            const timer = setTimeout(async () => {
                const currentStatus = agentStatus.get(agentId);
                if (currentStatus && currentStatus.disconnectTime === disconnectTime) {
                    await sendNotification(agentId, error);
                }
            }, serverConfig.server.minDisconnectDurationForNotification * 1000);

            agentStatus.set(agentId, { disconnectTime, timer });
            agents.updateAgentStatus(agentId, "offline", "", null, null, null, "");
        });
    });
}

function forceCloseRemove(agentId) {
    const connectionId = agentToConnection.get(agentId);
    if (connectionId) {
        const client = wsClients.get(connectionId);
        if (client) {
            try {
                client.close();
            } catch (err) {
                logger.warn(`Error closing WS Client for agent ${agentId}`);
            }
            wsClients.delete(connectionId);
            agentToConnection.delete(agentId);
        }
    }
}

function publishMessage(agentId, topic, message) {
    logger.info(`Publishing Message [${message}] for [${agentId}] with topic [${topic}]`);
    const connectionId = agentToConnection.get(agentId);
    if (!connectionId) {
        logger.warn(`Agent ${agentId} is not connected`);
        return;
    }

    const client = wsClients.get(connectionId);
    if (client) {
        logger.debug(`Sending command to Client: ${agentId}`);
        sendCommand(agentId, client, message);
        logger.info(`Published Message for [${agentId}] to topic [${topic}]`);
    } else {
        logger.warn(`No client found for connectionId ${connectionId}`);
    }
}

function publishPing(agentId) {
    const connectionId = agentToConnection.get(agentId);
    if (connectionId) {
        const client = wsClients.get(connectionId);
        if (client) {
            sendCommand(agentId, client, "ping");
        }
    }
}

function sendCommand(agentId, ws, command) {
    ws.send(command);
    logger.debug(`Sent command: ${command}`);
}

function handleStatusUpdate(message) {
    logger.debug(`Status update from client: ${JSON.stringify(message)}`);
}

function handleLogMessage(message) {
    logger.debug(`Log message from client: ${JSON.stringify(message)}`);
}

module.exports = { init, sendCommand, publishMessage, publishPing };