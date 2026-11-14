const crypto = require('crypto');
const WebSocket = require('ws');
const url = require('url');

var messProcessor = require('../agentMessageProcessor.js');
const notifier = require('../notify.js');
const agentAuth = require('./agentAuth');

// Configuration. Command and status topic names come from shared/protocol
// and are chosen by the caller (agentCommunication) when it publishes.
const PORT = serverConfig.websocket_server.port;

// State management
const wsClients = new Map(); // connectionId -> WebSocket
const agentToConnection = new Map(); // agentId -> connectionId
const pendingConnections = new Map(); // agentId -> connectionId
const agentStatus = new Map(); // agentId -> { disconnectTime, timer }

function socketIsOpen(socket) {
    return Boolean(socket && socket.readyState === WebSocket.OPEN);
}

function openConnectionFor(map, agentId) {
    const connectionId = map.get(agentId);
    if (!connectionId) return false;
    return socketIsOpen(wsClients.get(connectionId));
}

function rejectSocket(ws, code, reason) {
    ws.on('error', () => {});
    try {
        ws.close(code, reason);
    } catch (err) {
        logger.warn(`Failed to close rejected agent socket: ${err.message}`);
    }
}

function authorizationToken(request) {
    const header = request.headers && (request.headers.authorization || request.headers.Authorization);
    if (!header) return '';
    const match = String(header).match(/^Bearer\s+(.+)$/i);
    return match ? match[1].trim() : '';
}

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
        const claimedName = queryParams.name || '';
        const agentId = claimedName.split('~')[0];

        if (!agentId || agentId === 'anonymous') {
            logger.warn('Rejected WebSocket connection with no agent name');
            rejectSocket(ws, 4001, 'agent name required');
            return;
        }

        const token = authorizationToken(request);
        let tokenValid = agentAuth.presentedTokenMatches(agentId, token);
        if (!tokenValid && token) {
            tokenValid = await agentAuth.claimInstallToken(agentId, token);
        }
        const knownAgent = agents.getAgent(agentId);
        const decision = agentAuth.decideConnection({
            agentStatus: knownAgent ? knownAgent.status : null,
            hasCredential: agentAuth.hasCredential(agentId),
            tokenValid,
            liveSocket: openConnectionFor(agentToConnection, agentId),
            pendingSocket: openConnectionFor(pendingConnections, agentId),
        });

        if (decision === 'reject') {
            logger.warn(`Rejected WebSocket connection for agent [${agentId}]`);
            rejectSocket(ws, 4001, 'unauthorized');
            return;
        }

        const connectionId = `${agentId}~${crypto.randomBytes(4).toString('hex')}`;
        const approved = decision === 'approve-replace' || decision === 'legacy-accept';

        // Capture agent's remote address for server restart notifications
        const remoteAddress = request.socket.remoteAddress || request.connection.remoteAddress || 'unknown';
        const agentHost = remoteAddress.includes('::ffff:') ? remoteAddress.substring(7) : remoteAddress;

        if (approved && agentToConnection.has(agentId)) {
            const oldConnectionId = agentToConnection.get(agentId);
            const oldClient = wsClients.get(oldConnectionId);
            if (oldClient && oldClient !== ws) {
                try { oldClient.close(); } catch (err) { logger.warn(`Error closing previous socket for ${agentId}`); }
                wsClients.delete(oldConnectionId);
            }
        }

        if (approved && pendingConnections.has(agentId)) {
            const oldPendingId = pendingConnections.get(agentId);
            const oldPending = wsClients.get(oldPendingId);
            if (oldPending && oldPending !== ws) {
                try { oldPending.close(); } catch (err) { logger.warn(`Error closing pending socket for ${agentId}`); }
                wsClients.delete(oldPendingId);
            }
            pendingConnections.delete(agentId);
        }

        // Add new connection. Pending sockets are not command targets and
        // must not mark the agent online or replace an approved socket.
        wsClients.set(connectionId, ws);
        if (approved) {
            agentToConnection.set(agentId, connectionId);
        } else {
            pendingConnections.set(agentId, connectionId);
        }

        if (approved && agentStatus.has(agentId)) {
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

        if (approved) {
            agentStatus.set(agentId, { disconnectTime: null, timer: null });
            agents.updateAgentStatus(agentId, "online", "", null, null, null, "");
            agents.updateAgentAddress(agentId, agentHost);
            logger.info(`New client connected Agent: ${agentId}, connection ${connectionId}, host: ${agentHost}, Total clients: ${wsClients.size}`);
        } else {
            if (agents.getAgent(agentId)) agents.updateAgentAddress(agentId, agentHost);
            logger.info(`Pending agent [${agentId}] connected from ${agentHost}, connection ${connectionId}`);
        }

        ws.on('message', (message) => {
            try {
                const parsedMessage = JSON.parse(message);
                logger.debug(`Received message from Agent: ${agentId} on connection: ${connectionId}`);
                messProcessor.processMessage(parsedMessage.topic, message, "websocket", agentId);
            } catch (err) {
                logger.error('Failed to parse message:', err);
            }
        });

        ws.on('close', async () => {
            logger.warn(`WebSocket Client disconnected - removing [${connectionId}]`);
            const wasApproved = agentToConnection.get(agentId) === connectionId;
            wsClients.delete(connectionId);
            if (wasApproved) {
                agentToConnection.delete(agentId);
            }
            if (pendingConnections.get(agentId) === connectionId) {
                pendingConnections.delete(agentId);
            }
            if (!wasApproved) return;

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
            const wasApproved = agentToConnection.get(agentId) === connectionId;
            wsClients.delete(connectionId);
            if (wasApproved) {
                agentToConnection.delete(agentId);
            }
            if (pendingConnections.get(agentId) === connectionId) {
                pendingConnections.delete(agentId);
            }
            logger.error(`WebSocket error - removing [${connectionId}] :`, error);
            if (!wasApproved) return;

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

function hasOpenConnection(agentId) {
    return openConnectionFor(agentToConnection, agentId);
}

function sendDirect(agentId, payload) {
    const connectionId = agentToConnection.get(agentId) || pendingConnections.get(agentId);
    const client = connectionId ? wsClients.get(connectionId) : null;
    if (!client || !socketIsOpen(client)) {
        return false;
    }
    client.send(payload);
    logger.info(`Sent direct command to agent [${agentId}]`);
    return true;
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

module.exports = { init, sendCommand, publishMessage, publishPing, hasOpenConnection, sendDirect, forceCloseRemove };