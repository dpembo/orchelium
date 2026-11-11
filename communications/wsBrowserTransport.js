var socketIO = require('socket.io');
var io;

//------------------------------------------------------------------

/****************************
 * Websocket functions      *
 ****************************/
function init(server) {
    io = socketIO(server);

    // Handle incoming WebSocket connections
    io.on('connection', (socket) => {
        logger.debug('[wsBrowserTransport] A websocket client connected.');

        // Handle client disconnection
        socket.on('disconnect', () => {
            logger.debug('[wsBrowserTransport] A websocket client disconnected.');
        });

        // Handle room joining for scoped event subscription
        socket.on('join-room', (data) => {
            if (!data || !data.room) {
                logger.warn('[wsBrowserTransport] Invalid room join request');
                return;
            }
            socket.join(data.room);
            logger.debug(`[wsBrowserTransport] Client joined room [${data.room}]`);
        });

        // Handle room leaving
        socket.on('leave-room', (data) => {
            if (!data || !data.room) {
                logger.warn('[wsBrowserTransport] Invalid room leave request');
                return;
            }
            socket.leave(data.room);
            logger.debug(`[wsBrowserTransport] Client left room [${data.room}]`);
        });

        socket.on('delete', (data) => {
            logger.debug('[wsBrowserTransport] A delete request over websocket was requested for [' + data + ']');
            agents.deleteAgent(data);
            try {
                require('./agentAuth').forget(data);
            } catch (err) {
                logger.warn(`Failed to remove credentials for [${data}]: ${err.message}`);
            }
            emitNotification('deleted', data);
        });

        socket.on('ping', (data) => {
            logger.debug('[wsBrowserTransport] A ping was requested for [' + data + ']');
            //mqttTransport.publishPing(data);
            agentComms.pingAgent("websocket",data);
        });

    });
}

// Emit a notification to all connected clients
function emitNotification(event, message) {
    logger.info(`Emmiting notification to browser [${event} - ${message}]`);
    io.emit(event, message);
}

// Emit a schedule-specific event to all connected clients
function emitScheduleEvent(scheduleIndex, eventType, data) {
    if (!io) {
        logger.warn(`Socket.io not initialized, cannot emit schedule event`);
        return;
    }
    const eventName = `${eventType}:${scheduleIndex}`;
    logger.debug(`Emitting schedule event [${eventName}] with data: ${JSON.stringify(data).substring(0, 100)}`);
    io.emit(eventName, data);
}

// Emit an orchestration-specific event to all connected clients
function emitOrchestrationEvent(jobId, executionId, eventType, data) {
    if (!io) {
        logger.warn(`Socket.io not initialized, cannot emit orchestration event`);
        return;
    }
    const eventName = `${eventType}:${jobId}:${executionId}`;
    logger.debug(`Emitting orchestration event [${eventName}] for node/data update`);
    io.emit(eventName, data);
}

// Emit a script test event scoped to a room (so only authorized clients receive it)
// This prevents cross-user data leakage
function emitScriptTestEvent(eventType, executionId, data) {
    if (!io) {
        logger.warn(`Socket.io not initialized, cannot emit script test event`);
        return;
    }
    const room = `scriptTest:${executionId}`;
    const eventName = `${eventType}:${executionId}`;
    logger.debug(`Emitting script test event [${eventName}] to room [${room}]`);
    io.to(room).emit(eventName, data);
}

function getIO() {
    return io;
}


module.exports = { init, emitNotification, emitScheduleEvent, emitOrchestrationEvent, emitScriptTestEvent, getIO };