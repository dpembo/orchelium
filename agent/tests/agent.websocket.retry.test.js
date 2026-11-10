const net = require('net');
const { WebSocketHandler } = require('../src/transports/websocket');

describe('WebSocketHandler reconnect', () => {
  let server;
  let port;
  const sockets = new Set();

  beforeAll((done) => {
    server = net.createServer((socket) => {
      sockets.add(socket);
      socket.on('close', () => sockets.delete(socket));
    });
    server.listen(0, '127.0.0.1', () => {
      port = server.address().port;
      done();
    });
  });

  afterAll((done) => {
    for (const socket of sockets) socket.destroy();
    server.close(done);
  });

  it('schedules another attempt when the handshake times out', async () => {
    const handler = new WebSocketHandler('127.0.0.1', port, 'agent_test', () => {}, {
      connectionTimeout: 200,
    });

    await expect(handler.connect()).rejects.toBeTruthy();
    await new Promise((resolve) => setTimeout(resolve, 300));

    expect(handler.isReconnecting).toBe(true);
    handler.disconnect();
  });
});
