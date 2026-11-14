const fs = require('fs');
const os = require('os');
const path = require('path');
const jwt = require('jsonwebtoken');

function enrollmentJwt(payload, secret) {
  return jwt.sign(payload, secret, { algorithm: 'HS256', expiresIn: '1h' });
}

describe('removal from the hub', () => {
  let directory;
  let previousDirectory;
  let runtime;
  let markRemovedFromHub;

  beforeEach(() => {
    jest.resetModules();
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'orch-removed-'));
    previousDirectory = process.cwd();
    process.chdir(directory);
    runtime = require('../src/runtime');
    runtime.AGENT_TOKEN = 'abc123token';
    runtime.wsClient = { disconnect: jest.fn() };
    runtime.mqttClient = { disconnect: jest.fn() };
    ({ markRemovedFromHub } = require('../src/commands'));
  });

  afterEach(() => {
    process.chdir(previousDirectory);
    fs.rmSync(directory, { recursive: true, force: true });
  });

  test('clears the connection key and stops both transports', () => {
    markRemovedFromHub();
    markRemovedFromHub();

    expect(runtime.removedByHub).toBe(true);
    expect(runtime.AGENT_TOKEN).toBe('');
    const settings = fs.readFileSync(path.join(directory, 'settings.sh'), 'utf8');
    expect(settings).toContain('AGENT_TOKEN=""');
    expect(settings).toContain('AGENT_REMOVED="true"');
    expect(runtime.wsClient.disconnect).toHaveBeenCalledTimes(1);
    expect(runtime.mqttClient.disconnect).toHaveBeenCalledTimes(1);
  });
});

describe('first enrollment on the agent socket', () => {
  let directory;
  let previousDirectory;
  let runtime;
  let acceptFirstEnrollment;

  beforeEach(() => {
    jest.resetModules();
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'orch-enroll-'));
    previousDirectory = process.cwd();
    process.chdir(directory);
    runtime = require('../src/runtime');
    runtime.AGENT_ID = 'test';
    runtime.AGENT_TOKEN = '';
    runtime.wsClient = { reconnectNow: jest.fn(() => Promise.resolve()) };
    ({ acceptFirstEnrollment } = require('../src/commands'));
  });

  afterEach(() => {
    process.chdir(previousDirectory);
    fs.rmSync(directory, { recursive: true, force: true });
  });

  test('stores a token signed with a key the agent does not have', () => {
    const message = enrollmentJwt(
      { name: 'test', command: 'enroll', commandParams: 'abc123token' },
      'hub-only-key'
    );

    expect(acceptFirstEnrollment(message)).toBe(true);
    expect(runtime.AGENT_TOKEN).toBe('abc123token');
    expect(fs.readFileSync(path.join(directory, 'settings.sh'), 'utf8')).toContain('AGENT_TOKEN="abc123token"');
    expect(runtime.wsClient.reconnectNow).toHaveBeenCalled();
  });

  test('ignores enrollment for another agent, a signed command, and a second token', () => {
    const otherAgent = enrollmentJwt(
      { name: 'other', command: 'enroll', commandParams: 'stolen' },
      'hub-only-key'
    );
    const ping = enrollmentJwt({ name: 'test', command: 'ping' }, 'hub-only-key');

    expect(acceptFirstEnrollment(otherAgent)).toBe(false);
    expect(acceptFirstEnrollment(ping)).toBe(false);
    expect(runtime.AGENT_TOKEN).toBe('');

    runtime.AGENT_TOKEN = 'already-set';
    const again = enrollmentJwt(
      { name: 'test', command: 'enroll', commandParams: 'replacement' },
      'hub-only-key'
    );
    expect(acceptFirstEnrollment(again)).toBe(false);
    expect(runtime.AGENT_TOKEN).toBe('already-set');
    expect(fs.existsSync(path.join(directory, 'settings.sh'))).toBe(false);
  });
});
