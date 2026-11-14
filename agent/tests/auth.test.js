const jwt = require('jsonwebtoken');
const { padStringTo256Bits } = require('../../shared/keyMaterial');

function signed(payload, secret) {
  const token = jwt.sign(payload, padStringTo256Bits(secret), { algorithm: 'HS256', expiresIn: '1h' });
  return JSON.stringify(token);
}

describe('command verification', () => {
  let auth;
  let runtime;

  beforeEach(() => {
    jest.resetModules();
    runtime = require('../src/runtime');
    runtime.enckey = padStringTo256Bits('CHANGEIT');
    runtime.AGENT_TOKEN = '';
    auth = require('../src/auth');
  });

  test('an agent with no token accepts the hub key only', async () => {
    await expect(auth.validateJWTToken(signed({ command: 'ping' }, 'CHANGEIT'))).resolves.toMatchObject({ command: 'ping' });
    await expect(auth.validateJWTToken(signed({ command: 'ping' }, 'agent-secret'))).rejects.toThrow();
  });

  test('an agent with a token accepts that token and rejects the hub key', async () => {
    runtime.AGENT_TOKEN = 'agent-secret';
    await expect(auth.validateJWTToken(signed({ command: 'ping' }, 'agent-secret'))).resolves.toMatchObject({ command: 'ping' });
    await expect(auth.validateJWTToken(signed({ command: 'ping' }, 'CHANGEIT'))).rejects.toThrow();
  });

  test('a rotated agent accepts the previous token until a command arrives with the new one', async () => {
    runtime.AGENT_TOKEN = 'next-secret';
    runtime.previousAgentToken = 'current-secret';
    await expect(auth.validateJWTToken(signed({ command: 'ping' }, 'current-secret'))).resolves.toMatchObject({ command: 'ping' });
    expect(runtime.previousAgentToken).toBe('current-secret');
    await expect(auth.validateJWTToken(signed({ command: 'ping' }, 'next-secret'))).resolves.toMatchObject({ command: 'ping' });
    expect(runtime.previousAgentToken).toBe('');
    await expect(auth.validateJWTToken(signed({ command: 'ping' }, 'current-secret'))).rejects.toThrow();
  });
});
