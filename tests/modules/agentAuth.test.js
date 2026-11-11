const { sealSecret, openSecret } = require('../../shared/secretBox');
const agentAuth = require('../../communications/agentAuth');

describe('agent connection decisions', () => {
  test('a valid token may replace the live socket', () => {
    expect(agentAuth.decideConnection({
      agentStatus: 'online',
      hasCredential: true,
      tokenValid: true,
      liveSocket: true,
      pendingSocket: false,
    })).toBe('approve-replace');
  });

  test('a wrong token cannot replace or take a credentialed agent', () => {
    expect(agentAuth.decideConnection({
      agentStatus: 'online',
      hasCredential: true,
      tokenValid: false,
      liveSocket: true,
      pendingSocket: false,
    })).toBe('reject');

    expect(agentAuth.decideConnection({
      agentStatus: 'offline',
      hasCredential: true,
      tokenValid: false,
      liveSocket: false,
      pendingSocket: false,
    })).toBe('reject');
  });

  test('a legacy agent can take a free socket but cannot kick a live one', () => {
    expect(agentAuth.decideConnection({
      agentStatus: 'online',
      hasCredential: false,
      tokenValid: false,
      liveSocket: false,
      pendingSocket: false,
    })).toBe('legacy-accept');

    expect(agentAuth.decideConnection({
      agentStatus: 'online',
      hasCredential: false,
      tokenValid: false,
      liveSocket: true,
      pendingSocket: false,
    })).toBe('reject');
  });

  test('a new agent is pending and cannot displace a pending socket', () => {
    expect(agentAuth.decideConnection({
      agentStatus: null,
      hasCredential: false,
      tokenValid: false,
      liveSocket: false,
      pendingSocket: false,
    })).toBe('pending-accept');

    expect(agentAuth.decideConnection({
      agentStatus: 'register',
      hasCredential: false,
      tokenValid: false,
      liveSocket: false,
      pendingSocket: true,
    })).toBe('reject');
  });
});

describe('agent token seal', () => {
  beforeEach(() => {
    global.logger = {
      info: jest.fn(),
      debug: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    };
  });

  test('rotation seal opens only with the current token', () => {
    const current = 'current-token';
    const next = 'next-token';
    const sealed = sealSecret(next, current);
    expect(openSecret(sealed, current)).toBe(next);
    expect(() => openSecret(sealed, 'other-token')).toThrow();
  });

  test('issue and rotate keep the old token valid until the new one is presented', () => {
    const name = `agent-${Date.now()}`;
    const current = agentAuth.issue(name);
    expect(agentAuth.reveal(name).token).toBe(current);
    expect(agentAuth.presentedTokenMatches(name, current)).toBe(true);

    const sealed = agentAuth.beginRotation(name);
    const next = openSecret(sealed, current);
    expect(agentAuth.presentedTokenMatches(name, current)).toBe(true);
    expect(agentAuth.presentedTokenMatches(name, next)).toBe(true);
    expect(agentAuth.presentedTokenMatches(name, current)).toBe(false);
    agentAuth.forget(name);
  });
});
