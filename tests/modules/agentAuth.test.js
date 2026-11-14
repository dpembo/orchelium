process.env.ORCHELIUM_ENCRYPTION_KEY = 'agent-auth-test-key';
process.env.ORCHELIUM_KEY_ENFORCE = 'silent';

const jwt = require('jsonwebtoken');
const { sealSecret, openSecret } = require('../../shared/secretBox');
const { padStringTo256Bits } = require('../../shared/keyMaterial');
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

  test('an approved agent without a token is rejected', () => {
    expect(agentAuth.decideConnection({
      agentStatus: 'online',
      hasCredential: false,
      tokenValid: false,
      liveSocket: false,
      pendingSocket: false,
    })).toBe('reject');

    expect(agentAuth.decideConnection({
      agentStatus: 'offline',
      hasCredential: false,
      tokenValid: false,
      liveSocket: false,
      pendingSocket: false,
    })).toBe('reject');
  });

  test('an approved agent without a token cannot submit a message', () => {
    global.agents = { getAgent: () => ({ name: 'legacy', status: 'online' }) };
    expect(agentAuth.authorizeMessage('legacy', null)).toEqual({
      ok: false,
      reason: 'agent has no connection token',
    });
    delete global.agents;
  });

  test('an agent without a token is rejected', () => {
    expect(agentAuth.decideConnection({
      agentStatus: null,
      hasCredential: false,
      tokenValid: false,
      liveSocket: false,
      pendingSocket: false,
    })).toBe('reject');

    expect(agentAuth.authorizeMessage('new-agent', null)).toEqual({
      ok: false,
      reason: 'connection token required',
    });

    expect(agentAuth.decideConnection({
      agentStatus: 'register',
      hasCredential: false,
      tokenValid: false,
      liveSocket: false,
      pendingSocket: true,
    })).toBe('reject');
  });

  test('a valid token stays pending until the agent is approved', () => {
    expect(agentAuth.decideConnection({
      agentStatus: null,
      tokenValid: true,
    })).toBe('pending-accept');

    expect(agentAuth.decideConnection({
      agentStatus: 'register',
      tokenValid: true,
    })).toBe('pending-accept');

    expect(agentAuth.decideConnection({
      agentStatus: 'online',
      tokenValid: true,
    })).toBe('approve-replace');
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
    const db = require('../../db');
    jest.spyOn(db, 'putData').mockResolvedValue(undefined);
  });

  afterEach(() => {
    const db = require('../../db');
    if (db.putData.mockRestore) db.putData.mockRestore();
  });

  test('rotation seal opens only with the current token', () => {
    const current = 'current-token';
    const next = 'next-token';
    const sealed = sealSecret(next, current);
    expect(openSecret(sealed, current)).toBe(next);
    expect(() => openSecret(sealed, 'other-token')).toThrow();
  });

  test('an install key can be claimed once, under the agent name that presents it', async () => {
    const token = await agentAuth.issueInstallToken();
    const name = `install-${Date.now()}`;
    expect(await agentAuth.claimInstallToken(name, token)).toBe(true);
    expect(agentAuth.presentedTokenMatches(name, token)).toBe(true);
    expect(await agentAuth.claimInstallToken(`other-${Date.now()}`, token)).toBe(false);
    await agentAuth.forget(name);
  });

  test('issue and rotate keep the old token valid until the new one is presented', async () => {
    const name = `agent-${Date.now()}`;
    const current = await agentAuth.issue(name);
    expect(agentAuth.reveal(name).token).toBe(current);
    expect(agentAuth.presentedTokenMatches(name, current)).toBe(true);

    const sealed = await agentAuth.beginRotation(name);
    const next = openSecret(sealed, current);
    expect(agentAuth.presentedTokenMatches(name, current)).toBe(true);
    expect(agentAuth.presentedTokenMatches(name, next)).toBe(true);
    expect(agentAuth.presentedTokenMatches(name, current)).toBe(false);
    await agentAuth.forget(name);
  });

  test('a failed token write is rolled back and reported', async () => {
    const db = require('../../db');
    const name = `rollback-${Date.now()}`;
    db.putData.mockRejectedValueOnce(new Error('disk full'));
    await expect(agentAuth.issue(name)).rejects.toThrow('disk full');
    expect(agentAuth.hasCredential(name)).toBe(false);
  });

  test('a credential signs commands for that agent only, on either transport', async () => {
    const name = `signer-${Date.now()}`;
    const token = await agentAuth.issue(name);
    const payload = { name, command: 'ping' };
    const hubKey = padStringTo256Bits('agent-auth-test-key');
    const beforeCapability = agentAuth.signCommand(payload, 1);
    expect(jwt.verify(beforeCapability, hubKey, { algorithms: ['HS256'] })).toMatchObject(payload);

    agentAuth.noteCommandKey(name);
    const signed = agentAuth.signCommand(payload, 1);
    const agentKey = padStringTo256Bits(token);

    expect(jwt.verify(signed, agentKey, { algorithms: ['HS256'] })).toMatchObject(payload);
    expect(() => jwt.verify(signed, hubKey, { algorithms: ['HS256'] })).toThrow();

    const legacy = agentAuth.signCommand({ name: 'legacy-agent', command: 'ping' }, 1);
    expect(jwt.verify(legacy, hubKey, { algorithms: ['HS256'] })).toMatchObject({ name: 'legacy-agent', command: 'ping' });
    expect(() => jwt.verify(legacy, agentKey, { algorithms: ['HS256'] })).toThrow();
    await agentAuth.forget(name);
  });

  test('rotation keeps signing with the current token until the agent presents the new one', async () => {
    const name = `rotate-sign-${Date.now()}`;
    const current = await agentAuth.issue(name);
    agentAuth.noteCommandKey(name);
    const next = openSecret(await agentAuth.beginRotation(name), current);
    const during = agentAuth.signCommand({ name, command: 'ping' }, 1);

    expect(jwt.verify(during, padStringTo256Bits(current), { algorithms: ['HS256'] }).command).toBe('ping');
    expect(() => jwt.verify(during, padStringTo256Bits(next), { algorithms: ['HS256'] })).toThrow();

    agentAuth.presentedTokenMatches(name, next);
    const after = agentAuth.signCommand({ name, command: 'ping' }, 1);
    expect(jwt.verify(after, padStringTo256Bits(next), { algorithms: ['HS256'] }).command).toBe('ping');
    await agentAuth.forget(name);
  });
});
