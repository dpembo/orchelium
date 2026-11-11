const crypto = require('crypto');
const { sealSecret } = require('../shared/secretBox');

const DB_KEY = 'AGENT_CREDENTIALS';

let store = {};

function hashToken(token) {
  return crypto.createHash('sha256').update(String(token), 'utf8').digest('hex');
}

function safeEqual(left, right) {
  if (!left || !right) return false;
  const a = Buffer.from(left);
  const b = Buffer.from(right);
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

function persist() {
  try {
    const db = require('../db');
    if (!db || typeof db.putData !== 'function') return;
    db.putData(DB_KEY, store).catch((err) => {
      if (typeof logger !== 'undefined' && logger) {
        logger.error(`Failed to store agent credentials: ${err.message}`);
      }
    });
  } catch (err) {
    if (typeof logger !== 'undefined' && logger) {
      logger.error(`Failed to store agent credentials: ${err.message}`);
    }
  }
}

async function init() {
  try {
    const db = require('../db');
    const saved = await db.getData(DB_KEY);
    store = saved && typeof saved === 'object' ? saved : {};
  } catch (err) {
    store = {};
  }
}

function hasCredential(name) {
  const record = store[name];
  return Boolean(record && record.tokenHash);
}

function supportsAgentToken(agent) {
  return Boolean(agent && Array.isArray(agent.capabilities) && agent.capabilities.indexOf('agent-token') !== -1);
}

/**
 * Decide what a new WebSocket may do.
 * A valid token may replace the live socket. Anything else must not.
 * Agents approved before tokens existed may take a free socket, once.
 */
function decideConnection({ agentStatus, hasCredential: credentialPresent, tokenValid, liveSocket, pendingSocket }) {
  if (tokenValid) return 'approve-replace';
  if (credentialPresent) return 'reject';
  if (agentStatus && agentStatus !== 'register') {
    return liveSocket ? 'reject' : 'legacy-accept';
  }
  if (liveSocket || pendingSocket) return 'reject';
  return 'pending-accept';
}

function presentedTokenMatches(name, token) {
  if (!name || !token) return false;
  const record = store[name];
  if (!record) return false;
  const hash = hashToken(token);
  if (record.pendingTokenHash && safeEqual(hash, record.pendingTokenHash)) {
    record.tokenHash = record.pendingTokenHash;
    record.tokenEnc = record.pendingTokenEnc;
    record.pendingTokenHash = null;
    record.pendingTokenEnc = null;
    persist();
    return true;
  }
  return Boolean(record.tokenHash && safeEqual(hash, record.tokenHash));
}

function authorizeMessage(name, token) {
  if (hasCredential(name)) {
    if (presentedTokenMatches(name, token)) return { ok: true };
    return { ok: false, reason: 'connection token rejected' };
  }
  return { ok: true };
}

function issue(name) {
  const token = crypto.randomBytes(32).toString('hex');
  const passman = require('../passman');
  store[name] = {
    tokenHash: hashToken(token),
    tokenEnc: passman.encrypt(token),
    pendingTokenHash: null,
    pendingTokenEnc: null,
  };
  persist();
  return token;
}

function beginRotation(name) {
  const record = store[name];
  if (!record || !record.tokenEnc) {
    throw new Error('Agent has no connection token');
  }
  const passman = require('../passman');
  const current = passman.decrypt(record.tokenEnc);
  const next = crypto.randomBytes(32).toString('hex');
  record.pendingTokenHash = hashToken(next);
  record.pendingTokenEnc = passman.encrypt(next);
  persist();
  return sealSecret(next, current);
}

function cancelRotation(name) {
  const record = store[name];
  if (!record) return;
  record.pendingTokenHash = null;
  record.pendingTokenEnc = null;
  persist();
}

function reveal(name) {
  const record = store[name];
  if (!record || !record.tokenEnc) return null;
  const passman = require('../passman');
  return {
    token: passman.decrypt(record.tokenEnc),
    rotationPending: Boolean(record.pendingTokenHash),
  };
}

function forget(name) {
  if (!store[name]) return;
  delete store[name];
  persist();
}

module.exports = {
  init,
  hasCredential,
  supportsAgentToken,
  decideConnection,
  presentedTokenMatches,
  authorizeMessage,
  issue,
  beginRotation,
  cancelRotation,
  reveal,
  forget,
  hashToken,
};
