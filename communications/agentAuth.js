const crypto = require('crypto');
const { sealSecret } = require('../shared/secretBox');

const DB_KEY = 'AGENT_CREDENTIALS';
const INSTALL_TOKENS = '_installTokens';

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

let writeChain = Promise.resolve();

function logCredentialError(err) {
  if (typeof logger !== 'undefined' && logger) {
    logger.error(`Failed to store agent credentials: ${err.message}`);
  }
}

function rememberRecord(name) {
  if (!Object.prototype.hasOwnProperty.call(store, name)) return { existed: false };
  return { existed: true, record: cloneStore(store[name]) };
}

function restoreRecord(name, previous) {
  if (!previous.existed) delete store[name];
  else store[name] = previous.record;
}

/**
 * Write the credential map after earlier writes. The snapshot is taken when
 * this write runs, so a later change is not overwritten by an older one.
 */
function persist() {
  const run = writeChain.then(async () => {
    const db = require('../db');
    if (!db || typeof db.putData !== 'function') return;
    await db.putData(DB_KEY, cloneStore(store));
  });
  writeChain = run.then(() => {}, () => {});
  return run;
}

async function commitRecord(name, previous) {
  try {
    await persist();
  } catch (err) {
    restoreRecord(name, previous);
    logCredentialError(err);
    throw err;
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
 * A valid token may replace the live socket. A connection without one is refused.
 */
function decideConnection({ tokenValid }) {
  if (tokenValid) return 'approve-replace';
  return 'reject';
}

function presentedTokenMatches(name, token) {
  if (!name || !token) return false;
  const record = store[name];
  if (!record) return false;
  const hash = hashToken(token);
  if (record.pendingTokenHash && safeEqual(hash, record.pendingTokenHash)) {
    const previous = rememberRecord(name);
    record.tokenHash = record.pendingTokenHash;
    record.tokenEnc = record.pendingTokenEnc;
    record.pendingTokenHash = null;
    record.pendingTokenEnc = null;
    persist().catch((err) => {
      restoreRecord(name, previous);
      logCredentialError(err);
    });
    return true;
  }
  return Boolean(record.tokenHash && safeEqual(hash, record.tokenHash));
}

function authorizeMessage(name, token) {
  if (hasCredential(name)) {
    if (presentedTokenMatches(name, token)) return { ok: true };
    return { ok: false, reason: 'connection token rejected' };
  }
  let agent = null;
  try {
    if (typeof agents !== 'undefined' && agents && typeof agents.getAgent === 'function') {
      agent = agents.getAgent(name);
    }
  } catch (err) {
    agent = null;
  }
  if (agent && agent.status && agent.status !== 'register') {
    return { ok: false, reason: 'agent has no connection token' };
  }
  return { ok: false, reason: 'connection token required' };
}

function installTokenList() {
  if (!Array.isArray(store[INSTALL_TOKENS])) store[INSTALL_TOKENS] = [];
  return store[INSTALL_TOKENS];
}

async function issueInstallToken() {
  const token = crypto.randomBytes(32).toString('hex');
  const passman = require('../passman');
  const previous = cloneStore(installTokenList());
  installTokenList().push({
    tokenHash: hashToken(token),
    tokenEnc: passman.encrypt(token),
  });
  try {
    await persist();
  } catch (err) {
    store[INSTALL_TOKENS] = previous;
    logCredentialError(err);
    throw err;
  }
  return token;
}

async function claimInstallToken(name, token) {
  if (!name || !token || name === INSTALL_TOKENS) return false;
  if (presentedTokenMatches(name, token)) return true;
  const hash = hashToken(token);
  const list = installTokenList();
  const index = list.findIndex((record) => record && safeEqual(hash, record.tokenHash));
  if (index === -1 || hasCredential(name)) return false;
  const previousList = cloneStore(list);
  const previousAgent = rememberRecord(name);
  const record = list[index];
  list.splice(index, 1);
  store[name] = {
    tokenHash: record.tokenHash,
    tokenEnc: record.tokenEnc,
    pendingTokenHash: null,
    pendingTokenEnc: null,
  };
  try {
    await persist();
  } catch (err) {
    store[INSTALL_TOKENS] = previousList;
    restoreRecord(name, previousAgent);
    logCredentialError(err);
    return false;
  }
  return true;
}

async function issue(name) {
  const token = crypto.randomBytes(32).toString('hex');
  const passman = require('../passman');
  const previous = rememberRecord(name);
  store[name] = {
    tokenHash: hashToken(token),
    tokenEnc: passman.encrypt(token),
    pendingTokenHash: null,
    pendingTokenEnc: null,
  };
  await commitRecord(name, previous);
  return token;
}

async function beginRotation(name) {
  const record = store[name];
  if (!record || !record.tokenEnc) {
    throw new Error('Agent has no connection token');
  }
  const passman = require('../passman');
  const current = passman.decrypt(record.tokenEnc);
  const next = crypto.randomBytes(32).toString('hex');
  const previous = rememberRecord(name);
  record.pendingTokenHash = hashToken(next);
  record.pendingTokenEnc = passman.encrypt(next);
  await commitRecord(name, previous);
  return sealSecret(next, current);
}

async function cancelRotation(name) {
  const record = store[name];
  if (!record) return;
  const previous = rememberRecord(name);
  record.pendingTokenHash = null;
  record.pendingTokenEnc = null;
  await commitRecord(name, previous);
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

function cloneStore(source) {
  return JSON.parse(JSON.stringify(source));
}

function reencryptRecord(record, oldSecret, newSecret, passman) {
  if (!record || typeof record !== 'object') return;
  if (Array.isArray(record)) {
    record.forEach((item) => reencryptRecord(item, oldSecret, newSecret, passman));
    return;
  }
  if (record.tokenEnc) {
    record.tokenEnc = passman.encrypt(passman.decrypt(record.tokenEnc, oldSecret), newSecret);
  }
  if (record.pendingTokenEnc) {
    record.pendingTokenEnc = passman.encrypt(passman.decrypt(record.pendingTokenEnc, oldSecret), newSecret);
  }
}

function reencryptCredentialCopy(source, oldSecret, newSecret) {
  const passman = require('../passman');
  const next = cloneStore(source);
  for (const record of Object.values(next)) {
    reencryptRecord(record, oldSecret, newSecret, passman);
  }
  return next;
}

async function replaceCredentialStore(next) {
  const previous = cloneStore(store);
  store = next;
  try {
    await persist();
  } catch (err) {
    store = previous;
    logCredentialError(err);
    throw err;
  }
  return previous;
}

function currentCredentialStore() {
  return cloneStore(store);
}

async function forget(name) {
  if (!name || name === INSTALL_TOKENS || !store[name]) return;
  const previous = rememberRecord(name);
  delete store[name];
  await commitRecord(name, previous);
}

function noteCommandKey(name) {
  const record = store[name];
  if (!record || record.commandKey) return;
  record.commandKey = true;
  persist().catch((err) => {
    if (store[name] === record) record.commandKey = false;
    logCredentialError(err);
  });
}

/**
 * Sign a hub-to-agent command. An agent that has reported the command-key
 * capability is signed with its credential. Enrollment, older agents, and
 * agents with no credential use the hub key. MQTT and WebSocket both send
 * the result of this function.
 */
function signCommand(payload, expiryHours) {
  const passman = require('../passman');
  const name = payload && payload.name;
  const record = name && store[name];
  if (record && record.commandKey && record.tokenEnc) {
    const current = reveal(name);
    if (!current || !current.token) {
      throw new Error(`Agent ${name} has a credential that cannot sign commands`);
    }
    return passman.createJWTToken(payload, expiryHours, current.token);
  }
  return passman.createJWTToken(payload, expiryHours);
}

module.exports = {
  init,
  hasCredential,
  supportsAgentToken,
  decideConnection,
  presentedTokenMatches,
  authorizeMessage,
  issueInstallToken,
  claimInstallToken,
  issue,
  beginRotation,
  cancelRotation,
  reveal,
  forget,
  noteCommandKey,
  signCommand,
  currentCredentialStore,
  reencryptCredentialCopy,
  replaceCredentialStore,
  hashToken,
};
