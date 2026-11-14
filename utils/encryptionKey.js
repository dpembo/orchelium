const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { DEFAULT_ENCRYPTION_KEY } = require('../shared/keyMaterial');

const FILE_NAME = 'encryption-key';
const PASSMAN_PREFIX = '@PASSMAN:';

function readSecret(filePath) {
  try {
    return fs.readFileSync(filePath, 'utf8').trim();
  } catch (err) {
    if (err.code === 'ENOENT') return '';
    throw err;
  }
}

function writeSecret(filePath, secret, flag) {
  fs.writeFileSync(filePath, `${secret}\n`, { encoding: 'utf8', mode: 0o600, flag });
  fs.chmodSync(filePath, 0o600);
}

function valueLooksEncrypted(value) {
  if (typeof value !== 'string' || !value) return false;
  if (value.startsWith(PASSMAN_PREFIX)) return true;
  if (value.startsWith('gcm1:')) return true;
  // Legacy AES-CBC form: hexIv:hexCiphertext
  return /^[0-9a-fA-F]+:[0-9a-fA-F]+$/.test(value);
}

function configHasEncryptedSecrets(config) {
  if (!config || typeof config !== 'object') return false;
  const smtp = config.smtp && config.smtp.password;
  const mqtt = config.mqtt && config.mqtt.password;
  return valueLooksEncrypted(smtp) || valueLooksEncrypted(mqtt);
}

function credentialsHaveEncryptedSecrets(store) {
  if (!store || typeof store !== 'object') return false;
  for (const record of Object.values(store)) {
    if (!record || typeof record !== 'object') continue;
    if (valueLooksEncrypted(record.tokenEnc) || valueLooksEncrypted(record.pendingTokenEnc)) {
      return true;
    }
  }
  return false;
}

/**
 * Load the hub data-encryption secret.
 * Preference order: ORCHELIUM_ENCRYPTION_KEY, existing data/encryption-key,
 * then CHANGEIT when ciphertext already exists, otherwise a random value.
 */
function loadOrCreateEncryptionKey(dataDir, options = {}) {
  if (!dataDir) throw new Error('Encryption key directory is required');
  const hasEncryptedData = Boolean(options.hasEncryptedData);
  const envKey = typeof options.envKey === 'string' ? options.envKey.trim() : '';

  fs.mkdirSync(dataDir, { recursive: true });
  const filePath = path.join(dataDir, FILE_NAME);
  const existing = readSecret(filePath);

  if (envKey) {
    if (!existing) {
      try {
        writeSecret(filePath, envKey, 'wx');
      } catch (err) {
        if (err.code !== 'EEXIST') throw err;
      }
    }
    return {
      secret: envKey,
      created: !existing,
      source: 'env',
      recommendChange: envKey === DEFAULT_ENCRYPTION_KEY,
    };
  }

  if (existing) {
    return {
      secret: existing,
      created: false,
      source: 'file',
      recommendChange: existing === DEFAULT_ENCRYPTION_KEY,
    };
  }

  const secret = hasEncryptedData
    ? DEFAULT_ENCRYPTION_KEY
    : crypto.randomBytes(32).toString('hex');
  const source = hasEncryptedData ? 'legacy' : 'generated';

  try {
    writeSecret(filePath, secret, 'wx');
    return {
      secret,
      created: true,
      source,
      recommendChange: secret === DEFAULT_ENCRYPTION_KEY,
    };
  } catch (err) {
    if (err.code !== 'EEXIST') throw err;
  }

  const raced = readSecret(filePath);
  if (raced) {
    return {
      secret: raced,
      created: false,
      source: 'file',
      recommendChange: raced === DEFAULT_ENCRYPTION_KEY,
    };
  }
  writeSecret(filePath, secret, 'w');
  return {
    secret,
    created: true,
    source,
    recommendChange: secret === DEFAULT_ENCRYPTION_KEY,
  };
}

function writeEncryptionKey(dataDir, secret) {
  if (!dataDir) throw new Error('Encryption key directory is required');
  if (typeof secret !== 'string' || !secret.trim()) throw new Error('Encryption key is required');
  fs.mkdirSync(dataDir, { recursive: true });
  writeSecret(path.join(dataDir, FILE_NAME), secret.trim(), 'w');
}

module.exports = {
  FILE_NAME,
  PASSMAN_PREFIX,
  configHasEncryptedSecrets,
  credentialsHaveEncryptedSecrets,
  loadOrCreateEncryptionKey,
  valueLooksEncrypted,
  writeEncryptionKey,
};
