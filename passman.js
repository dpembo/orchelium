const crypto = require('crypto');
const jwt = require('jsonwebtoken');
const { DEFAULT_ENCRYPTION_KEY, padStringTo256Bits } = require('./shared/keyMaterial');
const GCM_PREFIX = 'gcm1:';

let key = null;
let rawSecret = null;

function requireKey() {
  if (!key) {
    throw new Error('Encryption key has not been initialised');
  }
  return key;
}

function setEncryptionSecret(secret) {
  if (typeof secret !== 'string' || !secret.trim()) {
    throw new Error('Encryption secret is required');
  }
  rawSecret = secret.trim();
  key = padStringTo256Bits(rawSecret);
  return key;
}

function getEncryptionSecret() {
  return rawSecret;
}

function usesDefaultEncryptionKey() {
  return rawSecret === DEFAULT_ENCRYPTION_KEY;
}

/**
 * Apply ORCHELIUM_KEY_ENFORCE when the live secret is still CHANGEIT.
 * Returns true when the process should keep running.
 */
function enforceEncryptionKeyPolicy(keyEnforceLevel) {
  const level = keyEnforceLevel || process.env.ORCHELIUM_KEY_ENFORCE || 'warn';
  if (!usesDefaultEncryptionKey()) {
    if (typeof logger !== 'undefined' && logger) {
      logger.info('Hub encryption key loaded');
    }
    return true;
  }

  switch (level) {
    case 'strict':
      if (typeof logger !== 'undefined' && logger) {
        logger.error('═══════════════════════════════════════════════════════════════');
        logger.error('CRITICAL: Default encryption key in use. Server will not start.');
        logger.error('═══════════════════════════════════════════════════════════════');
        logger.error('Change the encryption key in Settings, or set ORCHELIUM_ENCRYPTION_KEY.');
        logger.error('═══════════════════════════════════════════════════════════════');
      }
      process.exit(1);
      return false;
    case 'warn':
      if (typeof logger !== 'undefined' && logger) {
        logger.warn('═══════════════════════════════════════════════════════════════');
        logger.warn('⚠️  WARNING: Using default encryption key (CHANGEIT)');
        logger.warn('═══════════════════════════════════════════════════════════════');
        logger.warn('Stored secrets are encrypted with the published default key.');
        logger.warn('Change the encryption key in Settings when you can.');
        logger.warn('═══════════════════════════════════════════════════════════════');
      }
      break;
    case 'silent':
      if (typeof logger !== 'undefined' && logger) {
        logger.debug('Using default encryption key (not recommended for production)');
      }
      break;
    default:
      if (typeof logger !== 'undefined' && logger) {
        logger.warn('Unknown ORCHELIUM_KEY_ENFORCE value: ' + level);
      }
  }
  return true;
}

// Tests and tools that set ORCHELIUM_ENCRYPTION_KEY before require() keep working.
if (process.env.ORCHELIUM_ENCRYPTION_KEY) {
  setEncryptionSecret(process.env.ORCHELIUM_ENCRYPTION_KEY);
}

function checkKey() {
  requireKey();
}

function createJWTToken(payload, expiryHours, signingSecret) {
  if (typeof logger !== 'undefined' && logger) logger.debug('Generating JWT Auth Token');
  if (expiryHours === undefined || expiryHours == null) expiryHours = 3;
  const signOptions = {
    expiresIn: expiryHours + 'h',
    algorithm: 'HS256',
  };

  const signingKey = signingSecret ? padStringTo256Bits(String(signingSecret)) : requireKey();
  return jwt.sign(payload, signingKey, signOptions);
}

function validateJWTToken(inToken) {
  if (typeof logger !== 'undefined' && logger) logger.debug('Validating JWT Token');
  const verifyKey = requireKey();
  return new Promise((resolve, reject) => {
    jwt.verify(inToken, verifyKey, (err, decoded) => {
      if (err) {
        if (typeof logger !== 'undefined' && logger) logger.error('Validation failed', err);
        reject(err);
      } else {
        if (typeof logger !== 'undefined' && logger) logger.info('Validation Successfull');
        resolve(decoded);
      }
    });
  });
}

function activeKey(secret) {
  if (secret === undefined || secret === null || secret === '') return requireKey();
  return padStringTo256Bits(String(secret));
}

function encrypt(text, secret) {
  const keyBytes = activeKey(secret);
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', keyBytes, iv);
  const encrypted = Buffer.concat([cipher.update(text, 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return GCM_PREFIX + Buffer.concat([iv, tag, encrypted]).toString('base64');
}

function decrypt(text, secret) {
  // gcm1: values are authenticated. Older files stored iv:ciphertext with
  // AES-CBC. Those still decrypt, and nothing rewrites them on startup.
  if (typeof text === 'string' && text.startsWith(GCM_PREFIX)) {
    return decryptGcm(text.slice(GCM_PREFIX.length), secret);
  }
  return decryptLegacyCbc(text, secret);
}

function decryptGcm(payload, secret) {
  const keyBytes = activeKey(secret);
  const buf = Buffer.from(payload, 'base64');
  if (buf.length < 28) {
    throw new Error('Stored secret failed authentication');
  }
  const iv = buf.subarray(0, 12);
  const tag = buf.subarray(12, 28);
  const encrypted = buf.subarray(28);
  const decipher = crypto.createDecipheriv('aes-256-gcm', keyBytes, iv);
  decipher.setAuthTag(tag);
  try {
    return Buffer.concat([decipher.update(encrypted), decipher.final()]).toString('utf8');
  } catch (err) {
    throw new Error('Stored secret failed authentication');
  }
}

function decryptLegacyCbc(text, secret) {
  const keyBytes = activeKey(secret);
  const textParts = text.split(':');
  const iv = Buffer.from(textParts.shift(), 'hex');
  const encryptedText = Buffer.from(textParts.join(':'), 'hex');
  const decipher = crypto.createDecipheriv('aes-256-cbc', keyBytes, iv);
  let decrypted = decipher.update(encryptedText);
  decrypted = Buffer.concat([decrypted, decipher.final()]);
  return decrypted.toString();
}

function deriveKey(password) {
  const salt = Buffer.from('BackupHubSalt');
  const iterations = 100000;
  const keyLength = 32;
  const digest = 'sha256';
  return crypto.pbkdf2Sync(password, salt, iterations, keyLength, digest);
}

module.exports = {
  encrypt,
  decrypt,
  checkKey,
  createJWTToken,
  validateJWTToken,
  setEncryptionSecret,
  getEncryptionSecret,
  usesDefaultEncryptionKey,
  enforceEncryptionKeyPolicy,
};
