const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const FILE_NAME = 'session-secret';

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

/**
 * Load the Express session signing secret, creating one on first startup.
 * The file lives in the data directory so it survives a restart and is not
 * the hub encryption key. A new secret invalidates existing browser sessions.
 */
function loadOrCreateSessionSecret(dataDir) {
  if (!dataDir) throw new Error('Session secret directory is required');
  fs.mkdirSync(dataDir, { recursive: true });
  const filePath = path.join(dataDir, FILE_NAME);
  const existing = readSecret(filePath);
  if (existing) return { secret: existing, created: false };

  const secret = crypto.randomBytes(32).toString('hex');
  try {
    writeSecret(filePath, secret, 'wx');
    return { secret, created: true };
  } catch (err) {
    if (err.code !== 'EEXIST') throw err;
  }

  const raced = readSecret(filePath);
  if (raced) return { secret: raced, created: false };
  writeSecret(filePath, secret, 'w');
  return { secret, created: true };
}

module.exports = { loadOrCreateSessionSecret, FILE_NAME };
