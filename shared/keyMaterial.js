const crypto = require('crypto');

const DEFAULT_ENCRYPTION_KEY = 'CHANGEIT';

/**
 * Derive the 32-byte key both the hub and the agent use for HS256.
 * Strings of 32 bytes or more are hashed. Shorter strings are padded.
 * Callers keep their own logging and default-key policy.
 */
function padStringTo256Bits(inputString) {
  const blockSize = 32;
  const buf = Buffer.from(inputString, 'utf8');

  if (buf.length >= blockSize) {
    return crypto.createHash('sha256').update(buf).digest();
  }

  const paddingLength = blockSize - buf.length;
  const padding = Buffer.alloc(paddingLength, paddingLength);
  return Buffer.concat([buf, padding]);
}

module.exports = {
  DEFAULT_ENCRYPTION_KEY,
  padStringTo256Bits,
};
