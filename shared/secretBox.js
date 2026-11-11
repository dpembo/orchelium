const crypto = require('crypto');
const { padStringTo256Bits } = require('./keyMaterial');

/**
 * Seal a short secret so only the holder of `secret` can read it.
 * Used to rotate an agent token without putting the new token in the clear
 * on the shared MQTT command topic. This does not use the hub signing key.
 */
function sealSecret(plaintext, secret) {
  const key = padStringTo256Bits(String(secret));
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const encrypted = Buffer.concat([cipher.update(String(plaintext), 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return Buffer.concat([iv, tag, encrypted]).toString('base64');
}

function openSecret(sealed, secret) {
  const buf = Buffer.from(String(sealed), 'base64');
  if (buf.length < 28) {
    throw new Error('Invalid sealed secret');
  }
  const iv = buf.subarray(0, 12);
  const tag = buf.subarray(12, 28);
  const encrypted = buf.subarray(28);
  const key = padStringTo256Bits(String(secret));
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
  decipher.setAuthTag(tag);
  return Buffer.concat([decipher.update(encrypted), decipher.final()]).toString('utf8');
}

module.exports = { sealSecret, openSecret };
