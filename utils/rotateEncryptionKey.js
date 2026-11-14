const fs = require('fs');
const crypto = require('crypto');
const passman = require('../passman');
const { PASSMAN_PREFIX, writeEncryptionKey } = require('./encryptionKey');

const MIN_KEY_LENGTH = 16;

function plaintextPassword(value, oldSecret) {
  if (typeof value !== 'string' || value.length === 0) return '';
  if (value.startsWith(PASSMAN_PREFIX)) {
    return passman.decrypt(value.slice(PASSMAN_PREFIX.length), oldSecret);
  }
  return value;
}

function configWithEncryptedPasswords(serverConfig, secret, oldSecret) {
  const copy = JSON.parse(JSON.stringify(serverConfig));
  if (copy.smtp && copy.smtp.password) {
    const plain = plaintextPassword(copy.smtp.password, oldSecret);
    copy.smtp.password = plain ? PASSMAN_PREFIX + passman.encrypt(plain, secret) : '';
  }
  if (copy.mqtt && copy.mqtt.password) {
    const plain = plaintextPassword(copy.mqtt.password, oldSecret);
    copy.mqtt.password = plain ? PASSMAN_PREFIX + passman.encrypt(plain, secret) : '';
  }
  return JSON.stringify(copy, null, 2);
}

function assertPasswordsRoundTrip(serverConfig, oldSecret, newSecret) {
  for (const section of ['smtp', 'mqtt']) {
    const value = serverConfig[section] && serverConfig[section].password;
    if (!value) continue;
    const plain = plaintextPassword(value, oldSecret);
    const encrypted = passman.encrypt(plain, newSecret);
    if (passman.decrypt(encrypted, newSecret) !== plain) {
      throw new Error(`Could not re-encrypt the ${section} password`);
    }
  }
}

/**
 * Re-encrypt SMTP, MQTT, and stored agent tokens, then replace the key file.
 * Writes nothing until every current secret decrypts. Restores the previous
 * credentials and config if a later write fails.
 */
async function rotateHubEncryptionKey({
  dataDir,
  configPath,
  serverConfig,
  newSecret,
  currentSecret = passman.getEncryptionSecret(),
  readCredentials,
  writeCredentials,
}) {
  if (process.env.ORCHELIUM_ENCRYPTION_KEY) {
    throw new Error('Unset ORCHELIUM_ENCRYPTION_KEY before changing the key here. While that variable is set, a restart uses it instead of data/encryption-key.');
  }
  if (!currentSecret) throw new Error('The current encryption key is not loaded');
  let nextSecret = typeof newSecret === 'string' ? newSecret.trim() : '';
  let generated = false;
  if (!nextSecret) {
    nextSecret = crypto.randomBytes(32).toString('hex');
    generated = true;
  }
  if (nextSecret.length < MIN_KEY_LENGTH) {
    throw new Error(`Use at least ${MIN_KEY_LENGTH} characters`);
  }
  if (nextSecret === 'CHANGEIT') {
    throw new Error('Choose a key other than CHANGEIT');
  }
  if (nextSecret === currentSecret) {
    throw new Error('That is already the current encryption key');
  }

  assertPasswordsRoundTrip(serverConfig, currentSecret, nextSecret);
  const previousCredentials = await readCredentials();
  const nextCredentials = require('../communications/agentAuth').reencryptCredentialCopy(
    previousCredentials,
    currentSecret,
    nextSecret
  );

  const previousConfig = fs.readFileSync(configPath, 'utf8');
  const nextConfig = configWithEncryptedPasswords(serverConfig, nextSecret, currentSecret);

  try {
    await writeCredentials(nextCredentials);
  } catch (err) {
    throw new Error(`Could not store re-encrypted agent tokens: ${err.message}`);
  }

  try {
    fs.writeFileSync(configPath, nextConfig, 'utf8');
    writeEncryptionKey(dataDir, nextSecret);
  } catch (err) {
    try {
      await writeCredentials(previousCredentials);
      fs.writeFileSync(configPath, previousConfig, 'utf8');
    } catch (rollbackErr) {
      throw new Error(`Encryption key change failed (${err.message}) and rollback failed (${rollbackErr.message})`);
    }
    throw new Error(`Encryption key was not changed: ${err.message}`);
  }

  passman.setEncryptionSecret(nextSecret);
  return { secret: nextSecret, generated };
}

module.exports = { rotateHubEncryptionKey, configWithEncryptedPasswords, MIN_KEY_LENGTH };
