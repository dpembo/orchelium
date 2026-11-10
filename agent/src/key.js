const { padStringTo256Bits } = require('../../shared/keyMaterial');
const runtime = require('./runtime');
const { debug, DEBUG_LEVEL } = require('./log');

function initializeEncryptionKey() {
  let key = process.env.ORCHELIUM_ENCRYPTION_KEY;
  const keyEnforceLevel = process.env.ORCHELIUM_KEY_ENFORCE || 'warn';

  if (!key || key.length === 0) {
    key = 'CHANGEIT';

    switch (keyEnforceLevel) {
      case 'strict':
        console.error('═══════════════════════════════════════════════════════════════');
        console.error('CRITICAL: Default encryption key in use. Agent will not start.');
        console.error('═══════════════════════════════════════════════════════════════');
        console.error('Set ORCHELIUM_ENCRYPTION_KEY environment variable before starting.');
        console.error('This key must match the key configured on the Server.');
        console.error('═══════════════════════════════════════════════════════════════');
        process.exit(1);
        break;
      case 'warn':
        console.warn('═══════════════════════════════════════════════════════════════');
        console.warn('⚠️  WARNING: Using default encryption key (CHANGEIT)');
        console.warn('═══════════════════════════════════════════════════════════════');
        console.warn('This key should ONLY be used for development/testing.');
        console.warn('For production, set ORCHELIUM_ENCRYPTION_KEY environment variable.');
        console.warn('The same key must be set on the Server for communications.');
        console.warn('═══════════════════════════════════════════════════════════════');
        break;
      case 'silent':
        debug(DEBUG_LEVEL.DEBUG, 'Using default encryption key (not recommended for production)');
        break;
      default:
        console.warn('Unknown ORCHELIUM_KEY_ENFORCE value: ' + keyEnforceLevel);
    }
  } else {
    debug(DEBUG_LEVEL.INFO, 'Custom encryption key loaded from ORCHELIUM_ENCRYPTION_KEY');
  }

  return padStringTo256Bits(key);
}

function loadEncryptionKey() {
  runtime.enckey = initializeEncryptionKey();
  return runtime.enckey;
}

module.exports = { initializeEncryptionKey, loadEncryptionKey };
