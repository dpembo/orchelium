const crypto = require('crypto');
const { padStringTo256Bits } = require('../../shared/keyMaterial');

global.logger = {
  info() {},
  debug() {},
  warn() {},
  error() {},
};

const TEST_KEY = 'passman-test-key';
process.env.ORCHELIUM_ENCRYPTION_KEY = TEST_KEY;
process.env.ORCHELIUM_KEY_ENFORCE = 'silent';

function loadPassman() {
  let passman;
  jest.isolateModules(() => {
    passman = require('../../passman');
  });
  return passman;
}

function legacyEncrypt(plaintext, secret) {
  const key = padStringTo256Bits(secret);
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv('aes-256-cbc', key, iv);
  const encrypted = Buffer.concat([cipher.update(plaintext, 'utf8'), cipher.final()]);
  return `${iv.toString('hex')}:${encrypted.toString('hex')}`;
}

describe('passman secrets', () => {
  afterEach(() => {
    process.env.ORCHELIUM_ENCRYPTION_KEY = TEST_KEY;
  });

  test('new ciphertext is authenticated and rejects tampering', () => {
    const { encrypt, decrypt } = loadPassman();
    const ciphertext = encrypt('smtp-secret');

    expect(ciphertext.startsWith('gcm1:')).toBe(true);
    expect(decrypt(ciphertext)).toBe('smtp-secret');
    expect(decrypt(encrypt('café'))).toBe('café');

    const raw = Buffer.from(ciphertext.slice('gcm1:'.length), 'base64');
    raw[raw.length - 1] ^= 0x01;
    expect(() => decrypt(`gcm1:${raw.toString('base64')}`)).toThrow('Stored secret failed authentication');
  });

  test('a different key cannot read a new secret', () => {
    const ciphertext = loadPassman().encrypt('smtp-secret');
    process.env.ORCHELIUM_ENCRYPTION_KEY = 'another-passman-key';
    expect(() => loadPassman().decrypt(ciphertext)).toThrow('Stored secret failed authentication');
  });

  test('secrets stored with aes-256-cbc still decrypt', () => {
    const { decrypt } = loadPassman();
    const stored = legacyEncrypt('mqtt-secret', TEST_KEY);
    expect(stored.includes(':')).toBe(true);
    expect(stored.startsWith('gcm1:')).toBe(false);
    expect(decrypt(stored)).toBe('mqtt-secret');
  });
});
