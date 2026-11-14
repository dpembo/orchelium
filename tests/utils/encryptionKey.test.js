const fs = require('fs');
const os = require('os');
const path = require('path');
const { DEFAULT_ENCRYPTION_KEY } = require('../../shared/keyMaterial');
const {
  FILE_NAME,
  configHasEncryptedSecrets,
  credentialsHaveEncryptedSecrets,
  loadOrCreateEncryptionKey,
} = require('../../utils/encryptionKey');

describe('encrypted data detection', () => {
  test('finds @PASSMAN and gcm values in server config', () => {
    expect(configHasEncryptedSecrets({
      smtp: { password: '@PASSMAN:gcm1:abc' },
    })).toBe(true);
    expect(configHasEncryptedSecrets({
      mqtt: { password: 'plain' },
    })).toBe(false);
  });

  test('finds encrypted agent credential copies', () => {
    expect(credentialsHaveEncryptedSecrets({
      agent: { tokenEnc: 'gcm1:abc', tokenHash: 'x' },
    })).toBe(true);
    expect(credentialsHaveEncryptedSecrets({
      agent: { tokenHash: 'x' },
    })).toBe(false);
  });
});

describe('loadOrCreateEncryptionKey', () => {
  let dataDir;

  beforeEach(() => {
    dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'orchelium-enc-'));
  });

  afterEach(() => {
    fs.rmSync(dataDir, { recursive: true, force: true });
  });

  test('creates a random key when nothing is encrypted', () => {
    const first = loadOrCreateEncryptionKey(dataDir, { hasEncryptedData: false });
    const second = loadOrCreateEncryptionKey(dataDir, { hasEncryptedData: false });

    expect(first.created).toBe(true);
    expect(first.source).toBe('generated');
    expect(first.recommendChange).toBe(false);
    expect(first.secret).toMatch(/^[a-f0-9]{64}$/);
    expect(first.secret).not.toBe(DEFAULT_ENCRYPTION_KEY);
    expect(second).toEqual({
      secret: first.secret,
      created: false,
      source: 'file',
      recommendChange: false,
    });
  });

  test('writes CHANGEIT when ciphertext already exists', () => {
    const created = loadOrCreateEncryptionKey(dataDir, { hasEncryptedData: true });
    expect(created).toEqual({
      secret: DEFAULT_ENCRYPTION_KEY,
      created: true,
      source: 'legacy',
      recommendChange: true,
    });
    expect(fs.readFileSync(path.join(dataDir, FILE_NAME), 'utf8').trim()).toBe(DEFAULT_ENCRYPTION_KEY);
  });

  test('prefers ORCHELIUM_ENCRYPTION_KEY over inventing a value', () => {
    const created = loadOrCreateEncryptionKey(dataDir, {
      hasEncryptedData: true,
      envKey: 'operator-chosen-key',
    });
    expect(created.source).toBe('env');
    expect(created.secret).toBe('operator-chosen-key');
    expect(created.recommendChange).toBe(false);
    expect(fs.readFileSync(path.join(dataDir, FILE_NAME), 'utf8').trim()).toBe('operator-chosen-key');
  });
});
