const fs = require('fs');
const os = require('os');
const path = require('path');
const passman = require('../../passman');
const { rotateHubEncryptionKey } = require('../../utils/rotateEncryptionKey');
const { FILE_NAME } = require('../../utils/encryptionKey');

describe('rotateHubEncryptionKey', () => {
  let dataDir;
  let configPath;
  let credentials;
  const previousEnv = process.env.ORCHELIUM_ENCRYPTION_KEY;

  beforeEach(() => {
    delete process.env.ORCHELIUM_ENCRYPTION_KEY;
    passman.setEncryptionSecret('CHANGEIT');
    dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'orchelium-rotate-'));
    configPath = path.join(dataDir, 'server-config.json');
    const smtp = passman.encrypt('smtp-secret', 'CHANGEIT');
    const token = passman.encrypt('agent-token', 'CHANGEIT');
    credentials = { hub: { tokenEnc: token, pendingTokenEnc: null, tokenHash: 'hash' } };
    const diskConfig = {
      smtp: { password: `@PASSMAN:${smtp}` },
      mqtt: { password: 'mqtt-secret' },
    };
    fs.writeFileSync(configPath, JSON.stringify(diskConfig, null, 2));
  });

  afterEach(() => {
    fs.rmSync(dataDir, { recursive: true, force: true });
    passman.setEncryptionSecret(previousEnv || 'CHANGEIT');
    if (previousEnv === undefined) delete process.env.ORCHELIUM_ENCRYPTION_KEY;
    else process.env.ORCHELIUM_ENCRYPTION_KEY = previousEnv;
  });

  function memoryConfig() {
    return {
      smtp: { password: 'smtp-secret' },
      mqtt: { password: 'mqtt-secret' },
    };
  }

  test('re-encrypts passwords and agent tokens, then replaces the key file', async () => {
    const config = {
      smtp: { password: 'smtp-secret' },
      mqtt: { password: 'mqtt-secret' },
    };
    const result = await rotateHubEncryptionKey({
      dataDir,
      configPath,
      serverConfig: config,
      newSecret: 'a-new-homelab-key-value',
      readCredentials: async () => credentials,
      writeCredentials: async (next) => { credentials = next; },
    });

    const saved = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    expect(saved.smtp.password.startsWith('@PASSMAN:gcm1:')).toBe(true);
    expect(passman.decrypt(saved.smtp.password.slice('@PASSMAN:'.length), result.secret)).toBe('smtp-secret');
    expect(passman.decrypt(saved.mqtt.password.slice('@PASSMAN:'.length), result.secret)).toBe('mqtt-secret');
    expect(passman.decrypt(credentials.hub.tokenEnc, result.secret)).toBe('agent-token');
    expect(() => passman.decrypt(credentials.hub.tokenEnc, 'CHANGEIT')).toThrow();
    expect(fs.readFileSync(path.join(dataDir, FILE_NAME), 'utf8').trim()).toBe(result.secret);
    expect(passman.getEncryptionSecret()).toBe(result.secret);
    expect(config.smtp.password).toBe('smtp-secret');
    expect(config.mqtt.password).toBe('mqtt-secret');
  });

  test('leaves the old key in place when a stored token cannot be decrypted', async () => {
    credentials.hub.tokenEnc = passman.encrypt('agent-token', 'some-other-key');
    await expect(rotateHubEncryptionKey({
      dataDir,
      configPath,
      serverConfig: memoryConfig(),
      newSecret: 'a-new-homelab-key-value',
      readCredentials: async () => credentials,
      writeCredentials: async () => { throw new Error('should not write'); },
    })).rejects.toThrow('Stored secret failed authentication');
    expect(fs.existsSync(path.join(dataDir, FILE_NAME))).toBe(false);
    expect(passman.getEncryptionSecret()).toBe('CHANGEIT');
  });

  test('refuses to rotate while ORCHELIUM_ENCRYPTION_KEY is set', async () => {
    process.env.ORCHELIUM_ENCRYPTION_KEY = 'from-the-environment';
    await expect(rotateHubEncryptionKey({
      dataDir,
      configPath,
      serverConfig: memoryConfig(),
      newSecret: 'a-new-homelab-key-value',
      readCredentials: async () => credentials,
      writeCredentials: async () => { throw new Error('should not write'); },
    })).rejects.toThrow('ORCHELIUM_ENCRYPTION_KEY');
  });
});
