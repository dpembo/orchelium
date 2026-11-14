const fs = require('fs');
const os = require('os');
const path = require('path');
const { loadOrCreateSessionSecret, FILE_NAME } = require('../../utils/sessionSecret');

describe('loadOrCreateSessionSecret', () => {
  let dataDir;

  beforeEach(() => {
    dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'orchelium-session-'));
  });

  afterEach(() => {
    fs.rmSync(dataDir, { recursive: true, force: true });
  });

  test('creates a secret on first startup and reuses it', () => {
    const first = loadOrCreateSessionSecret(dataDir);
    const second = loadOrCreateSessionSecret(dataDir);
    const filePath = path.join(dataDir, FILE_NAME);

    expect(first.created).toBe(true);
    expect(first.secret).toMatch(/^[a-f0-9]{64}$/);
    expect(second).toEqual({ secret: first.secret, created: false });
    expect(fs.readFileSync(filePath, 'utf8').trim()).toBe(first.secret);
    expect(fs.statSync(filePath).mode & 0o777).toBe(0o600);
  });

  test('a second data directory gets its own secret', () => {
    const otherDir = fs.mkdtempSync(path.join(os.tmpdir(), 'orchelium-session-'));
    try {
      const first = loadOrCreateSessionSecret(dataDir);
      const second = loadOrCreateSessionSecret(otherDir);
      expect(second.secret).not.toBe(first.secret);
    } finally {
      fs.rmSync(otherDir, { recursive: true, force: true });
    }
  });

  test('an empty file is replaced', () => {
    fs.writeFileSync(path.join(dataDir, FILE_NAME), '   \n');
    const created = loadOrCreateSessionSecret(dataDir);
    expect(created.created).toBe(true);
    expect(created.secret).toMatch(/^[a-f0-9]{64}$/);
  });
});
