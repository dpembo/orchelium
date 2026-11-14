const os = require('os');
const path = require('path');
const fs = require('fs');
const { ZipArchive } = require('archiver');
const { PassThrough } = require('stream');
const backupManager = require('../../backupManager');

function zipWithEntries(entries) {
  return new Promise((resolve, reject) => {
    const archive = new ZipArchive({ zlib: { level: 9 } });
    const chunks = [];
    const sink = new PassThrough();
    sink.on('data', (chunk) => chunks.push(chunk));
    sink.on('end', () => resolve(Buffer.concat(chunks)));
    sink.on('error', reject);
    archive.on('error', reject);
    archive.pipe(sink);
    for (const entry of entries) {
      if (entry.type === 'dir') archive.append('', { name: entry.name });
      else archive.append(entry.body, { name: entry.name });
    }
    archive.finalize();
  });
}

function storedZip(name, body) {
  const zlib = require('zlib');
  const data = Buffer.from(body);
  const nameBuf = Buffer.from(name);
  const crc = zlib.crc32(data) >>> 0;
  const local = Buffer.alloc(30 + nameBuf.length + data.length);
  local.writeUInt32LE(0x04034b50, 0);
  local.writeUInt16LE(20, 4);
  local.writeUInt32LE(crc, 14);
  local.writeUInt32LE(data.length, 18);
  local.writeUInt32LE(data.length, 22);
  local.writeUInt16LE(nameBuf.length, 26);
  nameBuf.copy(local, 30);
  data.copy(local, 30 + nameBuf.length);
  const central = Buffer.alloc(46 + nameBuf.length);
  central.writeUInt32LE(0x02014b50, 0);
  central.writeUInt16LE(20, 4);
  central.writeUInt16LE(20, 6);
  central.writeUInt32LE(crc, 16);
  central.writeUInt32LE(data.length, 20);
  central.writeUInt32LE(data.length, 24);
  central.writeUInt16LE(nameBuf.length, 28);
  central.writeUInt32LE(0, 42);
  nameBuf.copy(central, 46);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(1, 8);
  end.writeUInt16LE(1, 10);
  end.writeUInt32LE(central.length, 12);
  end.writeUInt32LE(local.length, 16);
  return Buffer.concat([local, central, end]);
}

describe('safeZipDestination', () => {
  const root = path.resolve('/tmp/orchelium-restore');

  test('keeps entries inside the restore directory', () => {
    expect(backupManager.safeZipDestination(root, 'server-config.json')).toBe(path.join(root, 'server-config.json'));
    expect(backupManager.safeZipDestination(root, 'nested/jobs.json')).toBe(path.join(root, 'nested', 'jobs.json'));
  });

  test('rejects traversal and absolute paths', () => {
    expect(backupManager.safeZipDestination(root, '../server-config.json')).toBeNull();
    expect(backupManager.safeZipDestination(root, 'nested/../../data.db')).toBeNull();
    expect(backupManager.safeZipDestination(root, '/etc/passwd')).toBeNull();
  });
});

describe('backup zip extraction', () => {
  let target;

  beforeEach(() => {
    global.logger = { info() {}, debug() {}, warn() {}, error() {} };
    target = fs.mkdtempSync(path.join(os.tmpdir(), 'orchelium-zip-'));
  });

  afterEach(() => {
    fs.rmSync(target, { recursive: true, force: true });
  });

  test('writes a normal entry and refuses a traversal entry', async () => {
    const good = await zipWithEntries([{ name: 'notes.txt', body: 'ok' }]);
    await backupManager.extractZip(good, target);
    expect(fs.readFileSync(path.join(target, 'notes.txt'), 'utf8')).toBe('ok');

    const badDir = fs.mkdtempSync(path.join(os.tmpdir(), 'orchelium-zip-bad-'));
    try {
      const bad = storedZip('../outside.txt', 'no');
      await expect(backupManager.extractZip(bad, badDir)).rejects.toThrow('unsafe path');
      expect(fs.existsSync(path.join(path.dirname(badDir), 'outside.txt'))).toBe(false);
    } finally {
      fs.rmSync(badDir, { recursive: true, force: true });
    }
  });
});
