const fs = require('fs');
const os = require('os');
const path = require('path');
const { ZipArchive } = require('archiver');
const { PassThrough } = require('stream');
const pluginManager = require('../../pluginManager');

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
  nameBuf.copy(central, 46);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(1, 8);
  end.writeUInt16LE(1, 10);
  end.writeUInt32LE(central.length, 12);
  end.writeUInt32LE(local.length, 16);
  return Buffer.concat([local, central, end]);
}

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
    for (const entry of entries) archive.append(entry.body, { name: entry.name });
    archive.finalize();
  });
}

describe('plugin archive extraction', () => {
  let target;

  beforeEach(() => {
    global.logger = { info() {}, debug() {}, warn() {}, error() {} };
    target = fs.mkdtempSync(path.join(os.tmpdir(), 'orchelium-plugin-'));
  });

  afterEach(() => {
    fs.rmSync(target, { recursive: true, force: true });
  });

  test('writes every file before finishing and marks scripts executable', async () => {
    const zip = await zipWithEntries([
      { name: 'repo/run.sh', body: 'echo hi\n' },
      { name: 'repo/notes.txt', body: 'ok' },
    ]);

    await pluginManager.extractPluginArchive(zip, target);

    expect(fs.readFileSync(path.join(target, 'run.sh'), 'utf8')).toBe('echo hi\n');
    expect(fs.readFileSync(path.join(target, 'notes.txt'), 'utf8')).toBe('ok');
    expect(fs.statSync(path.join(target, 'run.sh')).mode & 0o111).not.toBe(0);
  });

  test('rejects an entry that would leave the plugin directory', async () => {
    const zip = storedZip('repo/../../outside.txt', 'no');

    await expect(pluginManager.extractPluginArchive(zip, target)).rejects.toThrow('unsafe path');
    expect(fs.existsSync(path.join(target, '..', 'outside.txt'))).toBe(false);
  });
});
