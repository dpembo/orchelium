const fs = require('fs');
const path = require('path');
const { normalizeScriptFilename, resolveScriptPath } = require('../../utils/scriptFilename');

describe('normalizeScriptFilename', () => {
  it('keeps a valid script name and adds a missing .sh suffix', () => {
    expect(normalizeScriptFilename('backup')).toBe('backup.sh');
    expect(normalizeScriptFilename(' backup.sh ')).toBe('backup.sh');
    expect(normalizeScriptFilename('nightly_job-1.sh')).toBe('nightly_job-1.sh');
  });

  it('rejects empty names, non-strings, and path characters', () => {
    expect(normalizeScriptFilename('')).toBe('');
    expect(normalizeScriptFilename('   ')).toBe('');
    expect(normalizeScriptFilename(null)).toBe('');
    expect(normalizeScriptFilename('../etc/passwd')).toBe('');
    expect(normalizeScriptFilename('scripts/backup.sh')).toBe('');
    expect(normalizeScriptFilename('bad name.sh')).toBe('');
  });
});

describe('resolveScriptPath', () => {
  const scriptsDir = path.resolve('/tmp/orchelium-scripts-root/scripts');

  it('resolves names that stay inside scripts/', () => {
    expect(resolveScriptPath('backup.sh', scriptsDir)).toBe(path.join(scriptsDir, 'backup.sh'));
    expect(resolveScriptPath('./nested/job.py', scriptsDir)).toBe(path.join(scriptsDir, 'nested', 'job.py'));
    expect(resolveScriptPath('  nightly.sh  ', scriptsDir)).toBe(path.join(scriptsDir, 'nightly.sh'));
    expect(resolveScriptPath('..hidden.sh', scriptsDir)).toBe(path.join(scriptsDir, '..hidden.sh'));
  });

  it('rejects traversal, absolute paths, and the scripts directory itself', () => {
    expect(resolveScriptPath('../data/server-config.json', scriptsDir)).toBeNull();
    expect(resolveScriptPath('foo/../../data/server-config.json', scriptsDir)).toBeNull();
    expect(resolveScriptPath('../scripts-evil/secret.json', scriptsDir)).toBeNull();
    expect(resolveScriptPath('/etc/passwd', scriptsDir)).toBeNull();
    expect(resolveScriptPath('..', scriptsDir)).toBeNull();
    expect(resolveScriptPath('.', scriptsDir)).toBeNull();
    expect(resolveScriptPath('', scriptsDir)).toBeNull();
    expect(resolveScriptPath('   ', scriptsDir)).toBeNull();
    expect(resolveScriptPath('backup.sh\0../data/server-config.json', scriptsDir)).toBeNull();
    expect(resolveScriptPath(null, scriptsDir)).toBeNull();
  });

  it('defaults to the application scripts directory', () => {
    expect(resolveScriptPath('backup.sh')).toBe(path.resolve(__dirname, '../../scripts/backup.sh'));
  });
});

describe('POST /saveScript is authenticated', () => {
  const root = path.join(__dirname, '../..');

  it('requires a session, a script permission, and a CSRF token', () => {
    const server = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
    const scripts = fs.readFileSync(path.join(root, 'routes/scripts.js'), 'utf8');
    const editor = fs.readFileSync(path.join(root, 'views/scripteditor.ejs'), 'utf8');
    const publicRoutesLine = server.split('\n').find((line) => line.includes('const publicRoutes'));
    const routeStart = scripts.indexOf("app.post('/saveScript'");
    const route = scripts.slice(routeStart, scripts.indexOf("app.get('/rest/script/:script'", routeStart));

    expect(publicRoutesLine).not.toContain('saveScript');
    expect(route).toContain('validateCsrf');
    expect(route).toContain('User.isAuthenticated');
    expect(route).toContain('PERMISSIONS.SCRIPTS_CREATE');
    expect(route).toContain('PERMISSIONS.SCRIPTS_EDIT');
    expect(route).toContain('normalizeScriptFilename');
    expect(editor).toContain("xhr.setRequestHeader('X-CSRF-Token', scriptEditorCsrf)");
    expect(editor).toContain("payload.status === 'success'");
  });
});
