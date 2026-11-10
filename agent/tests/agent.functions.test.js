// Tests for agent utility behavior.
// Pure contract helpers come from shared/. Logic that still lives inline in the
// agent modules is replicated here until those modules are required directly.

jest.mock('fs');
jest.mock('child_process');
jest.mock('os');

const fs = require('fs');
const os = require('os');
const crypto = require('crypto');
const { padStringTo256Bits } = require('../../shared/keyMaterial');
const { returnCodeFromExit: getReturnCodeFromExit, TOPICS } = require('../../shared/protocol');

// ─── getReturnCodeFromExit ────────────────────────────────────────────────────

describe('shared topics', () => {
  it('uses the orchelium command and status topics', () => {
    expect(TOPICS.command).toBe('orchelium/agent/command');
    expect(TOPICS.status).toBe('orchelium/agent/status');
  });
});

describe('getReturnCodeFromExit', () => {
  it('should return 999 for SIGKILL', () => {
    expect(getReturnCodeFromExit(null, 'SIGKILL')).toBe(999);
  });

  it('should return 998 for SIGTERM', () => {
    expect(getReturnCodeFromExit(null, 'SIGTERM')).toBe(998);
  });

  it('should return 99999 when code is null and signal is null', () => {
    expect(getReturnCodeFromExit(null, null)).toBe(99999);
  });

  it('should return 99999 when code and signal are both undefined', () => {
    expect(getReturnCodeFromExit(undefined, undefined)).toBe(99999);
  });

  it('should return 0 for a clean exit', () => {
    expect(getReturnCodeFromExit(0, null)).toBe(0);
  });

  it('should return 1 for a general failure exit', () => {
    expect(getReturnCodeFromExit(1, null)).toBe(1);
  });

  it('should return 127 for command-not-found exit', () => {
    expect(getReturnCodeFromExit(127, null)).toBe(127);
  });

  it('should prioritise SIGKILL signal over a non-null code', () => {
    expect(getReturnCodeFromExit(1, 'SIGKILL')).toBe(999);
  });

  it('should prioritise SIGTERM signal over a non-null code', () => {
    expect(getReturnCodeFromExit(0, 'SIGTERM')).toBe(998);
  });
});

// ─── padStringTo256Bits ───────────────────────────────────────────────────────

describe('padStringTo256Bits', () => {
  it('should produce a 32-byte output for short strings', () => {
    const result = padStringTo256Bits('hello');
    expect(result).toHaveLength(32);
  });

  it('should pad a short string to exactly 32 bytes', () => {
    const result = padStringTo256Bits('test');
    expect(result).toHaveLength(32);
  });

  it('should hash a 32-byte input to a 32-byte digest', () => {
    const input = 'a'.repeat(32);
    const result = padStringTo256Bits(input);
    expect(result).toEqual(crypto.createHash('sha256').update(input, 'utf8').digest());
    expect(result).toHaveLength(32);
  });

  it('should hash inputs longer than 32 UTF-8 bytes consistently', () => {
    const input = 'é'.repeat(17);
    const inputBuffer = Buffer.from(input, 'utf8');
    expect(inputBuffer.length).toBeGreaterThan(32);

    const result = padStringTo256Bits(input);
    expect(result).toEqual(crypto.createHash('sha256').update(inputBuffer).digest());
    expect(result).toHaveLength(32);
  });

  it('should preserve the original string at the start of the output', () => {
    const input = 'CHANGEIT';
    const result = padStringTo256Bits(input);
    expect(result.subarray(0, Buffer.byteLength(input, 'utf8')).toString('utf8')).toBe(input);
  });

  it('should use a PKCS-style padding byte equal to the padding length', () => {
    // 'test' = 4 bytes → padding = 32 - 4 = 28 bytes, each byte value = 28
    const input = 'test';
    const result = padStringTo256Bits(input);
    expect(result[result.length - 1]).toBe(28);
  });

  it('should handle an empty string by producing 32 bytes of padding', () => {
    const result = padStringTo256Bits('');
    expect(result.length).toBe(32);
    // All bytes are the padding value (32)
    expect(result[0]).toBe(32);
    expect(result[31]).toBe(32);
  });
});

// ─── sanitizeUnixFilename ─────────────────────────────────────────────────────

describe('sanitizeUnixFilename', () => {
  function sanitizeUnixFilename(filename) {
    return filename.replace(/[^a-zA-Z0-9_.-]/g, '_');
  }

  it('should replace spaces with underscores', () => {
    expect(sanitizeUnixFilename('backup job')).toBe('backup_job');
  });

  it('should replace forward slashes', () => {
    expect(sanitizeUnixFilename('path/to/file')).toBe('path_to_file');
  });

  it('should replace colons', () => {
    expect(sanitizeUnixFilename('job:2024')).toBe('job_2024');
  });

  it('should keep alphanumeric characters, underscores, dots, and hyphens', () => {
    expect(sanitizeUnixFilename('backup_job-2024.log')).toBe('backup_job-2024.log');
  });

  it('should replace all special characters in a complex name', () => {
    expect(sanitizeUnixFilename('backup:job/2024-*test?name.log')).toBe('backup_job_2024-_test_name.log');
  });

  it('should replace at-signs and percent characters', () => {
    expect(sanitizeUnixFilename('user@host%20')).toBe('user_host_20');
  });
});

// ─── getActiveJobKey ─────────────────────────────────────────────────────────

describe('getActiveJobKey', () => {
  function getActiveJobKey(executionId, jobName) {
    return executionId || jobName;
  }

  it('should return executionId when provided', () => {
    expect(getActiveJobKey('exec-123', 'daily-backup')).toBe('exec-123');
  });

  it('should fall back to jobName when executionId is null', () => {
    expect(getActiveJobKey(null, 'daily-backup')).toBe('daily-backup');
  });

  it('should fall back to jobName when executionId is undefined', () => {
    expect(getActiveJobKey(undefined, 'weekly-backup')).toBe('weekly-backup');
  });

  it('should fall back to jobName when executionId is empty string', () => {
    expect(getActiveJobKey('', 'monthly-backup')).toBe('monthly-backup');
  });
});

// ─── computeMetric ───────────────────────────────────────────────────────────

describe('computeMetric', () => {
  // Replicate computeMetric with injected dependencies for testability
  function getCPULoadPercentage(mockFs, mockOs) {
    try {
      const loadAvgData = mockFs.readFileSync('/proc/loadavg', 'utf-8');
      const [oneMinLoad] = loadAvgData.split(' ').map(parseFloat);
      const cpuCount = mockOs.cpus().length;
      return (oneMinLoad / cpuCount) * 100;
    } catch (e) {
      return 0;
    }
  }

  function getFileSystemUsagePercentage(mockExecSync) {
    try {
      const output = mockExecSync('df -h --output=pcent,target -x tmpfs -x devtmpfs').toString();
      const lines = output.split('\n').slice(1).filter(l => l.trim() !== '');
      return lines.map(line => {
        const [usedPercentage, mountPoint] = line.trim().split(/\s+/);
        return { mount: mountPoint, usage: parseInt(usedPercentage.replace('%', ''), 10) };
      });
    } catch (e) {
      return [];
    }
  }

  async function computeMetric(config, { mockFs, mockOs, mockExecSync, mockExecFileSync }) {
    const { type, path: targetPath, pattern } = config;

    if (targetPath !== undefined && targetPath !== null) {
      if (!/^[a-zA-Z0-9_./ -]+$/.test(targetPath)) {
        return { type, error: 'Invalid path: contains disallowed characters' };
      }
    }

    if (pattern !== undefined && pattern !== null) {
      if (!/^[a-zA-Z0-9_.*?[\]-]+$/.test(pattern)) {
        return { type, error: 'Invalid pattern: contains disallowed characters' };
      }
    }

    try {
      switch (type) {
        case 'cpu':
          return { type, value: getCPULoadPercentage(mockFs, mockOs), unit: 'percent' };

        case 'mount_usage': {
          const all = getFileSystemUsagePercentage(mockExecSync);
          const mount = targetPath ? all.find(m => m.mount === targetPath) : null;
          return { type, path: targetPath || null, value: mount ? mount.usage : null, unit: 'percent', allMounts: all };
        }

        case 'dir_size': {
          if (!targetPath) return { type, error: 'path is required for dir_size' };
          try {
            const output = mockExecSync(`du -sb "${targetPath}" 2>/dev/null || echo "0\t${targetPath}"`).toString().trim();
            const bytes = parseInt(output.split('\t')[0], 10);
            return { type, path: targetPath, value: bytes, unit: 'bytes' };
          } catch (duErr) {
            return { type, path: targetPath, error: duErr.message };
          }
        }

        case 'file_size': {
          if (!targetPath) return { type, error: 'path is required for file_size' };
          const stat = mockFs.statSync(targetPath);
          return { type, path: targetPath, value: stat.size, unit: 'bytes' };
        }

        case 'file_count': {
          if (!targetPath) return { type, error: 'path is required for file_count' };
          const findArgs = pattern
            ? [targetPath, '-maxdepth', '1', '-name', pattern]
            : [targetPath, '-maxdepth', '1', '-type', 'f'];
          const findOutput = mockExecFileSync('find', findArgs).toString().trim();
          const count = findOutput ? findOutput.split('\n').filter(l => l.length > 0).length : 0;
          return { type, path: targetPath, pattern: pattern || null, value: count, unit: 'count' };
        }

        case 'file_age': {
          if (!targetPath) return { type, error: 'path is required for file_age' };
          const mtimeStr = mockExecFileSync('stat', ['-c', '%Y', targetPath]).toString().trim();
          const ageSeconds = Math.floor(Date.now() / 1000) - parseInt(mtimeStr, 10);
          return { type, path: targetPath, value: ageSeconds, unit: 'seconds' };
        }

        default:
          return { type, error: `Unknown metric type: ${type}. Supported: cpu, mount_usage, dir_size, file_size, file_count, file_age` };
      }
    } catch (err) {
      return { type, path: targetPath || null, error: err.message };
    }
  }

  let mockDeps;

  beforeEach(() => {
    mockDeps = {
      mockFs: {
        readFileSync: jest.fn(),
        statSync: jest.fn(),
      },
      mockOs: {
        cpus: jest.fn().mockReturnValue([{}, {}, {}, {}]), // 4 CPUs
      },
      mockExecSync: jest.fn(),
      mockExecFileSync: jest.fn(),
    };
  });

  describe('path injection validation', () => {
    it('should reject paths containing semicolons', async () => {
      const result = await computeMetric({ type: 'dir_size', path: '/tmp;rm -rf /' }, mockDeps);
      expect(result.error).toMatch(/Invalid path/);
    });

    it('should reject paths containing backticks', async () => {
      const result = await computeMetric({ type: 'file_size', path: '/tmp/`whoami`' }, mockDeps);
      expect(result.error).toMatch(/Invalid path/);
    });

    it('should reject paths containing dollar signs', async () => {
      const result = await computeMetric({ type: 'dir_size', path: '/tmp/$(id)' }, mockDeps);
      expect(result.error).toMatch(/Invalid path/);
    });

    it('should reject paths containing pipe characters', async () => {
      const result = await computeMetric({ type: 'file_size', path: '/tmp/|cat /etc/passwd' }, mockDeps);
      expect(result.error).toMatch(/Invalid path/);
    });

    it('should reject paths containing ampersands', async () => {
      const result = await computeMetric({ type: 'dir_size', path: '/tmp/a && rm -rf /' }, mockDeps);
      expect(result.error).toMatch(/Invalid path/);
    });

    it('should accept valid standard unix paths', async () => {
      mockDeps.mockFs.statSync.mockReturnValue({ size: 1024 });
      const result = await computeMetric({ type: 'file_size', path: '/home/user/backups' }, mockDeps);
      expect(result.error).toBeUndefined();
    });

    it('should accept paths with spaces', async () => {
      mockDeps.mockFs.statSync.mockReturnValue({ size: 512 });
      const result = await computeMetric({ type: 'file_size', path: '/var/log/my file.log' }, mockDeps);
      expect(result.error).toBeUndefined();
    });

    it('should accept paths with dots and hyphens', async () => {
      mockDeps.mockFs.statSync.mockReturnValue({ size: 2048 });
      const result = await computeMetric({ type: 'file_size', path: '/opt/my-app/logs/app.log' }, mockDeps);
      expect(result.error).toBeUndefined();
    });
  });

  describe('pattern injection validation', () => {
    it('should reject patterns containing shell injection characters', async () => {
      const result = await computeMetric({ type: 'file_count', path: '/tmp', pattern: '*.log;rm -rf /' }, mockDeps);
      expect(result.error).toMatch(/Invalid pattern/);
    });

    it('should reject patterns with dollar signs', async () => {
      const result = await computeMetric({ type: 'file_count', path: '/tmp', pattern: '$(id)' }, mockDeps);
      expect(result.error).toMatch(/Invalid pattern/);
    });

    it('should accept valid glob patterns', async () => {
      mockDeps.mockExecFileSync.mockReturnValue('/tmp/a.log\n/tmp/b.log\n');
      const result = await computeMetric({ type: 'file_count', path: '/tmp', pattern: '*.log' }, mockDeps);
      expect(result.error).toBeUndefined();
      expect(result.value).toBe(2);
    });

    it('should accept patterns with question marks and brackets', async () => {
      mockDeps.mockExecFileSync.mockReturnValue('/tmp/a1.log\n');
      const result = await computeMetric({ type: 'file_count', path: '/tmp', pattern: 'file?.log' }, mockDeps);
      expect(result.error).toBeUndefined();
    });
  });

  describe('cpu metric', () => {
    it('should return cpu usage as a percentage', async () => {
      mockDeps.mockFs.readFileSync.mockReturnValue('2.0 1.5 1.0 5/1000 12345');
      const result = await computeMetric({ type: 'cpu' }, mockDeps);
      expect(result.type).toBe('cpu');
      expect(result.unit).toBe('percent');
      expect(result.value).toBe(50); // (2.0 / 4 CPUs) * 100
    });

    it('should return 0 when /proc/loadavg is unreadable', async () => {
      mockDeps.mockFs.readFileSync.mockImplementation(() => { throw new Error('ENOENT'); });
      const result = await computeMetric({ type: 'cpu' }, mockDeps);
      expect(result.value).toBe(0);
    });

    it('should scale correctly with different CPU counts', async () => {
      mockDeps.mockFs.readFileSync.mockReturnValue('4.0 3.0 2.0 8/1500 99999');
      mockDeps.mockOs.cpus.mockReturnValue([{}, {}, {}, {}, {}, {}, {}, {}]); // 8 CPUs
      const result = await computeMetric({ type: 'cpu' }, mockDeps);
      expect(result.value).toBe(50); // (4.0 / 8) * 100
    });
  });

  describe('mount_usage metric', () => {
    const dfOutput = 'Use%     Mounted on\n75%      /\n50%      /home\n90%      /var';

    it('should return usage for a specific mount point', async () => {
      mockDeps.mockExecSync.mockReturnValue(dfOutput);
      const result = await computeMetric({ type: 'mount_usage', path: '/var' }, mockDeps);
      expect(result.value).toBe(90);
      expect(result.unit).toBe('percent');
      expect(result.path).toBe('/var');
    });

    it('should return null value when the requested mount is not found', async () => {
      mockDeps.mockExecSync.mockReturnValue(dfOutput);
      const result = await computeMetric({ type: 'mount_usage', path: '/nonexistent' }, mockDeps);
      expect(result.value).toBeNull();
    });

    it('should populate allMounts with all discovered filesystems', async () => {
      mockDeps.mockExecSync.mockReturnValue(dfOutput);
      const result = await computeMetric({ type: 'mount_usage' }, mockDeps);
      expect(result.allMounts).toHaveLength(3);
      expect(result.allMounts[0]).toEqual({ mount: '/', usage: 75 });
      expect(result.allMounts[2]).toEqual({ mount: '/var', usage: 90 });
    });

    it('should return empty allMounts when execSync fails', async () => {
      mockDeps.mockExecSync.mockImplementation(() => { throw new Error('command not found'); });
      const result = await computeMetric({ type: 'mount_usage' }, mockDeps);
      expect(result.allMounts).toEqual([]);
    });

    it('should return null path when no path provided', async () => {
      mockDeps.mockExecSync.mockReturnValue(dfOutput);
      const result = await computeMetric({ type: 'mount_usage' }, mockDeps);
      expect(result.path).toBeNull();
    });
  });

  describe('dir_size metric', () => {
    it('should return directory size in bytes', async () => {
      mockDeps.mockExecSync.mockReturnValue('102400\t/var/log');
      const result = await computeMetric({ type: 'dir_size', path: '/var/log' }, mockDeps);
      expect(result.type).toBe('dir_size');
      expect(result.value).toBe(102400);
      expect(result.unit).toBe('bytes');
      expect(result.path).toBe('/var/log');
    });

    it('should return error when path is not provided', async () => {
      const result = await computeMetric({ type: 'dir_size' }, mockDeps);
      expect(result.error).toBe('path is required for dir_size');
    });

    it('should return error object when execSync throws', async () => {
      mockDeps.mockExecSync.mockImplementation(() => { throw new Error('Permission denied'); });
      const result = await computeMetric({ type: 'dir_size', path: '/root' }, mockDeps);
      expect(result.error).toBe('Permission denied');
      expect(result.path).toBe('/root');
    });
  });

  describe('file_size metric', () => {
    it('should return file size in bytes', async () => {
      mockDeps.mockFs.statSync.mockReturnValue({ size: 4096 });
      const result = await computeMetric({ type: 'file_size', path: '/var/log/syslog' }, mockDeps);
      expect(result.value).toBe(4096);
      expect(result.unit).toBe('bytes');
      expect(result.path).toBe('/var/log/syslog');
    });

    it('should return error when path is not provided', async () => {
      const result = await computeMetric({ type: 'file_size' }, mockDeps);
      expect(result.error).toBe('path is required for file_size');
    });

    it('should propagate statSync errors', async () => {
      mockDeps.mockFs.statSync.mockImplementation(() => { throw new Error('ENOENT: no such file'); });
      const result = await computeMetric({ type: 'file_size', path: '/missing/file' }, mockDeps);
      expect(result.error).toContain('ENOENT');
    });
  });

  describe('file_count metric', () => {
    it('should count files in a directory', async () => {
      mockDeps.mockExecFileSync.mockReturnValue('/tmp/a.log\n/tmp/b.log\n/tmp/c.log\n');
      const result = await computeMetric({ type: 'file_count', path: '/tmp' }, mockDeps);
      expect(result.value).toBe(3);
      expect(result.unit).toBe('count');
      expect(result.pattern).toBeNull();
    });

    it('should count only files matching a pattern', async () => {
      mockDeps.mockExecFileSync.mockReturnValue('/tmp/a.log\n/tmp/b.log\n');
      const result = await computeMetric({ type: 'file_count', path: '/tmp', pattern: '*.log' }, mockDeps);
      expect(result.pattern).toBe('*.log');
      expect(result.value).toBe(2);
    });

    it('should return 0 for an empty directory', async () => {
      mockDeps.mockExecFileSync.mockReturnValue('');
      const result = await computeMetric({ type: 'file_count', path: '/tmp' }, mockDeps);
      expect(result.value).toBe(0);
    });

    it('should return error when path is not provided', async () => {
      const result = await computeMetric({ type: 'file_count' }, mockDeps);
      expect(result.error).toBe('path is required for file_count');
    });

    it('should use pattern-based find args when pattern is provided', async () => {
      mockDeps.mockExecFileSync.mockReturnValue('');
      await computeMetric({ type: 'file_count', path: '/tmp', pattern: '*.bak' }, mockDeps);
      expect(mockDeps.mockExecFileSync).toHaveBeenCalledWith('find', ['/tmp', '-maxdepth', '1', '-name', '*.bak']);
    });

    it('should use type-based find args when no pattern provided', async () => {
      mockDeps.mockExecFileSync.mockReturnValue('');
      await computeMetric({ type: 'file_count', path: '/tmp' }, mockDeps);
      expect(mockDeps.mockExecFileSync).toHaveBeenCalledWith('find', ['/tmp', '-maxdepth', '1', '-type', 'f']);
    });
  });

  describe('file_age metric', () => {
    it('should return file age in seconds', async () => {
      const nowSecs = Math.floor(Date.now() / 1000);
      const mtimeSecs = nowSecs - 3600; // 1 hour ago
      mockDeps.mockExecFileSync.mockReturnValue(String(mtimeSecs));
      const result = await computeMetric({ type: 'file_age', path: '/var/log/app.log' }, mockDeps);
      expect(result.unit).toBe('seconds');
      expect(result.value).toBeGreaterThanOrEqual(3599);
      expect(result.value).toBeLessThanOrEqual(3601);
    });

    it('should return error when path is not provided', async () => {
      const result = await computeMetric({ type: 'file_age' }, mockDeps);
      expect(result.error).toBe('path is required for file_age');
    });

    it('should propagate stat command errors', async () => {
      mockDeps.mockExecFileSync.mockImplementation(() => { throw new Error('stat: cannot stat'); });
      const result = await computeMetric({ type: 'file_age', path: '/missing/file' }, mockDeps);
      expect(result.error).toContain('stat: cannot stat');
    });

    it('should call stat with the correct arguments', async () => {
      const nowSecs = Math.floor(Date.now() / 1000);
      mockDeps.mockExecFileSync.mockReturnValue(String(nowSecs));
      await computeMetric({ type: 'file_age', path: '/var/log/app.log' }, mockDeps);
      expect(mockDeps.mockExecFileSync).toHaveBeenCalledWith('stat', ['-c', '%Y', '/var/log/app.log']);
    });
  });

  describe('unknown metric type', () => {
    it('should return a descriptive error for an unknown type', async () => {
      const result = await computeMetric({ type: 'memory_usage' }, mockDeps);
      expect(result.error).toContain('Unknown metric type: memory_usage');
      expect(result.error).toContain('Supported:');
    });

    it('should include the unknown type name in the response', async () => {
      const result = await computeMetric({ type: 'disk_iops' }, mockDeps);
      expect(result.type).toBe('disk_iops');
    });
  });
});

// ─── handleCommand routing logic ─────────────────────────────────────────────

describe('handleCommand routing logic', () => {
  const AGENT_ID = 'test-agent';

  // Replicate the routing decisions that handleCommand makes after JWT validation
  function routeCommand(decodedPayload, handlers) {
    const {
      name: agentId,
      command,
      commandParams = '',
      executionId = null,
    } = decodedPayload;

    if (agentId !== AGENT_ID) {
      return 'ignored';
    }

    if (command === 'ping') {
      handlers.onPing();
      return 'ping';
    }

    if (command === 'queryMetric') {
      let metricConfig;
      try {
        metricConfig = JSON.parse(commandParams);
      } catch (e) {
        handlers.onMetricError(e.message);
        return 'metricError';
      }
      handlers.onMetric(metricConfig);
      return 'metric';
    }

    if (command === 'terminateExecution') {
      const targetId = commandParams || executionId;
      if (!targetId) {
        return 'terminateNoId';
      }
      handlers.onTerminate(targetId);
      return 'terminate';
    }

    handlers.onExecute(decodedPayload);
    return 'execute';
  }

  let handlers;

  beforeEach(() => {
    handlers = {
      onPing: jest.fn(),
      onMetric: jest.fn(),
      onMetricError: jest.fn(),
      onTerminate: jest.fn(),
      onExecute: jest.fn(),
    };
  });

  it('should ignore commands addressed to a different agent', () => {
    const result = routeCommand({ name: 'other-agent', command: 'ping' }, handlers);
    expect(result).toBe('ignored');
    expect(handlers.onPing).not.toHaveBeenCalled();
  });

  it('should route ping command to onPing handler', () => {
    const result = routeCommand({ name: AGENT_ID, command: 'ping' }, handlers);
    expect(result).toBe('ping');
    expect(handlers.onPing).toHaveBeenCalled();
  });

  it('should route queryMetric with valid JSON commandParams', () => {
    const result = routeCommand({
      name: AGENT_ID,
      command: 'queryMetric',
      commandParams: JSON.stringify({ type: 'cpu' }),
    }, handlers);
    expect(result).toBe('metric');
    expect(handlers.onMetric).toHaveBeenCalledWith({ type: 'cpu' });
  });

  it('should route queryMetric to error handler when commandParams is invalid JSON', () => {
    const result = routeCommand({
      name: AGENT_ID,
      command: 'queryMetric',
      commandParams: 'not-valid-json',
    }, handlers);
    expect(result).toBe('metricError');
    expect(handlers.onMetricError).toHaveBeenCalled();
    expect(handlers.onMetric).not.toHaveBeenCalled();
  });

  it('should route terminateExecution using commandParams as the target ID', () => {
    const result = routeCommand({
      name: AGENT_ID,
      command: 'terminateExecution',
      commandParams: 'exec-abc123',
    }, handlers);
    expect(result).toBe('terminate');
    expect(handlers.onTerminate).toHaveBeenCalledWith('exec-abc123');
  });

  it('should route terminateExecution using executionId when commandParams is empty', () => {
    const result = routeCommand({
      name: AGENT_ID,
      command: 'terminateExecution',
      commandParams: '',
      executionId: 'exec-fallback',
    }, handlers);
    expect(result).toBe('terminate');
    expect(handlers.onTerminate).toHaveBeenCalledWith('exec-fallback');
  });

  it('should return terminateNoId when neither commandParams nor executionId are provided', () => {
    const result = routeCommand({
      name: AGENT_ID,
      command: 'terminateExecution',
      commandParams: '',
      executionId: null,
    }, handlers);
    expect(result).toBe('terminateNoId');
    expect(handlers.onTerminate).not.toHaveBeenCalled();
  });

  it('should route an unknown command to onExecute', () => {
    const payload = { name: AGENT_ID, command: 'run-backup', jobName: 'daily' };
    const result = routeCommand(payload, handlers);
    expect(result).toBe('execute');
    expect(handlers.onExecute).toHaveBeenCalledWith(payload);
  });

  it('should build executionContext when executionMode is present', () => {
    const payload = {
      name: AGENT_ID,
      command: 'run-backup',
      jobName: 'test-job',
      executionMode: 'test',
      scriptName: 'backup.sh',
      scriptIdentity: 'script:backup.sh',
      sourceType: 'saved',
      scriptLabel: 'backup.sh',
    };

    // Replicate the executionContext building logic
    const { executionMode, scriptName, scriptIdentity, sourceType, scriptLabel } = payload;
    const executionContext = executionMode
      ? { executionMode, scriptName, scriptIdentity, sourceType, scriptLabel }
      : null;

    expect(executionContext).not.toBeNull();
    expect(executionContext.executionMode).toBe('test');
    expect(executionContext.scriptName).toBe('backup.sh');
  });

  it('should produce null executionContext when executionMode is absent', () => {
    const payload = { name: AGENT_ID, command: 'run-backup', jobName: 'daily', executionMode: null };
    const { executionMode, scriptName, scriptIdentity, sourceType, scriptLabel } = payload;
    const executionContext = executionMode
      ? { executionMode, scriptName, scriptIdentity, sourceType, scriptLabel }
      : null;
    expect(executionContext).toBeNull();
  });
});

// ─── backupComplete ──────────────────────────────────────────────────────────

describe('backupComplete', () => {
  it('should call publishLogData with empty string log, eta, and returnCode', () => {
    const publishLogData = jest.fn();

    function backupComplete(jobName, eta, manual, executionId = null, executionContext = null, completionReturnCode = null) {
      publishLogData('', jobName, eta, completionReturnCode, undefined, manual, executionId, executionContext);
    }

    backupComplete('daily-backup', 45.2, false, 'exec-123', null, 0);
    expect(publishLogData).toHaveBeenCalledWith('', 'daily-backup', 45.2, 0, undefined, false, 'exec-123', null);
  });

  it('should default executionId and completionReturnCode to null when not provided', () => {
    const publishLogData = jest.fn();

    function backupComplete(jobName, eta, manual, executionId = null, executionContext = null, completionReturnCode = null) {
      publishLogData('', jobName, eta, completionReturnCode, undefined, manual, executionId, executionContext);
    }

    backupComplete('weekly-backup', 120, true);
    expect(publishLogData).toHaveBeenCalledWith('', 'weekly-backup', 120, null, undefined, true, null, null);
  });

  it('should pass a non-zero return code on job failure', () => {
    const publishLogData = jest.fn();

    function backupComplete(jobName, eta, manual, executionId = null, executionContext = null, completionReturnCode = null) {
      publishLogData('', jobName, eta, completionReturnCode, undefined, manual, executionId, executionContext);
    }

    backupComplete('snapshot', 5.1, false, 'exec-789', null, 1);
    const args = publishLogData.mock.calls[0];
    expect(args[3]).toBe(1); // completionReturnCode position
  });

  it('should pass executionContext through to publishLogData', () => {
    const publishLogData = jest.fn();
    const ctx = { executionMode: 'test', scriptName: 'backup.sh' };

    function backupComplete(jobName, eta, manual, executionId = null, executionContext = null, completionReturnCode = null) {
      publishLogData('', jobName, eta, completionReturnCode, undefined, manual, executionId, executionContext);
    }

    backupComplete('scripted-job', 10, false, 'exec-ctx', ctx, 0);
    expect(publishLogData).toHaveBeenCalledWith('', 'scripted-job', 10, 0, undefined, false, 'exec-ctx', ctx);
  });
});

// ─── cleanupBackgroundTasks ──────────────────────────────────────────────────

describe('cleanupBackgroundTasks', () => {
  it('should clear all tracked intervals', () => {
    const cleared = [];
    const mockClear = id => cleared.push(id);
    let activeIntervals = [1, 2, 3];

    function cleanupBackgroundTasks() {
      activeIntervals.forEach(id => { try { mockClear(id); } catch (e) {} });
      activeIntervals = [];
    }

    cleanupBackgroundTasks();
    expect(cleared).toEqual([1, 2, 3]);
    expect(activeIntervals).toEqual([]);
  });

  it('should signal all active jobs with SIGTERM', () => {
    const signalled = [];
    const activeJobs = new Map();

    activeJobs.set('exec-1', { pid: 1001 });
    activeJobs.set('exec-2', { pid: 1002 });

    function signalTrackedJob(job, signal) {
      signalled.push({ pid: job.pid, signal });
    }

    function cleanupBackgroundTasks() {
      activeJobs.forEach(job => signalTrackedJob(job, 'SIGTERM'));
      activeJobs.clear();
    }

    cleanupBackgroundTasks();
    expect(signalled).toContainEqual({ pid: 1001, signal: 'SIGTERM' });
    expect(signalled).toContainEqual({ pid: 1002, signal: 'SIGTERM' });
    expect(activeJobs.size).toBe(0);
  });

  it('should handle empty collections without throwing', () => {
    let activeIntervals = [];
    const activeJobs = new Map();

    function cleanupBackgroundTasks() {
      activeIntervals.forEach(id => { try { clearInterval(id); } catch (e) {} });
      activeIntervals = [];
      activeJobs.forEach(() => {});
      activeJobs.clear();
    }

    expect(() => cleanupBackgroundTasks()).not.toThrow();
  });

  it('should clear activeJobs after signalling', () => {
    const activeJobs = new Map();
    activeJobs.set('exec-a', { pid: 2001 });

    function cleanupBackgroundTasks() {
      activeJobs.forEach(job => { try { process.kill(job.pid, 'SIGTERM'); } catch (e) {} });
      activeJobs.clear();
    }

    cleanupBackgroundTasks();
    expect(activeJobs.size).toBe(0);
  });
});

// ─── livenessProbe output structure ──────────────────────────────────────────

describe('livenessProbe output structure', () => {
  it('should contain all required top-level fields', () => {
    const probe = {
      status: 'ok',
      identifier: 'test-agent',
      uptime: '0d 0h 0m 5s',
      version: '1.0.0',
      installDir: '/opt/OrcheliumAgent',
      workingDir: '/tmp',
      agentStatus: 'idle',
      mqttServer: 'localhost',
      mqttPort: '1883',
      wsServer: 'localhost',
      wsServerPort: '49981',
      connectionMode: 'websocket',
      startupType: 'INSTALL',
      jobCount: 0,
      failJobCount: 0,
      successJobCount: 0,
      pubCount: 0,
      subCount: 0,
      commsStatus: 'WebSocket connection is active',
      filePermissionStatus: 'ok',
      fileSystemUsagePct: [],
      cpuPct: 0,
    };

    const required = [
      'status', 'identifier', 'uptime', 'version', 'agentStatus',
      'connectionMode', 'jobCount', 'failJobCount', 'successJobCount',
      'commsStatus', 'filePermissionStatus', 'fileSystemUsagePct', 'cpuPct',
    ];

    required.forEach(field => expect(probe).toHaveProperty(field));
  });

  it('should report unhealthy when comms status starts with ERROR:', () => {
    const commsStatus = 'ERROR: WebSocket connection issues';
    const filePermStatus = 'ok';
    const overall = (commsStatus.startsWith('ERROR:') || filePermStatus !== 'ok') ? 'unhealthy' : 'ok';
    expect(overall).toBe('unhealthy');
  });

  it('should report unhealthy when file permission check fails', () => {
    const commsStatus = 'WebSocket connection is active';
    const filePermStatus = 'No execute permissions capability.';
    const overall = (commsStatus.startsWith('ERROR:') || filePermStatus !== 'ok') ? 'unhealthy' : 'ok';
    expect(overall).toBe('unhealthy');
  });

  it('should report ok when both comms and file permissions are healthy', () => {
    const commsStatus = 'WebSocket connection is active';
    const filePermStatus = 'ok';
    const overall = (commsStatus.startsWith('ERROR:') || filePermStatus !== 'ok') ? 'unhealthy' : 'ok';
    expect(overall).toBe('ok');
  });

  it('should report unhealthy when both checks fail', () => {
    const commsStatus = 'ERROR: MQTT Connection issues';
    const filePermStatus = 'No execute permissions capability.';
    const overall = (commsStatus.startsWith('ERROR:') || filePermStatus !== 'ok') ? 'unhealthy' : 'ok';
    expect(overall).toBe('unhealthy');
  });

  describe('getUptime formatting', () => {
    function getUptime(uptimeSecs) {
      let uptime = uptimeSecs;
      const days = Math.floor(uptime / (24 * 60 * 60)); uptime %= (24 * 60 * 60);
      const hours = Math.floor(uptime / (60 * 60)); uptime %= (60 * 60);
      const minutes = Math.floor(uptime / 60);
      const seconds = Math.floor(uptime % 60);
      return `${days}d ${hours}h ${minutes}m ${seconds}s`;
    }

    it('should format zero uptime', () => {
      expect(getUptime(0)).toBe('0d 0h 0m 0s');
    });

    it('should format exactly one hour', () => {
      expect(getUptime(3600)).toBe('0d 1h 0m 0s');
    });

    it('should format one day', () => {
      expect(getUptime(86400)).toBe('1d 0h 0m 0s');
    });

    it('should format mixed days, hours, minutes, seconds', () => {
      expect(getUptime(90061)).toBe('1d 1h 1m 1s');
    });

    it('should format one hour, one minute, one second', () => {
      expect(getUptime(3661)).toBe('0d 1h 1m 1s');
    });
  });
});
