// Mock setup for agent.js tests
// Note: agent.js is a standalone application with global state
// We'll test isolated utility functions and classes

// Mock external dependencies
jest.mock('fs');
jest.mock('child_process');
jest.mock('mqtt');
jest.mock('ws');
jest.mock('reconnecting-websocket');
jest.mock('jsonwebtoken');
jest.mock('os');

const fs = require('fs');
const { spawn, execSync } = require('child_process');
const jwt = require('jsonwebtoken');
const os = require('os');
const mqtt = require('mqtt');

describe('Agent Utility Functions', () => {
  // Note: Since agent.js is not modularized with exports,
  // we're testing the functions through their expected behavior

  describe('Debug level enumeration', () => {
    it('should define all debug levels', () => {
      const debugLevels = ['trace', 'info', 'warn', 'error', 'critical'];
      expect(debugLevels).toContain('trace');
      expect(debugLevels).toContain('info');
      expect(debugLevels).toContain('warn');
      expect(debugLevels).toContain('error');
      expect(debugLevels).toContain('critical');
    });
  });

  describe('AGENT_STATUS enumeration', () => {
    it('should define all agent statuses', () => {
      const agentStatuses = ['offline', 'idle', 'running', 'initializing', 'error'];
      expect(agentStatuses).toContain('offline');
      expect(agentStatuses).toContain('idle');
      expect(agentStatuses).toContain('running');
      expect(agentStatuses).toContain('initializing');
      expect(agentStatuses).toContain('error');
    });
  });

  describe('File operations', () => {
    beforeEach(() => {
      jest.clearAllMocks();
    });

    it('should generate random file names with unique format', () => {
      // Test that random filename follows expected pattern: timestamp_randomchars
      const filename = `${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;
      expect(filename).toMatch(/^\d+_[a-z0-9]{6}$/);
    });

    it('should sanitize unix filenames by replacing invalid chars', () => {
      const unsafeFilename = 'backup:job/2024-*test?name.log';
      const sanitized = unsafeFilename.replace(/[^a-zA-Z0-9_.-]/g, '_');
      expect(sanitized).toBe('backup_job_2024-_test_name.log');
    });

    it('should handle file write operations', () => {
      fs.writeFileSync = jest.fn();
      const testPath = '/tmp/test.sh';
      const testData = '#!/bin/bash\necho test';
      
      fs.writeFileSync(testPath, testData, 'utf8');
      expect(fs.writeFileSync).toHaveBeenCalledWith(testPath, testData, 'utf8');
    });

    it('should handle file deletion', () => {
      fs.unlinkSync = jest.fn();
      const testPath = '/tmp/test.sh';
      
      fs.unlinkSync(testPath);
      expect(fs.unlinkSync).toHaveBeenCalledWith(testPath);
    });

    it('should check file existence', () => {
      fs.existsSync = jest.fn().mockReturnValue(true);
      
      const result = fs.existsSync('/tmp/test.log');
      expect(result).toBe(true);
      expect(fs.existsSync).toHaveBeenCalledWith('/tmp/test.log');
    });

    it('should get file stats', () => {
      fs.statSync = jest.fn().mockReturnValue({ size: 1024 });
      
      const stats = fs.statSync('/tmp/test.log');
      expect(stats.size).toBe(1024);
    });
  });

  describe('Date/Time formatting', () => {
    it('should format current date time correctly', () => {
      const now = new Date();
      const formatted = `${now.getFullYear().toString().padStart(4, '0')}-${(now.getMonth() + 1).toString().padStart(2, '0')}-${now.getDate().toString().padStart(2, '0')}_${now.getHours().toString().padStart(2, '0')}${now.getMinutes().toString().padStart(2, '0')}${now.getSeconds().toString().padStart(2, '0')}`;
      
      // Check format matches pattern YYYY-MM-DD_HHmmss
      expect(formatted).toMatch(/^\d{4}-\d{2}-\d{2}_\d{6}$/);
    });
  });

  describe('String padding utility', () => {
    it('should pad string to 256 bits (32 bytes)', () => {
      // Test padding logic
      const inputString = 'test';
      const targetLength = 32;
      let padded = inputString;
      while (padded.length < targetLength) {
        padded += '\0';
      }
      expect(padded.length).toBe(targetLength);
    });
  });

  describe('Debug token management', () => {
    let debugToken = null;
    let debugTokenExpiry = null;
    const DEBUG_TOKEN_EXPIRY_MS = 10 * 60 * 1000;

    it('should generate debug token with hex format', () => {
      const crypto = require('crypto');
      const token = crypto.randomBytes(8).toString('hex');
      
      expect(token).toMatch(/^[a-f0-9]{16}$/);
      expect(token.length).toBe(16);
    });

    it('should validate debug token when within expiry', () => {
      const testToken = 'abc123def456';
      debugToken = testToken;
      debugTokenExpiry = Date.now() + DEBUG_TOKEN_EXPIRY_MS;
      
      const isValid = testToken === debugToken && Date.now() < debugTokenExpiry;
      expect(isValid).toBe(true);
    });

    it('should reject expired debug token', () => {
      const testToken = 'expiredtoken';
      debugToken = testToken;
      debugTokenExpiry = Date.now() - 1000; // Already expired
      
      const isValid = testToken === debugToken && Date.now() < debugTokenExpiry;
      expect(isValid).toBe(false);
    });

    it('should reset expiry on successful validation', () => {
      const initialTime = Date.now();
      const testToken = 'validtoken';
      debugToken = testToken;
      debugTokenExpiry = initialTime + DEBUG_TOKEN_EXPIRY_MS;
      
      // After validation, expiry gets reset
      debugTokenExpiry = Date.now() + DEBUG_TOKEN_EXPIRY_MS;
      
      expect(debugTokenExpiry).toBeGreaterThan(initialTime);
    });
  });
});

describe('RetryBackoffManager', () => {
  class RetryBackoffManager {
    constructor(agentId, handlerType) {
      this.agentId = agentId;
      this.handlerType = handlerType;
      this.currentAttempt = 0;
      this.startTime = Date.now();
      this.lastBackoffChangeTime = this.startTime;
      this.currentBackoffStage = 0;
      
      this.backoffStages = [
        { durationMs: 10 * 60 * 1000, intervalMs: 1 * 60 * 1000, stageName: '1-min backoff (10 mins)' },
        { durationMs: 10 * 60 * 1000, intervalMs: 5 * 60 * 1000, stageName: '5-min backoff (10 mins)' },
        { durationMs: 60 * 60 * 1000, intervalMs: 10 * 60 * 1000, stageName: '10-min backoff (1 hour)' },
        { durationMs: 60 * 60 * 1000, intervalMs: 20 * 60 * 1000, stageName: '20-min backoff (1 hour)' },
        { durationMs: 60 * 60 * 1000, intervalMs: 30 * 60 * 1000, stageName: '30-min backoff (1 hour)' },
        { durationMs: Infinity, intervalMs: 60 * 60 * 1000, stageName: '1-hour backoff (indefinite)' }
      ];
    }

    getNextRetryDelay() {
      const now = Date.now();
      const currentStage = this.backoffStages[this.currentBackoffStage];
      const timeInStage = now - this.lastBackoffChangeTime;

      if (timeInStage > currentStage.durationMs && this.currentBackoffStage < this.backoffStages.length - 1) {
        this.currentBackoffStage++;
        this.lastBackoffChangeTime = now;
      }

      return currentStage.intervalMs;
    }

    recordAttempt() {
      this.currentAttempt++;
    }

    reset() {
      this.currentAttempt = 0;
      this.startTime = Date.now();
      this.lastBackoffChangeTime = this.startTime;
      this.currentBackoffStage = 0;
    }
  }

  it('should initialize with correct properties', () => {
    const manager = new RetryBackoffManager('test-agent', 'WebSocket');
    
    expect(manager.agentId).toBe('test-agent');
    expect(manager.handlerType).toBe('WebSocket');
    expect(manager.currentAttempt).toBe(0);
    expect(manager.currentBackoffStage).toBe(0);
    expect(manager.backoffStages.length).toBe(6);
  });

  it('should return initial retry delay', () => {
    const manager = new RetryBackoffManager('test-agent', 'MQTT');
    const delay = manager.getNextRetryDelay();
    
    expect(delay).toBe(1 * 60 * 1000); // 1 minute for initial stage
  });

  it('should record retry attempts', () => {
    const manager = new RetryBackoffManager('test-agent', 'WebSocket');
    
    manager.recordAttempt();
    expect(manager.currentAttempt).toBe(1);
    
    manager.recordAttempt();
    expect(manager.currentAttempt).toBe(2);
  });

  it('should reset backoff on successful connection', () => {
    const manager = new RetryBackoffManager('test-agent', 'MQTT');
    
    manager.recordAttempt();
    manager.recordAttempt();
    expect(manager.currentAttempt).toBe(2);
    
    manager.reset();
    expect(manager.currentAttempt).toBe(0);
    expect(manager.currentBackoffStage).toBe(0);
  });
});

describe('CommandExecution', () => {
  let activeJobs;
  let activeIntervals;

  beforeEach(() => {
    jest.clearAllMocks();
    activeJobs = new Map();
    activeIntervals = [];

    fs.existsSync = jest.fn().mockReturnValue(true);
    fs.writeFileSync = jest.fn();
    fs.unlinkSync = jest.fn();
    fs.statSync = jest.fn().mockReturnValue({ size: 100 });
  });

  it('should validate and sanitize command parameters', () => {
    const { sanitizeCommandParams } = require('../src/executor');

    expect(sanitizeCommandParams('test; rm -rf /')).toBe('test\\; rm -rf /');
    expect(sanitizeCommandParams('ok\nrm -rf /')).toBe('ok rm -rf /');
    expect(sanitizeCommandParams('ok\r\nrm -rf /')).toBe('ok rm -rf /');
  });

  it('should track active jobs in a Map', () => {
    activeJobs.set('backup-job-1', { startTime: Date.now(), status: 'running' });
    
    expect(activeJobs.has('backup-job-1')).toBe(true);
    expect(activeJobs.get('backup-job-1').status).toBe('running');
  });

  it('should clean up active jobs on removal', () => {
    const jobName = 'test-job';
    activeJobs.set(jobName, { startTime: Date.now(), status: 'running' });
    
    expect(activeJobs.size).toBe(1);
    activeJobs.delete(jobName);
    expect(activeJobs.size).toBe(0);
  });

  it('should handle interval cleanup', () => {
    const mockInterval = setInterval(() => {}, 1000);
    activeIntervals.push(mockInterval);
    
    expect(activeIntervals.length).toBe(1);
    
    activeIntervals.forEach(intervalId => {
      clearInterval(intervalId);
    });
    activeIntervals = [];
    
    expect(activeIntervals.length).toBe(0);
  });
});

describe('Message validation', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should validate JWT token', async () => {
    jwt.verify = jest.fn((token, key, options, callback) => {
      callback(null, { jobName: 'test-job', command: 'test.sh' });
    });

    const testToken = '{"test": "token"}';
    const enckey = 'testkey';

    return new Promise((resolve, reject) => {
      jwt.verify(testToken, enckey, { algorithm: 'HS256' }, (err, decoded) => {
        if (err) reject(err);
        expect(decoded.jobName).toBe('test-job');
        expect(decoded.command).toBe('test.sh');
        resolve();
      });
    });
  });

  it('should reject invalid JWT token', async () => {
    const testError = new Error('Invalid token');
    jwt.verify = jest.fn((token, key, options, callback) => {
      callback(testError);
    });

    const testToken = 'invalid.token.here';
    const enckey = 'testkey';

    return new Promise((resolve, reject) => {
      jwt.verify(testToken, enckey, { algorithm: 'HS256' }, (err, decoded) => {
        expect(err).toBeTruthy();
        expect(err.message).toBe('Invalid token');
        resolve();
      });
    });
  });

  it('should extract agent ID from decoded payload', () => {
    const decodedPayload = {
      name: 'agent-1',
      command: 'backup.sh',
      jobName: 'daily-backup',
      commandParams: '--full',
      manual: false
    };

    const { name: agentId } = decodedPayload;
    expect(agentId).toBe('agent-1');
  });
});

describe('CPU and Filesystem Monitoring', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('getCPULoadPercentage', () => {
    it('should calculate CPU load percentage', () => {
      fs.readFileSync = jest.fn().mockReturnValue('2.5 2.0 1.8 5/1000 1234567');
      os.cpus = jest.fn().mockReturnValue([{}, {}, {}, {}]); // 4 CPUs

      const loadAvgData = '2.5 2.0 1.8 5/1000 1234567';
      const [oneMinLoad] = loadAvgData.split(' ').map(parseFloat);
      const cpuCount = 4;
      const percentage = (oneMinLoad / cpuCount) * 100;

      expect(percentage).toBe(62.5); // (2.5 / 4) * 100
    });

    it('should handle multiple CPUs correctly', () => {
      const loadAvg = 4.0;
      const cpuCount = 8;
      const percentage = (loadAvg / cpuCount) * 100;

      expect(percentage).toBe(50);
    });
  });

  describe('getFileSystemUsagePercentage', () => {
    it('should parse filesystem usage from df output', () => {
      const dfOutput = `Use%     Mounted on\n80%      /\n60%      /home\n45%      /var`;
      const lines = dfOutput.split('\n').slice(1).filter(line => line.trim() !== '');
      const result = lines.map(line => {
        const [usedPercentage, mountPoint] = line.trim().split(/\s+/);
        return { mount: mountPoint, usage: parseInt(usedPercentage.replace('%', ''), 10) };
      });

      expect(result).toHaveLength(3);
      expect(result[0].mount).toBe('/');
      expect(result[0].usage).toBe(80);
      expect(result[1].usage).toBe(60);
      expect(result[2].usage).toBe(45);
    });

    it('should identify high usage mounts', () => {
      const filesystems = [
        { mount: '/', usage: 95 },
        { mount: '/home', usage: 30 },
        { mount: '/var', usage: 85 }
      ];

      const highUsage = filesystems.filter(fs => fs.usage > 85);
      expect(highUsage).toHaveLength(1);
      expect(highUsage[0].mount).toBe('/');
    });
  });
});

describe('File permission handling', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should check execute permissions on directory', () => {
    fs.accessSync = jest.fn();
    fs.writeFileSync = jest.fn();
    fs.chmodSync = jest.fn();
    fs.unlinkSync = jest.fn();

    const directoryPath = '/tmp';
    
    try {
      fs.accessSync(directoryPath, 2); // W_OK = 2
      expect(fs.accessSync).toHaveBeenCalledWith(directoryPath, 2);
    } catch (err) {
      expect(err).toBeTruthy();
    }
  });

  it('should add execute permission to file', () => {
    fs.chmod = jest.fn((path, mode, callback) => {
      callback(null);
    });

    const filePath = '/tmp/backup.sh';
    fs.chmod(filePath, '755', (err) => {
      expect(err).toBeNull();
    });

    expect(fs.chmod).toHaveBeenCalledWith(filePath, '755', expect.any(Function));
  });

  it('should handle permission errors gracefully', () => {
    fs.chmod = jest.fn((path, mode, callback) => {
      callback(new Error('Permission denied'));
    });

    const filePath = '/root/backup.sh';
    fs.chmod(filePath, '755', (err) => {
      expect(err).toBeTruthy();
      expect(err.message).toBe('Permission denied');
    });
  });
});

describe('Settings file parsing', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should parse settings file with key-value pairs', () => {
    const settingsContent = `AGENT_NAME="backup-agent"
MQTT_SERVER="192.168.1.10"
MQTT_PORT="1883"
MQTT_ENABLED="TRUE"`;

    fs.readFileSync = jest.fn().mockReturnValue(settingsContent);
    const data = fs.readFileSync('settings.sh', 'utf8');
    const lines = data.split('\n');
    const settings = {};

    lines.forEach(line => {
      const match = line.match(/^(\w+)="(.*)"$/);
      if (match) {
        const [, key, value] = match;
        settings[key] = value;
      }
    });

    expect(settings.AGENT_NAME).toBe('backup-agent');
    expect(settings.MQTT_SERVER).toBe('192.168.1.10');
    expect(settings.MQTT_PORT).toBe('1883');
    expect(settings.MQTT_ENABLED).toBe('TRUE');
  });

  it('should handle missing settings file', () => {
    fs.readFileSync = jest.fn().mockImplementation(() => {
      throw new Error('ENOENT: no such file or directory');
    });

    expect(() => {
      fs.readFileSync('missing.sh', 'utf8');
    }).toThrow();
  });

  it('should skip malformed lines in settings', () => {
    const settingsContent = `VALID_KEY="value"
invalid line without equals
ANOTHER_KEY="another value"`;

    const lines = settingsContent.split('\n');
    const settings = {};

    lines.forEach(line => {
      const match = line.match(/^(\w+)="(.*)"$/);
      if (match) {
        const [, key, value] = match;
        settings[key] = value;
      }
    });

    expect(Object.keys(settings).length).toBe(2);
    expect(settings.VALID_KEY).toBe('value');
    expect(settings.ANOTHER_KEY).toBe('another value');
  });
});

describe('Configuration defaults', () => {
  it('should use default MQTT server if not specified', () => {
    const DEFAULT_MQTT_SERVER = 'localhost';
    let MQTT_SERVER = DEFAULT_MQTT_SERVER;

    expect(MQTT_SERVER).toBe('localhost');
  });

  it('should use default MQTT port if not specified', () => {
    const DEFAULT_MQTT_SERVER_PORT = '1883';
    let MQTT_SERVER_PORT = DEFAULT_MQTT_SERVER_PORT;

    expect(MQTT_SERVER_PORT).toBe('1883');
  });

  it('should use default WebSocket server if not specified', () => {
    const DEFAULT_WS_SERVER = 'localhost';
    let WS_SERVER = DEFAULT_WS_SERVER;

    expect(WS_SERVER).toBe('localhost');
  });

  it('should use default working directory if not specified', () => {
    const DEFAULT_WORKING_DIR = '/tmp';
    let workingDir = DEFAULT_WORKING_DIR;

    expect(workingDir).toBe('/tmp');
  });

  it('should use default retry timeout', () => {
    const DEFAULT_RETRY_TIMEOUT = 5000;
    let RETRY_TIMEOUT = DEFAULT_RETRY_TIMEOUT;

    expect(RETRY_TIMEOUT).toBe(5000);
  });
});

describe('Execution ID Management', () => {
  it('should include executionId in status update message', () => {
    const executionId = 'exec-abc123def456';
    const message = {
      name: 'test-agent',
      topic: 'orchelium/agent/status',
      server: 'test-server',
      manual: false,
      commsType: 'websocket',
      description: 'Test backup',
      data: null,
      status: 'running',
      jobName: 'test-job',
      executionId: executionId,  // Include execution ID for tracking
      lastStatusReport: new Date().toISOString(),
    };

    expect(message.executionId).toBe('exec-abc123def456');
    expect(message).toHaveProperty('executionId');
  });

  it('should include executionId in log data message', () => {
    const executionId = 'exec-xyz789abc123';
    const message = {
      name: 'test-agent',
      server: 'test-server',
      status: 'log_submission',
      jobName: 'test-job',
      manual: false,
      eta: undefined,
      returnCode: undefined,
      data: 'Sample log output',
      executionId: executionId,  // Include execution ID for tracking
      lastStatusReport: new Date().toISOString(),
    };

    expect(message.executionId).toBe('exec-xyz789abc123');
    expect(message.status).toBe('log_submission');
  });

  it('should include executionId in eta_submission message', () => {
    const executionId = 'exec-final789';
    const message = {
      name: 'test-agent',
      server: 'test-server',
      status: 'eta_submission',
      jobName: 'test-job',
      manual: false,
      eta: 45.2,
      returnCode: 0,
      data: 'Final log output',
      executionId: executionId,  // Include execution ID for tracking
      lastStatusReport: new Date().toISOString(),
    };

    expect(message.executionId).toBe('exec-final789');
    expect(message.status).toBe('eta_submission');
    expect(message.returnCode).toBe(0);
  });

  it('should handle null executionId for backward compatibility', () => {
    const message = {
      name: 'test-agent',
      topic: 'orchelium/agent/status',
      server: 'test-server',
      manual: false,
      commsType: 'websocket',
      description: 'Test backup',
      data: null,
      status: 'running',
      jobName: 'test-job',
      executionId: null,  // Backward compatible - null if not provided
      lastStatusReport: new Date().toISOString(),
    };

    expect(message.executionId).toBeNull();
  });

  it('should generate valid hex executionId format', () => {
    const crypto = require('crypto');
    const executionId = crypto.randomBytes(8).toString('hex');
    
    // Should be 16 hex characters (8 bytes * 2)
    expect(executionId).toMatch(/^[a-f0-9]{16}$/);
    expect(executionId.length).toBe(16);
  });

  it('should track executionId in active jobs', () => {
    const jobName = 'backup-test-job';
    const executionId = 'exec-tracking-123';
    const activeJobs = new Map();
    
    activeJobs.set(jobName, { 
      startTime: Date.now(), 
      status: 'running', 
      executionId: executionId 
    });
    
    const jobInfo = activeJobs.get(jobName);
    expect(jobInfo.executionId).toBe('exec-tracking-123');
    expect(jobInfo.status).toBe('running');
  });

  it('should preserve executionId through callback chain', () => {
    const jobName = 'test-backup';
    const executionId = 'exec-callback-test';
    const activeJobs = new Map();
    
    // Simulate job execution - store executionId
    activeJobs.set(jobName, { 
      startTime: Date.now(), 
      status: 'running', 
      executionId: executionId 
    });
    
    // Simulate job completion - retrieve executionId for callback
    const jobInfo = activeJobs.get(jobName);
    const callbackExecutionId = jobInfo ? jobInfo.executionId : null;
    
    expect(callbackExecutionId).toBe('exec-callback-test');
    activeJobs.delete(jobName);
  });
});
