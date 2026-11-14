// Mock setup for orchestrationEngine module tests

// Mock db module before importing orchestrationEngine
jest.mock('../../db.js', () => ({
  getData: jest.fn(),
  putData: jest.fn(),
  deleteData: jest.fn(),
}));

// Mock fs module before importing orchestrationEngine
jest.mock('fs', () => {
  const actualFs = jest.requireActual('fs');
  return {
    ...actualFs,
    promises: {
      readFile: jest.fn(),
      writeFile: jest.fn(),
    },
  };
});

// Mock agents module before importing orchestrationEngine
jest.mock('../../agents.js', () => ({
  getAgent: jest.fn(),
  getDict: jest.fn(),
}));

// Mock wsBrowserTransport before importing orchestrationEngine
jest.mock('../../communications/wsBrowserTransport.js', () => ({
  emitOrchestrationEvent: jest.fn(),
  emitNotification: jest.fn(),
}));

// Mock history module before importing orchestrationEngine
jest.mock('../../history.js', () => ({
  add: jest.fn(),
  createHistoryItem: jest.fn(),
  getItems: jest.fn(),
}));

// Mock notify module before importing orchestrationEngine
jest.mock('../../notify.js', () => ({
  sendNotification: jest.fn(),
}));

// Mock orchestrationMonitor module before importing orchestrationEngine
jest.mock('../../orchestrationMonitor.js', () => ({
  getJobDefinitionVersion: jest.fn(),
}));

// Mock configuration module before importing orchestrationEngine
jest.mock('../../configuration.js', () => ({
  getConfig: jest.fn(),
}));

// Mock definitionStore module before importing orchestrationEngine
jest.mock('../../definitionStore.js', () => ({
  getOrchestration: jest.fn(),
  getBackend: jest.fn(),
}));

// Mock axios for HTTP execute node tests
jest.mock('axios', () => jest.fn());

const db = require('../../db.js');
const wsBrowserTransport = require('../../communications/wsBrowserTransport.js');
const notifier = require('../../notify.js');
const orchestrationMonitor = require('../../orchestrationMonitor.js');
const configuration = require('../../configuration.js');
const definitionStore = require('../../definitionStore.js');
const history = require('../../history.js');
const fs = require('fs');
const axios = require('axios');

describe('Orchestration Engine Module', () => {
  let orchestrationEngine;

  beforeEach(() => {
    jest.clearAllMocks();

    // Set up global mocks
    global.logger = {
      info: jest.fn(),
      debug: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    };

    global.serverConfig = {
      server: {
        timezone: 'Europe/London',
      },
    };

    global.db = db;
    
    // Mock db functions
    db.getData = jest.fn();
    db.putData = jest.fn();
    db.deleteData = jest.fn();

    global.agentComms = {
      sendCommand: jest.fn(),
    };

    global.wsBrowser = wsBrowserTransport;

    // Setup notifier mock
    notifier.sendNotification = jest.fn();

    // Setup history mocks
    history.add = jest.fn();
    history.createHistoryItem = jest.fn().mockImplementation((jobName, runDate, returnCode, runTime, log, manual, executionId, rerunFrom) => ({
      jobName,
      runDate,
      returnCode,
      runTime,
      log,
      manual,
      executionId,
      rerunFrom,
    }));
    history.getItems = jest.fn().mockReturnValue([]);

    // Setup orchestrationMonitor mock
    orchestrationMonitor.getJobDefinitionVersion = jest.fn();

    // Setup configuration mock
    configuration.getConfig = jest.fn().mockReturnValue(global.serverConfig);

    // Bridge definitionStore reads to existing db fixtures used in these tests
    definitionStore.getBackend = jest.fn().mockReturnValue('db');
    definitionStore.getOrchestration = jest.fn().mockImplementation(async (jobId) => {
      try {
        const all = await db.getData('ORCHESTRATION_JOBS');
        return (all && all[jobId]) ? all[jobId] : null;
      } catch (_err) {
        return null;
      }
    });

    // Import orchestrationEngine module (only once after mocks are set up)
    if (!orchestrationEngine) {
      orchestrationEngine = require('../../orchestrationEngine.js');
    }
  });

  afterEach(async () => {
    jest.clearAllMocks();
    // Wait for pending timers/promises to settle
    await new Promise(resolve => setImmediate(resolve));
    delete global.logger;
    delete global.serverConfig;
    delete global.db;
    delete global.agentComms;
    delete global.wsBrowser;
  });

  describe('waitForScriptCompletion()', () => {
    it('should wait for script completion and resolve with result', async () => {
      const completionPromise = orchestrationEngine.waitForScriptCompletion('job1', 5000);

      // Simulate script completion signal
      setTimeout(() => {
        orchestrationEngine.signalScriptCompletion('job1', {
          exitCode: 0,
          stdout: 'Script completed',
          stderr: '',
        });
      }, 100);

      const result = await completionPromise;

      expect(result).toHaveProperty('exitCode', 0);
      expect(result).toHaveProperty('stdout', 'Script completed');
    });

    it('should reject on timeout', async () => {
      await expect(orchestrationEngine.waitForScriptCompletion('job1', 100)).rejects.toThrow(
        'Script execution timeout'
      );
    });

    it('should handle multiple concurrent script completions', async () => {
      const promise1 = orchestrationEngine.waitForScriptCompletion('job1', 5000);
      const promise2 = orchestrationEngine.waitForScriptCompletion('job2', 5000);

      setTimeout(() => {
        orchestrationEngine.signalScriptCompletion('job1', { exitCode: 0, stdout: 'Job1' });
        orchestrationEngine.signalScriptCompletion('job2', { exitCode: 1, stdout: 'Job2' });
      }, 100);

      const [result1, result2] = await Promise.all([promise1, promise2]);

      expect(result1.exitCode).toBe(0);
      expect(result2.exitCode).toBe(1);
    });

    it('should handle concurrent executions of the SAME job with different executionIds', async () => {
      // Simulate two concurrent executions of the same orchestration job with different executionIds
      // This proves the fix for the concurrency bug where jobName must include executionId
      const jobName1 = 'Orchestration [test-job] Execution [exec-uuid-1] Node [node-1]';
      const jobName2 = 'Orchestration [test-job] Execution [exec-uuid-2] Node [node-1]';

      const promise1 = orchestrationEngine.waitForScriptCompletion(jobName1, 5000);
      const promise2 = orchestrationEngine.waitForScriptCompletion(jobName2, 5000);

      // Simulate completions in reverse order to ensure they don't cross-talk
      setTimeout(() => {
        orchestrationEngine.signalScriptCompletion(jobName2, { 
          exitCode: 1, 
          stdout: 'Output from execution 2',
          stderr: '' 
        });
        orchestrationEngine.signalScriptCompletion(jobName1, { 
          exitCode: 0, 
          stdout: 'Output from execution 1',
          stderr: '' 
        });
      }, 100);

      const [result1, result2] = await Promise.all([promise1, promise2]);

      // Verify each execution got its own result (not cross-talked)
      expect(result1.exitCode).toBe(0);
      expect(result1.stdout).toBe('Output from execution 1');
      expect(result2.exitCode).toBe(1);
      expect(result2.stdout).toBe('Output from execution 2');
    });
  });

  describe('signalScriptCompletion()', () => {
    it('should signal successful script completion', () => {
      const completionPromise = orchestrationEngine.waitForScriptCompletion('job1', 5000);

      orchestrationEngine.signalScriptCompletion('job1', {
        exitCode: 0,
        stdout: 'Success',
      });

      return expect(completionPromise).resolves.toEqual(
        expect.objectContaining({ exitCode: 0, stdout: 'Success' })
      );
    });

    it('should ignore signal if no pending execution', () => {
      // Should not throw, just log warning
      expect(() => {
        orchestrationEngine.signalScriptCompletion('nonexistent', { exitCode: 0 });
      }).not.toThrow();

      expect(logger.warn).toHaveBeenCalled();
    });
  });

  describe('evaluateNumericCondition()', () => {
    it('should evaluate equality operator ==', () => {
      expect(orchestrationEngine.evaluateNumericCondition(10, '==', 10)).toBe(true);
      expect(orchestrationEngine.evaluateNumericCondition(10, '==', 5)).toBe(false);
    });

    it('should evaluate alternative equality operator =', () => {
      expect(orchestrationEngine.evaluateNumericCondition(10, '=', 10)).toBe(true);
      expect(orchestrationEngine.evaluateNumericCondition(10, '=', 5)).toBe(false);
    });

    it('should evaluate not equal operator !=', () => {
      expect(orchestrationEngine.evaluateNumericCondition(10, '!=', 5)).toBe(true);
      expect(orchestrationEngine.evaluateNumericCondition(10, '!=', 10)).toBe(false);
    });

    it('should evaluate greater than operator >', () => {
      expect(orchestrationEngine.evaluateNumericCondition(15, '>', 10)).toBe(true);
      expect(orchestrationEngine.evaluateNumericCondition(5, '>', 10)).toBe(false);
      expect(orchestrationEngine.evaluateNumericCondition(10, '>', 10)).toBe(false);
    });

    it('should evaluate greater than or equal operator >=', () => {
      expect(orchestrationEngine.evaluateNumericCondition(15, '>=', 10)).toBe(true);
      expect(orchestrationEngine.evaluateNumericCondition(10, '>=', 10)).toBe(true);
      expect(orchestrationEngine.evaluateNumericCondition(5, '>=', 10)).toBe(false);
    });

    it('should evaluate less than operator <', () => {
      expect(orchestrationEngine.evaluateNumericCondition(5, '<', 10)).toBe(true);
      expect(orchestrationEngine.evaluateNumericCondition(15, '<', 10)).toBe(false);
      expect(orchestrationEngine.evaluateNumericCondition(10, '<', 10)).toBe(false);
    });

    it('should evaluate less than or equal operator <=', () => {
      expect(orchestrationEngine.evaluateNumericCondition(5, '<=', 10)).toBe(true);
      expect(orchestrationEngine.evaluateNumericCondition(10, '<=', 10)).toBe(true);
      expect(orchestrationEngine.evaluateNumericCondition(15, '<=', 10)).toBe(false);
    });

    it('should support negated operators with ! prefix', () => {
      expect(orchestrationEngine.evaluateNumericCondition(10, '!=', 5)).toBe(true);
      expect(orchestrationEngine.evaluateNumericCondition(15, '!>', 10)).toBe(false);
      expect(orchestrationEngine.evaluateNumericCondition(5, '!>', 10)).toBe(true);
      expect(orchestrationEngine.evaluateNumericCondition(10, '!==', 10)).toBe(false);
      expect(orchestrationEngine.evaluateNumericCondition(10, '!==', 5)).toBe(true);
    });

    it('should handle string numbers via parseFloat', () => {
      expect(orchestrationEngine.evaluateNumericCondition('10', '==', '10')).toBe(true);
      expect(orchestrationEngine.evaluateNumericCondition('15.5', '>', '10.5')).toBe(true);
      expect(orchestrationEngine.evaluateNumericCondition('5.5', '<', '10.5')).toBe(true);
    });

    it('should default unknown operators to equality', () => {
      expect(orchestrationEngine.evaluateNumericCondition(10, 'unknown', 10)).toBe(true);
      expect(orchestrationEngine.evaluateNumericCondition(10, 'unknown', 5)).toBe(false);
      expect(logger.warn).toHaveBeenCalledWith('Unknown operator: unknown, defaulting to ==');
    });
  });

  describe('getExecutionHistory()', () => {
    it('should retrieve execution history for a job', async () => {
      const mockExecutions = {
        job1: [
          {
            jobId: 'job1',
            executionId: 'exec1',
            status: 'completed',
            finalStatus: 'success',
          },
        ],
      };
      db.getData.mockResolvedValue(mockExecutions);

      const result = await orchestrationEngine.getExecutionHistory('job1');

      expect(db.getData).toHaveBeenCalledWith('ORCHESTRATION_EXECUTIONS');
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(1);
    });

    it('should return empty array when no history exists', async () => {
      db.getData.mockRejectedValue(new Error('NotFoundError: Key not found'));

      const result = await orchestrationEngine.getExecutionHistory('job1');

      expect(result).toEqual([]);
    });

    it('should return empty array for job with no executions', async () => {
      db.getData.mockResolvedValue({});

      const result = await orchestrationEngine.getExecutionHistory('job1');

      expect(result).toEqual([]);
    });

    it('should throw non-NotFoundError exceptions', async () => {
      db.getData.mockRejectedValue(new Error('Database connection failed'));

      await expect(orchestrationEngine.getExecutionHistory('job1')).rejects.toThrow(
        'Database connection failed'
      );
    });
  });

  describe('saveExecutionResult()', () => {
    it('should save execution result to database', async () => {
      db.getData.mockResolvedValue({});

      const executionLog = {
        jobId: 'job1',
        executionId: 'exec1',
        status: 'completed',
        finalStatus: 'success',
        startTime: new Date().toISOString(),
        endTime: new Date().toISOString(),
      };

      await orchestrationEngine.saveExecutionResult(executionLog);

      expect(db.putData).toHaveBeenCalled();
      const savedData = db.putData.mock.calls[0][1];
      expect(savedData.job1).toBeDefined();
      expect(savedData.job1.length).toBe(1);
      expect(savedData.job1[0].executionId).toBe('exec1');
    });

    it('should initialize execution data if not found', async () => {
      db.getData.mockRejectedValueOnce(new Error('NotFoundError: Key not found'))
        .mockResolvedValueOnce({}); // Second call for subsequent saves

      const executionLog = {
        jobId: 'job1',
        executionId: 'exec1',
        status: 'completed',
        finalStatus: 'success',
      };

      await orchestrationEngine.saveExecutionResult(executionLog);

      expect(db.putData).toHaveBeenCalled();
    });

    it('should limit execution history to last 100 entries', async () => {
      // Create mock data with 101 existing executions
      const existingExecutions = {
        job1: Array.from({ length: 100 }, (_, i) => ({
          jobId: 'job1',
          executionId: `exec${i}`,
          finalStatus: 'success',
        })),
      };

      db.getData.mockResolvedValue(existingExecutions);

      const newExecutionLog = {
        jobId: 'job1',
        executionId: 'exec101',
        status: 'completed',
        finalStatus: 'success',
      };

      await orchestrationEngine.saveExecutionResult(newExecutionLog);

      const savedData = db.putData.mock.calls[0][1];
      expect(savedData.job1.length).toBe(100); // Should keep only last 100
      expect(savedData.job1[99].executionId).toBe('exec101'); // New one should be last
    });

    it('should handle database errors gracefully', async () => {
      db.getData.mockRejectedValue(new Error('Database connection failed'));

      const executionLog = {
        jobId: 'job1',
        executionId: 'exec1',
        finalStatus: 'success',
      };

      // Should not throw, should log error
      await expect(
        orchestrationEngine.saveExecutionResult(executionLog)
      ).resolves.toBeUndefined();

      expect(logger.error).toHaveBeenCalled();
    });

    it('should maintain multiple jobs execution history', async () => {
      db.getData.mockResolvedValue({
        job1: [{ jobId: 'job1', executionId: 'exec1' }],
      });

      const newExecution = {
        jobId: 'job2',
        executionId: 'exec2',
        finalStatus: 'success',
      };

      await orchestrationEngine.saveExecutionResult(newExecution);

      const savedData = db.putData.mock.calls[0][1];
      expect(savedData.job1).toBeDefined();
      expect(savedData.job2).toBeDefined();
      expect(savedData.job2[0].executionId).toBe('exec2');
    });

    it('should send notification when orchestration fails with jobFailEnabled true', async () => {
      db.getData.mockResolvedValue({});
      orchestrationMonitor.getJobDefinitionVersion.mockResolvedValue({
        id: 'job1',
        name: 'My Orchestration',
      });
      global.serverConfig.server.jobFailEnabled = 'true';

      const executionLog = {
        jobId: 'job1',
        executionId: 'exec1',
        status: 'completed',
        finalStatus: 'failure',
        visitedNodes: ['start', 'execute1'],
        errors: [{ node: 'execute1', message: 'Script failed' }],
        scriptOutputs: {
          execute1: {
            exitCode: 1,
            stdout: 'Error output',
            stderr: '',
          },
        },
        endTime: new Date().toISOString(),
      };

      await orchestrationEngine.saveExecutionResult(executionLog);

      expect(notifier.sendNotification).toHaveBeenCalledWith(
        'My Orchestration - Orchestration Failed',
        expect.stringContaining('Node [execute1] failed'),
        'WARNING',
        expect.stringContaining('/orchestration/monitor.html?jobId=job1&executionId=exec1')
      );
    });

    it('should not send notification when orchestration succeeds', async () => {
      db.getData.mockResolvedValue({});
      orchestrationMonitor.getJobDefinitionVersion.mockResolvedValue({
        id: 'job1',
        name: 'My Orchestration',
      });
      global.serverConfig.server.jobFailEnabled = 'true';

      const executionLog = {
        jobId: 'job1',
        executionId: 'exec1',
        status: 'completed',
        finalStatus: 'success',
        visitedNodes: ['start', 'execute1', 'end'],
        endTime: new Date().toISOString(),
      };

      await orchestrationEngine.saveExecutionResult(executionLog);

      expect(notifier.sendNotification).not.toHaveBeenCalled();
    });

    it('should not send notification when jobFailEnabled is false', async () => {
      db.getData.mockResolvedValue({});
      orchestrationMonitor.getJobDefinitionVersion.mockResolvedValue({
        id: 'job1',
        name: 'My Orchestration',
      });
      global.serverConfig.server.jobFailEnabled = 'false';

      const executionLog = {
        jobId: 'job1',
        executionId: 'exec1',
        status: 'completed',
        finalStatus: 'failure',
        visitedNodes: ['start', 'execute1'],
        errors: [{ node: 'execute1', message: 'Script failed' }],
        endTime: new Date().toISOString(),
      };

      await orchestrationEngine.saveExecutionResult(executionLog);

      expect(notifier.sendNotification).not.toHaveBeenCalled();
    });

    it('should include execution ID in notification', async () => {
      db.getData.mockResolvedValue({});
      orchestrationMonitor.getJobDefinitionVersion.mockResolvedValue({
        id: 'job1',
        name: 'Test Orchestration',
      });
      global.serverConfig.server.jobFailEnabled = 'true';

      const executionLog = {
        jobId: 'job1',
        executionId: 'abc123def456',
        status: 'completed',
        finalStatus: 'failure',
        visitedNodes: ['start', 'execute1'],
        errors: [{ node: 'execute1', message: 'Node failed' }],
        endTime: new Date().toISOString(),
      };

      await orchestrationEngine.saveExecutionResult(executionLog);

      const callArgs = notifier.sendNotification.mock.calls[0];
      expect(callArgs[1]).toContain('abc123def456');
      expect(callArgs[3]).toContain('abc123def456');
    });

    it('should use exit code in root cause when no errors array', async () => {
      db.getData.mockResolvedValue({});
      orchestrationMonitor.getJobDefinitionVersion.mockResolvedValue({
        id: 'job1',
        name: 'Test Orchestration',
      });
      global.serverConfig.server.jobFailEnabled = 'true';

      const executionLog = {
        jobId: 'job1',
        executionId: 'exec1',
        status: 'completed',
        finalStatus: 'failure',
        visitedNodes: ['start', 'execute1'],
        errors: [],
        scriptOutputs: {
          execute1: {
            exitCode: 2,
            stdout: 'Failed',
            stderr: '',
          },
        },
        endTime: new Date().toISOString(),
      };

      await orchestrationEngine.saveExecutionResult(executionLog);

      const callArgs = notifier.sendNotification.mock.calls[0];
      expect(callArgs[1]).toContain('exit code 2');
    });

    it('should handle notification errors gracefully', async () => {
      db.getData.mockResolvedValue({});
      orchestrationMonitor.getJobDefinitionVersion.mockResolvedValue({
        id: 'job1',
        name: 'Test Orchestration',
      });
      global.serverConfig.server.jobFailEnabled = 'true';
      notifier.sendNotification.mockImplementation(() => {
        throw new Error('Notification service unavailable');
      });

      const executionLog = {
        jobId: 'job1',
        executionId: 'exec1',
        status: 'completed',
        finalStatus: 'failure',
        visitedNodes: ['start', 'execute1'],
        errors: [{ node: 'execute1', message: 'Failed' }],
        endTime: new Date().toISOString(),
      };

      // Should not throw, should log warning
      await expect(
        orchestrationEngine.saveExecutionResult(executionLog)
      ).resolves.toBeUndefined();

      expect(logger.warn).toHaveBeenCalledWith(
        expect.stringContaining('Failed to send orchestration failure notification')
      );
    });

    it('should create synthetic node history when script is missing before agent execution', async () => {
      db.getData.mockResolvedValue({});
      orchestrationMonitor.getJobDefinitionVersion.mockResolvedValue({
        id: 'job1',
        name: 'Missing Script Workflow',
      });
      global.serverConfig.server.jobFailEnabled = 'true';

      const executionLog = {
        jobId: 'job1',
        executionId: 'exec-missing-script',
        status: 'failed',
        finalStatus: 'error',
        manual: true,
        startTime: new Date().toISOString(),
        endTime: new Date().toISOString(),
        scriptOutputs: {
          'node-2': {
            script: 'testnew.sh',
            exitCode: 1,
            stdout: 'Script cannot be found: [./scripts/testnew.sh]',
            stderr: 'Script cannot be found: [./scripts/testnew.sh]',
            startTime: new Date().toISOString(),
            endTime: new Date().toISOString(),
          },
        },
      };

      await orchestrationEngine.saveExecutionResult(executionLog);

      expect(history.createHistoryItem).toHaveBeenCalledWith(
        'Orchestration [job1] Execution [exec-missing-script] Node [node-2]',
        expect.any(String),
        1,
        expect.any(Number),
        expect.stringContaining('Script cannot be found'),
        true,
        'exec-missing-script',
        null
      );
      expect(history.add).toHaveBeenCalled();
    });
  });

  describe('getExecutionIndexById()', () => {
    it('should find execution by ID', async () => {
      const mockExecutions = [
        { executionId: 'exec1', finalStatus: 'success' },
        { executionId: 'exec2', finalStatus: 'failed' },
        { executionId: 'exec3', finalStatus: 'success' },
      ];

      db.getData.mockResolvedValue({ job1: mockExecutions });

      const index = await orchestrationEngine.getExecutionIndexById('job1', 'exec2');

      expect(index).toBe(1);
    });

    it('should return -1 when execution not found', async () => {
      const mockExecutions = [
        { executionId: 'exec1', finalStatus: 'success' },
      ];

      db.getData.mockResolvedValue({ job1: mockExecutions });

      const index = await orchestrationEngine.getExecutionIndexById('job1', 'nonexistent');

      expect(index).toBe(-1);
    });

    it('should return -1 when history is empty', async () => {
      db.getData.mockResolvedValue({});

      const index = await orchestrationEngine.getExecutionIndexById('job1', 'exec1');

      expect(index).toBe(-1);
    });

    it('should handle database errors gracefully', async () => {
      db.getData.mockRejectedValue(new Error('Database error'));

      const index = await orchestrationEngine.getExecutionIndexById('job1', 'exec1');

      expect(index).toBe(-1);
      expect(logger.error).toHaveBeenCalled();
    });
  });

  describe('activeOrchestrationExecutions tracking', () => {
    it('should expose activeOrchestrationExecutions object', () => {
      expect(orchestrationEngine.activeOrchestrationExecutions).toBeDefined();
      expect(typeof orchestrationEngine.activeOrchestrationExecutions).toBe('object');
    });
  });

  describe('executeJob()', () => {
    // Helper function to create a mock orchestration job
    const createMockJob = (override = {}) => {
      const baseJob = {
        id: 'test-job',
        name: 'Test Orchestration',
        currentVersion: 1,
        versions: [
          {
            nodes: [
              { id: 'start-node', type: 'start', data: { name: 'Start' } },
              { id: 'execute-node', type: 'execute', data: { script: 'test.sh', agent: 'agent1', parameters: '' } },
              { id: 'end-node', type: 'end-success', data: { name: 'End Success' } },
            ],
            edges: [
              { from: 'start-node', fromPort: 'out', to: 'execute-node' },
              { from: 'execute-node', fromPort: 'out', to: 'end-node' },
            ],
          },
        ],
      };
      return { ...baseJob, ...override };
    };

    beforeEach(() => {
      // Mock fs.readFile for script reading
      require('fs').promises.readFile.mockResolvedValue('#!/bin/bash\necho "test"');
      
      // Mock agents.getAgent - MUST include status: 'online' or orchestrationEngine treats it as offline
      const agents = require('../../agents.js');
      agents.getAgent.mockReturnValue({ name: 'test-agent', id: 'agent1', status: 'online' });
      agents.getDict.mockReturnValue({ agent1: { name: 'test-agent', status: 'online' } });
    });

    describe('Happy Path Scenarios', () => {
      it('should execute simple start -> execute -> end-success path', async () => {
        const mockJob = createMockJob();
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        // Mock script completion signal to happen after execution
        setTimeout(() => {
          const jobName = 'Orchestration [test-job] Execution [test-exec1] Node [execute-node]';
          orchestrationEngine.signalScriptCompletion(jobName, {
            exitCode: 0,
            stdout: 'Script output',
            stderr: '',
          });
        }, 50);

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.jobId).toBe('test-job');
        expect(result.executionId).toBe('test-exec1');
        expect(result.status).toBe('completed');
        expect(result.finalStatus).toBe('success');
        expect(result.visitedNodes).toContain('start-node');
        expect(result.visitedNodes).toContain('execute-node');
        expect(result.visitedNodes).toContain('end-node');
        expect(result.scriptOutputs['execute-node']).toBeDefined();
        expect(result.scriptOutputs['execute-node'].exitCode).toBe(0);
      });

      it('should emit orchestrationNodeStarted and orchestrationNodeCompleted events for each node', async () => {
        const mockJob = createMockJob();
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        // Reset the mock to ensure clean call tracking
        wsBrowserTransport.emitOrchestrationEvent.mockClear();

        setTimeout(() => {
          const jobName = 'Orchestration [test-job] Execution [test-exec1] Node [execute-node]';
          orchestrationEngine.signalScriptCompletion(jobName, {
            exitCode: 0,
            stdout: 'Script output',
            stderr: '',
          });
        }, 50);

        await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        // At least some events should have been emitted
        expect(wsBrowserTransport.emitOrchestrationEvent).toHaveBeenCalled();
        
        // Verify events included start and completion for nodes
        const calls = wsBrowserTransport.emitOrchestrationEvent.mock.calls;
        
        // Should have calls with orchestrationNodeStarted
        const hasNodeStarted = calls.some(call => call[2] === 'orchestrationNodeStarted');
        expect(hasNodeStarted).toBe(true);
        
        // Should have calls with orchestrationNodeCompleted
        const hasNodeCompleted = calls.some(call => call[2] === 'orchestrationNodeCompleted');
        expect(hasNodeCompleted).toBe(true);
      });

      it('should send script content and parameters to agent', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
                { 
                  id: 'execute-node', 
                  type: 'execute', 
                  data: { 
                    script: 'backup.sh', 
                    agent: 'agent1', 
                    parameters: '--incremental --compress' 
                  } 
                },
                { id: 'end-node', type: 'end-success', data: { name: 'End Success' } },
              ],
              edges: [
                { from: 'start-node', fromPort: 'out', to: 'execute-node' },
                { from: 'execute-node', fromPort: 'out', to: 'end-node' },
              ],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        setTimeout(() => {
          const jobName = 'Orchestration [test-job] Execution [test-exec1] Node [execute-node]';
          orchestrationEngine.signalScriptCompletion(jobName, {
            exitCode: 0,
            stdout: 'Backup complete',
            stderr: '',
          });
        }, 50);

        await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        // Verify script content was sent to agent
        expect(global.agentComms.sendCommand).toHaveBeenCalledWith(
          'agent1',
          'execute/orchestrationScript',
          '#!/bin/bash\necho "test"',
          '--incremental --compress',
          expect.stringContaining('[test-job]'),
          undefined,
          false,
          'test-exec1'
        );
      });

      it('should clear old logs before executing script', async () => {
        const mockJob = createMockJob();
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        setTimeout(() => {
          const jobName = 'Orchestration [test-job] Execution [test-exec1] Node [execute-node]';
          orchestrationEngine.signalScriptCompletion(jobName, {
            exitCode: 0,
            stdout: 'Output',
            stderr: '',
          });
        }, 50);

        await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        // Verify deleteData was called to clear old logs
        expect(db.deleteData).toHaveBeenCalledWith(
          expect.stringContaining('test-agent_Orchestration')
        );
      });

      it('should record node metrics (timing) for all nodes', async () => {
        const mockJob = createMockJob();
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        setTimeout(() => {
          const jobName = 'Orchestration [test-job] Execution [test-exec1] Node [execute-node]';
          orchestrationEngine.signalScriptCompletion(jobName, {
            exitCode: 0,
            stdout: 'Output',
            stderr: '',
          });
        }, 50);

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.nodeMetrics['start-node']).toBeDefined();
        expect(result.nodeMetrics['start-node'].duration).toBeGreaterThanOrEqual(0);
        expect(result.nodeMetrics['execute-node']).toBeDefined();
        expect(result.nodeMetrics['end-node']).toBeDefined();
      });

      it('should handle manual execution flag', async () => {
        const mockJob = createMockJob();
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        setTimeout(() => {
          const jobName = 'Orchestration [test-job] Execution [test-exec1] Node [execute-node]';
          orchestrationEngine.signalScriptCompletion(jobName, {
            exitCode: 0,
            stdout: 'Output',
            stderr: '',
          });
        }, 50);

        const result = await orchestrationEngine.executeJob('test-job', true, 'test-exec1');

        expect(result.manual).toBe(true);
        // Verify isManual was passed to agent (it's the 7th parameter)
        expect(global.agentComms.sendCommand).toHaveBeenCalled();
        const callArgs = global.agentComms.sendCommand.mock.calls[0];
        expect(callArgs[6]).toBe(true); // isManual is the 7th parameter (index 6)
      });

      it('should handle multiple execute nodes in sequence', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
                { id: 'execute-1', type: 'execute', data: { script: 'script1.sh', agent: 'agent1' } },
                { id: 'execute-2', type: 'execute', data: { script: 'script2.sh', agent: 'agent1' } },
                { id: 'end-node', type: 'end-success', data: { name: 'End' } },
              ],
              edges: [
                { from: 'start-node', fromPort: 'out', to: 'execute-1' },
                { from: 'execute-1', fromPort: 'out', to: 'execute-2' },
                { from: 'execute-2', fromPort: 'out', to: 'end-node' },
              ],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        // Mock sendCommand to intercept and signal completions
        let sendCommandCallCount = 0;
        global.agentComms.sendCommand.mockImplementation((agentId, command, script, params, jobName) => {
          sendCommandCallCount++;
          // Stagger the responses
          setTimeout(() => {
            orchestrationEngine.signalScriptCompletion(jobName, {
              exitCode: 0,
              stdout: `Execution ${sendCommandCallCount} completed`,
              stderr: '',
            });
          }, 100);
        });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.visitedNodes).toEqual(['start-node', 'execute-1', 'execute-2', 'end-node']);
        expect(result.scriptOutputs['execute-1']).toBeDefined();
        expect(result.scriptOutputs['execute-2']).toBeDefined();
        expect(result.finalStatus).toBe('success');
      }, 30000);
    });

    describe('Condition Node - Return Code Evaluation', () => {
      it('should evaluate return_code condition with == operator', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
                { id: 'execute-node', type: 'execute', data: { script: 'test.sh', agent: 'agent1' } },
                {
                  id: 'condition-node',
                  type: 'condition',
                  data: {
                    conditionType: 'return_code',
                    operator: '==',
                    conditionValue: '0',
                  },
                },
                { id: 'end-success', type: 'end-success', data: { name: 'Success' } },
                { id: 'end-failure', type: 'end-failure', data: { name: 'Failure' } },
              ],
              edges: [
                { from: 'start-node', fromPort: 'out', to: 'execute-node' },
                { from: 'execute-node', fromPort: 'out', to: 'condition-node' },
                { from: 'condition-node', fromPort: 'true', to: 'end-success' },
                { from: 'condition-node', fromPort: 'false', to: 'end-failure' },
              ],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        setTimeout(() => {
          orchestrationEngine.signalScriptCompletion(
            'Orchestration [test-job] Execution [test-exec1] Node [execute-node]',
            { exitCode: 0, stdout: 'Success', stderr: '' }
          );
        }, 50);

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.conditionEvaluations['condition-node']).toBeDefined();
        expect(result.conditionEvaluations['condition-node'].result).toBe(true);
        expect(result.visitedNodes).toContain('end-success');
        expect(result.finalStatus).toBe('success');
      });

      it('should route to false branch when return_code condition fails', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
                { id: 'execute-node', type: 'execute', data: { script: 'test.sh', agent: 'agent1' } },
                {
                  id: 'condition-node',
                  type: 'condition',
                  data: {
                    conditionType: 'return_code',
                    operator: '==',
                    conditionValue: '0',
                  },
                },
                { id: 'end-success', type: 'end-success', data: { name: 'Success' } },
                { id: 'end-failure', type: 'end-failure', data: { name: 'Failure' } },
              ],
              edges: [
                { from: 'start-node', fromPort: 'out', to: 'execute-node' },
                { from: 'execute-node', fromPort: 'out', to: 'condition-node' },
                { from: 'condition-node', fromPort: 'true', to: 'end-success' },
                { from: 'condition-node', fromPort: 'false', to: 'end-failure' },
              ],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        setTimeout(() => {
          orchestrationEngine.signalScriptCompletion(
            'Orchestration [test-job] Execution [test-exec1] Node [execute-node]',
            { exitCode: 1, stdout: 'Failed', stderr: 'Error' }
          );
        }, 50);

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.conditionEvaluations['condition-node'].result).toBe(false);
        expect(result.visitedNodes).toContain('end-failure');
        expect(result.finalStatus).toBe('failure');
      });

      it('should support all comparison operators for return_code', async () => {
        const testCases = [
          { operator: '>', exitCode: 5, conditionValue: '3', expectedResult: true },
          { operator: '<', exitCode: 3, conditionValue: '5', expectedResult: true },
          { operator: '>=', exitCode: 5, conditionValue: '5', expectedResult: true },
          { operator: '<=', exitCode: 5, conditionValue: '5', expectedResult: true },
          { operator: '!=', exitCode: 1, conditionValue: '0', expectedResult: true },
        ];

        for (const testCase of testCases) {
          const mockJob = createMockJob({
            versions: [
              {
                nodes: [
                  { id: 'start-node', type: 'start', data: { name: 'Start' } },
                  { id: 'execute-node', type: 'execute', data: { script: 'test.sh', agent: 'agent1' } },
                  {
                    id: 'condition-node',
                    type: 'condition',
                    data: {
                      conditionType: 'return_code',
                      operator: testCase.operator,
                      conditionValue: testCase.conditionValue,
                    },
                  },
                  { id: 'end-success', type: 'end-success', data: { name: 'Success' } },
                  { id: 'end-failure', type: 'end-failure', data: { name: 'Failure' } },
                ],
                edges: [
                  { from: 'start-node', fromPort: 'out', to: 'execute-node' },
                  { from: 'execute-node', fromPort: 'out', to: 'condition-node' },
                  { from: 'condition-node', fromPort: 'true', to: 'end-success' },
                  { from: 'condition-node', fromPort: 'false', to: 'end-failure' },
                ],
              },
            ],
          });
          db.getData.mockResolvedValue({ 'test-job': mockJob });

          setTimeout(() => {
            orchestrationEngine.signalScriptCompletion(
              'Orchestration [test-job] Execution [test-exec-' + testCase.operator + '] Node [execute-node]',
              { exitCode: testCase.exitCode, stdout: 'Output', stderr: '' }
            );
          }, 50);

          jest.clearAllMocks();
          const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec-' + testCase.operator);

          expect(result.conditionEvaluations['condition-node'].result).toBe(testCase.expectedResult);
        }
      });
    });

    describe('Condition Node - Output Contains Evaluation', () => {
      it('should evaluate output_contains condition when string is present', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
                { id: 'execute-node', type: 'execute', data: { script: 'test.sh', agent: 'agent1' } },
                {
                  id: 'condition-node',
                  type: 'condition',
                  data: {
                    conditionType: 'output_contains',
                    operator: '==',
                    conditionValue: 'SUCCESS',
                  },
                },
                { id: 'end-success', type: 'end-success', data: { name: 'Success' } },
                { id: 'end-failure', type: 'end-failure', data: { name: 'Failure' } },
              ],
              edges: [
                { from: 'start-node', fromPort: 'out', to: 'execute-node' },
                { from: 'execute-node', fromPort: 'out', to: 'condition-node' },
                { from: 'condition-node', fromPort: 'true', to: 'end-success' },
                { from: 'condition-node', fromPort: 'false', to: 'end-failure' },
              ],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        setTimeout(() => {
          orchestrationEngine.signalScriptCompletion(
            'Orchestration [test-job] Execution [test-exec1] Node [execute-node]',
            { exitCode: 0, stdout: 'Backup completed with SUCCESS status', stderr: '' }
          );
        }, 50);

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.conditionEvaluations['condition-node'].result).toBe(true);
        expect(result.finalStatus).toBe('success');
      });

      it('should handle output_contains with negation operator !=', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
                { id: 'execute-node', type: 'execute', data: { script: 'test.sh', agent: 'agent1' } },
                {
                  id: 'condition-node',
                  type: 'condition',
                  data: {
                    conditionType: 'output_contains',
                    operator: '!=',
                    conditionValue: 'ERROR',
                  },
                },
                { id: 'end-success', type: 'end-success', data: { name: 'Success' } },
                { id: 'end-failure', type: 'end-failure', data: { name: 'Failure' } },
              ],
              edges: [
                { from: 'start-node', fromPort: 'out', to: 'execute-node' },
                { from: 'execute-node', fromPort: 'out', to: 'condition-node' },
                { from: 'condition-node', fromPort: 'true', to: 'end-success' },
                { from: 'condition-node', fromPort: 'false', to: 'end-failure' },
              ],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        setTimeout(() => {
          orchestrationEngine.signalScriptCompletion(
            'Orchestration [test-job] Execution [test-exec1] Node [execute-node]',
            { exitCode: 0, stdout: 'Backup completed successfully', stderr: '' }
          );
        }, 50);

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.conditionEvaluations['condition-node'].result).toBe(true);
      });
    });

    describe('Condition Node - Regex Match Evaluation', () => {
      it('should evaluate regex_match condition with valid pattern', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
                { id: 'execute-node', type: 'execute', data: { script: 'test.sh', agent: 'agent1' } },
                {
                  id: 'condition-node',
                  type: 'condition',
                  data: {
                    conditionType: 'regex_match',
                    operator: '==',
                    conditionValue: '^Backup (completed|finished)',
                  },
                },
                { id: 'end-success', type: 'end-success', data: { name: 'Success' } },
                { id: 'end-failure', type: 'end-failure', data: { name: 'Failure' } },
              ],
              edges: [
                { from: 'start-node', fromPort: 'out', to: 'execute-node' },
                { from: 'execute-node', fromPort: 'out', to: 'condition-node' },
                { from: 'condition-node', fromPort: 'true', to: 'end-success' },
                { from: 'condition-node', fromPort: 'false', to: 'end-failure' },
              ],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        setTimeout(() => {
          orchestrationEngine.signalScriptCompletion(
            'Orchestration [test-job] Execution [test-exec1] Node [execute-node]',
            { exitCode: 0, stdout: 'Backup completed successfully', stderr: '' }
          );
        }, 50);

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.conditionEvaluations['condition-node'].result).toBe(true);
        expect(result.finalStatus).toBe('success');
      });

      it('should reject invalid regex patterns', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
                { id: 'execute-node', type: 'execute', data: { script: 'test.sh', agent: 'agent1' } },
                {
                  id: 'condition-node',
                  type: 'condition',
                  data: {
                    conditionType: 'regex_match',
                    operator: '==',
                    conditionValue: '[invalid(regex',
                  },
                },
                { id: 'end-success', type: 'end-success', data: { name: 'Success' } },
              ],
              edges: [
                { from: 'start-node', fromPort: 'out', to: 'execute-node' },
                { from: 'execute-node', fromPort: 'out', to: 'condition-node' },
                { from: 'condition-node', fromPort: 'true', to: 'end-success' },
              ],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        setTimeout(() => {
          orchestrationEngine.signalScriptCompletion(
            'Orchestration [test-job] Execution [test-exec1] Node [execute-node]',
            { exitCode: 0, stdout: 'Output', stderr: '' }
          );
        }, 50);

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.finalStatus).toBe('error');
        expect(result.errors.length).toBeGreaterThan(0);
        expect(result.errors[0].message).toContain('Invalid regex');
      });
    });

    describe('Condition Node - Execution Time Evaluation', () => {
      it('should evaluate execution_time condition', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
                { id: 'execute-node', type: 'execute', data: { script: 'test.sh', agent: 'agent1' } },
                {
                  id: 'condition-node',
                  type: 'condition',
                  data: {
                    conditionType: 'execution_time',
                    operator: '<',
                    conditionValue: '10',
                  },
                },
                { id: 'end-success', type: 'end-success', data: { name: 'Success' } },
                { id: 'end-failure', type: 'end-failure', data: { name: 'Failure' } },
              ],
              edges: [
                { from: 'start-node', fromPort: 'out', to: 'execute-node' },
                { from: 'execute-node', fromPort: 'out', to: 'condition-node' },
                { from: 'condition-node', fromPort: 'true', to: 'end-success' },
                { from: 'condition-node', fromPort: 'false', to: 'end-failure' },
              ],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        setTimeout(() => {
          orchestrationEngine.signalScriptCompletion(
            'Orchestration [test-job] Execution [test-exec1] Node [execute-node]',
            { exitCode: 0, stdout: 'Completed', stderr: '' }
          );
        }, 50);

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.conditionEvaluations['condition-node']).toBeDefined();
        expect(result.conditionEvaluations['condition-node'].result).toBe(true);
      });
    });

    describe('End Node Handling', () => {
      it('should set finalStatus to success at end-success node', async () => {
        const mockJob = createMockJob();
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        setTimeout(() => {
          orchestrationEngine.signalScriptCompletion(
            'Orchestration [test-job] Execution [test-exec1] Node [execute-node]',
            { exitCode: 0, stdout: 'Output', stderr: '' }
          );
        }, 50);

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.finalStatus).toBe('success');
        expect(result.visitedNodes[result.visitedNodes.length - 1]).toBe('end-node');
      });

      it('should set finalStatus to failure at end-failure node', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
                { id: 'end-failure', type: 'end-failure', data: { name: 'End Failure' } },
              ],
              edges: [{ from: 'start-node', fromPort: 'out', to: 'end-failure' }],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.finalStatus).toBe('failure');
      });

      it('should determine status from last execute node if no explicit end node', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
                { id: 'execute-node', type: 'execute', data: { script: 'test.sh', agent: 'agent1' } },
              ],
              edges: [{ from: 'start-node', fromPort: 'out', to: 'execute-node' }],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        setTimeout(() => {
          orchestrationEngine.signalScriptCompletion(
            'Orchestration [test-job] Execution [test-exec1] Node [execute-node]',
            { exitCode: 0, stdout: 'Output', stderr: '' }
          );
        }, 50);

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.finalStatus).toBe('success');
      });
    });

    describe('Error Handling', () => {
      it('should throw error when job not found', async () => {
        db.getData.mockResolvedValue({});

        const result = await orchestrationEngine.executeJob('nonexistent-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.finalStatus).toBe('error');
        expect(result.errors.length).toBeGreaterThan(0);
        expect(result.errors[0].message).toContain('not found');
      });

      it('should throw error when no start node found', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'execute-node', type: 'execute', data: { script: 'test.sh', agent: 'agent1' } },
                { id: 'end-node', type: 'end-success', data: { name: 'End' } },
              ],
              edges: [{ from: 'execute-node', fromPort: 'out', to: 'end-node' }],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.finalStatus).toBe('error');
        expect(result.errors[0].message).toContain('No start node');
      });

      it('should throw error when node not found in graph', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
              ],
              edges: [{ from: 'start-node', fromPort: 'out', to: 'nonexistent-node' }],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.finalStatus).toBe('error');
        expect(result.errors[0].message).toContain('not found in orchestration');
      });

      it('should throw error when execute node has no script', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
                { id: 'execute-node', type: 'execute', data: { agent: 'agent1' } },
                { id: 'end-node', type: 'end-success', data: { name: 'End' } },
              ],
              edges: [
                { from: 'start-node', fromPort: 'out', to: 'execute-node' },
                { from: 'execute-node', fromPort: 'out', to: 'end-node' },
              ],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.errors[0].message).toContain('no script');
      });

      it('should throw error when execute node has no agent', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
                { id: 'execute-node', type: 'execute', data: { script: 'test.sh' } },
                { id: 'end-node', type: 'end-success', data: { name: 'End' } },
              ],
              edges: [
                { from: 'start-node', fromPort: 'out', to: 'execute-node' },
                { from: 'execute-node', fromPort: 'out', to: 'end-node' },
              ],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.errors[0].message).toContain('no agent');
      });

      it('should refuse to read a script path outside scripts/', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
                { id: 'execute-node', type: 'execute', data: { script: '../data/server-config.json', agent: 'agent1', parameters: '' } },
                { id: 'end-node', type: 'end-success', data: { name: 'End Success' } },
              ],
              edges: [
                { from: 'start-node', fromPort: 'out', to: 'execute-node' },
                { from: 'execute-node', fromPort: 'out', to: 'end-node' },
              ],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });
        require('fs').promises.readFile.mockClear();
        global.agentComms.sendCommand.mockClear();

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.errors[0].message).toContain('outside the scripts directory');
        expect(require('fs').promises.readFile).not.toHaveBeenCalled();
        expect(global.agentComms.sendCommand).not.toHaveBeenCalled();
      });

      it('should throw error when script file not found', async () => {
        const mockJob = createMockJob();
        db.getData.mockResolvedValue({ 'test-job': mockJob });
        const missingScriptErr = new Error('ENOENT: no such file');
        missingScriptErr.code = 'ENOENT';
        require('fs').promises.readFile.mockRejectedValue(missingScriptErr);

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.finalStatus).toBe('error');
        expect(result.errors[0].message).toContain('Script cannot be found');
        expect(result.scriptOutputs['execute-node']).toBeDefined();
        expect(result.scriptOutputs['execute-node'].exitCode).toBe(1);
        expect(result.scriptOutputs['execute-node'].stdout).toContain('Script cannot be found');
      });

      it('should throw error when unknown node type encountered', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
                { id: 'unknown-node', type: 'unknown-type', data: {} },
              ],
              edges: [{ from: 'start-node', fromPort: 'out', to: 'unknown-node' }],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.errors[0].message).toContain('Unknown node type');
      });

      it('should throw error when start node has no outgoing connection', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
              ],
              edges: [],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.errors[0].message).toContain('no outgoing connection');
      });

      it('should throw error when condition has no true branch', async () => {
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
                { id: 'execute-node', type: 'execute', data: { script: 'test.sh', agent: 'agent1' } },
                {
                  id: 'condition-node',
                  type: 'condition',
                  data: {
                    conditionType: 'return_code',
                    operator: '==',
                    conditionValue: '0',
                  },
                },
              ],
              edges: [
                { from: 'start-node', fromPort: 'out', to: 'execute-node' },
                { from: 'execute-node', fromPort: 'out', to: 'condition-node' },
              ],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        setTimeout(() => {
          orchestrationEngine.signalScriptCompletion(
            'Orchestration [test-job] Execution [test-exec1] Node [execute-node]',
            { exitCode: 0, stdout: 'Output', stderr: '' }
          );
        }, 50);

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.errors[0].message).toContain('no true branch');
      });
    });

    describe('Edge Cases', () => {
      it('should handle empty script output', async () => {
        const mockJob = createMockJob();
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        setTimeout(() => {
          orchestrationEngine.signalScriptCompletion(
            'Orchestration [test-job] Execution [test-exec1] Node [execute-node]',
            { exitCode: 0, stdout: '', stderr: '' }
          );
        }, 50);

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('completed');
        expect(result.scriptOutputs['execute-node'].stdout).toBe('');
      });

      it('should detect infinite loops and stop at maxIterations', async () => {
        // Create a self-referencing execute node (infinite loop)
        const mockJob = createMockJob({
          versions: [
            {
              nodes: [
                { id: 'start-node', type: 'start', data: { name: 'Start' } },
                { id: 'execute-node', type: 'execute', data: { script: 'test.sh', agent: 'agent1' } },
              ],
              edges: [
                { from: 'start-node', fromPort: 'out', to: 'execute-node' },
                { from: 'execute-node', fromPort: 'out', to: 'execute-node' }, // Self-reference creates infinite loop
              ],
            },
          ],
        });
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        // Keep signaling for the looping node
        let callCount = 0;
        global.agentComms.sendCommand.mockImplementation(() => {
          callCount++;
          setTimeout(() => {
            orchestrationEngine.signalScriptCompletion(
              `Orchestration [test-job] Execution [test-exec1] Node [execute-node]`,
              { exitCode: 0, stdout: 'Completed', stderr: '' }
            );
          }, 10);
        });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.finalStatus).toBe('error');
        expect(result.errors[0].message).toContain('exceeded maximum iterations');
      }, 30000);

      it('should handle execution with onNodeComplete callback', async () => {
        const mockJob = createMockJob();
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        const onNodeCompleteSpy = jest.fn();

        setTimeout(() => {
          orchestrationEngine.signalScriptCompletion(
            'Orchestration [test-job] Execution [test-exec1] Node [execute-node]',
            { exitCode: 0, stdout: 'Output', stderr: '' }
          );
        }, 50);

        await orchestrationEngine.executeJob('test-job', false, 'test-exec1', onNodeCompleteSpy);

        // onNodeComplete should be called for each node completion
        expect(onNodeCompleteSpy).toHaveBeenCalled();
        expect(onNodeCompleteSpy.mock.calls.length).toBeGreaterThanOrEqual(3);
      });

      it('should generate unique executionId if not provided', async () => {
        const mockJob = createMockJob();
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        // Mock sendCommand to capture the job name and signal back with it
        global.agentComms.sendCommand.mockImplementation((agentId, command, script, params, jobName) => {
          setTimeout(() => {
            orchestrationEngine.signalScriptCompletion(jobName, {
              exitCode: 0,
              stdout: 'Output',
              stderr: '',
            });
          }, 50);
        });

        const result = await orchestrationEngine.executeJob('test-job', false);

        expect(result.executionId).toBeDefined();
        expect(result.executionId.length).toBeGreaterThan(0);
        expect(result.status).toBe('completed');
        expect(result.finalStatus).toBe('success');
      }, 20000);
    });

    describe('Concurrency', () => {
      it('should track multiple concurrent executions of same job', async () => {
        const mockJob = createMockJob();
        db.getData.mockResolvedValue({ 'test-job': mockJob });

        // Start two concurrent executions
        const exec1Promise = orchestrationEngine.executeJob('test-job', false, 'exec-1');
        const exec2Promise = orchestrationEngine.executeJob('test-job', false, 'exec-2');

        // Send completion signals
        setTimeout(() => {
          orchestrationEngine.signalScriptCompletion(
            'Orchestration [test-job] Execution [exec-1] Node [execute-node]',
            { exitCode: 0, stdout: 'Output 1', stderr: '' }
          );
          orchestrationEngine.signalScriptCompletion(
            'Orchestration [test-job] Execution [exec-2] Node [execute-node]',
            { exitCode: 1, stdout: 'Output 2', stderr: '' }
          );
        }, 50);

        const [result1, result2] = await Promise.all([exec1Promise, exec2Promise]);

        expect(result1.executionId).toBe('exec-1');
        expect(result2.executionId).toBe('exec-2');
        expect(result1.scriptOutputs['execute-node'].exitCode).toBe(0);
        expect(result2.scriptOutputs['execute-node'].exitCode).toBe(1);
      });
    });

    describe('HTTP Execute Node', () => {
      const createHttpJob = (httpData, extraNodes = [], extraEdges = []) => createMockJob({
        versions: [{
          nodes: [
            { id: 'start-node', type: 'start', data: {} },
            { id: 'http-node', type: 'execute', data: { executeType: 'http', httpHeaders: [], httpAuthType: 'none', ...httpData } },
            { id: 'end-node', type: 'end-success', data: {} },
            ...extraNodes,
          ],
          edges: [
            { from: 'start-node', fromPort: 'out', to: 'http-node' },
            { from: 'http-node', fromPort: 'out', to: 'end-node' },
            ...extraEdges,
          ],
        }],
      });

      it('should execute HTTP GET and set exitCode 0 for 200 response', async () => {
        axios.mockResolvedValue({ status: 200, data: { result: 'ok' } });
        db.getData.mockResolvedValue({ 'test-job': createHttpJob({ httpMethod: 'GET', httpUrl: 'https://api.example.com/status' }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.finalStatus).toBe('success');
        const output = result.scriptOutputs['http-node'];
        expect(output.exitCode).toBe(0);
        expect(output.httpStatus).toBe(200);
        expect(output.agent).toBe('http');
        expect(output.httpMethod).toBe('GET');
        expect(output.httpUrl).toBe('https://api.example.com/status');
      });

      it('should set exitCode 1 for non-2xx HTTP response', async () => {
        axios.mockResolvedValue({ status: 404, data: 'Not Found' });
        db.getData.mockResolvedValue({ 'test-job': createHttpJob({ httpMethod: 'GET', httpUrl: 'https://api.example.com/missing' }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.scriptOutputs['http-node'].exitCode).toBe(1);
        expect(result.scriptOutputs['http-node'].httpStatus).toBe(404);
        expect(result.scriptOutputs['http-node'].stderr).toContain('404');
      });

      it('should continue execution after non-2xx response', async () => {
        axios.mockResolvedValue({ status: 500, data: 'Server Error' });
        db.getData.mockResolvedValue({ 'test-job': createHttpJob({ httpMethod: 'GET', httpUrl: 'https://api.example.com/error' }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        // Execution continues to end-node despite HTTP failure
        expect(result.visitedNodes).toContain('end-node');
        expect(result.scriptOutputs['http-node'].exitCode).toBe(1);
      });

      it('should handle HTTP network errors gracefully and continue execution', async () => {
        axios.mockRejectedValue(new Error('ECONNREFUSED: Connection refused'));
        db.getData.mockResolvedValue({ 'test-job': createHttpJob({ httpMethod: 'GET', httpUrl: 'https://unreachable.example.com' }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.scriptOutputs['http-node'].exitCode).toBe(1);
        expect(result.scriptOutputs['http-node'].status).toBe('failed');
        expect(result.scriptOutputs['http-node'].stderr).toContain('ECONNREFUSED');
        expect(result.visitedNodes).toContain('end-node');
      });

      it('should throw when HTTP node has no URL configured', async () => {
        db.getData.mockResolvedValue({ 'test-job': createHttpJob({ httpMethod: 'GET', httpUrl: '' }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.errors[0].message).toContain('no URL');
      });

      it('should add bearer token to Authorization header', async () => {
        axios.mockResolvedValue({ status: 200, data: {} });
        db.getData.mockResolvedValue({ 'test-job': createHttpJob({
          httpMethod: 'GET', httpUrl: 'https://api.example.com/secure',
          httpAuthType: 'bearer', httpAuthBearerToken: 'my-secret-token',
        }) });

        await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(axios.mock.calls[0][0].headers.Authorization).toBe('Bearer my-secret-token');
      });

      it('should throw when bearer token is empty', async () => {
        db.getData.mockResolvedValue({ 'test-job': createHttpJob({
          httpMethod: 'GET', httpUrl: 'https://api.example.com/secure',
          httpAuthType: 'bearer', httpAuthBearerToken: '',
        }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.errors[0].message).toContain('bearer auth but token is empty');
      });

      it('should configure basic auth credentials', async () => {
        axios.mockResolvedValue({ status: 200, data: {} });
        db.getData.mockResolvedValue({ 'test-job': createHttpJob({
          httpMethod: 'POST', httpUrl: 'https://api.example.com/data',
          httpAuthType: 'basic', httpAuthUsername: 'admin', httpAuthPassword: 's3cr3t',
        }) });

        await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(axios.mock.calls[0][0].auth).toEqual({ username: 'admin', password: 's3cr3t' });
      });

      it('should throw when basic auth credentials are incomplete', async () => {
        db.getData.mockResolvedValue({ 'test-job': createHttpJob({
          httpMethod: 'POST', httpUrl: 'https://api.example.com/data',
          httpAuthType: 'basic', httpAuthUsername: 'admin', httpAuthPassword: '',
        }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.errors[0].message).toContain('username/password are incomplete');
      });

      it('should add API key to the configured header', async () => {
        axios.mockResolvedValue({ status: 200, data: {} });
        db.getData.mockResolvedValue({ 'test-job': createHttpJob({
          httpMethod: 'GET', httpUrl: 'https://api.example.com/data',
          httpAuthType: 'apikey', httpAuthApiKeyHeader: 'X-API-Key', httpAuthApiKeyValue: 'abc-123',
        }) });

        await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(axios.mock.calls[0][0].headers['X-API-Key']).toBe('abc-123');
      });

      it('should throw when API key value is empty', async () => {
        db.getData.mockResolvedValue({ 'test-job': createHttpJob({
          httpMethod: 'GET', httpUrl: 'https://api.example.com/data',
          httpAuthType: 'apikey', httpAuthApiKeyHeader: 'X-API-Key', httpAuthApiKeyValue: '',
        }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.errors[0].message).toContain('API key auth but header/value are incomplete');
      });

      it('should add custom request headers', async () => {
        axios.mockResolvedValue({ status: 200, data: {} });
        db.getData.mockResolvedValue({ 'test-job': createHttpJob({
          httpMethod: 'GET', httpUrl: 'https://api.example.com/data',
          httpHeaders: [
            { key: 'Content-Type', value: 'application/json' },
            { key: 'X-Custom-Header', value: 'my-value' },
          ],
        }) });

        await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        const { headers } = axios.mock.calls[0][0];
        expect(headers['Content-Type']).toBe('application/json');
        expect(headers['X-Custom-Header']).toBe('my-value');
      });

      it('should parse and attach JSON body for POST requests', async () => {
        axios.mockResolvedValue({ status: 201, data: { id: 42 } });
        db.getData.mockResolvedValue({ 'test-job': createHttpJob({
          httpMethod: 'POST', httpUrl: 'https://api.example.com/items',
          httpBody: '{"name":"test","value":42}',
        }) });

        await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(axios.mock.calls[0][0].data).toEqual({ name: 'test', value: 42 });
      });

      it('should not attach body for GET requests', async () => {
        axios.mockResolvedValue({ status: 200, data: {} });
        db.getData.mockResolvedValue({ 'test-job': createHttpJob({
          httpMethod: 'GET', httpUrl: 'https://api.example.com/items',
          httpBody: '{"ignored":true}',
        }) });

        await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(axios.mock.calls[0][0].data).toBeUndefined();
      });

      it('should use configured timeout', async () => {
        axios.mockResolvedValue({ status: 200, data: {} });
        db.getData.mockResolvedValue({ 'test-job': createHttpJob({
          httpMethod: 'GET', httpUrl: 'https://api.example.com/data', httpTimeoutMs: 5000,
        }) });

        await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(axios.mock.calls[0][0].timeout).toBe(5000);
      });

      it('should use default timeout of 30000ms when not configured', async () => {
        axios.mockResolvedValue({ status: 200, data: {} });
        db.getData.mockResolvedValue({ 'test-job': createHttpJob({
          httpMethod: 'GET', httpUrl: 'https://api.example.com/data',
        }) });

        await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(axios.mock.calls[0][0].timeout).toBe(30000);
      });

      it('should record HTTP response data as serialized JSON in stdout', async () => {
        axios.mockResolvedValue({ status: 200, data: { status: 'healthy', uptime: 9999 } });
        db.getData.mockResolvedValue({ 'test-job': createHttpJob({
          httpMethod: 'GET', httpUrl: 'https://api.example.com/health',
        }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        const stdout = result.scriptOutputs['http-node'].stdout;
        expect(stdout).toContain('healthy');
        expect(stdout).toContain('9999');
      });
    });

    describe('Wait Node', () => {
      it('should delay for the configured number of seconds and complete successfully', async () => {
        db.getData.mockResolvedValue({ 'test-job': createMockJob({
          versions: [{
            nodes: [
              { id: 'start-node', type: 'start', data: {} },
              { id: 'wait-node', type: 'wait', data: { waitSeconds: 0.01 } },
              { id: 'end-node', type: 'end-success', data: {} },
            ],
            edges: [
              { from: 'start-node', fromPort: 'out', to: 'wait-node' },
              { from: 'wait-node', fromPort: 'out', to: 'end-node' },
            ],
          }],
        }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.finalStatus).toBe('success');
        expect(result.visitedNodes).toContain('wait-node');
      });

      it('should record wait output with correct fields in scriptOutputs', async () => {
        db.getData.mockResolvedValue({ 'test-job': createMockJob({
          versions: [{
            nodes: [
              { id: 'start-node', type: 'start', data: {} },
              { id: 'wait-node', type: 'wait', data: { waitSeconds: 0.01 } },
              { id: 'end-node', type: 'end-success', data: {} },
            ],
            edges: [
              { from: 'start-node', fromPort: 'out', to: 'wait-node' },
              { from: 'wait-node', fromPort: 'out', to: 'end-node' },
            ],
          }],
        }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        const output = result.scriptOutputs['wait-node'];
        expect(output).toBeDefined();
        expect(output.exitCode).toBe(0);
        expect(output.agent).toBe('wait');
        expect(output.stdout).toContain('Waited');
        expect(output.waitSeconds).toBe(0.01);
        expect(output.startTime).toBeDefined();
        expect(output.endTime).toBeDefined();
      });

      it('should use default of 5 seconds when waitSeconds is invalid', async () => {
        db.getData.mockResolvedValue({ 'test-job': createMockJob({
          versions: [{
            nodes: [
              { id: 'start-node', type: 'start', data: {} },
              { id: 'wait-node', type: 'wait', data: { waitSeconds: -1 } },
              { id: 'end-node', type: 'end-success', data: {} },
            ],
            edges: [
              { from: 'start-node', fromPort: 'out', to: 'wait-node' },
              { from: 'wait-node', fromPort: 'out', to: 'end-node' },
            ],
          }],
        }) });

        jest.useFakeTimers();
        const execPromise = orchestrationEngine.executeJob('test-job', false, 'test-exec1');
        await jest.runAllTimersAsync();
        jest.useRealTimers();

        const result = await execPromise;

        expect(result.scriptOutputs['wait-node'].waitSeconds).toBe(5);
        expect(result.scriptOutputs['wait-node'].stdout).toContain('5 seconds');
      });

      it('should return success when wait node has no outgoing edges', async () => {
        db.getData.mockResolvedValue({ 'test-job': createMockJob({
          versions: [{
            nodes: [
              { id: 'start-node', type: 'start', data: {} },
              { id: 'wait-node', type: 'wait', data: { waitSeconds: 0.01 } },
            ],
            edges: [
              { from: 'start-node', fromPort: 'out', to: 'wait-node' },
            ],
          }],
        }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.finalStatus).toBe('success');
      });

      it('should throw when wait node has multiple outgoing connections', async () => {
        db.getData.mockResolvedValue({ 'test-job': createMockJob({
          versions: [{
            nodes: [
              { id: 'start-node', type: 'start', data: {} },
              { id: 'wait-node', type: 'wait', data: { waitSeconds: 0.01 } },
              { id: 'end-1', type: 'end-success', data: {} },
              { id: 'end-2', type: 'end-success', data: {} },
            ],
            edges: [
              { from: 'start-node', fromPort: 'out', to: 'wait-node' },
              { from: 'wait-node', fromPort: 'out', to: 'end-1' },
              { from: 'wait-node', fromPort: 'out', to: 'end-2' },
            ],
          }],
        }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.errors[0].message).toContain('multiple outgoing connections');
      });
    });

    describe('Notify Node', () => {
      const createNotifyJob = (notifyData) => createMockJob({
        versions: [{
          nodes: [
            { id: 'start-node', type: 'start', data: {} },
            { id: 'notify-node', type: 'notify', data: notifyData },
            { id: 'end-node', type: 'end-success', data: {} },
          ],
          edges: [
            { from: 'start-node', fromPort: 'out', to: 'notify-node' },
            { from: 'notify-node', fromPort: 'out', to: 'end-node' },
          ],
        }],
      });

      it('should send notification with correct title, body, and type', async () => {
        db.getData.mockResolvedValue({ 'test-job': createNotifyJob({
          notifyType: 'WARNING', notifyTitle: 'Backup Done', notifyBody: 'All backups completed.',
        }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(notifier.sendNotification).toHaveBeenCalledWith(
          'Backup Done', 'All backups completed.', 'WARNING', undefined
        );
        expect(result.finalStatus).toBe('success');
      });

      it('should use INFORMATION as default notify type', async () => {
        db.getData.mockResolvedValue({ 'test-job': createNotifyJob({
          notifyTitle: 'Hello', notifyBody: 'World',
        }) });

        await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(notifier.sendNotification).toHaveBeenCalledWith(
          'Hello', 'World', 'INFORMATION', undefined
        );
      });

      it('should pass URL to sendNotification when notifyUrl is configured', async () => {
        db.getData.mockResolvedValue({ 'test-job': createNotifyJob({
          notifyType: 'INFORMATION', notifyTitle: 'Status',
          notifyBody: 'Check the link', notifyUrl: '/dashboard',
        }) });

        await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(notifier.sendNotification).toHaveBeenCalledWith(
          'Status', 'Check the link', 'INFORMATION', '/dashboard'
        );
      });

      it('should record notification fields in scriptOutputs', async () => {
        db.getData.mockResolvedValue({ 'test-job': createNotifyJob({
          notifyType: 'ERROR', notifyTitle: 'Alert', notifyBody: 'Something failed',
        }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        const output = result.scriptOutputs['notify-node'];
        expect(output).toBeDefined();
        expect(output.exitCode).toBe(0);
        expect(output.agent).toBe('notify');
        expect(output.notifyType).toBe('ERROR');
        expect(output.notifyTitle).toBe('Alert');
        expect(output.notifyBody).toBe('Something failed');
        expect(output.status).toBe('completed');
      });

      it('should throw when notification title is missing', async () => {
        db.getData.mockResolvedValue({ 'test-job': createNotifyJob({
          notifyType: 'INFORMATION', notifyTitle: '', notifyBody: 'Some message',
        }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.errors[0].message).toContain('missing a title');
      });

      it('should throw when notification body is missing', async () => {
        db.getData.mockResolvedValue({ 'test-job': createNotifyJob({
          notifyType: 'INFORMATION', notifyTitle: 'Alert', notifyBody: '',
        }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.errors[0].message).toContain('missing a message');
      });

      it('should continue execution with exitCode 1 when sendNotification throws', async () => {
        notifier.sendNotification.mockRejectedValue(new Error('SMTP connection failed'));
        db.getData.mockResolvedValue({ 'test-job': createNotifyJob({
          notifyType: 'INFORMATION', notifyTitle: 'Alert', notifyBody: 'Message',
        }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.scriptOutputs['notify-node'].exitCode).toBe(1);
        expect(result.scriptOutputs['notify-node'].status).toBe('failed');
        expect(result.scriptOutputs['notify-node'].stderr).toContain('SMTP connection failed');
        // Execution continues to end-node despite notification failure
        expect(result.visitedNodes).toContain('end-node');
      });

      it('should return success when notify node has no outgoing edges', async () => {
        db.getData.mockResolvedValue({ 'test-job': createMockJob({
          versions: [{
            nodes: [
              { id: 'start-node', type: 'start', data: {} },
              { id: 'notify-node', type: 'notify', data: { notifyTitle: 'Done', notifyBody: 'Finished' } },
            ],
            edges: [
              { from: 'start-node', fromPort: 'out', to: 'notify-node' },
            ],
          }],
        }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.finalStatus).toBe('success');
      });

      it('should throw when notify node has multiple outgoing connections', async () => {
        db.getData.mockResolvedValue({ 'test-job': createMockJob({
          versions: [{
            nodes: [
              { id: 'start-node', type: 'start', data: {} },
              { id: 'notify-node', type: 'notify', data: { notifyTitle: 'Alert', notifyBody: 'Msg' } },
              { id: 'end-1', type: 'end-success', data: {} },
              { id: 'end-2', type: 'end-success', data: {} },
            ],
            edges: [
              { from: 'start-node', fromPort: 'out', to: 'notify-node' },
              { from: 'notify-node', fromPort: 'out', to: 'end-1' },
              { from: 'notify-node', fromPort: 'out', to: 'end-2' },
            ],
          }],
        }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.errors[0].message).toContain('multiple outgoing connections');
      });
    });

    describe('Split-Join Node', () => {
      // Auto-signal helper: all sendCommand calls resolve with exitCode 0 after a short delay
      const autoSignal = (exitCode = 0) => {
        global.agentComms.sendCommand.mockImplementation((agentId, command, script, params, jobName) => {
          setTimeout(() => {
            orchestrationEngine.signalScriptCompletion(jobName, { exitCode, stdout: 'OK', stderr: '' });
          }, 20);
        });
      };

      it('should fan out to all parallel branches in split mode', async () => {
        db.getData.mockResolvedValue({ 'test-job': createMockJob({
          versions: [{
            nodes: [
              { id: 'start-node', type: 'start', data: {} },
              { id: 'split-node', type: 'split-join', data: { mode: 'split' } },
              { id: 'branch-1', type: 'execute', data: { script: 'a.sh', agent: 'agent1', parameters: '' } },
              { id: 'branch-2', type: 'execute', data: { script: 'b.sh', agent: 'agent1', parameters: '' } },
              { id: 'end-1', type: 'end-success', data: {} },
              { id: 'end-2', type: 'end-success', data: {} },
            ],
            edges: [
              { id: 'e1', from: 'start-node', fromPort: 'out', to: 'split-node' },
              { id: 'e2', from: 'split-node', fromPort: 'out', to: 'branch-1' },
              { id: 'e3', from: 'split-node', fromPort: 'out', to: 'branch-2' },
              { id: 'e4', from: 'branch-1', fromPort: 'out', to: 'end-1' },
              { id: 'e5', from: 'branch-2', fromPort: 'out', to: 'end-2' },
            ],
          }],
        }) });
        autoSignal(0);

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.finalStatus).toBe('success');
        expect(result.visitedNodes).toContain('split-node');
        expect(result.visitedNodes).toContain('branch-1');
        expect(result.visitedNodes).toContain('branch-2');
        expect(result.scriptOutputs['branch-1']).toBeDefined();
        expect(result.scriptOutputs['branch-2']).toBeDefined();
      });

      it('should wait for all branches before releasing join node with waitAll strategy', async () => {
        db.getData.mockResolvedValue({ 'test-job': createMockJob({
          versions: [{
            nodes: [
              { id: 'start-node', type: 'start', data: {} },
              { id: 'split-node', type: 'split-join', data: { mode: 'split' } },
              { id: 'branch-1', type: 'execute', data: { script: 'a.sh', agent: 'agent1', parameters: '' } },
              { id: 'branch-2', type: 'execute', data: { script: 'b.sh', agent: 'agent1', parameters: '' } },
              { id: 'join-node', type: 'split-join', data: { mode: 'join', joinStrategy: 'waitAll' } },
              { id: 'end-node', type: 'end-success', data: {} },
            ],
            edges: [
              { id: 'e1', from: 'start-node', fromPort: 'out', to: 'split-node' },
              { id: 'e2', from: 'split-node', fromPort: 'out', to: 'branch-1' },
              { id: 'e3', from: 'split-node', fromPort: 'out', to: 'branch-2' },
              { id: 'e4', from: 'branch-1', fromPort: 'out', to: 'join-node' },
              { id: 'e5', from: 'branch-2', fromPort: 'out', to: 'join-node' },
              { id: 'e6', from: 'join-node', fromPort: 'out', to: 'end-node' },
            ],
          }],
        }) });
        autoSignal(0);

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.finalStatus).toBe('success');
        expect(result.visitedNodes).toContain('branch-1');
        expect(result.visitedNodes).toContain('branch-2');
        expect(result.visitedNodes).toContain('join-node');
        expect(result.visitedNodes).toContain('end-node');
        // Both execute branches must have produced output (both ran)
        expect(result.scriptOutputs['branch-1']).toBeDefined();
        expect(result.scriptOutputs['branch-2']).toBeDefined();
      });

      it('should release join on first branch arrival with waitAny strategy', async () => {
        db.getData.mockResolvedValue({ 'test-job': createMockJob({
          versions: [{
            nodes: [
              { id: 'start-node', type: 'start', data: {} },
              { id: 'split-node', type: 'split-join', data: { mode: 'split' } },
              { id: 'branch-1', type: 'execute', data: { script: 'fast.sh', agent: 'agent1', parameters: '' } },
              { id: 'branch-2', type: 'execute', data: { script: 'slow.sh', agent: 'agent1', parameters: '' } },
              { id: 'join-node', type: 'split-join', data: { mode: 'join', joinStrategy: 'waitAny' } },
              { id: 'end-node', type: 'end-success', data: {} },
            ],
            edges: [
              { id: 'e1', from: 'start-node', fromPort: 'out', to: 'split-node' },
              { id: 'e2', from: 'split-node', fromPort: 'out', to: 'branch-1' },
              { id: 'e3', from: 'split-node', fromPort: 'out', to: 'branch-2' },
              { id: 'e4', from: 'branch-1', fromPort: 'out', to: 'join-node' },
              { id: 'e5', from: 'branch-2', fromPort: 'out', to: 'join-node' },
              { id: 'e6', from: 'join-node', fromPort: 'out', to: 'end-node' },
            ],
          }],
        }) });

        // branch-1 arrives at join first; branch-2 arrives second (after join is released)
        let callCount = 0;
        global.agentComms.sendCommand.mockImplementation((agentId, command, script, params, jobName) => {
          callCount++;
          const delay = callCount === 1 ? 10 : 50;
          setTimeout(() => {
            orchestrationEngine.signalScriptCompletion(jobName, { exitCode: 0, stdout: 'OK', stderr: '' });
          }, delay);
        });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.finalStatus).toBe('success');
        expect(result.visitedNodes).toContain('join-node');
        expect(result.visitedNodes).toContain('end-node');
      });

      it('should return merged failure when any parallel branch fails', async () => {
        db.getData.mockResolvedValue({ 'test-job': createMockJob({
          versions: [{
            nodes: [
              { id: 'start-node', type: 'start', data: {} },
              { id: 'split-node', type: 'split-join', data: { mode: 'split' } },
              { id: 'branch-1', type: 'execute', data: { script: 'ok.sh', agent: 'agent1', parameters: '' } },
              { id: 'branch-2', type: 'execute', data: { script: 'fail.sh', agent: 'agent1', parameters: '' } },
              { id: 'end-node', type: 'end-failure', data: {} },
            ],
            edges: [
              { id: 'e1', from: 'start-node', fromPort: 'out', to: 'split-node' },
              { id: 'e2', from: 'split-node', fromPort: 'out', to: 'branch-1' },
              { id: 'e3', from: 'split-node', fromPort: 'out', to: 'branch-2' },
              { id: 'e4', from: 'branch-1', fromPort: 'out', to: 'end-node' },
              { id: 'e5', from: 'branch-2', fromPort: 'out', to: 'end-node' },
            ],
          }],
        }) });
        autoSignal(1);

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.finalStatus).toBe('failure');
      });

      it('should throw when split node has no outgoing paths', async () => {
        db.getData.mockResolvedValue({ 'test-job': createMockJob({
          versions: [{
            nodes: [
              { id: 'start-node', type: 'start', data: {} },
              { id: 'split-node', type: 'split-join', data: { mode: 'split' } },
            ],
            edges: [
              { id: 'e1', from: 'start-node', fromPort: 'out', to: 'split-node' },
            ],
          }],
        }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.errors[0].message).toContain('no outgoing paths');
      });

      it('should throw when join node has multiple outgoing connections', async () => {
        db.getData.mockResolvedValue({ 'test-job': createMockJob({
          versions: [{
            nodes: [
              { id: 'start-node', type: 'start', data: {} },
              { id: 'split-node', type: 'split-join', data: { mode: 'split' } },
              { id: 'branch-1', type: 'execute', data: { script: 'a.sh', agent: 'agent1', parameters: '' } },
              { id: 'join-node', type: 'split-join', data: { mode: 'join' } },
              { id: 'end-1', type: 'end-success', data: {} },
              { id: 'end-2', type: 'end-success', data: {} },
            ],
            edges: [
              { id: 'e1', from: 'start-node', fromPort: 'out', to: 'split-node' },
              { id: 'e2', from: 'split-node', fromPort: 'out', to: 'branch-1' },
              { id: 'e3', from: 'branch-1', fromPort: 'out', to: 'join-node' },
              { id: 'e4', from: 'join-node', fromPort: 'out', to: 'end-1' },
              { id: 'e5', from: 'join-node', fromPort: 'out', to: 'end-2' },
            ],
          }],
        }) });
        autoSignal(0);

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.errors[0].message).toContain('multiple outgoing connections');
      });

      it('should throw when split-join node has an invalid mode', async () => {
        db.getData.mockResolvedValue({ 'test-job': createMockJob({
          versions: [{
            nodes: [
              { id: 'start-node', type: 'start', data: {} },
              { id: 'split-node', type: 'split-join', data: { mode: 'invalid' } },
            ],
            edges: [
              { id: 'e1', from: 'start-node', fromPort: 'out', to: 'split-node' },
            ],
          }],
        }) });

        const result = await orchestrationEngine.executeJob('test-job', false, 'test-exec1');

        expect(result.status).toBe('failed');
        expect(result.errors[0].message).toContain('invalid mode');
      });
    });
  });

  describe('Integration Tests', () => {
    it('should handle execution lifecycle: save and retrieve', async () => {
      db.getData.mockResolvedValue({});

      const executionLog = {
        jobId: 'integration-job',
        executionId: 'int-exec1',
        status: 'completed',
        finalStatus: 'success',
        startTime: '2026-03-27T00:00:00Z',
        endTime: '2026-03-27T00:05:00Z',
        visitedNodes: ['start', 'execute1', 'end'],
        nodeMetrics: {
          start: { duration: 0.1 },
          execute1: { duration: 300 },
          end: { duration: 0.1 },
        },
      };

      // Save execution
      await orchestrationEngine.saveExecutionResult(executionLog);
      expect(db.putData).toHaveBeenCalled();

      // Retrieve execution history
      db.getData.mockResolvedValue({
        'integration-job': [executionLog],
      });

      const history = await orchestrationEngine.getExecutionHistory('integration-job');
      expect(history.length).toBe(1);
      expect(history[0].executionId).toBe('int-exec1');

      // Find specific execution
      const index = await orchestrationEngine.getExecutionIndexById('integration-job', 'int-exec1');
      expect(index).toBe(0);
    });

    it('should handle script completion workflow', async () => {
      // Start waiting for script
      const completionPromise = orchestrationEngine.waitForScriptCompletion('workflow-job', 5000);

      // Simulate agent sending script completion
      setTimeout(() => {
        orchestrationEngine.signalScriptCompletion('workflow-job', {
          exitCode: 0,
          stdout: 'Backup completed successfully',
          stderr: '',
        });
      }, 50);

      const result = await completionPromise;

      expect(result.exitCode).toBe(0);
      expect(result.stdout).toContain('Backup completed');
    });
  });
});
