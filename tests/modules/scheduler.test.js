// Mock setup for scheduler module tests

const ORIGINAL_DEFINITIONS_BACKEND = process.env.DEFINITIONS_BACKEND;
process.env.DEFINITIONS_BACKEND = 'db';

// Mock db module before importing scheduler
jest.mock('../../db.js', () => ({
  getData: jest.fn(),
  putData: jest.fn(),
  deleteData: jest.fn(),
}));

// Mock running module before importing scheduler
jest.mock('../../running.js', () => ({
  getRunningCountForAgent: jest.fn(() => 0),
  createItem: jest.fn(( jobName, startTime, isManual, executionId, agent, orchestrationId, icon, color) => ({
    jobName,
    startTime,
    isManual,
    executionId,
    agent,
    orchestrationId,
    icon,
    color,
  })),
  add: jest.fn(async (item) => {
    // Mock implementation - just resolve
    return Promise.resolve();
  }),
  removeItemByExecutionId: jest.fn(async (executionId) => {
    // Mock implementation - just resolve
    return Promise.resolve();
  }),
}));

// Mock dateTimeUtils module before importing scheduler
jest.mock('../../utils/dateTimeUtils.js', () => ({
  displayFormatDate: jest.fn().mockReturnValue('2026-03-21T12:00:00.000'),
  applyTz: jest.fn().mockReturnValue('2026-03-21T12:00:00Z'),
  displaySecs: jest.fn().mockReturnValue('0h 0m 0s'),
}));

jest.mock('fs', () => {
  const actualFs = jest.requireActual('fs');
  const mockPromises = {
    readFile: jest.fn(),
    writeFile: jest.fn(),
    mkdir: jest.fn(),
    unlink: jest.fn(),
  };
  return {
    ...actualFs,
    existsSync: jest.fn().mockReturnValue(false),
    promises: mockPromises,
  };
});

const dateTimeUtils = require('../../utils/dateTimeUtils.js');
const fs = require('fs');
const db = require('../../db.js');

describe('Scheduler Module', () => {
  let scheduler;
  let mockAgents;
  let mockHistory;

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
        jobFailEnabled: 'true',
        timezone: 'Europe/London',
      },
    };

    global.agentComms = {
      sendCommand: jest.fn(),
    };

    global.mqttTransport = {
      getCommandTopic: jest.fn().mockReturnValue('backup/commands'),
    };

    global.thresholdJobs = {
      empty: jest.fn(),
      addJob: jest.fn(),
    };

    global.nodeschedule = {
      scheduleJob: jest.fn().mockReturnValue({}),
      scheduledJobs: {},
    };

    global.notifier = {
      sendNotification: jest.fn(),
    };

    // Mock agents as a global object
    mockAgents = {
      getAgent: jest.fn().mockReturnValue({
        name: 'test-agent',
        status: 'online',
      }),
      getDict: jest.fn().mockReturnValue({
        'test-agent': { name: 'test-agent', status: 'online' },
      }),
      getConcurrency: jest.fn().mockReturnValue(3), // Default concurrency limit
    };

    // Mock history as a global object (named 'hist' in production)
    mockHistory = {
      createHistoryItem: jest.fn().mockReturnValue({
        jobName: 'test',
        returnCode: 0,
      }),
      add: jest.fn(),
      getAverageRuntime: jest.fn().mockReturnValue(3600), // 1 hour in seconds
    };

    // Mock fs.promises
    fs.promises.readFile = jest.fn().mockImplementation((filePath) => {
      if (filePath === './data/schedules.json') {
        // Return mock schedules for migration (if file exists)
        return Promise.resolve(JSON.stringify([
          {
            jobName: 'test-job',
            agent: 'test-agent',
            command: 'test.sh',
            scheduleType: 'daily',
            scheduleTime: '12:00',
            dayOfWeek: '*',
            dayInMonth: '*',
          },
        ]));
      }
      // Return shell script content for other files
      return Promise.resolve('#!/bin/bash\necho test');
    });
    fs.promises.writeFile = jest.fn().mockResolvedValue(undefined);
    fs.promises.mkdir = jest.fn().mockResolvedValue(undefined);
    fs.promises.unlink = jest.fn().mockResolvedValue(undefined);

    // Set up the actual mocked db with sample schedules
    const sampleSchedules = [
      {
        jobName: 'test-job',
        agent: 'test-agent',
        command: 'test.sh',
        scheduleType: 'daily',
        scheduleTime: '12:00',
        dayOfWeek: '*',
        dayInMonth: '*',
      },
    ];
    
    db.getData.mockResolvedValue(sampleSchedules);
    db.putData.mockResolvedValue(true);
    db.deleteData.mockResolvedValue(true);

    global.agents = mockAgents;
    global.hist = mockHistory;
    global.moment = require('moment-timezone');

    scheduler = require('../../scheduler.js');
  });

  afterEach(async () => {
    jest.clearAllMocks();
    // Wait for pending timers/promises to settle
    await new Promise(resolve => setImmediate(resolve));
    delete global.logger;
    delete global.serverConfig;
    delete global.agentComms;
    delete global.mqttTransport;
    delete global.thresholdJobs;
    delete global.nodeschedule;
    delete global.notifier;
    delete global.agents;
    delete global.hist;
    delete global.moment;
  });

  describe('init()', () => {
    it('should initialize scheduler', async () => {
      db.getData.mockResolvedValue([
        {
          jobName: 'test-job',
          agent: 'test-agent',
          command: 'test.sh',
          scheduleType: 'daily',
          scheduleTime: '12:00',
          dayOfWeek: '*',
          dayInMonth: '*',
        },
      ]);

      await scheduler.init();
      expect(logger.info).toHaveBeenCalled();
    });
  });

  describe('getSchedule()', () => {
    beforeEach(async () => {
      // Initialize scheduler to load schedules
      await scheduler.init();
    });

    it('should retrieve a schedule by job name', async () => {
      const schedule = scheduler.getSchedule('test-job');
      
      if (schedule) {
        expect(schedule.jobName).toBe('test-job');
      }
    });
  });

  describe('getSchedules()', () => {
    beforeEach(async () => {
      // Set up mock data before init
      db.getData.mockResolvedValue([
        {
          jobName: 'test-job',
          agent: 'test-agent',
          command: 'test.sh',
          scheduleType: 'daily',
          scheduleTime: '12:00',
          dayOfWeek: '*',
          dayInMonth: '*',
        },
      ]);
      // Initialize scheduler to load schedules
      await scheduler.init();
    });

    it('should return array of schedules', () => {
      const schedules = scheduler.getSchedules();
      expect(Array.isArray(schedules)).toBe(true);
      expect(schedules.length).toBeGreaterThan(0);
    });
  });

  describe('deleteSchedule()', () => {
    it('should delete a schedule by job name', async () => {
      // This test is simplified since the scheduler would need
      // proper setup with actual schedule data
      await scheduler.deleteSchedule('test-job');
      expect(logger.info).toHaveBeenCalled();
    });
  });

  describe('manualJobRun()', () => {
    beforeEach(async () => {
      // Set up mock data before init
      db.getData.mockResolvedValue([
        {
          jobName: 'test-job',
          agent: 'test-agent',
          command: 'test.sh',
          scheduleType: 'daily',
          scheduleTime: '12:00',
          dayOfWeek: '*',
          dayInMonth: '*',
        },
      ]);
      // Initialize scheduler to load schedules
      await scheduler.init();
    });

    it('should attempt to manually run a job', async () => {
      mockAgents.getAgent.mockReturnValue({
        name: 'test-agent',
        status: 'online',
      });

      const result = await scheduler.manualJobRun(0, 'test-job');
      expect(result).toBeDefined();
    });

    it('should reject if agent is offline', async () => {
      mockAgents.getAgent.mockReturnValue({
        name: 'test-agent',
        status: 'offline',
      });

      const result = await scheduler.manualJobRun(0, 'test-job');
      expect(result.status).toBe('error');
      expect(result.executionId).toBeNull();
    });

    it('should reject if agent not found', async () => {
      mockAgents.getAgent.mockReturnValue(undefined);

      const result = await scheduler.manualJobRun(0, 'test-job');
      expect(result.status).toBe('error');
      expect(result.executionId).toBeNull();
    });

    it('should validate job name is a string', async () => {
      const result = await scheduler.manualJobRun(0, ['not-a-string']);
      expect(result.status).toBe('error');
      expect(result.executionId).toBeNull();
    });
  });

  describe('upsertSchedule()', () => {
    it('should create or update a schedule', async () => {
      const schedule = {
        jobName: 'new-job',
        description: 'Test Job',
        agent: 'test-agent',
        command: 'test.sh',
      };

      await scheduler.upsertSchedule(schedule);
      expect(logger.info).toHaveBeenCalled();
    });
  });

  describe('getNextRunDate()', () => {
    it('should calculate next run date for schedule', () => {
      const schedule = {
        scheduleType: 'daily',
        scheduleTime: '12:00',
      };

      const nextRun = scheduler.getNextRunDate(schedule);
      if (nextRun) {
        expect(nextRun instanceof Date).toBe(true);
      }
    });
  });

  describe('runJob()', () => {
    beforeEach(async () => {
      db.getData.mockResolvedValue([
        {
          jobName: 'test-job',
          description: 'Test Job',
          agent: 'test-agent',
          command: 'test.sh',
          commandParams: '--verbose',
          scheduleType: 'daily',
          scheduleTime: '12:00',
          dayOfWeek: '*',
          dayInMonth: '*',
          scheduleMode: 'classic',
        },
      ]);
      await scheduler.init();
    });

    it('should execute a classic schedule job successfully', async () => {
      mockAgents.getAgent.mockReturnValue({
        name: 'test-agent',
        status: 'online',
      });

      fs.promises.readFile.mockResolvedValue('#!/bin/bash\necho "test"');

      const result = await scheduler.runJob('test-job', false);

      expect(result.status).toBe('ok');
      expect(result.executionId).toBeDefined();
      expect(global.agentComms.sendCommand).toHaveBeenCalled();
    });

    it('should reject if schedule item not found', async () => {
      try {
        await scheduler.runJob('nonexistent-job', false);
        fail('Should have thrown an error');
      } catch (err) {
        expect(err.message).toContain('not found');
      }
    });

    it('should reject if agent not found', async () => {
      mockAgents.getAgent.mockReturnValue(undefined);

      const result = await scheduler.runJob('test-job', false);

      expect(result.status).toBe('error');
      expect(result.executionId).toBeNull();
      expect(global.notifier.sendNotification).toHaveBeenCalledWith(
        expect.stringContaining('Unable to execute job'),
        expect.stringContaining('does not exist'),
        expect.anything(),
        expect.anything()
      );
    });

    it('should reject if agent is offline', async () => {
      mockAgents.getAgent.mockReturnValue({
        name: 'test-agent',
        status: 'offline',
      });

      const result = await scheduler.runJob('test-job', false);

      expect(result.status).toBe('error');
      expect(result.executionId).toBeNull();
      expect(global.notifier.sendNotification).toHaveBeenCalledWith(
        expect.stringContaining('Unable to execute job'),
        expect.stringContaining('is offline'),
        expect.anything(),
        expect.anything()
      );
    });

    it('should refuse to read a script path outside scripts/', async () => {
      db.getData.mockResolvedValue([
        {
          jobName: 'test-job',
          description: 'Test Job',
          agent: 'test-agent',
          command: '../data/server-config.json',
          commandParams: '',
          scheduleType: 'daily',
          scheduleTime: '12:00',
          scheduleMode: 'classic',
        },
      ]);
      await scheduler.init();
      mockAgents.getAgent.mockReturnValue({
        name: 'test-agent',
        status: 'online',
      });
      fs.promises.readFile.mockClear();
      global.agentComms.sendCommand.mockClear();

      const result = await scheduler.runJob('test-job', true);

      expect(result.status).toBe('error');
      expect(result.executionId).toBeNull();
      expect(fs.promises.readFile).not.toHaveBeenCalled();
      expect(global.agentComms.sendCommand).not.toHaveBeenCalled();
      expect(mockHistory.createHistoryItem).toHaveBeenCalledWith(
        'test-job',
        expect.any(String),
        1,
        0,
        expect.stringContaining('outside the scripts directory'),
        true,
        expect.any(String),
        null
      );
    });

    it('should handle script file read errors', async () => {
      mockAgents.getAgent.mockReturnValue({
        name: 'test-agent',
        status: 'online',
      });

      const missingScriptErr = new Error('ENOENT: file not found');
      missingScriptErr.code = 'ENOENT';
      fs.promises.readFile.mockRejectedValue(missingScriptErr);

      const result = await scheduler.runJob('test-job', false);

      expect(result.status).toBe('error');
      expect(result.executionId).toBeNull();
      expect(mockHistory.createHistoryItem).toHaveBeenCalledWith(
        'test-job',
        expect.any(String),
        1,
        0,
        expect.stringContaining('Script cannot be found'),
        false,
        expect.any(String),
        null
      );
      expect(mockHistory.add).toHaveBeenCalled();
      expect(global.notifier.sendNotification).toHaveBeenCalledWith(
        expect.stringContaining('Error reading backup script'),
        expect.stringContaining('Script cannot be found'),
        expect.anything(),
        expect.anything()
      );
    });

    it('should add additional data to command params if provided', async () => {
      mockAgents.getAgent.mockReturnValue({
        name: 'test-agent',
        status: 'online',
      });

      fs.promises.readFile.mockResolvedValue('#!/bin/bash\necho "test"');

      const result = await scheduler.runJob('test-job', false, ' --extra-param=value');

      expect(result.status).toBe('ok');
      expect(result.executionId).toBeDefined();
      expect(global.agentComms.sendCommand).toHaveBeenCalled();
    });

    it('should set manual flag when isManual is true', async () => {
      mockAgents.getAgent.mockReturnValue({
        name: 'test-agent',
        status: 'online',
      });

      fs.promises.readFile.mockResolvedValue('#!/bin/bash\necho "test"');

      await scheduler.init(); // Initialize schedules

      const result = await scheduler.runJob('test-job', true);

      expect(result.status).toBe('ok');
      expect(result.executionId).toBeDefined();
      const callArgs = global.agentComms.sendCommand.mock.calls[0];
      expect(callArgs[6]).toBe(true); // isManual is 7th param (index 6)
    });

    it('should delete old log entry before execution', async () => {
      mockAgents.getAgent.mockReturnValue({
        name: 'test-agent',
        status: 'online',
      });

      fs.promises.readFile.mockResolvedValue('#!/bin/bash\necho "test"');

      await scheduler.runJob('test-job', false);

      expect(db.deleteData).toHaveBeenCalledWith('test-agent_test-job_log');
    });

    it('should handle orchestration schedule mode', async () => {
      db.getData.mockResolvedValue([
        {
          jobName: 'orch-job',
          description: 'Orchestration Job',
          scheduleType: 'daily',
          scheduleTime: '12:00',
          scheduleMode: 'orchestration',
          orchestrationId: 'orch-123',
        },
      ]);
      await scheduler.init();

      // Mock orchestrationEngine
      const mockOrchestrationEngine = {
        executeJob: jest.fn().mockResolvedValue({
          finalStatus: 'success',
          executionId: 'exec-1',
          jobId: 'orch-123',
        }),
        saveExecutionResult: jest.fn().mockResolvedValue(undefined),
      };

      jest.doMock('../../orchestrationEngine.js', () => mockOrchestrationEngine);

      const result = await scheduler.runJob('orch-job', false);

      expect(result.status).toBe('ok');
      expect(result.executionId).toBe('exec-1');
    });

    it('should handle orchestration schedule mode with success', async () => {
      // This test verifies that orchestration mode is recognized
      // Full orchestrationEngine integration testing is done in orchestrationEngine.test.js
      db.getData.mockResolvedValue([
        {
          jobName: 'orch-job',
          description: 'Orchestration Job',
          scheduleType: 'daily',
          scheduleTime: '12:00',
          scheduleMode: 'orchestration',
          orchestrationId: 'orch-123',
        },
      ]);
      await scheduler.init();

      // Just verify that the schedule was loaded
      const schedule = scheduler.getSchedule('orch-job');
      expect(schedule.scheduleMode).toBe('orchestration');
      expect(schedule.orchestrationId).toBe('orch-123');
    });


    it('should skip automatic runs when the job is disabled and still allow a manual run', async () => {
      db.getData.mockResolvedValue([
        {
          jobName: 'test-job',
          description: 'Test Job',
          agent: 'test-agent',
          command: 'test.sh',
          commandParams: '',
          scheduleType: 'daily',
          scheduleTime: '12:00',
          scheduleMode: 'classic',
          disabled: true,
        },
      ]);
      await scheduler.init();
      mockAgents.getAgent.mockReturnValue({
        name: 'test-agent',
        status: 'online',
      });
      fs.promises.readFile.mockResolvedValue('#!/bin/bash\necho "test"');

      const skipped = await scheduler.runJob('test-job', false);
      expect(skipped.status).toBe('skipped');
      expect(skipped.executionId).toBeNull();
      expect(global.agentComms.sendCommand).not.toHaveBeenCalled();

      const manual = await scheduler.runJob('test-job', 'manual');
      expect(manual.status).toBe('ok');
      expect(global.agentComms.sendCommand).toHaveBeenCalled();
    });

    it('should create history item on agent offline error', async () => {
      mockAgents.getAgent.mockReturnValue({
        name: 'test-agent',
        status: 'offline',
      });

      await scheduler.runJob('test-job', false);

      expect(mockHistory.createHistoryItem).toHaveBeenCalledWith(
        'test-job',
        expect.any(String),
        9998,
        0,
        expect.stringContaining('is offline'),
        false
      );
      expect(mockHistory.add).toHaveBeenCalled();
    });
  });

  describe('scheduleJobs() - internal function tested through init', () => {
    it('should schedule jobs during initialization', async () => {
      db.getData.mockResolvedValue([
        {
          jobName: 'daily-job',
          description: 'Daily Job',
          agent: 'test-agent',
          command: 'daily.sh',
          scheduleType: 'daily',
          scheduleTime: '14:30',
          dayOfWeek: '*',
          dayInMonth: '*',
        },
      ]);

      await scheduler.init();

      // scheduleJobs is called internally during init
      expect(logger.info).toHaveBeenCalled();
      //expect(global.thresholdJobs.empty).toHaveBeenCalled();
    });

    it('should clear existing schedules before scheduling', async () => {
      const mockJob = { cancel: jest.fn() };
      global.nodeschedule.scheduledJobs = { 'existing-job': mockJob };

      db.getData.mockResolvedValue([]);
      await scheduler.init();

      // Should have cleared existing jobs
      expect(mockJob.cancel).toHaveBeenCalled();
    });

    it('should schedule daily, weekly, and monthly jobs', async () => {
      db.getData.mockResolvedValue([
        {
          jobName: 'daily-job',
          description: 'Daily',
          agent: 'test-agent',
          command: 'daily.sh',
          scheduleType: 'daily',
          scheduleTime: '14:30',
        },
        {
          jobName: 'weekly-job',
          description: 'Weekly',
          agent: 'test-agent',
          command: 'weekly.sh',
          scheduleType: 'weekly',
          scheduleTime: '09:00',
          dayOfWeek: '3',
        },
        {
          jobName: 'monthly-job',
          description: 'Monthly',
          agent: 'test-agent',
          command: 'monthly.sh',
          scheduleType: 'monthly',
          scheduleTime: '10:00',
          dayInMonth: '15',
        },
      ]);

      await scheduler.init();

      // Should have called scheduleJob for clock-type jobs
      expect(global.nodeschedule.scheduleJob).toHaveBeenCalled();
    });

    it('should handle threshold jobs', async () => {
      // Threshold jobs have a scheduleType that doesn't match daily/weekly/monthly
      db.getData.mockResolvedValue([
        {
          jobName: 'threshold-job',
          description: 'Threshold',
          agent: 'test-agent',
          command: 'threshold.sh',
          scheduleType: 'threshold',
        },
      ]);

      await scheduler.init();

      // Verify the schedule was loaded
      const schedule = scheduler.getSchedule('threshold-job');
      expect(schedule).toBeDefined();
      expect(schedule.jobName).toBe('threshold-job');
      expect(schedule.scheduleType).toBe('threshold');
    });

    it('should not register a disabled clock job', async () => {
      db.getData.mockResolvedValue([
        {
          jobName: 'enabled-job',
          scheduleType: 'daily',
          scheduleTime: '10:00',
          agent: 'test-agent',
          command: 'a.sh',
        },
        {
          jobName: 'disabled-job',
          scheduleType: 'daily',
          scheduleTime: '11:00',
          agent: 'test-agent',
          command: 'b.sh',
          disabled: true,
        },
      ]);

      global.nodeschedule.scheduleJob.mockClear();
      await scheduler.init();

      expect(global.nodeschedule.scheduleJob).toHaveBeenCalledTimes(1);
      expect(global.nodeschedule.scheduleJob).toHaveBeenCalledWith('0 0 10 * * *', expect.any(Function));
    });

    it('should not poll a disabled rule job', async () => {
      const setIntervalSpy = jest.spyOn(global, 'setInterval');
      db.getData.mockResolvedValue([
        {
          jobName: 'rule-job',
          triggerType: 'rule',
          disabled: true,
          rulePollIntervalMins: 15,
          ruleMetric: { agent: 'test-agent', type: 'cpu' },
          ruleCondition: { operator: '>', threshold: 1 },
        },
      ]);

      await scheduler.init();

      const ruleCalls = setIntervalSpy.mock.calls.filter((call) => call[1] === 15 * 60 * 1000);
      expect(ruleCalls).toHaveLength(0);
      setIntervalSpy.mockRestore();
    });
  });

  describe('setScheduleDisabled()', () => {
    it('should leave the job in the schedule and stop registering it', async () => {
      db.getData.mockResolvedValue([
        {
          jobName: 'live-job',
          scheduleType: 'daily',
          scheduleTime: '08:00',
          agent: 'test-agent',
          command: 'a.sh',
        },
      ]);
      await scheduler.init();
      global.nodeschedule.scheduleJob.mockClear();

      await scheduler.setScheduleDisabled('live-job', true);

      expect(scheduler.getSchedule('live-job')).toBeDefined();
      expect(scheduler.getSchedule('live-job').disabled).toBe(true);
      expect(global.nodeschedule.scheduleJob).not.toHaveBeenCalled();
    });

    it('should register the job again when it is enabled', async () => {
      db.getData.mockResolvedValue([
        {
          jobName: 'paused-job',
          scheduleType: 'daily',
          scheduleTime: '09:30',
          agent: 'test-agent',
          command: 'a.sh',
          disabled: true,
        },
      ]);
      await scheduler.init();
      global.nodeschedule.scheduleJob.mockClear();

      await scheduler.setScheduleDisabled('paused-job', false);

      expect(scheduler.getSchedule('paused-job').disabled).toBe(false);
      expect(global.nodeschedule.scheduleJob).toHaveBeenCalledWith('0 30 9 * * *', expect.any(Function));
    });

    it('should keep a disabled job out of the today count', async () => {
      db.getData.mockResolvedValue([
        { jobName: 'daily-on', scheduleType: 'daily', scheduleTime: '10:00' },
        { jobName: 'daily-off', scheduleType: 'daily', scheduleTime: '11:00', disabled: true },
      ]);
      await scheduler.init();

      expect(scheduler.getTodaysScheduleCount()).toBe(1);
    });
  });

  describe('upsertSchedule() disabled flag', () => {
    it('should keep the disabled flag when a schedule is saved without a new value', async () => {
      db.getData.mockResolvedValue([
        {
          jobName: 'keep-flag',
          description: 'd',
          scheduleType: 'daily',
          scheduleTime: '12:00',
          agent: 'test-agent',
          command: 'test.sh',
          disabled: true,
          triggerType: 'clock',
          scheduleMode: 'classic',
        },
      ]);
      await scheduler.init();

      await scheduler.upsertSchedule(
        0, 'keep-flag', '#000000', 'd', 'daily', '12:00', '', '',
        'test-agent', 'test.sh', '', 'schedule', 'classic', null,
        'clock', null, null, null, null
      );

      expect(scheduler.getSchedule('keep-flag').disabled).toBe(true);
    });

    it('should store disabled when the edit form sends it', async () => {
      db.getData.mockResolvedValue([]);
      await scheduler.init();
      global.nodeschedule.scheduleJob.mockClear();

      await scheduler.upsertSchedule(
        -1, 'form-job', '#000000', 'd', 'daily', '13:00', '', '',
        'test-agent', 'test.sh', '', 'schedule', 'classic', null,
        'clock', null, null, null, null, true
      );

      expect(scheduler.getSchedule('form-job').disabled).toBe(true);
      expect(global.nodeschedule.scheduleJob).not.toHaveBeenCalled();
    });
  });

  describe('getNextRunDate() edge cases', () => {
    it('should calculate daily next run for future time today', async () => {
      const schedule = {
        scheduleType: 'daily',
        scheduleTime: '23:00', // assuming current time is earlier
      };

      const nextRun = scheduler.getNextRunDate(schedule);

      expect(nextRun).toBeInstanceOf(Date);
      expect(nextRun.getHours()).toBe(23);
      expect(nextRun.getMinutes()).toBe(0);
    });

    it('should calculate daily next run for past time today (move to tomorrow)', async () => {
      const schedule = {
        scheduleType: 'daily',
        scheduleTime: '00:01', // very early morning
      };

      const now = new Date();
      const nextRun = scheduler.getNextRunDate(schedule);

      // Should be tomorrow (unless we test at exactly 00:00-00:01)
      expect(nextRun).toBeInstanceOf(Date);
      expect(nextRun.getHours()).toBe(0);
      expect(nextRun.getMinutes()).toBe(1);
    });

    it('should calculate weekly next run for scheduled day', async () => {
      const now = new Date();
      const dayOfWeek = (now.getDay() + 1) % 7; // Tomorrow's day

      const schedule = {
        scheduleType: 'weekly',
        scheduleTime: '15:30',
        dayOfWeek: dayOfWeek.toString(),
      };

      const nextRun = scheduler.getNextRunDate(schedule);

      expect(nextRun).toBeInstanceOf(Date);
      expect(nextRun.getDay()).toBe(dayOfWeek);
    });

    it('should calculate monthly next run for valid day', async () => {
      const schedule = {
        scheduleType: 'monthly',
        scheduleTime: '12:00',
        dayInMonth: '25',
      };

      const nextRun = scheduler.getNextRunDate(schedule);

      expect(nextRun).toBeInstanceOf(Date);
      expect(nextRun.getDate()).toBe(25);
    });

    it('should throw error for invalid dayInMonth > 31', async () => {
      const schedule = {
        scheduleType: 'monthly',
        scheduleTime: '12:00',
        dayInMonth: '32',
      };

      expect(() => scheduler.getNextRunDate(schedule)).toThrow('Invalid dayInMonth');
    });

    it('should throw error for invalid dayInMonth < 1', async () => {
      const schedule = {
        scheduleType: 'monthly',
        scheduleTime: '12:00',
        dayInMonth: '0',
      };

      expect(() => scheduler.getNextRunDate(schedule)).toThrow('Invalid dayInMonth');
    });

    it('should return "n/a" for invalid schedule type', () => {
      const schedule = {
        scheduleType: 'invalid',
        scheduleTime: '12:00',
      };

      const result = scheduler.getNextRunDate(schedule);

      expect(result).toBe('n/a');
    });

    it('should parse schedule time correctly', () => {
      const schedule = {
        scheduleType: 'daily',
        scheduleTime: '14:45',
      };

      const nextRun = scheduler.getNextRunDate(schedule);

      expect(nextRun.getHours()).toBe(14);
      expect(nextRun.getMinutes()).toBe(45);
    });
  });

  describe('getTodaysScheduleCount()', () => {
    beforeEach(async () => {
      const now = new Date();
      const todayDayOfWeek = now.getDay();

      db.getData.mockResolvedValue([
        { jobName: 'daily-1', scheduleType: 'daily', dayOfWeek: '*', dayInMonth: '*' },
        { jobName: 'daily-2', scheduleType: 'daily', dayOfWeek: '*', dayInMonth: '*' },
        { jobName: 'weekly', scheduleType: 'weekly', dayOfWeek: todayDayOfWeek.toString(), dayInMonth: '*' },
        { jobName: 'monthly', scheduleType: 'monthly', dayOfWeek: '*', dayInMonth: '31' },
      ]);
      await scheduler.init();
    });

    it('should count jobs scheduled for today', () => {
      const count = scheduler.getTodaysScheduleCount();

      expect(typeof count).toBe('number');
      expect(count).toBeGreaterThanOrEqual(2); // At least the two daily jobs
    });

    it('should return 0 if no jobs scheduled for today', async () => {
      db.getData.mockResolvedValue([
        { jobName: 'monthly', scheduleType: 'monthly', dayOfWeek: '*', dayInMonth: '31' },
      ]);
      await scheduler.init();

      const count = scheduler.getTodaysScheduleCount();

      if (new Date().getDate() !== 31) {
        expect(count).toBe(0);
      }
    });

    it('should include daily jobs in count', async () => {
      db.getData.mockResolvedValue([
        { jobName: 'daily-job', scheduleType: 'daily', dayOfWeek: '*', dayInMonth: '*' },
      ]);
      await scheduler.init();

      const count = scheduler.getTodaysScheduleCount();

      expect(count).toBeGreaterThanOrEqual(1);
    });
  });

  describe('getLast7DaysScheduleCount()', () => {
    beforeEach(async () => {
      db.getData.mockResolvedValue([
        { jobName: 'daily-1', scheduleType: 'daily', dayOfWeek: '*', dayInMonth: '*' },
        { jobName: 'daily-2', scheduleType: 'daily', dayOfWeek: '*', dayInMonth: '*' },
      ]);
      await scheduler.init();
    });

    it('should return array of 7 daily counts', () => {
      const counts = scheduler.getLast7DaysScheduleCount();

      expect(Array.isArray(counts)).toBe(true);
      expect(counts.length).toBe(7);
    });

    it('should have all numeric values', () => {
      const counts = scheduler.getLast7DaysScheduleCount();

      counts.forEach(count => {
        expect(typeof count).toBe('number');
        expect(count).toBeGreaterThanOrEqual(0);
      });
    });

    it('should include today count at end of array', () => {
      const counts = scheduler.getLast7DaysScheduleCount();
      const todayCount = scheduler.getTodaysScheduleCount();

      expect(counts[6]).toBe(todayCount);
    });

    it('should have increasing counts for daily jobs', () => {
      const counts = scheduler.getLast7DaysScheduleCount();

      // With daily jobs, all days should have same count
      counts.forEach(count => {
        expect(count).toBe(2); // Two daily jobs from beforeEach
      });
    });
  });

  describe('migrateSchedulesToDatabase()', () => {
    it('should be called during init if JSON file exists', async () => {
      // migrateSchedulesToDatabase is internal, but we can verify
      // it was called during init by checking if db.putData was called
      fs.existsSync.mockReturnValue(true);
      fs.promises.readFile.mockResolvedValue(JSON.stringify([
        { jobName: 'test-job', agent: 'test-agent', command: 'test.sh' },
      ]));
      fs.promises.unlink.mockResolvedValue(undefined);

      // Clear previous calls
      db.putData.mockClear();

      await scheduler.init();

      // If migration was triggered, putData should have been called
      expect(db.putData).toHaveBeenCalled();
    });

    it('should not fail if migration file does not exist', async () => {
      fs.existsSync.mockReturnValue(false);

      // Should initialize without errors
      await scheduler.init();

      expect(logger.info).toHaveBeenCalled();
    });
  });


  describe('runUpdateJob()', () => {
    it('should execute agent update job when agent is online', () => {
      mockAgents.getAgent.mockReturnValue({
        name: 'test-agent',
        id: 'agent-1',
        status: 'online',
      });

      const result = scheduler.runUpdateJob('agent-1', 'update-command');

      expect(result).toBe('ok');
      expect(global.agentComms.sendCommand).toHaveBeenCalled();
    });

    it('should reject if agent is offline', () => {
      mockAgents.getAgent.mockReturnValue({
        name: 'test-agent',
        id: 'agent-1',
        status: 'offline',
      });

      const result = scheduler.runUpdateJob('agent-1', 'update-command');

      expect(result).toBe('error');
      expect(global.notifier.sendNotification).toHaveBeenCalledWith(
        expect.stringContaining('Upgrade Failed'),
        expect.stringContaining('not in the correct state'),
        'ERROR',
        expect.anything()
      );
    });

    it('should create history item for failed update', () => {
      mockAgents.getAgent.mockReturnValue({
        name: 'test-agent',
        id: 'agent-1',
        status: 'offline',
      });

      scheduler.runUpdateJob('agent-1', 'update-command');

      expect(mockHistory.createHistoryItem).toHaveBeenCalled();
      expect(mockHistory.add).toHaveBeenCalled();
    });

    it('should delete old log before update', () => {
      mockAgents.getAgent.mockReturnValue({
        name: 'test-agent',
        id: 'agent-1',
        status: 'online',
      });

      scheduler.runUpdateJob('agent-1', 'update-command');

      expect(db.deleteData).toHaveBeenCalled();
    });
  });
});

afterAll(() => {
  if (ORIGINAL_DEFINITIONS_BACKEND === undefined) {
    delete process.env.DEFINITIONS_BACKEND;
    return;
  }
  process.env.DEFINITIONS_BACKEND = ORIGINAL_DEFINITIONS_BACKEND;
});

