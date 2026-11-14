// Mock setup for orchestration module tests

const ORIGINAL_DEFINITIONS_BACKEND = process.env.DEFINITIONS_BACKEND;
process.env.DEFINITIONS_BACKEND = 'db';

// Mock db module before importing orchestration
jest.mock('../../db.js', () => ({
  getData: jest.fn(),
  putData: jest.fn(),
  deleteData: jest.fn(),
}));

// Mock fs module before importing orchestration
jest.mock('fs', () => {
  const actualFs = jest.requireActual('fs');
  return {
    ...actualFs,
    promises: {
      readdir: jest.fn(),
      readFile: jest.fn(),
      writeFile: jest.fn(),
    },
  };
});

// Mock orchestrationEngine before importing orchestration
jest.mock('../../orchestrationEngine.js', () => ({
  executeJob: jest.fn(),
  getExecutionHistory: jest.fn(),
  saveExecutionResult: jest.fn(),
}));

// Mock wsBrowserTransport before importing orchestration
jest.mock('../../communications/wsBrowserTransport.js', () => ({
  emitNotification: jest.fn(),
}));

const db = require('../../db.js');
const fs = require('fs');

describe('Orchestration Module', () => {
  let orchestration;

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

    // Mock fs.promises
    fs.promises = {
      readdir: jest.fn(),
      readFile: jest.fn(),
      writeFile: jest.fn(),
    };

    // Import orchestration module (only once after mocks are set up)
    if (!orchestration) {
      orchestration = require('../../orchestration.js');
    }
  });

  afterEach(() => {
    delete global.logger;
    delete global.serverConfig;
    delete global.db;
  });

  describe('init()', () => {
    it('should initialize and load orchestration jobs from database', async () => {
      const mockJobs = {
        job1: {
          id: 'job1',
          name: 'Test Job',
          versions: [{ version: 1, nodes: [], edges: [], createdAt: '2026-03-27T00:00:00Z' }],
        },
      };
      db.getData.mockResolvedValue(mockJobs);

      const result = await orchestration.init();

      expect(db.getData).toHaveBeenCalledWith('ORCHESTRATION_JOBS');
      expect(Object.keys(result).length).toBe(1);
      expect(logger.info).toHaveBeenCalledWith('Initializing Orchestration Module');
    });

    it('should handle missing orchestration jobs gracefully', async () => {
      db.getData.mockRejectedValue(new Error('NotFoundError: Key not found'));

      const result = await orchestration.init();

      expect(result).toEqual({});
      expect(logger.info).toHaveBeenCalledWith('No orchestration jobs found, starting with empty config');
    });

    it('should throw non-NotFoundError exceptions', async () => {
      db.getData.mockRejectedValue(new Error('Database connection failed'));

      await expect(orchestration.init()).rejects.toThrow('Database connection failed');
    });
  });

  describe('getAllJobs()', () => {
    it('should return all orchestration jobs from database', async () => {
      const mockJobs = {
        job1: { id: 'job1', name: 'Job 1' },
        job2: { id: 'job2', name: 'Job 2' },
      };
      db.getData.mockResolvedValue(mockJobs);

      const result = await orchestration.getAllJobs();

      expect(db.getData).toHaveBeenCalledWith('ORCHESTRATION_JOBS');
      expect(Object.keys(result).length).toBe(2);
    });

    it('should return empty object when no jobs exist', async () => {
      db.getData.mockRejectedValue(new Error('NotFoundError: Key not found'));

      const result = await orchestration.getAllJobs();

      expect(result).toEqual({});
    });
  });

  describe('getJob()', () => {
    it('should retrieve a job by ID with current version', async () => {
      const mockJobs = {
        job1: {
          id: 'job1',
          name: 'Test Job',
          description: 'A test job',
          type: 'orchestration',
          createdAt: '2026-03-27T00:00:00Z',
          updatedAt: '2026-03-27T01:00:00Z',
          currentVersion: 1,
          versions: [
            {
              version: 1,
              nodes: [{ id: 'node1', type: 'execute', data: { script: 'test.sh' } }],
              edges: [],
              createdAt: '2026-03-27T00:00:00Z',
            },
          ],
        },
      };
      db.getData.mockResolvedValue(mockJobs);

      const result = await orchestration.getJob('job1');

      expect(result).toHaveProperty('id', 'job1');
      expect(result).toHaveProperty('name', 'Test Job');
      expect(result.nodes.length).toBe(1);
    });

    it('should throw error when job not found', async () => {
      db.getData.mockResolvedValue({});

      await expect(orchestration.getJob('nonexistent')).rejects.toThrow(
        'Orchestration job [nonexistent] not found'
      );
    });

    it('should log warning when error occurs', async () => {
      db.getData.mockRejectedValue(new Error('Database error'));

      await expect(orchestration.getJob('job1')).rejects.toThrow();
      expect(logger.warn).toHaveBeenCalled();
    });
  });

  describe('getJobVersion()', () => {
    beforeEach(() => {
      const mockJobs = {
        job1: {
          id: 'job1',
          name: 'Test Job',
          description: 'A test job',
          type: 'orchestration',
          createdAt: '2026-03-27T00:00:00Z',
          updatedAt: '2026-03-27T02:00:00Z',
          currentVersion: 2,
          versions: [
            {
              version: 1,
              nodes: [{ id: 'node1', type: 'execute', data: { script: 'v1.sh' } }],
              edges: [],
              createdAt: '2026-03-27T00:00:00Z',
            },
            {
              version: 2,
              nodes: [{ id: 'node1', type: 'execute', data: { script: 'v2.sh' } }],
              edges: [],
              createdAt: '2026-03-27T01:00:00Z',
            },
          ],
        },
      };
      db.getData.mockResolvedValue(mockJobs);
    });

    it('should retrieve current version when version="current"', async () => {
      const result = await orchestration.getJobVersion('job1', 'current');

      expect(result.version).toBe(2);
      expect(result.currentVersion).toBe(2);
    });

    it('should retrieve latest version when version="latest"', async () => {
      const result = await orchestration.getJobVersion('job1', 'latest');

      expect(result.version).toBe(2);
    });

    it('should retrieve specific version by number', async () => {
      const result = await orchestration.getJobVersion('job1', 1);

      expect(result.version).toBe(1);
    });

    it('should throw error when specific version not found', async () => {
      await expect(orchestration.getJobVersion('job1', 99)).rejects.toThrow(
        'Version 99 not found for orchestration job [job1]'
      );
    });

    it('should throw error when job not found', async () => {
      db.getData.mockResolvedValue({});

      await expect(orchestration.getJobVersion('nonexistent', 'current')).rejects.toThrow(
        'Orchestration job [nonexistent] not found'
      );
    });
  });

  describe('saveJob()', () => {
    it('should create a new orchestration job', async () => {
      db.getData.mockResolvedValue({});
      const jobDef = {
        name: 'New Job',
        description: 'A new job',
        nodes: [{ id: 'node1', type: 'execute', data: { script: 'test.sh' } }],
        edges: [],
      };

      const result = await orchestration.saveJob('new-job', jobDef);

      expect(result).toHaveProperty('id', 'new-job');
      expect(result).toHaveProperty('name', 'New Job');
      expect(result).toHaveProperty('currentVersion', 1);
      expect(result.versions.length).toBe(1);
      expect(result.versions[0].version).toBe(1);
      expect(db.putData).toHaveBeenCalled();
      expect(logger.info).toHaveBeenCalledWith('Saving orchestration job [new-job]');
    });

    it('should reject an execute script path outside scripts/', async () => {
      db.getData.mockResolvedValue({});
      const jobDef = {
        name: 'Bad Job',
        nodes: [{ id: 'node1', type: 'execute', data: { script: '../data/server-config.json' } }],
        edges: [],
      };

      await expect(orchestration.saveJob('bad-job', jobDef)).rejects.toThrow('outside the scripts directory');
      expect(db.putData).not.toHaveBeenCalled();
    });

    it('should create new version when job definition changes', async () => {
      const mockJobs = {
        job1: {
          id: 'job1',
          name: 'Old Job',
          description: 'Old description',
          type: 'orchestration',
          createdAt: '2026-03-27T00:00:00Z',
          updatedAt: '2026-03-27T00:00:00Z',
          currentVersion: 1,
          versions: [
            {
              version: 1,
              nodes: [{ id: 'node1', type: 'execute', data: { script: 'old.sh' } }],
              edges: [],
              createdAt: '2026-03-27T00:00:00Z',
            },
          ],
        },
      };
      db.getData.mockResolvedValue(mockJobs);

      const jobDef = {
        name: 'New Name',
        description: 'New description',
        nodes: [{ id: 'node1', type: 'execute', data: { script: 'new.sh' } }],
        edges: [],
      };

      const result = await orchestration.saveJob('job1', jobDef);

      expect(result.currentVersion).toBe(2);
      expect(result.versions.length).toBe(2);
    });

    it('should not create new version if definition unchanged', async () => {
      const existingJob = {
        id: 'job1',
        name: 'Test Job',
        description: 'Test description',
        type: 'orchestration',
        createdAt: '2026-03-27T00:00:00Z',
        updatedAt: '2026-03-27T00:00:00Z',
        currentVersion: 1,
        versions: [
          {
            version: 1,
            nodes: [{ id: 'node1', type: 'execute' }],
            edges: [],
            createdAt: '2026-03-27T00:00:00Z',
          },
        ],
      };

      db.getData.mockResolvedValue({ job1: existingJob });

      const jobDef = {
        name: 'Test Job',
        description: 'Test description',
        nodes: [{ id: 'node1', type: 'execute' }],
        edges: [],
      };

      const result = await orchestration.saveJob('job1', jobDef);

      expect(result.currentVersion).toBe(1);
      expect(result.versions.length).toBe(1);
    });

    it('should handle database errors', async () => {
      db.getData.mockRejectedValue(new Error('Database error'));

      const jobDef = { name: 'Job', description: '', nodes: [], edges: [] };

      await expect(orchestration.saveJob('job1', jobDef)).rejects.toThrow('Database error');
      expect(logger.error).toHaveBeenCalled();
    });
  });

  describe('deleteJob()', () => {
    it('should delete an existing orchestration job', async () => {
      const mockJobs = {
        job1: { id: 'job1', name: 'Job 1' },
        job2: { id: 'job2', name: 'Job 2' },
      };
      db.getData.mockResolvedValue(mockJobs);

      const result = await orchestration.deleteJob('job1');

      expect(result).toBe(true);
      expect(db.putData).toHaveBeenCalled();
      const savedJobs = db.putData.mock.calls[0][1];
      expect(savedJobs).not.toHaveProperty('job1');
      expect(logger.info).toHaveBeenCalledWith('Deleting orchestration job [job1]');
    });

    it('should throw error when job not found', async () => {
      db.getData.mockResolvedValue({});

      await expect(orchestration.deleteJob('nonexistent')).rejects.toThrow(
        'Orchestration job [nonexistent] not found'
      );
    });

    it('should handle database errors during delete', async () => {
      db.getData.mockRejectedValue(new Error('Database error'));

      await expect(orchestration.deleteJob('job1')).rejects.toThrow('Database error');
      expect(logger.error).toHaveBeenCalled();
    });
  });

  describe('migrateToVersionedFormat()', () => {
    it('should migrate old format jobs to versioned format', async () => {
      const mockJobs = {
        job1: {
          id: 'job1',
          name: 'Old Job',
          description: 'Old format',
          type: 'orchestration',
          nodes: [{ id: 'node1', type: 'execute' }],
          edges: [],
          createdAt: '2026-03-27T00:00:00Z',
          updatedAt: '2026-03-27T00:00:00Z',
        },
      };
      db.getData.mockResolvedValue(mockJobs);

      const migratedCount = await orchestration.migrateToVersionedFormat();

      expect(migratedCount).toBe(1);
      expect(db.putData).toHaveBeenCalled();

      const savedJobs = db.putData.mock.calls[0][1];
      expect(savedJobs.job1.versions).toBeDefined();
      expect(savedJobs.job1.versions.length).toBe(1);
      expect(savedJobs.job1.currentVersion).toBe(1);
    });

    it('should skip already migrated jobs', async () => {
      const mockJobs = {
        job1: {
          id: 'job1',
          name: 'Already Migrated',
          versions: [{ version: 1, nodes: [], edges: [], createdAt: '2026-03-27T00:00:00Z' }],
          currentVersion: 1,
        },
      };
      db.getData.mockResolvedValue(mockJobs);

      const migratedCount = await orchestration.migrateToVersionedFormat();

      expect(migratedCount).toBe(0);
      expect(logger.debug).toHaveBeenCalledWith('Job [job1] already migrated, skipping');
    });

    it('should handle no jobs needing migration', async () => {
      db.getData.mockResolvedValue({});

      const migratedCount = await orchestration.migrateToVersionedFormat();

      expect(migratedCount).toBe(0);
      expect(logger.info).toHaveBeenCalledWith('No orchestrations needed migration');
    });

    it('should handle database errors during migration', async () => {
      db.getData.mockRejectedValue(new Error('Database error'));

      await expect(orchestration.migrateToVersionedFormat()).rejects.toThrow('Database error');
      expect(logger.error).toHaveBeenCalled();
    });

    it('should migrate multiple jobs', async () => {
      const mockJobs = {
        job1: {
          id: 'job1',
          name: 'Job 1',
          description: 'Desc 1',
          type: 'orchestration',
          nodes: [],
          edges: [],
          createdAt: '2026-03-27T00:00:00Z',
        },
        job2: {
          id: 'job2',
          name: 'Job 2',
          description: 'Desc 2',
          nodes: [],
          edges: [],
          createdAt: '2026-03-27T01:00:00Z',
          updatedAt: '2026-03-27T02:00:00Z',
        },
      };
      db.getData.mockResolvedValue(mockJobs);

      const migratedCount = await orchestration.migrateToVersionedFormat();

      expect(migratedCount).toBe(2);
      expect(db.putData).toHaveBeenCalled();

      const savedJobs = db.putData.mock.calls[0][1];
      expect(savedJobs.job1.versions).toBeDefined();
      expect(savedJobs.job2.versions).toBeDefined();
    });
  });

  describe('getAvailableScripts()', () => {
    it('should return list of available scripts', async () => {
      fs.promises.readdir.mockResolvedValue(['backup.sh', 'restore.js', 'cleanup.py']);

      const result = await orchestration.getAvailableScripts();

      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(3);
      expect(result[0]).toHaveProperty('id', 'backup.sh');
      expect(result[0]).toHaveProperty('name', 'backup');
      expect(result[0]).toHaveProperty('type', 'script');
    });

    it('should filter only valid script extensions', async () => {
      fs.promises.readdir.mockResolvedValue(['script.sh', 'code.js', 'data.py', 'readme.txt', 'file.md']);

      const result = await orchestration.getAvailableScripts();

      expect(result.length).toBe(3);
      expect(result.map(s => s.filename)).toContain('script.sh');
      expect(result.map(s => s.filename)).toContain('code.js');
      expect(result.map(s => s.filename)).toContain('data.py');
    });

    it('should handle empty scripts directory', async () => {
      fs.promises.readdir.mockResolvedValue([]);

      const result = await orchestration.getAvailableScripts();

      expect(result).toEqual([]);
    });

    it('should handle file read errors gracefully', async () => {
      fs.promises.readdir.mockRejectedValue(new Error('Permission denied'));

      const result = await orchestration.getAvailableScripts();

      expect(result).toEqual([]);
      expect(logger.warn).toHaveBeenCalledWith(
        expect.stringContaining('Error reading available scripts')
      );
    });

    it('should remove file extensions correctly', async () => {
      fs.promises.readdir.mockResolvedValue(['backup_db.sh', 'restore_vault.sh']);

      const result = await orchestration.getAvailableScripts();

      expect(result[0].name).toBe('backup_db');
      expect(result[1].name).toBe('restore_vault');
    });
  });

  describe('Integration Tests', () => {
    it('should create, retrieve, and delete a job', async () => {
      // Create
      db.getData.mockResolvedValue({});
      const jobDef = {
        name: 'Integration Test Job',
        description: 'Test',
        nodes: [{ id: 'n1', type: 'execute' }],
        edges: [],
      };

      const created = await orchestration.saveJob('integration-job', jobDef);
      expect(created.id).toBe('integration-job');

      // Retrieve
      db.getData.mockResolvedValue({ 'integration-job': created });
      const retrieved = await orchestration.getJob('integration-job');
      expect(retrieved.name).toBe('Integration Test Job');

      // Delete
      const deleted = await orchestration.deleteJob('integration-job');
      expect(deleted).toBe(true);
    });

    it('should handle multiple versions of same job', async () => {
      db.getData.mockResolvedValue({});

      // Create version 1
      const v1 = {
        name: 'Multi Version Job',
        description: 'Version 1',
        nodes: [{ id: 'n1', type: 'execute', data: { script: 'v1.sh' } }],
        edges: [],
      };

      const saved1 = await orchestration.saveJob('multi-job', v1);
      expect(saved1.currentVersion).toBe(1);

      // Simulate update to version 2
      const v2 = {
        name: 'Multi Version Job',
        description: 'Version 2',
        nodes: [{ id: 'n1', type: 'execute', data: { script: 'v2.sh' } }],
        edges: [],
      };

      db.getData.mockResolvedValue({ 'multi-job': saved1 });

      const saved2 = await orchestration.saveJob('multi-job', v2);
      expect(saved2.currentVersion).toBe(2);
      expect(saved2.versions.length).toBe(2);
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
