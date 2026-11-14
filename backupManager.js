/**
 * Backup and Restore Manager
 * Handles exporting and importing Orchelium settings and data
 */

const fs = require('fs').promises;
const fsSync = require('fs');
const path = require('path');
const archiver = require('archiver');
const { ZipArchive } = require('archiver');
const unzipper = require('unzipper');
const { AppError, handleError } = require('./utils/errorHandler.js');

// Database instances
const db = require('./db.js');
const definitionStore = require('./definitionStore.js');
const agentHistoryDb = require('./agentHistory.js');
const configuration = require('./configuration.js');

const DATA_DIR = './data';
const DATABASES = {
  main: './data/data.db',
  user: './data/user.db',
  agentHistory: './data/agentHistory.db',
};

const CONFIG_FILE = './data/server-config.json';

/**
 * Get available backup items
 * Returns list of what can be backed up
 */
function getBackupItems() {
  return {
    serverConfig: {
      name: 'Server Configuration',
      description: 'MQTT, SMTP, WebSocket, Notifications, Icons settings',
    },
    agentsConfig: {
      name: 'Agents Configuration',
      description: 'All registered agent configurations',
    },
    schedules: {
      name: 'Job Schedules & Rules',
      description: 'All configured Job schedules and metric-based rules',
    },
    jobHistory: {
      name: 'Job Execution History',
      description: 'Historical logs of all job executions',
    },
    orchestrationJobs: {
      name: 'Orchestration Jobs',
      description: 'All configured orchestration job definitions',
    },
    orchestrationExecutions: {
      name: 'Orchestration Execution History',
      description: 'Historical records of orchestration job executions',
    },
    agentHistory: {
      name: 'Agent Connection History',
      description: 'Historical logs of agent connections',
    },
    webhooks: {
      name: 'Webhooks',
      description: 'All configured webhooks and their API keys for external job triggers',
    },
    userAccounts: {
      name: 'User Accounts',
      description: 'All registered user accounts (passwords are bcrypt-hashed)',
    },
  };
}

/**
 * Create a backup zip file
 * @param {Object} options - Backup options
 * @param {boolean} options.serverConfig - Include server configuration
 * @param {boolean} options.agentsConfig - Include agents configuration
 * @param {boolean} options.schedules - Include schedules
 * @param {boolean} options.jobHistory - Include job execution history
 * @param {boolean} options.orchestrationJobs - Include orchestration jobs
 * @param {boolean} options.orchestrationExecutions - Include orchestration executions
 * @param {boolean} options.agentHistory - Include agent history
 * @param {boolean} options.userAccounts - Include user accounts
 * @returns {Promise<Buffer>} - Zip file buffer
 */
async function createBackup(options = {}) {
  try {
    const {
      serverConfig = true,
      agentsConfig = true,
      schedules = true,
      jobHistory = true,
      orchestrationJobs = true,
      orchestrationExecutions = true,
      agentHistory = true,
      webhooks = true,
      userAccounts = true,
    } = options;

    logger.info('Starting backup process with options:', options);

    // Create an in-memory zip archive
    const archive = new ZipArchive({ zlib: { level: 6 } });
    const chunks = [];

    // Capture archived data in buffer
    archive.on('data', (chunk) => {
      chunks.push(chunk);
    });

    archive.on('error', (err) => {
      logger.error('Archive error:', err);
      throw new AppError('Failed to create backup archive', 500);
    });

    // Finalize is async, so we need to wait for it
    await new Promise(async (resolve, reject) => {
      archive.on('finish', resolve);
      archive.on('error', reject);

      try {
        // Add files to archive based on selection, then finalize
        await addFilesToArchive(archive, {
          serverConfig,
          agentsConfig,
          schedules,
          jobHistory,
          orchestrationJobs,
          orchestrationExecutions,
          agentHistory,
          webhooks,
          userAccounts,
        });

        // Finalize only after all files have been appended
        await archive.finalize();
      } catch (err) {
        reject(err);
      }
    });

    const backupBuffer = Buffer.concat(chunks);
    logger.info(`Backup created successfully (${backupBuffer.length} bytes)`);
    return backupBuffer;
  } catch (err) {
    logger.error('Backup creation failed:', err);
    throw new AppError(`Backup failed: ${err.message}`, 500);
  }
}

/**
 * Helper function to add files to archive
 * @private
 */
async function addFilesToArchive(archive, options) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const backupDir = `backup-${timestamp}`;

  try {
    // Add server config
    if (options.serverConfig && fsSync.existsSync(CONFIG_FILE)) {
      const configContent = await fs.readFile(CONFIG_FILE, 'utf8');
      archive.append(configContent, { name: `${backupDir}/server-config.json` });
      logger.debug('Added server configuration to backup');
    }

    // Add agents config
    if (options.agentsConfig) {
      try {
        const agents = await db.getData('AGENTS_CONFIG');
        if (agents) {
          archive.append(JSON.stringify(agents, null, 2), {
            name: `${backupDir}/agents-config.json`,
          });
          logger.debug('Added agents configuration to backup');
        }
      } catch (err) {
        logger.debug('No agents config found in database');
      }
    }

    // Add webhooks
    if (options.webhooks) {
      try {
        const webhooksIndex = await db.getData('WEBHOOKS_INDEX');
        if (webhooksIndex && Object.keys(webhooksIndex).length > 0) {
          // Get all webhook data
          const webhooksData = {};
          for (const [jobId, webhookIds] of Object.entries(webhooksIndex)) {
            webhooksData[jobId] = [];
            for (const webhookId of webhookIds) {
              const key = `WEBHOOK_DATA_${jobId}_${webhookId}`;
              try {
                const webhookData = await db.getData(key);
                if (webhookData) {
                  webhooksData[jobId].push(webhookData);
                }
              } catch (err) {
                logger.debug(`Could not retrieve webhook data for ${key}`);
              }
            }
          }
          const backupData = {
            index: webhooksIndex,
            data: webhooksData,
          };
          archive.append(JSON.stringify(backupData, null, 2), {
            name: `${backupDir}/webhooks.json`,
          });
          logger.debug('Added webhooks to backup');
        }
      } catch (err) {
        logger.debug('No webhooks found in database');
      }
    }

    // Add schedules
    if (options.schedules) {
      try {
        const schedules = await definitionStore.listJobs();
        if (schedules) {
          archive.append(JSON.stringify(schedules, null, 2), {
            name: `${backupDir}/schedules-config.json`,
          });
          logger.debug('Added schedules to backup');
        }
      } catch (err) {
        logger.debug('No schedules found in database');
      }
    }

    // Add job history
    if (options.jobHistory) {
      try {
        const jobHistory = await db.getData('JOB_HISTORY');
        if (jobHistory) {
          archive.append(JSON.stringify(jobHistory, null, 2), {
            name: `${backupDir}/job-history.json`,
          });
          logger.debug('Added job execution history to backup');
        }
      } catch (err) {
        logger.debug('No job history found in database');
      }
    }

    // Add orchestration jobs
    if (options.orchestrationJobs) {
      try {
        const orchJobs = await definitionStore.listOrchestrations();
        if (orchJobs) {
          archive.append(JSON.stringify(orchJobs, null, 2), {
            name: `${backupDir}/orchestration-jobs.json`,
          });
          logger.debug('Added orchestration jobs to backup');
        }
      } catch (err) {
        logger.debug('No orchestration jobs found in database');
      }
    }

    // Add orchestration executions
    if (options.orchestrationExecutions) {
      try {
        const orchExecutions = await db.getData('ORCHESTRATION_EXECUTIONS');
        if (orchExecutions) {
          archive.append(JSON.stringify(orchExecutions, null, 2), {
            name: `${backupDir}/orchestration-executions.json`,
          });
          logger.debug('Added orchestration execution history to backup');
        }
      } catch (err) {
        logger.debug('No orchestration executions found in database');
      }
    }

    // Add agent history
    if (options.agentHistory) {
      try {
        const historyEntries = await agentHistoryDb.exportAll();
        if (historyEntries.length > 0) {
          archive.append(JSON.stringify(historyEntries, null, 2), {
            name: `${backupDir}/agent-history.json`,
          });
          logger.debug(`Added ${historyEntries.length} history records to backup`);
        }
      } catch (err) {
        logger.debug('No agent history found in database');
      }
    }

    // Add user accounts (with bcrypt-hashed passwords)
    if (options.userAccounts) {
      try {
        const { Level } = require('level');
        const userDb = new Level('./data/user.db', { valueEncoding: 'json' });
        const users = [];
        for await (const [username, userData] of userDb.iterator({})) {
          users.push({ username, ...userData });
        }
        await userDb.close();
        if (users.length > 0) {
          archive.append(JSON.stringify(users, null, 2), {
            name: `${backupDir}/user-accounts.json`,
          });
          logger.debug(`Added ${users.length} user accounts to backup`);
        }
      } catch (err) {
        logger.debug('No user accounts found in database');
      }
    }

    // Add metadata
    const metadata = {
      backupVersion: '1.0',
      backupDate: new Date().toISOString(),
      appVersion: require('./version.js').getVersion(),
      items: {
        serverConfig: options.serverConfig,
        agentsConfig: options.agentsConfig,
        schedules: options.schedules,
        jobHistory: options.jobHistory,
        orchestrationJobs: options.orchestrationJobs,
        orchestrationExecutions: options.orchestrationExecutions,
        agentHistory: options.agentHistory,
        userAccounts: options.userAccounts,
        webhooks: options.webhooks,
      },
    };
    archive.append(JSON.stringify(metadata, null, 2), {
      name: `${backupDir}/backup-metadata.json`,
    });

    logger.debug('Backup archive created successfully');
  } catch (err) {
    logger.error('Error adding files to backup archive:', err);
    throw err;
  }
}

/**
 * Restore backup from zip file
 * @param {Buffer} zipBuffer - Zip file buffer
 * @returns {Promise<Object>} - Restore results with recommendations
 */
async function restoreBackup(zipBuffer) {
  let tempDir = null;

  try {
    // Create temporary directory for extraction
    tempDir = path.join(DATA_DIR, `.restore_${Date.now()}`);
    await fs.mkdir(tempDir, { recursive: true });

    logger.info('Starting backup restore process');

    // Extract zip to temp directory
    await extractZip(zipBuffer, tempDir);

    // Find the backup directory (should be the only directory in temp)
    const files = await fs.readdir(tempDir);
    const backupDirName = files.find((f) => f.startsWith('backup-'));

    if (!backupDirName) {
      throw new AppError('Invalid backup file: no backup directory found', 400);
    }

    const backupPath = path.join(tempDir, backupDirName);

    // Read and parse metadata
    const metadataPath = path.join(backupPath, 'backup-metadata.json');
    let metadata = {};

    if (fsSync.existsSync(metadataPath)) {
      const metadataContent = await fs.readFile(metadataPath, 'utf8');
      metadata = JSON.parse(metadataContent);
      logger.info('Backup metadata:', metadata);
    }

    const results = {
      success: true,
      itemsRestored: [],
      warnings: [],
      recommendations: ['Restart Orchelium server to ensure all changes take effect.'],
    };

    // Restore server config
    if (metadata.items?.serverConfig) {
      const configPath = path.join(backupPath, 'server-config.json');
      if (fsSync.existsSync(configPath)) {
        await restoreServerConfig(configPath);
        results.itemsRestored.push('Server Configuration');
        logger.info('Server configuration restored');
      } else {
        results.warnings.push('Server configuration not found in backup');
      }
    }

    // Restore agents config
    if (metadata.items?.agentsConfig) {
      const agentsPath = path.join(backupPath, 'agents-config.json');
      if (fsSync.existsSync(agentsPath)) {
        await restoreAgentsConfig(agentsPath);
        results.itemsRestored.push('Agents Configuration');
        logger.info('Agents configuration restored');
      } else {
        results.warnings.push('Agents configuration not found in backup');
      }
    }

    // Restore schedules
    if (metadata.items?.schedules) {
      const schedulesPath = path.join(backupPath, 'schedules-config.json');
      if (fsSync.existsSync(schedulesPath)) {
        await restoreSchedules(schedulesPath);
        results.itemsRestored.push('Backup Schedules');
        logger.info('Schedules restored');
      } else {
        results.warnings.push('Schedules not found in backup');
      }
    }

    // Restore job history
    if (metadata.items?.jobHistory) {
      const jobHistoryPath = path.join(backupPath, 'job-history.json');
      if (fsSync.existsSync(jobHistoryPath)) {
        await restoreJobHistory(jobHistoryPath);
        results.itemsRestored.push('Job Execution History');
        logger.info('Job history restored');
      } else {
        results.warnings.push('Job history not found in backup');
      }
    }

    // Restore orchestration jobs
    if (metadata.items?.orchestrationJobs) {
      const orchJobsPath = path.join(backupPath, 'orchestration-jobs.json');
      if (fsSync.existsSync(orchJobsPath)) {
        await restoreOrchestrationJobs(orchJobsPath);
        results.itemsRestored.push('Orchestration Jobs');
        logger.info('Orchestration jobs restored');
      } else {
        results.warnings.push('Orchestration jobs not found in backup');
      }
    }

    // Restore orchestration executions
    if (metadata.items?.orchestrationExecutions) {
      const orchExecutionsPath = path.join(backupPath, 'orchestration-executions.json');
      if (fsSync.existsSync(orchExecutionsPath)) {
        await restoreOrchestrationExecutions(orchExecutionsPath);
        results.itemsRestored.push('Orchestration Execution History');
        logger.info('Orchestration executions restored');
      } else {
        results.warnings.push('Orchestration execution history not found in backup');
      }
    }

    // Restore agent history
    if (metadata.items?.agentHistory) {
      const historyPath = path.join(backupPath, 'agent-history.json');
      if (fsSync.existsSync(historyPath)) {
        await restoreAgentHistory(historyPath);
        results.itemsRestored.push('Agent Connection History');
        logger.info('Agent history restored');
      } else {
        results.warnings.push('Agent history not found in backup');
      }
    }

    // Restore user accounts
    if (metadata.items?.userAccounts) {
      const usersPath = path.join(backupPath, 'user-accounts.json');
      if (fsSync.existsSync(usersPath)) {
        await restoreUserAccounts(usersPath);
        results.itemsRestored.push('User Accounts');
        logger.info('User accounts restored');
      } else {
        results.warnings.push('User accounts not found in backup');
      }
    }

    // Restore webhooks
    if (metadata.items?.webhooks) {
      const webhooksPath = path.join(backupPath, 'webhooks.json');
      if (fsSync.existsSync(webhooksPath)) {
        await restoreWebhooks(webhooksPath);
        results.itemsRestored.push('Webhooks');
        logger.info('Webhooks restored');
      } else {
        results.warnings.push('Webhooks not found in backup');
      }
    }

    logger.info('Backup restore completed successfully');
    return results;
  } catch (err) {
    logger.error('Backup restore failed:', err);
    throw new AppError(`Restore failed: ${err.message}`, err.statusCode || 500);
  } finally {
    // Clean up temporary directory
    if (tempDir && fsSync.existsSync(tempDir)) {
      try {
        await fs.rm(tempDir, { recursive: true, force: true });
        logger.debug('Temporary restore directory cleaned up');
      } catch (cleanupErr) {
        logger.warn('Failed to clean up temporary restore directory:', cleanupErr);
      }
    }
  }
}

/**
 * Extract zip file to directory.
 * Rejects links and entries that would write outside targetDir.
 * @private
 */
function safeZipDestination(targetDir, entryPath) {
  if (typeof entryPath !== 'string' || !entryPath || entryPath.includes('\0')) return null;
  const normalized = entryPath.replace(/\\/g, '/');
  if (normalized.startsWith('/') || normalized.split('/').includes('..')) return null;
  const root = path.resolve(targetDir);
  const dest = path.resolve(root, normalized);
  if (dest !== root && !dest.startsWith(root + path.sep)) return null;
  return dest;
}

async function extractZip(zipBuffer, targetDir) {
  const root = path.resolve(targetDir);
  await fs.mkdir(root, { recursive: true });
  const directory = await unzipper.Open.buffer(zipBuffer);
  const MAX_ENTRIES = 10000;
  const MAX_BYTES = 1024 * 1024 * 1024;
  if (directory.files.length > MAX_ENTRIES) {
    throw new AppError('Backup archive has too many entries', 400);
  }
  let totalBytes = 0;
  for (const entry of directory.files) {
    const entryType = entry.type;
    if (entryType === 'SymbolicLink' || entryType === 'Link') {
      throw new AppError('Backup archive contains a link', 400);
    }
    const dest = safeZipDestination(root, entry.path);
    if (!dest) {
      throw new AppError('Backup archive contains an unsafe path', 400);
    }
    if (entryType === 'Directory' || entry.path.endsWith('/')) {
      await fs.mkdir(dest, { recursive: true });
      continue;
    }
    await fs.mkdir(path.dirname(dest), { recursive: true });
    const { pipeline } = require('stream/promises');
    const { Transform } = require('stream');
    const limited = new Transform({
      transform(chunk, _encoding, callback) {
        totalBytes += chunk.length;
        if (totalBytes > MAX_BYTES) {
          callback(new AppError('Backup archive is too large when extracted', 400));
          return;
        }
        callback(null, chunk);
      },
    });
    await pipeline(entry.stream(), limited, fsSync.createWriteStream(dest));
  }
}

/**
 * Restore server configuration
 * @private
 */
async function restoreServerConfig(configPath) {
  const content = await fs.readFile(configPath, 'utf8');
  const config = JSON.parse(content);

  // Validate config structure
  if (!config.server) {
    throw new AppError('Invalid server configuration format', 400);
  }

  // Update global config and save
  Object.assign(serverConfig, config);
  await configuration.saveServerConfig(serverConfig);
  logger.info('Server configuration restored and saved');
}

/**
 * Restore agents configuration
 * @private
 */
async function restoreAgentsConfig(agentsPath) {
  const content = await fs.readFile(agentsPath, 'utf8');
  const agentsConfig = JSON.parse(content);

  // Validate it's an object (agents are stored as a keyed object, not an array)
  if (!agentsConfig || typeof agentsConfig !== 'object' || Array.isArray(agentsConfig)) {
    throw new AppError('Invalid agents configuration format', 400);
  }

  // Save to database
  await db.putData('AGENTS_CONFIG', agentsConfig);
  logger.info(`Restored ${Object.keys(agentsConfig).length} agent configurations`);
}

/**
 * Restore schedules
 * @private
 */
async function restoreSchedules(schedulesPath) {
  const content = await fs.readFile(schedulesPath, 'utf8');
  const schedules = JSON.parse(content);

  // Validate it's an array
  if (!Array.isArray(schedules)) {
    throw new AppError('Invalid schedules format', 400);
  }

  await definitionStore.replaceJobs(schedules);
  logger.info(`Restored ${schedules.length} schedules`);
}

/**
 * Restore agent history
 * @private
 */
async function restoreAgentHistory(historyPath) {
  const content = await fs.readFile(historyPath, 'utf8');
  const historyData = JSON.parse(content);

  // Validate it's an array of { key, value } entries
  if (!Array.isArray(historyData)) {
    throw new AppError('Invalid agent history format', 400);
  }

  await agentHistoryDb.importAll(historyData);
  logger.info(`Restored ${historyData.length} agent history records`);
}

/**
 * Restore user accounts
 * @private
 */
async function restoreUserAccounts(usersPath) {
  const content = await fs.readFile(usersPath, 'utf8');
  const users = JSON.parse(content);

  // Validate it's an array
  if (!Array.isArray(users)) {
    throw new AppError('Invalid user accounts format', 400);
  }

  // Restore each user to the user database
  const { Level } = require('level');
  const userDb = new Level('./data/user.db', { valueEncoding: 'json' });
  
  try {
    for (const user of users) {
      const { username, ...userData } = user;
      if (username) {
        await userDb.put(username, userData);
      }
    }
    logger.info(`Restored ${users.length} user accounts`);
  } finally {
    await userDb.close();
  }
}

/**
 * Restore job history
 * @private
 */
async function restoreJobHistory(jobHistoryPath) {
  const content = await fs.readFile(jobHistoryPath, 'utf8');
  const jobHistory = JSON.parse(content);

  // Validate it's an array
  if (!Array.isArray(jobHistory)) {
    throw new AppError('Invalid job history format', 400);
  }

  // Save to database
  await db.putData('JOB_HISTORY', jobHistory);
  logger.info(`Restored ${jobHistory.length} job history records`);
}

/**
 * Restore orchestration jobs
 * @private
 */
async function restoreOrchestrationJobs(orchJobsPath) {
  const content = await fs.readFile(orchJobsPath, 'utf8');
  const orchJobs = JSON.parse(content);

  // Validate it's an object (keyed by job ID)
  if (!orchJobs || typeof orchJobs !== 'object' || Array.isArray(orchJobs)) {
    throw new AppError('Invalid orchestration jobs format', 400);
  }

  await definitionStore.replaceOrchestrations(orchJobs);
  logger.info(`Restored ${Object.keys(orchJobs).length} orchestration jobs`);
}

/**
 * Restore orchestration executions
 * @private
 */
async function restoreOrchestrationExecutions(orchExecutionsPath) {
  const content = await fs.readFile(orchExecutionsPath, 'utf8');
  const orchExecutions = JSON.parse(content);

  // Validate it's an object (keyed by job ID)
  if (!orchExecutions || typeof orchExecutions !== 'object' || Array.isArray(orchExecutions)) {
    throw new AppError('Invalid orchestration executions format', 400);
  }

  // Save to database
  await db.putData('ORCHESTRATION_EXECUTIONS', orchExecutions);
  logger.info(`Restored orchestration execution history`);
}

/**
 * Restore webhooks
 * @private
 */
async function restoreWebhooks(webhooksPath) {
  const content = await fs.readFile(webhooksPath, 'utf8');
  const webhooksData = JSON.parse(content);

  // Validate the structure has both index and data
  if (!webhooksData || typeof webhooksData !== 'object' || !webhooksData.index || !webhooksData.data) {
    throw new AppError('Invalid webhooks format', 400);
  }

  // Restore the webhooks index
  await db.putData('WEBHOOKS_INDEX', webhooksData.index);
  
  // Restore individual webhook entries
  let webhookCount = 0;
  for (const jobId in webhooksData.data) {
    const jobWebhooks = webhooksData.data[jobId];
    for (const webhookId in jobWebhooks) {
      const webhookData = jobWebhooks[webhookId];
      const key = `WEBHOOK_DATA_${jobId}_${webhookId}`;
      await db.putData(key, webhookData);
      webhookCount++;
    }
  }
  
  logger.info(`Restored ${webhookCount} webhook configurations`);
}

module.exports = {
  getBackupItems,
  createBackup,
  restoreBackup,
  safeZipDestination,
  extractZip,
};
