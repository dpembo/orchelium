/**
 * Orchestration Module
 * Manages orchestration job definitions (DAGs of scripts with conditions)
 * Stores and retrieves orchestration definitions from the configured definition store
 */

const definitionStore = require('./definitionStore.js');
const { resolveScriptPath } = require('./utils/scriptFilename.js');

function assertExecuteScriptsConfined(nodes) {
  if (!Array.isArray(nodes)) return;
  for (const node of nodes) {
    if (!node || node.type !== 'execute') continue;
    const executeType = String((node.data && node.data.executeType) || 'script').toLowerCase();
    if (executeType === 'http') continue;
    const script = node.data && node.data.script;
    if (typeof script !== 'string' || !script.trim()) continue;
    if (!resolveScriptPath(script)) {
      const err = new Error('Script path is outside the scripts directory');
      err.statusCode = 400;
      err.code = 'EOUTSIDE';
      throw err;
    }
  }
}

/**
 * Initialize orchestration by loading from definition store
 */
async function init() {
  try {
    logger.info("Initializing Orchestration Module");
    const data = await definitionStore.listOrchestrations();
    const count = Object.keys(data || {}).length;
    if (count === 0) {
      logger.info("No orchestration jobs found, starting with empty config");
      return {};
    }
    logger.info(`Loaded ${count} orchestration jobs`);
    return data;
  } catch (err) {
    if (err.message && err.message.includes('NotFoundError')) {
      logger.info("No orchestration jobs found, starting with empty config");
      return {};
    }
    throw err;
  }
}

/**
 * Get all orchestration jobs
 * @returns {Promise<Object>} Dictionary of orchestration jobs
 */
async function getAllJobs() {
  return definitionStore.listOrchestrations();
}

/**
 * Get a specific orchestration job by ID
 * Returns the current/active version of the job
 * @param {string} jobId - The orchestration job ID
 * @returns {Promise<Object>} The orchestration job definition with current version
 */
async function getJob(jobId) {
  try {
    const jobs = await getAllJobs();
    const job = jobs[jobId];
    if (!job) {
      throw new Error(`Orchestration job [${jobId}] not found`);
    }
    return getJobWithVersion(job);
  } catch (err) {
    logger.warn(`Error getting orchestration job [${jobId}]: ${err.message}`);
    throw err;
  }
}

/**
 * Get a specific version of an orchestration job
 * @param {string} jobId - The orchestration job ID
 * @param {number} version - The version number (or 'latest'/'current')
 * @returns {Promise<Object>} The orchestration job with specified version
 */
async function getJobVersion(jobId, version = 'current') {
  try {
    const jobs = await getAllJobs();
    const job = jobs[jobId];
    if (!job) {
      throw new Error(`Orchestration job [${jobId}] not found`);
    }
    
    let versionData;
    
    if (version === 'current' || version === 'latest') {
      // Return current version
      versionData = job.versions[job.versions.length - 1];
    } else {
      // Get specific version number
      const versionNum = parseInt(version);
      versionData = job.versions.find(v => v.version === versionNum);
      if (!versionData) {
        throw new Error(`Version ${versionNum} not found for orchestration job [${jobId}]`);
      }
    }
    
    return {
      id: job.id,
      name: job.name,
      description: job.description,
      type: job.type,
      icon: job.icon || 'schema',
      color: job.color || '#000000',
      nodes: versionData.nodes || [],
      edges: versionData.edges || [],
      createdAt: job.createdAt,
      updatedAt: job.updatedAt,
      version: versionData.version,
      currentVersion: job.currentVersion,
      totalVersions: job.versions.length,
      versionCreatedAt: versionData.createdAt
    };
  } catch (err) {
    logger.warn(`Error getting version ${version} of orchestration job [${jobId}]: ${err.message}`);
    throw err;
  }
}

/**
 * Internal helper: Get job with current version merged in for backwards compatibility
 * @private
 */
function getJobWithVersion(job) {
  const currentVersionData = job.versions[job.versions.length - 1];
  return {
    id: job.id,
    name: job.name,
    description: job.description,
    type: job.type,
    icon: job.icon || 'schema',
    color: job.color || '#000000',
    nodes: currentVersionData.nodes || [],
    edges: currentVersionData.edges || [],
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    version: currentVersionData.version,
    currentVersion: job.currentVersion,
    totalVersions: job.versions.length
  };
}

/**
 * Create or update an orchestration job
 * @param {string} jobId - The orchestration job ID
 * @param {Object} jobDefinition - The job definition with nodes/edges
 * @returns {Promise<Object>} The created/updated job
 */
async function saveJob(jobId, jobDefinition) {
  try {
    logger.info(`Saving orchestration job [${jobId}]`);
    assertExecuteScriptsConfined(jobDefinition && jobDefinition.nodes);
    
    const jobs = await getAllJobs();
    const now = new Date().toISOString();
    
    // Get existing job or create new
    let job = jobs[jobId];
    
    if (!job) {
      // New orchestration - initialize with version 1
      job = {
        id: jobId,
        name: jobDefinition.name,
        description: jobDefinition.description || '',
        icon: jobDefinition.icon || 'schema',
        color: jobDefinition.color || '#000000',
        type: 'orchestration',
        createdAt: now,
        updatedAt: now,
        currentVersion: 1,
        versions: [
          {
            version: 1,
            nodes: jobDefinition.nodes || [],
            edges: jobDefinition.edges || [],
            createdAt: now
          }
        ]
      };
    } else {
      // Existing orchestration - check if definition changed
      const currentVersionData = job.versions[job.versions.length - 1];
      const definitionChanged = JSON.stringify({
        nodes: jobDefinition.nodes,
        edges: jobDefinition.edges,
        name: jobDefinition.name,
        description: jobDefinition.description
      }) !== JSON.stringify({
        nodes: currentVersionData.nodes,
        edges: currentVersionData.edges,
        name: job.name,
        description: job.description
      });
      
      if (definitionChanged) {
        // Create new version
        const newVersion = (job.versions[job.versions.length - 1].version || 0) + 1;
        job.versions.push({
          version: newVersion,
          nodes: jobDefinition.nodes || [],
          edges: jobDefinition.edges || [],
          createdAt: now
        });
        job.currentVersion = newVersion;
      }
      
      // Update metadata (icon and color don't create new versions)
      job.name = jobDefinition.name;
      job.description = jobDefinition.description || '';
      job.icon = jobDefinition.icon || job.icon || 'schema';
      job.color = jobDefinition.color || job.color || '#000000';
      job.updatedAt = now;
    }
    
    await definitionStore.saveOrchestration(jobId, job);
    logger.info(`Successfully saved orchestration job [${jobId}] at version ${job.currentVersion}`);
    
    return job;
  } catch (err) {
    logger.error(`Error saving orchestration job [${jobId}]: ${err.message}`);
    throw err;
  }
}

/**
 * Delete an orchestration job
 * @param {string} jobId - The orchestration job ID to delete
 * @returns {Promise<boolean>} True if deleted successfully
 */
async function deleteJob(jobId) {
  try {
    logger.info(`Deleting orchestration job [${jobId}]`);

    const existing = await definitionStore.getOrchestration(jobId);
    if (!existing) {
      throw new Error(`Orchestration job [${jobId}] not found`);
    }

    await definitionStore.deleteOrchestration(jobId);
    
    logger.info(`Successfully deleted orchestration job [${jobId}]`);
    return true;
  } catch (err) {
    logger.error(`Error deleting orchestration job [${jobId}]: ${err.message}`);
    throw err;
  }
}

/**
 * Get list of all available scripts (for palette in UI)
 * Scans scripts directory to find available scripts
 * @returns {Promise<Array>} Array of available script objects
 */
async function getAvailableScripts() {
  try {
    const fs = require('fs').promises;
    const path = require('path');
    
    const scriptsDir = path.join(__dirname, 'scripts');
    const files = await fs.readdir(scriptsDir);
    
    // Filter for shell scripts and common script files
    const scriptFiles = files.filter(f => 
      f.endsWith('.sh') || 
      f.endsWith('.js') || 
      f.endsWith('.py')
    );
    
    const scripts = scriptFiles.map(file => ({
      id: file,
      name: file.replace(/\.[^/.]+$/, ''), // Remove extension
      filename: file,
      type: 'script'
    }));
    
    logger.debug(`Found ${scripts.length} available scripts`);
    return scripts;
  } catch (err) {
    logger.warn(`Error reading available scripts: ${err.message}`);
    return [];
  }
}

/**
 * Migrate existing orchestrations to versioned format
 * Call this once to convert old format to new versioned format
 * @returns {Promise<number>} Number of orchestrations migrated
 */
async function migrateToVersionedFormat() {
  try {
    logger.info("Starting migration to versioned orchestration format");
    
    const jobs = await getAllJobs();
    let migratedCount = 0;
    
    for (const jobId in jobs) {
      const job = jobs[jobId];
      
      // Check if already migrated (has versions array)
      if (job.versions && Array.isArray(job.versions)) {
        logger.debug(`Job [${jobId}] already migrated, skipping`);
        continue;
      }
      
      // Migrate old format to new format
      logger.info(`Migrating job [${jobId}] to versioned format`);
      
      jobs[jobId] = {
        id: job.id,
        name: job.name,
        description: job.description || '',
        type: job.type || 'orchestration',
        createdAt: job.createdAt,
        updatedAt: job.updatedAt || job.createdAt,
        currentVersion: 1,
        versions: [
          {
            version: 1,
            nodes: job.nodes || [],
            edges: job.edges || [],
            createdAt: job.createdAt
          }
        ]
      };
      
      migratedCount++;
    }
    
    if (migratedCount > 0) {
      await definitionStore.replaceOrchestrations(jobs);
      logger.info(`Successfully migrated ${migratedCount} orchestrations to versioned format`);
    } else {
      logger.info("No orchestrations needed migration");
    }
    
    return migratedCount;
  } catch (err) {
    logger.error(`Error during migration to versioned format: ${err.message}`);
    throw err;
  }
}

async function migrateToFilesystem(options = {}) {
  return definitionStore.migrateOrchestrationsFromDbToFilesystem(options);
}

module.exports = {
  init,
  getAllJobs,
  getJob,
  getJobVersion,
  migrateToVersionedFormat,
  migrateToFilesystem,
  saveJob,
  deleteJob,
  getAvailableScripts,
  getAvailablePlugins: async function getAvailablePlugins() {
    try {
      const pluginRegistry = require('./pluginRegistry.js');
      return pluginRegistry.getPluginsForClient();
    } catch (err) {
      logger.warn(`Error getting available plugins: ${err.message}`);
      return [];
    }
  },
  executeJob: require('./orchestrationEngine').executeJob,
  getExecutionHistory: require('./orchestrationEngine').getExecutionHistory,
  saveExecutionResult: require('./orchestrationEngine').saveExecutionResult
};
