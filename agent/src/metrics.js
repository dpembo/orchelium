const fs = require('fs');
const os = require('os');
const { execSync, execFileSync } = require('child_process');
const { debug, DEBUG_LEVEL } = require('./log');

function getCPULoadPercentage() {
  try {
    const loadAvgData = fs.readFileSync('/proc/loadavg', 'utf-8');
    const [oneMinLoad] = loadAvgData.split(' ').map(parseFloat);
    const cpuCount = os.cpus().length;
    return (oneMinLoad / cpuCount) * 100;
  } catch (error) {
    debug(DEBUG_LEVEL.ERROR, 'Error reading load average:', error);
    return 0;
  }
}

function getFileSystemUsagePercentage() {
  try {
    const output = execSync('df -h --output=pcent,target -x tmpfs -x devtmpfs').toString();
    const lines = output.split('\n').slice(1).filter(line => line.trim() !== '');
    return lines.map(line => {
      const [usedPercentage, mountPoint] = line.trim().split(/\s+/);
      return { mount: mountPoint, usage: parseInt(usedPercentage.replace('%', ''), 10) };
    });
  } catch (error) {
    debug(DEBUG_LEVEL.ERROR, `Error checking disk usage: ${error.message}`);
    return [];
  }
}

async function computeMetric(config) {
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
        return { type, value: getCPULoadPercentage(), unit: 'percent' };

      case 'mount_usage': {
        const all = getFileSystemUsagePercentage();
        const mount = targetPath ? all.find(m => m.mount === targetPath) : null;
        return { type, path: targetPath || null, value: mount ? mount.usage : null, unit: 'percent', allMounts: all };
      }

      case 'dir_size': {
        if (!targetPath) return { type, error: 'path is required for dir_size' };
        try {
          const output = execSync(`du -sb "${targetPath}" 2>/dev/null || echo "0\\t${targetPath}"`).toString().trim();
          const bytes = parseInt(output.split('\t')[0], 10);
          return { type, path: targetPath, value: bytes, unit: 'bytes' };
        } catch (duErr) {
          return { type, path: targetPath, error: duErr.message };
        }
      }

      case 'file_size': {
        if (!targetPath) return { type, error: 'path is required for file_size' };
        const stat = fs.statSync(targetPath);
        return { type, path: targetPath, value: stat.size, unit: 'bytes' };
      }

      case 'file_count': {
        if (!targetPath) return { type, error: 'path is required for file_count' };
        const findArgs = pattern
          ? [targetPath, '-maxdepth', '1', '-name', pattern]
          : [targetPath, '-maxdepth', '1', '-type', 'f'];
        const findOutput = execFileSync('find', findArgs).toString().trim();
        const count = findOutput ? findOutput.split('\n').filter(l => l.length > 0).length : 0;
        return { type, path: targetPath, pattern: pattern || null, value: count, unit: 'count' };
      }

      case 'file_age': {
        if (!targetPath) return { type, error: 'path is required for file_age' };
        const mtimeStr = execFileSync('stat', ['-c', '%Y', targetPath]).toString().trim();
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

module.exports = {
  getCPULoadPercentage,
  getFileSystemUsagePercentage,
  computeMetric,
};
