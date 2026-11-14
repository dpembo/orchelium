const path = require('path');

const SCRIPT_FILENAME_REGEX = /^[a-zA-Z0-9_.-]+\.sh$/;
const SCRIPTS_DIR = path.resolve(__dirname, '..', 'scripts');

function normalizeScriptFilename(rawName) {
  if (typeof rawName !== 'string') return '';
  let scriptName = rawName.trim();
  if (!scriptName) return '';
  if (!scriptName.endsWith('.sh')) scriptName += '.sh';
  if (!SCRIPT_FILENAME_REGEX.test(scriptName)) return '';
  return scriptName;
}

// Resolve a job or orchestration script path and require it to stay inside scripts/.
// Returns null for empty values, null bytes, absolute paths, and any traversal
// that would leave the scripts directory (for example ../data/server-config.json).
function resolveScriptPath(command, scriptsDir = SCRIPTS_DIR) {
  if (typeof command !== 'string' || command.includes('\0')) return null;
  const trimmed = command.trim();
  if (!trimmed) return null;

  const base = path.resolve(scriptsDir);
  const scriptPath = path.resolve(base, trimmed);
  const rel = path.relative(base, scriptPath);
  if (!rel || path.isAbsolute(rel) || rel.split(path.sep).includes('..')) return null;
  return scriptPath;
}

module.exports = { SCRIPT_FILENAME_REGEX, SCRIPTS_DIR, normalizeScriptFilename, resolveScriptPath };
