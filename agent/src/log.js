const runtime = require('./runtime');

const DEBUG_LEVEL = Object.freeze({
  TRACE: 'trace',
  INFO: 'info',
  WARN: 'warn',
  ERROR: 'error',
  CRITICAL: 'critical',
});

const DEBUG_STYLES = Object.freeze({
  trace: { code: '\x1b[37m\x1b[2m', label: '[TRACE]' },
  info: { code: '\x1b[37m\x1b[1m', label: '[INFO]' },
  warn: { code: '\x1b[33m', label: '[WARN]' },
  error: { code: '\x1b[31m', label: '[ERROR]' },
  critical: { code: '\x1b[37m\x1b[41m', label: '[CRITICAL]' },
  reset: '\x1b[0m',
  dim: '\x1b[2m',
});

function debug(level, ...args) {
  if (!Object.values(DEBUG_LEVEL).includes(level)) {
    console.error(`Invalid debug level: ${level}. Valid levels: ${Object.values(DEBUG_LEVEL).join(', ')}`);
    return;
  }

  if (level === DEBUG_LEVEL.TRACE && !runtime.DEBUG_MODE) {
    return;
  }

  const style = DEBUG_STYLES[level];
  const now = new Date();
  const timestamp = `${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}:${now.getSeconds().toString().padStart(2, '0')}.${now.getMilliseconds().toString().padStart(3, '0')}`;

  const message = args.map(arg => {
    if (typeof arg === 'string') return arg;
    if (arg instanceof Error) return `${arg.name}: ${arg.message}`;
    return JSON.stringify(arg);
  }).join(' ');

  console.log(`${style.code}${DEBUG_STYLES.dim}[${timestamp}]${DEBUG_STYLES.reset}${style.code} ${style.label} ${message}${DEBUG_STYLES.reset}`);
}

module.exports = { DEBUG_LEVEL, debug };
