const { debug, DEBUG_LEVEL } = require('../log');

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
      { durationMs: Infinity, intervalMs: 60 * 60 * 1000, stageName: '1-hour backoff (indefinite)' },
    ];
  }

  getNextRetryDelay() {
    const now = Date.now();
    const currentStage = this.backoffStages[this.currentBackoffStage];
    const timeInStage = now - this.lastBackoffChangeTime;

    if (timeInStage > currentStage.durationMs && this.currentBackoffStage < this.backoffStages.length - 1) {
      this.currentBackoffStage++;
      this.lastBackoffChangeTime = now;
      const nextStage = this.backoffStages[this.currentBackoffStage];
      debug(DEBUG_LEVEL.INFO, `Backoff stage changed to: ${nextStage.stageName}`);
      return nextStage.intervalMs;
    }

    return currentStage.intervalMs;
  }

  recordAttempt() {
    this.currentAttempt++;
    const elapsed = Math.floor((Date.now() - this.startTime) / 1000);
    const currentStage = this.backoffStages[this.currentBackoffStage];
    debug(DEBUG_LEVEL.INFO, `[${this.handlerType}] ${this.agentId} - Retry attempt ${this.currentAttempt} after ${elapsed}s (${currentStage.stageName})`);
  }

  reset() {
    this.currentAttempt = 0;
    this.startTime = Date.now();
    this.lastBackoffChangeTime = this.startTime;
    this.currentBackoffStage = 0;
    debug(DEBUG_LEVEL.INFO, `[${this.handlerType}] ${this.agentId} - Connection successful, backoff reset`);
  }
}

module.exports = { RetryBackoffManager };
