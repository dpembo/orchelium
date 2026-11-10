module.exports = {
  testEnvironment: 'node',
  coveragePathIgnorePatterns: ['/node_modules/'],
  testMatch: ['**/tests/**/*.test.js'],
  collectCoverageFrom: [
    'agents.js',
    'scheduler.js',
    'history.js',
    'running.js',
    'db.js',
    'agent/src/**/*.js',
    'shared/**/*.js',
    'utils/**/*.js',
  ],
  testTimeout: 10000,
  forceExit: true,
  detectOpenHandles: false,
  transformIgnorePatterns: [
    '/node_modules/(?!(archiver|zip-stream|compress-commons)/)',
  ],
};