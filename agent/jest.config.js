module.exports = {
  testEnvironment: 'node',
  coveragePathIgnorePatterns: ['/node_modules/'],
  testMatch: ['**/tests/**/*.test.js'],
  collectCoverageFrom: [
    'src/**/*.js',
    '../shared/**/*.js',
  ],
  testTimeout: 10000,
  // Ignore node_modules in tests
  modulePathIgnorePatterns: ['/node_modules/'],
  // Transform files to handle Node.js features
  transform: {},
};
