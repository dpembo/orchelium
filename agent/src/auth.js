const jwt = require('jsonwebtoken');
const runtime = require('./runtime');
const { padStringTo256Bits } = require('../../shared/keyMaterial');

function commandVerificationKeys() {
  if (runtime.AGENT_TOKEN) {
    const keys = [padStringTo256Bits(String(runtime.AGENT_TOKEN))];
    if (runtime.previousAgentToken) keys.push(padStringTo256Bits(String(runtime.previousAgentToken)));
    return keys;
  }
  return runtime.enckey ? [runtime.enckey] : [];
}

function validateJWTToken(inToken) {
  return new Promise((resolve, reject) => {
    const keys = commandVerificationKeys();
    if (keys.length === 0) {
      reject(new Error('No command key'));
      return;
    }
    let parsed;
    try {
      parsed = JSON.parse(inToken);
    } catch (err) {
      reject(err);
      return;
    }
    const tryKey = (index) => {
      jwt.verify(parsed, keys[index], { algorithms: ['HS256'] }, (err, decoded) => {
        if (err) {
          if (index + 1 < keys.length) {
            tryKey(index + 1);
            return;
          }
          reject(err);
          return;
        }
        if (index === 0) runtime.previousAgentToken = '';
        resolve(decoded);
      });
    };
    tryKey(0);
  });
}

module.exports = { validateJWTToken };
