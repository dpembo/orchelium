const jwt = require('jsonwebtoken');
const runtime = require('./runtime');

function validateJWTToken(inToken) {
  return new Promise((resolve, reject) => {
    jwt.verify(JSON.parse(inToken), runtime.enckey, { algorithm: 'HS256' }, (err, decoded) => {
      err ? reject(err) : resolve(decoded);
    });
  });
}

module.exports = { validateJWTToken };
