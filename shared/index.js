/**
 * Code that is safe to ship inside the agent bundle.
 * Key derivation and the message contract live here.
 * JWT signing, password storage, and server transports stay on the hub.
 */

module.exports = {
  ...require('./keyMaterial'),
  ...require('./protocol'),
};
