const crypto = require('crypto');
const jwt = require('jsonwebtoken');
const { padStringTo256Bits } = require('./shared/keyMaterial');
const algorithm = 'aes-256-cbc';

function initializeEncryptionKey() {
  let key = process.env.ORCHELIUM_ENCRYPTION_KEY;
  const keyEnforceLevel = process.env.ORCHELIUM_KEY_ENFORCE || 'warn'; // 'strict', 'warn', or 'silent'
  
  if(!key || key.length === 0) {
    const DEFAULT_KEY = "CHANGEIT";
    key = DEFAULT_KEY;
    
    switch(keyEnforceLevel) {
      case 'strict':
        logger.error("═══════════════════════════════════════════════════════════════");
        logger.error("CRITICAL: Default encryption key in use. Server will not start.");
        logger.error("═══════════════════════════════════════════════════════════════");
        logger.error("Set ORCHELIUM_ENCRYPTION_KEY environment variable before starting.");
        logger.error("This key must match the key configured on all agents.");
        logger.error("═══════════════════════════════════════════════════════════════");
        process.exit(1);
      case 'warn':
        logger.warn("═══════════════════════════════════════════════════════════════");
        logger.warn("⚠️  WARNING: Using default encryption key (CHANGEIT)");
        logger.warn("═══════════════════════════════════════════════════════════════");
        logger.warn("This key should ONLY be used for development/testing.");
        logger.warn("For production, set ORCHELIUM_ENCRYPTION_KEY environment variable.");
        logger.warn("The same key must be set on all agents for communications.");
        logger.warn("═══════════════════════════════════════════════════════════════");
        break;
      case 'silent':
        logger.debug("Using default encryption key (not recommended for production)");
        break;
      default:
        logger.warn("Unknown ORCHELIUM_KEY_ENFORCE value: " + keyEnforceLevel);
    }
  } else {
    logger.info("Custom encryption key loaded from ORCHELIUM_ENCRYPTION_KEY");
  }
  
  return padStringTo256Bits(key);
}

var key = initializeEncryptionKey();

function checkKey()
{ 
  // Key validation now done during initialization in initializeEncryptionKey()
  // This function kept for backwards compatibility
}
/**
 * Creates a JWT Token
 * @param {} payload 
 */
function createJWTToken(payload,expiryHours){
  logger.debug("Generating JWT Auth Token");
  if(expiryHours===undefined||expiryHours==null)expiryHours=3;
  const signOptions = {
    expiresIn: expiryHours + 'h', // Token expiration time
    algorithm: 'HS256'
    // Other options if needed
  };

  const token = jwt.sign(payload, key, signOptions);
  //logger.debug(JSON.stringify(token));
  return token;
}

function validateJWTToken(inToken) {
  logger.debug("Validating JWT Token");
  //logger.debug("KEY: [" + key + "]");
  return new Promise((resolve, reject) => {
    jwt.verify(inToken, key, (err, decoded) => {
      if (err) {
        // Token verification failed
        logger.error("Validation failed",err);
        reject(err);
      } else {
        // Token is valid, 'decoded' contains the decoded payload
        logger.info("Validation Successfull");
        resolve(decoded);
      }
    });
  });
}

function encrypt(text) {
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv(algorithm, Buffer.from(key), iv);
  let encrypted = cipher.update(text);
  encrypted = Buffer.concat([encrypted, cipher.final()]);
  return iv.toString('hex') + ':' + encrypted.toString('hex');
}

function decrypt(text) {
  const textParts = text.split(':');
  const iv = Buffer.from(textParts.shift(), 'hex');
  const encryptedText = Buffer.from(textParts.join(':'), 'hex');
  const decipher = crypto.createDecipheriv(algorithm, Buffer.from(key), iv);
  let decrypted = decipher.update(encryptedText);
  decrypted = Buffer.concat([decrypted, decipher.final()]);
  return decrypted.toString();
}

//new key derivation function using PBKDF2
function deriveKey(password) {
  const salt = Buffer.from('BackupHubSalt'); // Use a consistent salt or store it securely
  const iterations = 100000; // NIST recommended minimum
  const keyLength = 32; // 256 bits for aes-256
  const digest = 'sha256';
  return crypto.pbkdf2Sync(password, salt, iterations, keyLength, digest);
}

/* Example usage */
//const plaintextPassword = 'yourPlainTextPassword';

/* Encrypt the password */
//const encryptedPassword = encrypt(plaintextPassword);
//console.log('Encrypted Password:', encryptedPassword);

/* Decrypt the password */
//const decryptedPassword = decrypt(encryptedPassword);
//console.log('Decrypted Password:', decryptedPassword);


module.exports = { encrypt, decrypt, checkKey, createJWTToken, validateJWTToken };
