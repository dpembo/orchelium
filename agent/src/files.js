const fs = require('fs');
const path = require('path');
const { debug, DEBUG_LEVEL } = require('./log');

function checkExecutePermissionCapability(directoryPath, liveness) {
  try {
    fs.accessSync(directoryPath, fs.constants.W_OK);
    const tempFilePath = path.join(directoryPath, '.tmp_permission_test');
    fs.writeFileSync(tempFilePath, 'Temporary file content', 'utf8');
    fs.chmodSync(tempFilePath, '755');
    fs.unlinkSync(tempFilePath);
    debug(DEBUG_LEVEL.TRACE, 'Confirmed execute permissions capability.');
    return 'ok';
  } catch (err) {
    debug(DEBUG_LEVEL.ERROR, `Permission capability check failed: ${err.message}`);
    debug(DEBUG_LEVEL.CRITICAL, 'No execute permissions capability.');
    return liveness ? 'No execute permissions capability.' : process.exit(1);
  }
}

function addExecutePermission(filePath) {
  fs.chmod(filePath, '755', (err) => {
    err ? debug(DEBUG_LEVEL.CRITICAL, `Error setting execute permission: ${err}`) : debug(DEBUG_LEVEL.TRACE, 'Execute permission added.');
  });
}

function generateRandomFileName() {
  return `${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;
}

function writeFileAndWait(filePath, dataToWrite) {
  try {
    fs.writeFileSync(filePath, dataToWrite, 'utf8');
    debug(DEBUG_LEVEL.TRACE, 'File written successfully!');
  } catch (err) {
    debug(DEBUG_LEVEL.ERROR, `Error writing file: ${err}`);
  }
}

function createEmptyFile(filePath) {
  try {
    fs.writeFileSync(filePath, '', 'utf8');
    debug(DEBUG_LEVEL.TRACE, `Empty file created: ${filePath}`);
  } catch (err) {
    debug(DEBUG_LEVEL.ERROR, `Error creating file: ${filePath} (${err.message})`);
  }
}

function deleteFile(filePathToDelete) {
  try {
    fs.unlinkSync(filePathToDelete);
    debug(DEBUG_LEVEL.TRACE, `File removed: ${filePathToDelete}`);
  } catch (err) {
    debug(DEBUG_LEVEL.ERROR, `Error deleting file: ${err}`);
  }
}

function sanitizeUnixFilename(filename) {
  return filename.replace(/[^a-zA-Z0-9_.-]/g, '_');
}

function getCurrentDateTimeFormatted() {
  const now = new Date();
  return `${now.getFullYear().toString().padStart(4, '0')}-${(now.getMonth() + 1).toString().padStart(2, '0')}-${now.getDate().toString().padStart(2, '0')}_${now.getHours().toString().padStart(2, '0')}${now.getMinutes().toString().padStart(2, '0')}${now.getSeconds().toString().padStart(2, '0')}`;
}

module.exports = {
  checkExecutePermissionCapability,
  addExecutePermission,
  generateRandomFileName,
  writeFileAndWait,
  createEmptyFile,
  deleteFile,
  sanitizeUnixFilename,
  getCurrentDateTimeFormatted,
};
