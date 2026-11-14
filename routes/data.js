module.exports = function register(app) {
app.get('/rest/data/:key', User.isAuthenticated, validateSensitiveDataToken, asyncHandler(async (req, res) => {
  try {
    const value = await db.getData(req.params.key);
    res.json({ key: req.params.key, value });
  } catch (err) {
    res.status(404).json({ success: false, message: 'Key not found' });
  }
}));

app.put('/rest/data/:key', User.isAuthenticated, validateSensitiveDataToken, asyncHandler(async (req, res) => {
  try {
    await db.putData(req.params.key, req.body.value);
    res.json({ success: true });
  } catch (err) {
    logger.error(`Error storing data [${req.params.key}]: ${err.message}`);
    res.status(400).json({ success: false, message: `Error storing data: ${err.message}` });
  }
}));

app.delete('/rest/data/:key', User.isAuthenticated, validateSensitiveDataToken, asyncHandler(async (req, res) => {
  try {
    await db.deleteData(req.params.key);
    res.json({ success: true });
  } catch (err) {
    logger.error(`Error deleting data [${req.params.key}]: ${err.message}`);
    res.status(400).json({ success: false, message: `Error deleting data: ${err.message}` });
  }
}));
};
