module.exports = function register(app) {
app.get('/rest/servertime',User.isAuthenticated, async (request, response) => {
  var dateTime = new Date();
  var serverTime={};
  serverTime.dateTime = dateTime;
  response.json(serverTime);
});

app.get('/rest/eta',User.isAuthenticated, (request, response) => {
  //populateBackupConfig();
  response.header('Content-type', 'application/json');
  response.send(JSON.stringify(config,null,2));
});
};
