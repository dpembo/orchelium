module.exports = function register(app) {
app.delete('/rest/notifications', User.isAuthenticated, (req, res) => {
  logger.info("Delete all notifications");
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  logger.info("Deleting all Notifications");
  var resp ="";
  try{
    resp = notificationData.deleteAll();  
  }
  catch(err){
    res.sendStatus(503);
  }
  logger.debug("all notifications deleted");
  res.sendStatus(200);
});

app.delete('/rest/notifications/:index', User.isAuthenticated, (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  const index = req.params.index;
  logger.info("Deleting Notification at index: " + index);
  var resp ="";
  try{
    resp = notificationData.deleteItem(index);  
  }
  catch(err){
    res.sendStatus(503);
  }
  logger.debug("notification at index [" + index + "] deleted");
  res.sendStatus(200);
});

app.get('/rest/notifications', User.isAuthenticated, (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  var notifyObj = notificationData.getItems();
  res.setHeader("Content-Type","Application/JSON");
  res.send(notifyObj);
  //res.sendStatus(200);
});

app.get('/rest/notifications/count', User.isAuthenticated, (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  var notifyCount = notificationData.getCount();
  res.setHeader("Content-Type","Application/JSON");
  res.send(`{"count":${notifyCount}}`);
  //res.sendStatus(200);
});

app.get('/rest/notifty/test', User.isAuthenticated, (req, res) => {
  const user = req.session.user;
  if (!user) {
    return res.redirect('/register.html');
  }
  notifier.sendNotification("Test Notification","This is a test notification from Orchelium Server.","INFORMATION","/settings.html");
  res.sendStatus(200);
});

app.post('/rest/notifications', User.isAuthenticated, asyncHandler(async (req, res) => {
  const { type, title, description, url } = req.body;
  const runDate = new Date().toISOString();
  const item = notificationData.createNotificationItem(runDate, type, title, description, url);
  notificationData.add(item);
  res.status(201).json({ success: true, item });
}));

app.put('/rest/notifications/:index', User.isAuthenticated, asyncHandler(async (req, res) => {
  const index = parseInt(req.params.index);
  const { type, title, description, url } = req.body;
  let items = notificationData.getItems();
  if (index < 0 || index >= items.length) {
    return res.status(404).json({ success: false, message: 'Notification not found' });
  }
  items[index] = notificationData.createNotificationItem(new Date().toISOString(), type, title, description, url);
  await notificationData.updateDbPromise();
  res.json({ success: true, item: items[index] });
}));
};
