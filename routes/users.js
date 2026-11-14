module.exports = function register(app) {
app.get('/users.html', User.isAuthenticated, User.requirePermission(PERMISSIONS.USERS_MANAGE), asyncHandler(async (req, res) => {
  const users = await User.getAllUsers();
  const smtpEnabledValue = serverConfig?.smtp?.enabled;
  const smtpEnabled = smtpEnabledValue === true || String(smtpEnabledValue).toLowerCase() === 'true';
  const smtpHostConfigured = typeof serverConfig?.smtp?.host === 'string' && serverConfig.smtp.host.trim().length > 0;
  const smtpPortConfigured = serverConfig?.smtp?.port !== undefined && serverConfig.smtp.port !== null && String(serverConfig.smtp.port).trim().length > 0;
  const smtpConfigured = smtpEnabled && smtpHostConfigured && smtpPortConfigured;
  res.render('userManagement', {
    users,
    PERMISSIONS,
    ROLE_PRESETS: User.ROLE_PRESETS,
    DEFAULT_INVITED_PERMISSIONS: User.DEFAULT_INVITED_PERMISSIONS,
    csrf: req.csrfToken(),
    version,
    smtpConfigured,
  });
}));

app.get('/rest/users', User.isAuthenticated, User.requirePermission(PERMISSIONS.USERS_MANAGE), asyncHandler(async (req, res) => {
  const users = await User.getAllUsers();
  res.json(users);
}));

app.get('/rest/users/:username', User.isAuthenticated, User.requirePermission(PERMISSIONS.USERS_MANAGE), asyncHandler(async (req, res) => {
  const user = await User.getUserByUsername(req.params.username.toLowerCase());
  if (!user) return res.status(404).json({ success: false, message: 'User not found' });
  const { password, resetPasswordToken, resetPasswordExpires, inviteToken, inviteExpires, ...safe } = user;
  res.json(safe);
}));

app.post('/rest/users/:username/resend-invite', User.isAuthenticated, User.requirePermission(PERMISSIONS.USERS_MANAGE), asyncHandler(async (req, res) => {
  await User.regenerateInviteToken(req.params.username.toLowerCase());
  res.json({ success: true, message: 'Invite resent' });
}));

app.put('/rest/users/:username/permissions', User.isAuthenticated, User.requirePermission(PERMISSIONS.USERS_MANAGE), express.json(), asyncHandler(async (req, res) => {
  const { permissions } = req.body;
  if (!Array.isArray(permissions)) return res.status(400).json({ success: false, message: 'permissions must be an array' });
  await User.updateUserPermissions(req.params.username.toLowerCase(), permissions);
  res.json({ success: true });
}));

app.put('/rest/users/:username/activate', User.isAuthenticated, User.requirePermission(PERMISSIONS.USERS_MANAGE), asyncHandler(async (req, res) => {
  await User.setUserActive(req.params.username.toLowerCase(), true);
  res.json({ success: true });
}));

app.put('/rest/users/:username/deactivate', User.isAuthenticated, User.requirePermission(PERMISSIONS.USERS_MANAGE), asyncHandler(async (req, res) => {
  await User.setUserActive(req.params.username.toLowerCase(), false);
  res.json({ success: true });
}));

app.delete('/rest/users/:username', User.isAuthenticated, User.requirePermission(PERMISSIONS.USERS_MANAGE), asyncHandler(async (req, res) => {
  await User.deleteUser(req.params.username.toLowerCase());
  res.json({ success: true });
}));

app.post('/rest/users/invite', User.isAuthenticated, User.requirePermission(PERMISSIONS.USERS_MANAGE), express.json(), asyncHandler(async (req, res) => {
  const { username, email, permissions } = req.body;
  if (!username || !email) return res.status(400).json({ success: false, message: 'username and email are required' });
  const smtpEnabledValue = serverConfig?.smtp?.enabled;
  const smtpEnabled = smtpEnabledValue === true || String(smtpEnabledValue).toLowerCase() === 'true';
  const smtpHostConfigured = typeof serverConfig?.smtp?.host === 'string' && serverConfig.smtp.host.trim().length > 0;
  const smtpPortConfigured = serverConfig?.smtp?.port !== undefined && serverConfig.smtp.port !== null && String(serverConfig.smtp.port).trim().length > 0;
  if (!(smtpEnabled && smtpHostConfigured && smtpPortConfigured)) {
    return res.status(400).json({ success: false, message: 'SMTP must be enabled and configured before sending invites. Please configure SMTP in Settings.' });
  }
  const result = await User.createInvitedUser(username, email, permissions);
  res.status(201).json({ success: true, message: 'User invited', username: result.user.username });
}));

app.post('/rest/users/:username/reset-password', User.isAuthenticated, User.requirePermission(PERMISSIONS.USERS_MANAGE), asyncHandler(async (req, res) => {
  await User.generateResetToken(req.params.username.toLowerCase());
  res.json({ success: true, message: 'Password reset email sent' });
}));
};
