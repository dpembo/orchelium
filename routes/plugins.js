module.exports = function register(app) {
app.get('/plugins.html', User.isAuthenticated, User.requirePermission(PERMISSIONS.PLUGINS_MANAGE), asyncHandler(async (req, res) => {
  res.render('pluginManager', { version, user: req.session.user, csrf: req.csrfToken() });
}));

app.get('/rest/plugins/registry', User.isAuthenticated, User.requirePermission(PERMISSIONS.PLUGINS_MANAGE), asyncHandler(async (req, res) => {
  const pluginRegistry = require('../pluginRegistry.js');
  const remoteRegistry = await pluginManager.fetchRemoteRegistry(serverConfig.pluginRegistry);
  //console.log('REGISTRY SAMPLE', JSON.stringify(remoteRegistry, null, 2));
  const installed = pluginRegistry.getRegistry();

  const rawBase   = (serverConfig.pluginRegistry && serverConfig.pluginRegistry.url || '').replace(/\/registry\.json$/, '');
  const sourceBase = (remoteRegistry.source || '').replace(/\/$/, '');
  const plugins = remoteRegistry.plugins.map(p => {
    const local = installed[p.name];
    return {
      ...p,
      installed:        !!local,
      installedVersion: local ? (local.version || null) : null,
      updateAvailable:  !!(local && local.version && p.version && local.version !== p.version),
      //iconUrl:          rawBase && p.path ? `${rawBase}/${p.path}/icon.svg` : null,
      docsUrl:          sourceBase && p.path ? `${sourceBase}/blob/main/${p.path}/docs.md` : null,
    };
  });

  res.json({ success: true, plugins, registryVersion: remoteRegistry.registryVersion, updated: remoteRegistry.updated });
}));

app.post('/rest/plugins/install/:name', User.isAuthenticated, User.requirePermission(PERMISSIONS.PLUGINS_MANAGE), express.json(), asyncHandler(async (req, res) => {
  const pluginName    = req.params.name;
  const registryEntry = req.body && req.body.registryEntry;

  if (!registryEntry || typeof registryEntry !== 'object') {
    return res.status(400).json({ success: false, message: 'Request body must include registryEntry' });
  }

  await pluginManager.installPlugin(pluginName, registryEntry, serverConfig.pluginRegistry);
  await require('../pluginRegistry.js').reload();
  logger.info(`[PLUGIN MANAGER] Plugin installed: ${pluginName} by ${req.session.user && req.session.user.username}`);
  res.json({ success: true, message: `Plugin '${pluginName}' installed successfully` });
}));

app.delete('/rest/plugins/:name', User.isAuthenticated, User.requirePermission(PERMISSIONS.PLUGINS_MANAGE), asyncHandler(async (req, res) => {
  const pluginName = req.params.name;
  await pluginManager.uninstallPlugin(pluginName);
  await require('../pluginRegistry.js').reload();
  logger.info(`[PLUGIN MANAGER] Plugin uninstalled: ${pluginName} by ${req.session.user && req.session.user.username}`);
  res.json({ success: true, message: `Plugin '${pluginName}' uninstalled successfully` });
}));
};
