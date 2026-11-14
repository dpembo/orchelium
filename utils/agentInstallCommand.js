function shellQuote(value) {
  return "'" + String(value).replace(/'/g, `'\\''`) + "'";
}

function shellArg(flag, value) {
  if (value === undefined || value === null || value === '') return '';
  return ' ' + flag + '=' + shellQuote(value);
}

function isEnabled(value) {
  return value === true || String(value || '').toUpperCase() === 'TRUE';
}

function buildAgentInstallCommand(config, baseUrl, agentToken) {
  const origin = String(baseUrl || '').replace(/\/+$/, '');
  const mqtt = (config && config.mqtt) || {};
  const websocket = (config && config.websocket_server) || {};

  let params = '';
  if (isEnabled(mqtt.enabled)) {
    params += shellArg('--mqttServer', mqtt.server);
    params += shellArg('--mqttPort', mqtt.port);
    params += shellArg('--mqttUsername', mqtt.username);
    params += shellArg('--mqttPassword', mqtt.password);
  }
  if (isEnabled(websocket.enabled)) {
    params += shellArg('--wsServer', websocket.server);
    params += shellArg('--wsPort', websocket.port);
  }
  params += shellArg('--agentToken', agentToken);

  return 'wget -qO- ' + shellQuote(origin + '/agent/install.sh')
    + ' | bash -s -- --orcheliumUrl=' + shellQuote(origin)
    + ' --installDir=' + shellQuote('/opt/OrcheliumAgent')
    + params;
}

module.exports = { buildAgentInstallCommand, shellQuote };
