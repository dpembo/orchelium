const fs = require('fs');
const path = require('path');
const { buildAgentInstallCommand } = require('../../utils/agentInstallCommand');

const SECRET = 'broker-secret-do-not-leak';

function config(overrides = {}) {
  return {
    mqtt: {
      enabled: 'true',
      server: 'mqtt.internal',
      port: 1883,
      username: 'orchelium',
      password: SECRET,
      ...overrides.mqtt,
    },
    websocket_server: {
      enabled: 'false',
      server: 'ws.internal',
      port: 8080,
      ...overrides.websocket_server,
    },
  };
}

describe('buildAgentInstallCommand', () => {
  it('includes the broker password only inside the install command for an enabled broker', () => {
    const command = buildAgentInstallCommand(config(), 'https://hub.example:8443');

    expect(command).toContain(`--mqttPassword='${SECRET}'`);
    expect(command).toContain("--mqttUsername='orchelium'");
    expect(command).toContain("--mqttServer='mqtt.internal'");
    expect(command).toContain("--mqttPort='1883'");
    expect(command).toContain("wget -qO- 'https://hub.example:8443/agent/install.sh'");
    expect(command).toContain("--orcheliumUrl='https://hub.example:8443'");
    expect(command).not.toContain('--wsServer');
  });

  it('omits broker credentials when MQTT is disabled', () => {
    const command = buildAgentInstallCommand(config({ mqtt: { enabled: 'false' } }), 'http://hub.local');

    expect(command).not.toContain(SECRET);
    expect(command).not.toContain('mqttPassword');
    expect(command).not.toContain('mqttUsername');
    expect(command).not.toContain('mqtt.internal');
  });

  it('omits an empty broker password', () => {
    const command = buildAgentInstallCommand(config({ mqtt: { password: '' } }), 'http://hub.local');

    expect(command).not.toContain('mqttPassword');
    expect(command).toContain("--mqttUsername='orchelium'");
  });

  it('shell-quotes a password that contains quotes and spaces', () => {
    const command = buildAgentInstallCommand(
      config({ mqtt: { password: "p'ass word" } }),
      'http://hub.local'
    );

    expect(command).toContain(`--mqttPassword='p'\\''ass word'`);
    expect(command).not.toContain("--mqttPassword=p'ass");
  });

  it('includes websocket arguments when that transport is enabled', () => {
    const command = buildAgentInstallCommand(
      config({ websocket_server: { enabled: true } }),
      'http://hub.local/'
    );

    expect(command).toContain("--wsServer='ws.internal'");
    expect(command).toContain("--wsPort='8080'");
    expect(command).toContain("wget -qO- 'http://hub.local/agent/install.sh'");
  });
});

describe('agent status does not embed the broker password', () => {
  const root = path.join(__dirname, '../..');

  it('does not pass MQTT credentials into the agent status template', () => {
    const agents = fs.readFileSync(path.join(root, 'routes/agents.js'), 'utf8');
    const template = fs.readFileSync(path.join(root, 'views/agentstatus.ejs'), 'utf8');
    const route = agents.slice(
      agents.indexOf("app.get('/agentstatus.html'"),
      agents.indexOf("app.post('/agent-submit.html'")
    );
    const installStart = agents.indexOf("app.get('/rest/agents/install-command'");
    const installRoute = agents.slice(
      installStart,
      agents.indexOf("app.get('/rest/agents',", installStart + 1)
    );

    expect(route).not.toContain('MQTT_PASSWORD');
    expect(route).not.toContain('mqtt.password');
    expect(route).not.toContain('MQTT_USERNAME');
    expect(template).not.toContain('MQTT_PASSWORD');
    expect(template).not.toContain('mqtt.password');
    expect(template).not.toContain('mqttPassword');
    expect(installRoute).toContain('PERMISSIONS.AGENTS_CREATE');
    expect(installRoute).toContain('buildAgentInstallCommand');
    expect(installRoute).toContain('no-store');
  });
});
