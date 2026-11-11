const runtime = require('./runtime');

function printBanner() {
  const version = runtime.version;
  console.log('\x1b[32m                                888                                                  888      ');
  console.log('                                888                                                  888      ');
  console.log('                                888                                                  888      ');
  console.log('88888b.   .d88b.  88888b.d88b.  88888b.   .d88b.       .d8888b  .d88b.      888  888 888  888 ');
  console.log('888 "88b d8P  Y8b 888 "888 "88b 888 "88b d88""88b     d88P"    d88""88b     888  888 888 .88P ');
  console.log('888  888 88888888 888  888  888 888  888 888  888     888      888  888     888  888 888888K  ');
  console.log('888 d88P Y8b.     888  888  888 888 d88P Y88..88P d8b Y88b.    Y88..88P d8b Y88b 888 888 "88b ');
  console.log('88888P"   "Y8888  888  888  888 88888P"   "Y88P"  Y8P  "Y8888P  "Y88P"  Y8P  "Y88888 888  888 ');
  console.log('888                                                                                           ');
  console.log('888                                                                                           ');
  console.log('888                                                                                           ');
  console.log('\x1b[36m _________________');
  console.log('|# \x1b[30m\x1b[47m:           :\x1b[0m\x1b[36m #|');
  console.log('|  \x1b[30m\x1b[47m: Orchelium :\x1b[0m\x1b[36m  |');
  console.log('|  \x1b[30m\x1b[47m:   Agent   :\x1b[0m\x1b[36m  |');
  console.log('|  \x1b[30m\x1b[47m:           :\x1b[0m\x1b[36m  |       \x1b[32mVersion: ' + version + '\x1b[36m');
  console.log('|  \x1b[30m\x1b[47m:___________:\x1b[0m\x1b[36m  |');
  console.log('|    \x1b[90m _________\x1b[36m   |');
  console.log('|    \x1b[90m| __      |\x1b[36m  |');
  console.log('|    \x1b[90m||  |     |\x1b[36m  |');
  console.log('\\____\x1b[90m||__|_____|\x1b[36m__|\x1b[0m');
  console.log('');
  console.log('\x1b[Orcheliuim Agent\x1b[33m');
  console.log('\x1b[32m----------------------------------------------------------------------------------------------\x1b[0m\n');
}

function printConfiguration() {
  const colors = {
    brightYellow: '\x1b[93m',
    yellow: '\x1b[33m',
    reset: '\x1b[0m',
  };

  const separator = `${colors.brightYellow}${'═'.repeat(63)}${colors.reset}`;
  const divider = `${colors.yellow}${'─'.repeat(63)}${colors.reset}`;

  console.log(`\n${separator}`);
  console.log(`${colors.brightYellow}  🤖 ORCHELIUM AGENT CONFIGURATION${colors.reset}`);
  console.log(`${separator}\n`);

  console.log(`${colors.brightYellow}  📋 Agent Information${colors.reset}`);
  console.log(`${divider}`);
  console.log(`${colors.brightYellow}    🔹 Agent Name:${colors.reset}          ${colors.yellow}${runtime.AGENT_ID}${colors.reset}`);
  console.log(`${colors.brightYellow}    🔹 Host Name:${colors.reset}           ${colors.yellow}${runtime.HOSTNAME}${colors.reset}`);

  console.log(`\n${colors.brightYellow}  🔌 Connection Settings${colors.reset}`);
  console.log(`${divider}`);
  if (runtime.useMQTT) {
    console.log(`${colors.brightYellow}    🔹 Mode:${colors.reset}                ${colors.yellow}MQTT${colors.reset}`);
    console.log(`${colors.brightYellow}    🔹 Server:${colors.reset}              ${colors.yellow}${runtime.MQTT_SERVER}${colors.reset}`);
    console.log(`${colors.brightYellow}    🔹 Port:${colors.reset}                ${colors.yellow}${runtime.MQTT_SERVER_PORT}${colors.reset}`);
    console.log(`${colors.brightYellow}    🔹 Username:${colors.reset}            ${colors.yellow}${runtime.MQTT_USERNAME ? runtime.MQTT_USERNAME : 'none'}${colors.reset}`);
  } else {
    console.log(`${colors.brightYellow}    🔹 Mode:${colors.reset}                ${colors.yellow}WebSocket${colors.reset}`);
    console.log(`${colors.brightYellow}    🔹 Server:${colors.reset}              ${colors.yellow}${runtime.WS_SERVER}${colors.reset}`);
    console.log(`${colors.brightYellow}    🔹 Port:${colors.reset}                ${colors.yellow}${runtime.WS_SERVER_PORT}${colors.reset}`);
  }
  console.log(`${colors.brightYellow}    🔹 Webserver Port:${colors.reset}      ${colors.yellow}${runtime.PORT}${colors.reset}`);
  console.log(`${colors.brightYellow}    🔹 Connection Timeout:${colors.reset}  ${colors.yellow}${runtime.RETRY_TIMEOUT / 1000} secs${colors.reset}`);
  console.log(`${colors.brightYellow}    🔹 Connection token:${colors.reset}    ${colors.yellow}${runtime.AGENT_TOKEN ? 'configured' : 'not set'}${colors.reset}`);

  console.log(`\n${colors.brightYellow}  🌍 Environment${colors.reset}`);
  console.log(`${divider}`);
  console.log(`${colors.brightYellow}    🔹 Working Dir:${colors.reset}        ${colors.yellow}${runtime.workingDir}${colors.reset}`);
  console.log(`${colors.brightYellow}    🔹 Install Dir:${colors.reset}        ${colors.yellow}${runtime.INSTALL_DIR}${colors.reset}`);

  console.log(`\n${separator}\n`);
}

module.exports = { printBanner, printConfiguration };
