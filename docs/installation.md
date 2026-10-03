# Getting Started
This secition of the docs guides you through getting started with Orchelium

## Orcherlium Server

Orchelium is recommended to be installed and executed through the released container image in this repository, however you can run this directly on your server using node

### Container Setup

#### Pre-requisites
Please ensure you have Docker or any other container execution platform installed.  This documentation assumes the usage of Docker. For windows users, you can develop and test using WSL (Windows Subsystem for Linux), however this isn't recommended for an operational server instance.

#### Start an instance of this image

The latest version image can be found here:
```ghcr.io/dpembo/orchelium/hub:latest```

If you want specific versions, you can find these in the packages section of this repository

Starting an Orchelium server instance is simple:

```
docker run \
  -d \
  --name Orchelium \
  -e TZ=Europe/London \
  -p 8082:8082 \
  -p 49991:49981 \
  --restart unless-stopped \
  -v /custom/Orchelium/data:/usr/src/app/data \
  -v /custom/Orchelium/scripts:/usr/src/app/scripts \
  -v /custom/Orchelium/logs:/usr/src/app/logs \
  -v /custom/Orchelium/plugins:/usr/src/app/plugins \
  ghcr.io/dpembo/orchelium/hub:latest
```

#### Docker Run Parameters

| Parameter | Description | Example |
|---|---|---|
| `-d` | Run the container in the background (detached) | `-d` |
| `--name` | Name given to the container | `--name Orchelium` |
| `-p <host>:8082` | Web application port. Change only the host side of the mapping if 8082 is in use; do not change the container port | `-p 8082:8082` |
| `-p <host>:49981` | WebSocket server port used for hub/agent communication. Must match the port agents connect to | `-p 49991:49981` |
| `--restart` | Restart policy so the hub restarts after a reboot or failure | `--restart unless-stopped` |
| `-v <host path>:/usr/src/app/data` | Holds all Orchelium data including job history, user setup, configuration and statistics | `-v /custom/Orchelium/data:/usr/src/app/data` |
| `-v <host path>:/usr/src/app/scripts` | Where the backup/shell scripts you schedule are stored | `-v /custom/Orchelium/scripts:/usr/src/app/scripts` |
| `-v <host path>:/usr/src/app/logs` | Directory where log files are written | `-v /custom/Orchelium/logs:/usr/src/app/logs` |
| `-v <host path>:/usr/src/app/plugins` | Where plugins are stored | `-v /custom/Orchelium/plugins:/usr/src/app/plugins` |
| `-e <name>=<value>` | Sets an environment variable in the container (see below) | `-e TZ=Europe/London` |

#### Environment Variables

Set using `-e NAME=value` on `docker run`, or under `environment:` in Docker Compose.

| Environment Variable | Description | Example |
|---|---|---|
| TZ | Time zone to ensure the container operates in your correct time zone for display of date/times.  Time zone names follow the standard IANA database, of which you can find a list via [wikipedia](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)| Europe/London |
| ORCHELIUM_ENCRYPTION_KEY | This variable is used to provide the encryption key used between the Hub and Agents to ensure the data/commmands cannot be compromised, or the link from agent to server be misued.  This has a default value, but its recommended to change this.  Note that the environment variable has to be set the same on the server and any environment where an agent is deployed for the communication to work correctly|MySecretKey|
| ORCHELIUM_KEY_ENFORCE | Controls server behaviour when `ORCHELIUM_ENCRYPTION_KEY` is not set (i.e. the default key `CHANGEIT` is in use). Accepted values: `strict` — server refuses to start; `warn` (default) — server starts but logs a prominent warning; `silent` — server starts with no warning (not recommended outside development). This variable must be set consistently on both the server and all agents. | `warn` |

#### Docker Compose

The equivalent of the `docker run` command above as a `docker-compose.yml`:

```yaml
services:
  orchelium:
    image: ghcr.io/dpembo/orchelium/hub:latest
    container_name: Orchelium
    restart: unless-stopped
    environment:
      - TZ=Europe/London
      # - ORCHELIUM_ENCRYPTION_KEY=MySecretKey
      # - ORCHELIUM_KEY_ENFORCE=warn
    ports:
      - "8082:8082"
      - "49981:49981"
    volumes:
      - /custom/Orchelium/data:/usr/src/app/data
      - /custom/Orchelium/scripts:/usr/src/app/scripts
      - /custom/Orchelium/logs:/usr/src/app/logs
      - /custom/Orchelium/plugins:/usr/src/app/plugins
```

Start it with `docker compose up -d`.

#### Upgrading

If you are upgrading an existing installation, including migrating job/orchestration definitions to filesystem storage, see [Upgrade](./Upgrade.md).

### Manual Installation
Detailed instructions are provided here as it's recommended to run this from the container image, however it is just a node.js server applciation, so can be setup by: 
* Cloning the repo
* Installing Node (v2x)
* Using npm to install libs
* Setting environment variables appropritate
* Launching the app (server.js)

## Orchelium Console ##
Login to the Orchelium  Console via a webbrowser on your server/ip with the given port, which by default is 8082. e.g.
```http://localhost:8082```

### Initial User creation
First you will be asked to create a user:

![image info](./screens/setup1.png)
You need to provide the following:
* A username
* An email address to associate with that username
* A password

Then Press 'REGISTER' to continue

### Login
After creating the user, it will then ask you to login with that user.

![image info](./screens/setup2.png)

**Note:**

```If for any reason you cannot login, simply delete the user.db directory found in the data location, and then restart the server.```

### Welcome
You'll now be taken to the welcome screen
![image info](./screens/setup3.png)
Please press 'NEXT' to continue

### Server Settings
Next you'll be asked to provide some server settings
![image info](./screens/setup4.png)
* **Timezone** is utlized to ensure dates are shown in the configured timezone.  It's recommended the timezone match that of the server runtime, or the environment variable provided to a container. If an environment variable is set, this will ne defaulted to that value.
* **Hostname** is the name of the host used in emails, notifications, etc
* **Web Server Port** is the webserver port. Please note, that if you are running as a container, you should not change this, and simply change the port mapping for the container.
* **Websocker Server Port** is the port for the websocker server, which is used by agents to communicate with Orcheliuim.  It's recommended to leave this as the default.

Once completed/confirmed, please press "Next"

* **Completed** you've now completed the initial setup, press next to launch the Orchelium Console.
![image info](./screens/setup5.png)



## Orchelium Agent

### Navigate to Agents Provision
Once logged into the Hub, navigate to the agents screen from the top menu

![image info](./screens/agents-icon.png)  

Which will take you here:
![image info](./screens/agent-empty.png)  


### Add Agent
1. simply press the (+) button in in the agents screen, which will bring up the agent install command
![image info](./screens/agent-deploy.png)

2. Press the copy icon (or copy the full command text), and paste this into a terminal running as root or with appropriate permissions.
![image info](./screens/deploy-1.png)
then press return to execute

3. As you've run this from the Orchelium console, a number of parameters are defaulted in the command you copied, therefore you'll next see a message indicating this:
![image info](./screens/deploy-2.png)
Please press 'ok' to continue.

4. You now get the option to continue with the defaulted values (recommended), or to start a new setup, or simply exit the agent installer.
![image info](./screens/deploy-3.png)
Leave this as "Use Provided Settings" and press ok.

5. Next you can provide a suitable agent name.  Please note agent names must be unique, so ensure as you deploy other agents you provide a unique name.  You could base this in the servername, or even the last octet of the IP Address.
![image info](./screens/deploy-4.png)
Provide an agent name, and press "OK" to continue

6. The next choice is whether to use MQTT or Websocket.
![image info](./screens/deploy-5.png)
Websocket is recommended, therefore leave this options as WebSocket and choose 'OK' to continue

7. Next you need to provide the server name.  
![image info](./screens/deploy-6.png)
The default will already be provided from the command you pasted, so you can leave this empty and press 'OK' to select the default

8. Next confirm/modify the WebSocket server port:
![image info](./screens/deploy-7.png)
Its recommended to leave this as the default and should match the configured port from the server, then press "OK" to continue.

9. Next confirm the working directory where scripts are created and executed.
![image info](./screens/deploy-8.png)
By default this is set to /tmp, but can be changed to any path where the agent can access.  Then press "OK" to continue.

10. The final choice is to determine how to start the agent.  There are multiple choices here
![image info](./screens/deploy-9.png) Use PM2. See [here](https://www.npmjs.com/package/pm2), use Crontab, Run as a service, Configure and create a container and execute (requires docker), or none, which will require manual execution, i.e. ```node agent.js```.  Please determine the most appropriate option, or if not known, proceeed with PM2.

At this point, the agent will run through the installation process and start.

Now switch back to the Orchelium console, where you'll see a notification once the agent has started:

![image info](./screens/deploy-10.png)

Simply press "OK" to confirm the connected agent
![image info](./screens/deploy-11.png)

Then press submit, and your agent will be added
![image info](./screens/agent-list.png)

**Congratulations, you've added your first agent!**

---

## Related Documentation

- [Upgrading from old versions](./Upgrade.md) Upgrade information when moving from a version prior to 2026.06.06.xx
- [Job Schedules](./backup-schedules.md): Creating and managing schedules
- [Orchestrations](./orchestrations.md): Building complex  workflows
- [Settings Configuration](./settings-config.md): Server and agent configuration
- [User Management](./user-management.md): User accounts and permissions
- [REST API Reference](./REST_API_REFERENCE.md): Programmatic access
- [Back to Documentation Index](./README.MD)


