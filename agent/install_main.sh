#!/bin/bash

TOTAL_STEPS=11
STEP=0

ENABLE_LOGGING=true
LOG_FILE="agent-install.log"

# Function to log messages to a file if logging is enabled
log_message() {
  local message="$1"
  if [ "$ENABLE_LOGGING" = true ]; then
    echo "$message" >> "$LOG_FILE"
  fi
}

# Function to install dialog based on the package manager
install_dialog() {
  if [ -x "$(command -v apt-get)" ]; then
    echo "Installing dialog using apt-get..."
    sudo apt-get update
    sudo apt-get install -y dialog
  elif [ -x "$(command -v dnf)" ]; then
    echo "Installing dialog using dnf..."
    sudo dnf install -y dialog
  elif [ -x "$(command -v yum)" ]; then
    echo "Installing dialog using yum..."
    sudo yum install -y dialog
  elif [ -x "$(command -v apk)" ]; then
    echo "Installing dialog using apk..."
    sudo apk add dialog
  elif [ -x "$(command -v pacman)" ]; then
    echo "Installing dialog using pacman..."
    sudo pacman -Syu --noconfirm dialog
  elif [ -x "$(command -v zypper)" ]; then
    echo "Installing dialog using zypper..."
    sudo zypper install -y dialog
  elif [ -x "$(command -v emerge)" ]; then
    echo "Installing dialog using emerge..."
    sudo emerge --ask dialog
  else
    echo "Could not determine the package manager. Please install 'dialog' manually."
    exit 1
  fi
}

# Function to update the progress bar with custom text
update_progress() {
  STEP=$((STEP + 1))
  PERCENT=$((STEP * 100 / TOTAL_STEPS))
  
  # End redirection for dialog
  exec 1>&3 2>&4
  
  # Update progress dialog
  echo "$PERCENT" | dialog --gauge "$1" 10 70 0
  
  # Restart redirection
  exec 1>>"$LOG_FILE" 2>&1
}

# Function to replace text in a file
# Usage: replace_text "old_text" "new_text" /path/to/your/textfile.txt
replace_text() {
    local search_text="$1"
    local replacement_text="$2"
    local file_path="$3"

    # Check if the file exists
    if [ ! -f "$file_path" ]; then
        log_message "File $file_path does not exist."
        return 1
    fi

    # Perform the replacement
    sed -i "s|$search_text|$replacement_text|g" "$file_path"
    #echo "Replacement complete: $search_text -> $replacement_text in $file_path"
}



# Check if dialog is installed
if ! command -v dialog > /dev/null 2>&1; then
  echo "dialog is not installed."
  install_dialog
else
  echo "dialog is already installed."
fi

exec 3>&1 4>&2
exec 1>>"$LOG_FILE" 2>&1

update_progress "Checking and installing dependencies... please wait"
sleep 1;


update_progress "Checking Running as root"
sleep 1;
# Check if running with sudo privileges
if [ "$EUID" -ne 0 ]; then
    exec 1>&3 2>&4
    clear
    echo "Please run the script with sudo or as root."
    exit 1
fi

update_progress "Checking/installing CURL"
log_message "  * Checking curl"
if ! command -v curl &> /dev/null; then
    apt update
    apt install -y curl
fi

update_progress "Checking/installing GIT"
log_message "  * Checking git"
# Check and install git package if needed
if ! command -v git &> /dev/null; then
    apt update
    apt install -y git
fi


# Variables
INSTALL_DIR="${1:-/opt/OrcheliumAgent}"


# After the welcome message, continue with your script
# Clear the screen after the dialog is closed
#echo "Continuing with the script..."

update_progress "Checking for node.js/nvm"
log_message " ---------------------------- "
log_message " Installing Node Requirements "
log_message " ---------------------------- "

NVM_DIR="$HOME/.nvm"
NVM_SCRIPT="$NVM_DIR/nvm.sh"

log_message "  * Checking NVM"
if [ -d "$NVM_DIR" ] && [ -f "$NVM_SCRIPT" ]; then
    update_progress "NVM exists"
    log_message "  * NVM exists"
    [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"

else
    log_message "  * NVM doesn't exist - installing"
    update_progress "Installing NVM"
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.38.0/install.sh | bash
    [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
fi

log_message "  * Checking Node v20.0.5"
# Check if Node.js v20.0.5 is installed
update_progress "Installing Node v20.5.0"
nvm install v20.5.0

update_progress "Getting node path"
nodePath=`nvm which 20.5.0`


update_progress "Upating for install location"

findText="/opt/OrcheliumAgent";
replText="$INSTALL_DIR"
replace_text "$findText" "$replText" startup_cron.sh
replace_text "$findText" "$replText" startup_pm2.sh
replace_text "$findText" "$replText" uninstall.sh
replace_text "$findText" "$replText" startup_svc.sh
replace_text "$findText" "$replText" startup_container.sh

update_progress "Checking/creating install directory"
# Check if INSTALL_DIR exists
if [ ! -d "$INSTALL_DIR" ]; then
  log_message "Directory $INSTALL_DIR does not exist. Creating it..."
  mkdir -p "$INSTALL_DIR"
  if [ $? -eq 0 ]; then
    log_message "Directory $INSTALL_DIR created successfully."
  else
    exec 1>&3 2>&4
    clear
    echo "Failed to create directory $INSTALL_DIR. Exiting."
    exit 1
  fi
else
  log_message "Directory $INSTALL_DIR already exists."
fi

update_progress "Copying contents to $INSTALL_DIR"
log_message "Copying contents to $INSTALL_DIR..."

# Check if the source and destination are the same
if [ "$(realpath .)" = "$(realpath "$INSTALL_DIR")" ]; then
  log_message "Source and destination are the same. Skipping copy."
else
  cp -r . "$INSTALL_DIR"
  
  if [ $? -eq 0 ]; then
    log_message "Contents copied successfully."
  else
    exec 1>&3 2>&4
    clear
    echo "Failed to copy contents. Exiting."
    exit 1
  fi
fi

#End redirection
exec 1>&3 2>&4

cd $INSTALL_DIR

LOG_FILE="/var/log/OrcheliumAgent.log"
SETTINGS_FILE="settings.sh"

WELCOME_MESSAGE="Welcome to Orchelium Agent Installer!\n\nThis script will guide you through the installation process to get the agent up and running. Press OK to continue"

# Load settings if available
if [ -f "$SETTINGS_FILE" ]; then
    source "$SETTINGS_FILE"
    WELCOME_MESSAGE="Welcome to Orchelium Agent Installer - Some configuration has been provided during the provisioning, or this has been installed previously.  Please OK then select an option to contiune"
fi

dialog --title "Message" --msgbox "$WELCOME_MESSAGE" 10 50
clear
# Display a dialog box with three options using a menu
dialog --title "Installation Options" --radiolist "Choose one of the following options:" 15 50 3 \
"1" "Use Provided Settings" ON \
"2" "Configure Manually" OFF \
"3" "Exit" OFF 2>installtype
# Get the user selection from the tempfile
user_choice=$(<installtype)
# Remove the tempfile
rm -f installtype
clear
case $user_choice in
    1)
        echo "Proceeding with provided settings..."
        source "$SETTINGS_FILE"
        ;;
    2)
        echo "Proceeding with manual configuration..."
        ;;
    3)
        echo "Exiting from install."
        exit 1
        ;;
    *)
        echo "Invalid option. Exiting."
        exit 1
        ;;
esac    

echo " ----------------------- "
echo " Configuring Agent "
echo " ----------------------- "

echo "  * Installing NPM Packages"

cd $INSTALL_DIR
npm install

# Prompt for Agent Name
# If settings file doesn't exist or variables are not set, prompt the user
if [ -z "$AGENT_NAME" ]; then
    dialog --inputbox "Enter the Agent Name:" 10 40 2> agent_name.txt
    AGENT_NAME=$(cat agent_name.txt)
fi 

# Prompt for Orchelium hostname
if [ -z "$ORCHELIUM_SERVER" ]; then
    dialog --inputbox "Enter Orchelium Hostname (default: localhost)" 10 40 2> ORCHELIUM_SERVER.txt
    clear
    ORCHELIUM_SERVER=$(cat ORCHELIUM_SERVER.txt)
    if [ -z "$ORCHELIUM_SERVER" ]; then
        ORCHELIUM_SERVER="localhost"
    fi
fi

# Prompt for BackhupHub Server Port
if [ -z "$ORCHELIUIM_PORT" ]; then
    dialog --inputbox "Enter Orchelium Port (default: 8082):" 10 40 2> ORCHELIUIM_PORT.txt
    clear
    ORCHELIUIM_PORT=$(cat ORCHELIUIM_PORT.txt)
    if [ -z "$ORCHELIUIM_PORT" ]; then
        ORCHELIUIM_PORT="8082"
    fi
fi


if [ -z "$MQTT_SERVER" ] && [ -z "$WS_SERVER" ]; then
  CHOICE=$(dialog --clear \
                  --backtitle "Protocol Selection" \
                  --title "Choose a Protocol" \
                  --menu "Please select the protocol to use:" \
                  15 50 2 \
                  "1" "MQTT" \
                  "2" "WebSocket" \
                  2>&1 >/dev/tty)
  clear  # Clear the screen after the dialog is closed
fi 

case $CHOICE in
  1)
    #echo "You chose MQTT."
    if [ -z "$MQTT_SERVER" ]; then
        # Prompt for MQTT Server
        dialog --inputbox "Enter MQTT Server Hostname:" 10 40 2> mqtt_server.txt
        clear
        MQTT_SERVER=$(cat mqtt_server.txt)
        MQTT_ENABLED="true"
    fi

    # Prompt for MQTT Port
    if [ -z "$MQTT_PORT" ]; then
        dialog --inputbox "Enter MQTT Port (default: 1883):" 10 40 2> mqtt_port.txt
        clear
        MQTT_PORT=$(cat mqtt_port.txt)
        if [ -z "$MQTT_PORT" ]; then
            MQTT_PORT="1883"
        fi
    fi

    if [ -z "$MQTT_USERNAME" ]; then
        dialog --inputbox "Enter MQTT Username (leave empty if the broker does not require a login):" 10 70 2> mqtt_user.txt
        clear
        MQTT_USERNAME=$(cat mqtt_user.txt)
        rm -f mqtt_user.txt
    fi

    if [ -n "$MQTT_USERNAME" ] && [ -z "$MQTT_PASSWORD" ]; then
        dialog --inputbox "Enter MQTT Password:" 10 60 2> mqtt_pass.txt
        clear
        MQTT_PASSWORD=$(cat mqtt_pass.txt)
        rm -f mqtt_pass.txt
    fi
    ;;

  2)
    #echo "You chose WebSocket."
    # Prompt for Orchelium hostname
    MQTT_ENABLED="false"
    if [ -z "$WS_SERVER" ]; then
        dialog --inputbox "Enter Websocket Server Hostname (default: $ORCHELIUM_SERVER)" 10 40 2> ws_server.txt
        clear
        WS_SERVER=$(cat ws_server.txt)
        if [ -z "$WS_SERVER" ]; then
            WS_SERVER="$ORCHELIUM_SERVER"
        fi
    fi

    if [ -z "$WS_PORT" ]; then
        dialog --inputbox "Enter Websocket Server Hostname (default: 49981)" 10 40 2> ws_port.txt
        clear
        WS_PORT=$(cat ws_port.txt)
        if [ -z "$WS_PORT" ]; then
            WS_PORT="49981"
        fi
    fi
    ;;
  *)
    echo "Values provided from settings file"
    ;;
esac



# Prompt for Working Directory
if [ -z "$WORKING_DIR" ]; then
    dialog --inputbox "Enter Working Directory (default: /tmp):" 10 40 2> working_dir.txt
    clear
    WORKING_DIR=$(cat working_dir.txt)
fi

# Set default Working Directory to /tmp if empty
if [ -z "$WORKING_DIR" ]; then
    WORKING_DIR="/tmp"
fi

# Present the select dialog for startup method
if [ -z "$STARTUP_TYPE" ]; then
    STARTUP_TYPE=none   
    dialog --menu "Choose a startup method:" 15 40 3 \
        "pm2" "Start with PM2" \
        "Crontab" "Start with Crontab" \
        "Service" "Start via init.d as a service" \
        "Docker" "Build and start a docker container" \
        "none" "No Auto Startup" 2> choice.txt
    clear
    STARTUP_TYPE=$(cat choice.txt)
fi

NODE_COMMAND=`which node`

# Save settings to the settings file
echo "AGENT_NAME=\"$AGENT_NAME\"" > "$SETTINGS_FILE"
echo "MQTT_ENABLED=\"$MQTT_ENABLED\"" >> "$SETTINGS_FILE"
if [ "$MQTT_ENABLED" = "true" ]; then
    echo "MQTT_SERVER=\"$MQTT_SERVER\"" >> "$SETTINGS_FILE"
    echo "MQTT_PORT=\"$MQTT_PORT\"" >> "$SETTINGS_FILE"
    if [ -n "$MQTT_USERNAME" ]; then
        echo "MQTT_USERNAME=\"$MQTT_USERNAME\"" >> "$SETTINGS_FILE"
        echo "MQTT_PASSWORD=\"$MQTT_PASSWORD\"" >> "$SETTINGS_FILE"
    fi
fi
echo "ORCHELIUM_SERVER=\"$ORCHELIUM_SERVER\"" >> "$SETTINGS_FILE"
echo "ORCHELIUIM_PORT=\"$ORCHELIUIM_PORT\"" >> "$SETTINGS_FILE"
if [ "$WS_ENABLED" = "true" ]; then
  echo "WS_SERVER=\"$WS_SERVER\"" >> "$SETTINGS_FILE"
  echo "WS_PORT=\"$WS_PORT\"" >> "$SETTINGS_FILE"
fi
echo "WORKING_DIR=\"$WORKING_DIR\"" >> "$SETTINGS_FILE"
echo "STARTUP_TYPE=\"$STARTUP_TYPE\"" >> "$SETTINGS_FILE"
echo "INSTALL_DIR=\"$INSTALL_DIR\"" >> "$SETTINGS_FILE"
echo "NODE_COMMAND=\"$NODE_COMMAND\"" >> "$SETTINGS_FILE"
if [ -n "$AGENT_TOKEN" ]; then
    echo "AGENT_TOKEN=\"$AGENT_TOKEN\"" >> "$SETTINGS_FILE"
fi
echo "CONNECTION_RETRIES=\"360\"" >> "$SETTINGS_FILE"
echo "CONNECTION_DELAY=\"30000\"" >> "$SETTINGS_FILE"
echo "CONNECTION_TIMEOUT=\"5000\"" >> "$SETTINGS_FILE"


case "$STARTUP_TYPE" in
    "pm2")
        echo "You selected pm2 for startup."
        source "startup_pm2.sh"
        ;;
    "Crontab")
        echo "You chose to use Crontab for startup."
        source "startup_cron.sh"
        ;;
    "none")
        echo "No auto startup is configured"
        exit 0
        ;;
    "Service")
        echo "You chose to use init.d for startup."
        source "setup_svc.sh"
        ;;
     "Docker")
        echo "You chose to use docker for startup."        
        source startup_container.sh
        ;;
    *)
        echo "Invalid choice"
        ;;
esac

echo " ----------------------- "
echo " Cleaning up "
echo " ----------------------- "
# Clean up
echo "  * Removing temporary files"
rm -f agent_name.txt mqtt_server.txt working_dir.txt mqtt_port.txt choice.txt ws_server.txt ws_port.txt

echo " ----------------------- "
echo " Complete "
echo " ----------------------- "
echo "  * Installation completed. Application is managed by $STARTUP_TYPE"
