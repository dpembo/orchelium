#!/bin/bash
INSTALL_DIR="/opt/OrcheliumAgent"

# Default values for variables
mqttServer=""
mqttPort=""
mqttUsername=""
mqttPassword=""
wsServer=""
wsPort=""
mqttEnabled="false"
wsEnabled="false"
orchelium=""

# Help function to display usage information
function show_help() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --orcheliumUrl=<url>     (Mandatory) URL of Orchelium."
    echo "  --installDir=<path>      Installation directory (default: /opt/OrcheliumAgent)."
    echo "  --mqttServer=<ip>        IP address of the MQTT server."
    echo "  --mqttPort=<port>        Port of the MQTT server."
    echo "  --mqttUsername=<user>    Username when the MQTT broker requires a login."
    echo "  --mqttPassword=<secret>  Password when the MQTT broker requires a login."
    echo "  --wsServer=<ip>          IP address of the WebSocket server."
    echo "  --wsPort=<port>          Port of the WebSocket server."
    echo "  -?, --?, -h, --help      Show this help message."
    exit 0
}

# Parse command-line arguments
for arg in "$@"
do
    case $arg in
        --orcheliumUrl=*)
            orcheliumUrl="${arg#*=}"
            ;;
        --installDir=*)
            INSTALL_DIR="${arg#*=}"
            ;;
        --mqttServer=*)
            mqttServer="${arg#*=}"
            mqttEnabled="true"
            ;;
        --mqttPort=*)
            mqttPort="${arg#*=}"
            ;;
        --mqttUsername=*)
            mqttUsername="${arg#*=}"
            ;;
        --mqttPassword=*)
            mqttPassword="${arg#*=}"
            ;;
        --wsServer=*)
            wsServer="${arg#*=}"
            wsEnabled="true"
            ;;
        --wsPort=*)
            wsPort="${arg#*=}"
            ;;
        -\? | --\? | -h | --help)
            show_help
            ;;
        *)
            echo "Unknown argument: $arg"
            show_help
            ;;
    esac
done

# Ensure orcheliumUrl is provided
if [ -z "$orcheliumUrl" ]; then
    echo "Error: --orcheliumUrl is mandatory."
    show_help
fi


# Checks if a directory exists, exits if so, else creates
check_directory() {
    #dir_name=$INSTALL_DIR

    if [ -d "$INSTALL_DIR" ]; then
        echo "Agent Installation directory: '$INSTALL_DIR' already exists. Aborting installation"
        exit 1
    else
        echo "Directory '$INSTALL_DIR' does not exist, creating"
        if ! mkdir -p "$INSTALL_DIR"; then
            echo "Error: Failed to create directory '$INSTALL_DIR'."
            echo "Please ensure you run as a user with the appopriate permissions"
            echo "EXITING INSTALLATION"
            exit 1
        fi
    fi
}

# Function to install wget based on linux distro
install_wget() {
    distro=$1

    case "$distro" in
        "debian")
            apt-get update
            apt-get install -y wget
            ;;
        "fedora")
            dnf install -y wget
            ;;
        "centos"|"rhel")
            yum install -y wget
            ;;
        *)
            echo "Unsupported distribution. Please install wget manually."
            ;;
    esac
}

install_sed() {
    distro=$1

    case "$distro" in
        "debian")
            apt-get update
            apt-get install -y sed
            ;;
        "fedora")
            dnf install -y sed
            ;;
        "centos"|"rhel")
            yum install -y sed
            ;;
        *)
            echo "Unsupported distribution. Please install sed manually."
            ;;
    esac
}

# Prompt function for user choice
prompt_user() {
    while true; do
        echo "Distribution detection failed. Would you like to:"
        echo "1. Exit"
        echo "2. Continue without installing dependencies"
        read -p "Enter your choice (1 or 2): " choice < /dev/tty
        case $choice in
            1)
                echo "Exiting installation."
                exit 1
                ;;
            2)
                echo "Proceeding without installing dependencies."
                break
                ;;
            *)
                echo "Invalid choice. Please select 1 or 2."
                ;;
        esac
    done
}

# Detect the Linux distribution
if [ -f /etc/debian_version ]; then
    distribution="debian"
elif [ -f /etc/redhat-release ]; then
    release_info=$(< /etc/redhat-release)
    if [[ $release_info == *"Fedora"* ]]; then
        distribution="fedora"
    elif [[ $release_info == *"CentOS"* ]]; then
        distribution="centos"
    elif [[ $release_info == *"Red Hat"* ]]; then
        distribution="rhel"
    else
        echo "Distribution detection failed."
        prompt_user
    fi
else
    echo "Distribution detection failed."
    prompt_user
fi

# Check and install wget
if ! command -v wget > /dev/null 2>&1; then
    echo "wget is not installed. Attempting to install..."
    install_wget "$distribution"
fi

if ! command -v sed > /dev/null 2>&1; then
    echo "sed is not installed. Attempting to install..."
    install_sed "$distribution"
fi

echo '
                                 888                                                  888
                                 888                                                  888
                                 888                                                  888
 88888b.   .d88b.  88888b.d88b.  88888b.   .d88b.       .d8888b  .d88b.      888  888 888  888
 888 "88b d8P  Y8b 888 "888 "88b 888 "88b d88""88b     d88P"    d88""88b     888  888 888 .88P
 888  888 88888888 888  888  888 888  888 888  888     888      888  888     888  888 888888K
 888 d88P Y8b.     888  888  888 888 d88P Y88..88P d8b Y88b.    Y88..88P d8b Y88b 888 888 "88b
 88888P"   "Y8888  888  888  888 88888P"   "Y88P"  Y8P  "Y8888P  "Y88P"  Y8P  "Y88888 888  888
 888
 888
 888
 _________________
|# :           : #|
|  :  BACK-UP  :  |
|  :    HUB    :  |
|  :   AGENT   :  |
|  :___________:  |
|     _________   |
|    | __      |  |
|    ||  |     |  |
\____||__|_____|__|

'

echo "================================================================================"
echo "Installation"
echo "================================================================================"

echo '1. Check/Create Install directory
'
echo "Checking directory: $INSTALL_DIR"
check_directory "$INSTALL_DIR"

echo '2. Change to install location
'
cd "$INSTALL_DIR"


echo '3. Download agent bundle from Orchelium
'

wget "$orcheliumUrl/agent/agent.tar"

echo '4. Untar the Agent bundle
'
# untar
tar -xvf agent.tar

echo '5. Set MQTT Settings
'

hostname=$(echo "$orcheliumUrl" | sed -E 's|.*://([^:/]+).*|\1|')
port=$(echo "$orcheliumUrl" | sed -E 's|.*://[^:/]+:([0-9]+).*|\1|')
SETTINGS_FILE="settings.sh"

echo "MQTT_ENABLED=\"$mqttEnabled\"" >> "$SETTINGS_FILE"
echo "MQTT_SERVER=\"$mqttServer\"" >> "$SETTINGS_FILE"
echo "MQTT_PORT=\"$mqttPort\"" >> "$SETTINGS_FILE"
echo "MQTT_USERNAME=\"$mqttUsername\"" >> "$SETTINGS_FILE"
echo "MQTT_PASSWORD=\"$mqttPassword\"" >> "$SETTINGS_FILE"
echo "ORCHELIUM_SERVER=\"$hostname\"" >> "$SETTINGS_FILE"
echo "ORCHELIUIM_PORT=\"$port\"" >> "$SETTINGS_FILE"
echo "WS_ENABLED=\"$wsEnabled\"" >> "$SETTINGS_FILE"
echo "WS_SERVER=\"$wsServer\"" >> "$SETTINGS_FILE"
echo "WS_PORT=\"$wsPort\"" >> "$SETTINGS_FILE"

# Print the results
echo "Orchelium URL          : $orcheliumUrl"
echo "Hostname               : $hostname"
echo "Port                   : $port"
echo "Installation Directory : $INSTALL_DIR"
echo "MQTT Server            : $mqttServer"
echo "MQTT Port              : $mqttPort"
echo "MQTT Username          : ${mqttUsername:-<none>}"
echo "MQTT Password          : $(if [ -n "$mqttPassword" ]; then echo set; else echo '<none>'; fi)"
echo "MQTT Enabled           : $mqttEnabled"
echo "WS Server              : $wsServer"
echo "WS Port                : $wsPort"
echo "WS Enabled             : $wsEnabled"

echo '6. Run the installer
'

bash ./install_main.sh "$INSTALL_DIR"
