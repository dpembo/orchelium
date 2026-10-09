/**
 * Classic orchestration properties panel — adapted for OrchEditor / Drawflow.
 */
(function (global) {
  'use strict';

  var selectedNode = null;
  var availableScripts = [];
  var availableAgents = [];
  var availablePlugins = [];

  function nodes() {
    return (global.OrchEditor && global.OrchEditor.getNodes()) || {};
  }

  function renderNode(nodeId) {
    if (global.OrchEditor && nodeId) global.OrchEditor.syncNodeFromProperties(nodeId);
  }

  function commitHistorySnapshot() {}
  function scheduleHistoryCommit() { if (selectedNode) renderNode(selectedNode); }


  function forceNativeSelect(el) {
    if (!el) return;
    // Destroy Materialize instance if any
    try {
      if (window.M && M.FormSelect) {
        var inst = M.FormSelect.getInstance(el);
        if (inst) inst.destroy();
      }
    } catch (e) { /* ignore */ }
    el.style.display = 'block';
    el.classList.remove('browser-default'); // optional
    // Ensure browser-default so Materialize leaves it alone on future inits
    el.classList.add('browser-default');
  }

  function forceAllPropertySelectsNative() {
    var panel = document.getElementById('orchestration-properties');
    if (!panel) return;
    panel.querySelectorAll('select').forEach(forceNativeSelect);
  }

  function getSplitJoinPorts(mode) {
    if (mode === 'join') return [{ id: 'out', label: 'next' }];
    return [{ id: 'out', label: 'paths' }];
  }

  function populatePluginPalette() {
    /* handled by Drawflow builder page */
  }


function normalizeAlias(name) {
      return (name || '')
        .toLowerCase()
        .replace(/[\s\-]+/g, '_')
        .replace(/[^a-z0-9_]/g, '')
        .replace(/__+/g, '_')
        .replace(/^_+|_+$/g, '');
    }

    function headersToMultiline(headers) {
      if (!Array.isArray(headers)) return '';
      return headers
        .filter(h => h && h.key)
        .map(h => `${h.key}: ${h.value || ''}`)
        .join('\n');
    }


    function parseHeadersMultiline(rawHeaders) {
      if (!rawHeaders || typeof rawHeaders !== 'string') return [];
      return rawHeaders
        .split('\n')
        .map(line => line.trim())
        .filter(Boolean)
        .map(line => {
          const sep = line.indexOf(':');
          if (sep === -1) return { key: line, value: '' };
          return {
            key: line.slice(0, sep).trim(),
            value: line.slice(sep + 1).trim()
          };
        })
        .filter(h => h.key);
    }


    function ensureExecuteDefaults(node) {
      if (!node || node.type !== 'execute') return;
      if (!node.data) node.data = {};
      if (!node.data.executeType) node.data.executeType = 'script';

      if (node.data.executeType === 'http') {
        if (!node.data.httpMethod) node.data.httpMethod = 'GET';
        if (!node.data.httpTimeoutMs) node.data.httpTimeoutMs = 30000;
        if (!node.data.httpAuthType) node.data.httpAuthType = 'none';
        if (!Array.isArray(node.data.httpHeaders)) node.data.httpHeaders = [];
        if (typeof node.data.httpBody !== 'string') node.data.httpBody = '';
        if (!node.icon || node.icon === 'settings') node.icon = 'http';
        if (node.data.actionName) {
          node.label = node.data.actionName;
        } else if (!node.label || node.label === 'Execute' || node.label.startsWith('Execute:')) {
          node.label = `HTTP ${node.data.httpMethod}`;
        }
      } else {
        if (!node.icon || node.icon === 'http') node.icon = 'settings';
        if (!node.data.scriptTimeoutMs) node.data.scriptTimeoutMs = 3600000;
        if (node.data.actionName) {
          node.label = node.data.actionName;
        } else if (!node.label || node.label === 'Execute' || node.label.startsWith('HTTP ')) {
          node.label = node.data.script ? `Execute: ${node.data.script.split('/').pop()}` : 'Execute Script';
        }
      }
    }


    function ensureWaitDefaults(node) {
      if (!node || node.type !== 'wait') return;
      if (!node.data) node.data = {};
      const waitSeconds = parseFloat(node.data.waitSeconds);
      node.data.waitSeconds = Number.isFinite(waitSeconds) && waitSeconds > 0 ? waitSeconds : 5;
      if (!node.icon || node.icon === 'device_hub') node.icon = 'schedule';
      if (node.data.actionName) {
        node.label = node.data.actionName;
      } else if (!node.label || node.label === 'Wait') {
        node.label = `Wait ${node.data.waitSeconds}s`;
      }
    }


    function ensureNotifyDefaults(node) {
      if (!node || node.type !== 'notify') return;
      if (!node.data) node.data = {};
      if (!node.data.notifyType) node.data.notifyType = 'INFORMATION';
      if (typeof node.data.notifyTitle !== 'string') node.data.notifyTitle = '';
      if (typeof node.data.notifyBody !== 'string') node.data.notifyBody = '';
      if (typeof node.data.notifyUrl !== 'string') node.data.notifyUrl = '';
      if (!node.icon || node.icon === 'device_hub') node.icon = 'notifications';
      if (node.data.actionName) {
        node.label = node.data.actionName;
      } else if (!node.label || node.label === 'Notify') {
        node.label = node.data.notifyTitle ? `Notify: ${node.data.notifyTitle}` : 'Notify';
      }
    }
    
    /**
     * Given a desired alias and the nodeId that will own it,
     * return the alias if unique, or append _2, _3, ... until it is.
     */
    function ensureUniqueAlias(desiredAlias, ownerNodeId) {
      if (!desiredAlias) return null;
      const existingAliases = {};
      Object.entries(nodes()).forEach(([id, n]) => {
        if (id !== ownerNodeId && n.data && n.data.alias) {
          existingAliases[n.data.alias] = true;
        }
      });
      if (!existingAliases[desiredAlias]) return desiredAlias;
      let i = 2;
      while (existingAliases[`${desiredAlias}_${i}`]) i++;
      return `${desiredAlias}_${i}`;
    }

    /**
     * Scan all node data string fields and replace references to oldAlias with newAlias
     * in #{nodes.OLD_ALIAS.} patterns (rename-refactor).
     */
    function renameAliasReferences(oldAlias, newAlias) {
      if (!oldAlias || !newAlias || oldAlias === newAlias) return;
      const pattern = new RegExp(`#\\{nodes\\.${oldAlias.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\.`, 'g');
      const replacement = `#{nodes.${newAlias}.`;
      const scanFields = ['parameters', 'httpUrl', 'httpBody', 'notifyBody', 'notifyTitle', 'notifyUrl'];
      Object.values(nodes()).forEach(n => {
        if (!n.data) return;
        scanFields.forEach(field => {
          if (typeof n.data[field] === 'string' && n.data[field].includes('#{nodes.')) {
            n.data[field] = n.data[field].replace(pattern, replacement);
          }
        });
        // Also scan httpHeaders values
        if (Array.isArray(n.data.httpHeaders)) {
          n.data.httpHeaders = n.data.httpHeaders.map(h => ({
            ...h,
            value: typeof h.value === 'string' ? h.value.replace(pattern, replacement) : h.value
          }));
        }
      });
    }

    /**
     * Normalise a node action name to a valid alias identifier.
     * Rules: lowercase, spaces/hyphens → underscore, strip non-[a-z0-9_],
     *        collapse multiple underscores, trim leading/trailing underscores.
     */
    function normalizeAlias(name) {
      return (name || '')
        .toLowerCase()
        .replace(/[\s\-]+/g, '_')
        .replace(/[^a-z0-9_]/g, '')
        .replace(/__+/g, '_')
        .replace(/^_+|_+$/g, '');
    }

    /**
     * Generate a unique alias for a new node of the given type.
     * Tries <type>_1, <type>_2, ... until unused.
     */

    function generateUniqueAlias(type) {
      const existingAliases = new Set(
        Object.values(nodes()).map(n => n.data && n.data.alias).filter(Boolean)
      );
      const base = normalizeAlias(type) || 'node';
      let i = 1;
      while (existingAliases.has(`${base}_${i}`)) i++;
      return `${base}_${i}`;
    }


    function updatePropertiesPanel() {
      const propsEmpty = document.getElementById('properties-empty');
      const propsContent = document.getElementById('properties-content');
      const propsPanel = document.getElementById('orchestration-properties');
      
      if (!selectedNode || !nodes()[selectedNode]) {
        propsEmpty.style.display = 'block';
        propsContent.style.display = 'none';
        propsPanel.classList.remove('show');
        var zcEarly = document.getElementById('zoom-controls');
        if (zcEarly) zcEarly.classList.remove('shift-left');
        return;
      }
      
      const node = nodes()[selectedNode];
      propsEmpty.style.display = 'none';
      propsContent.style.display = 'block';
      propsPanel.classList.add('show');
      document.getElementById('zoom-controls').classList.add('shift-left');
      
      const executeTypeLabel = node.type === 'execute' && (node.data?.executeType || 'script') === 'http'
        ? 'Execute (HTTP Request)'
        : (node.type === 'execute' ? 'Execute (Script)' : node.type === 'wait' ? 'Wait' : node.type === 'notify' ? 'Notification' : node.type === 'plugin' ? 'Plugin (' + (node.data?.pluginName || '') + ')' : node.type.charAt(0).toUpperCase() + node.type.slice(1));
      document.getElementById('prop-type').textContent = executeTypeLabel;
      
      document.getElementById('prop-execute').style.display = node.type === 'execute' ? 'block' : 'none';
      document.getElementById('prop-condition').style.display = node.type === 'condition' ? 'block' : 'none';
      document.getElementById('prop-split-join').style.display = node.type === 'split-join' ? 'block' : 'none';
      document.getElementById('prop-wait').style.display = node.type === 'wait' ? 'block' : 'none';
      document.getElementById('prop-notify').style.display = node.type === 'notify' ? 'block' : 'none';
      document.getElementById('prop-end').style.display = node.type.startsWith('end') ? 'block' : 'none';
      document.getElementById('prop-plugin').style.display = node.type === 'plugin' ? 'block' : 'none';
      document.getElementById('prop-start').style.display = node.type === 'start' ? 'block' : 'none';

      const isActionType = node.type === 'execute' || node.type === 'wait' || node.type === 'notify' || node.type === 'plugin';
      document.getElementById('prop-action-name-section').style.display = isActionType ? 'block' : 'none';
      
      if (node.type === 'execute') {
        ensureExecuteDefaults(node);

        const executeTypeSelect = document.getElementById('prop-execute-type');
        const scriptFieldWrap = document.getElementById('prop-execute-script-fields');
        const httpFieldWrap = document.getElementById('prop-execute-http-fields');

        const scriptSelect = document.getElementById('prop-script');
        const paramsField = document.getElementById('prop-params');
        const scriptInfo = document.getElementById('prop-script-info-detail');

        const httpMethod = document.getElementById('prop-http-method');
        const httpUrl = document.getElementById('prop-http-url');
        const httpTimeout = document.getElementById('prop-http-timeout');
        const httpHeaders = document.getElementById('prop-http-headers');
        const httpBody = document.getElementById('prop-http-body');
        const httpAuthType = document.getElementById('prop-http-auth-type');
        const httpAuthBearer = document.getElementById('prop-http-auth-bearer');
        const httpAuthUser = document.getElementById('prop-http-auth-user');
        const httpAuthPass = document.getElementById('prop-http-auth-pass');
        const httpAuthApiKeyHeader = document.getElementById('prop-http-auth-apikey-header');
        const httpAuthApiKeyValue = document.getElementById('prop-http-auth-apikey-value');

        const authBearerSection = document.getElementById('prop-http-auth-bearer-section');
        const authBasicSection = document.getElementById('prop-http-auth-basic-section');
        const authApiKeySection = document.getElementById('prop-http-auth-apikey-section');

        const updateExecuteNodeLabelIcon = () => {
          if (node.data.actionName) {
            node.label = node.data.actionName;
          } else if ((node.data.executeType || 'script') === 'http') {
            node.icon = 'http';
            const method = (node.data.httpMethod || 'GET').toUpperCase();
            const shortUrl = (node.data.httpUrl || '').trim();
            node.label = shortUrl ? `HTTP ${method}: ${shortUrl}` : `HTTP ${method}`;
          } else {
            node.icon = 'settings';
            node.label = 'Execute: ' + (node.data.script ? node.data.script.split('/').pop() : 'Script');
          }
          renderNode(selectedNode);
        };

        const updateExecuteTypeVisibility = () => {
          const executeType = executeTypeSelect.value || 'script';
          node.data.executeType = executeType;
          const isHttp = executeType === 'http';
          scriptFieldWrap.style.display = isHttp ? 'none' : 'block';
          httpFieldWrap.style.display = isHttp ? 'block' : 'none';
          if (isHttp) {
            scriptInfo.innerHTML = '';
          }
          updateExecuteNodeLabelIcon();
          document.getElementById('prop-type').textContent = isHttp ? 'Execute (HTTP Request)' : 'Execute (Script)';
        };

        const updateHttpAuthVisibility = () => {
          const authType = httpAuthType.value || 'none';
          authBearerSection.style.display = authType === 'bearer' ? 'block' : 'none';
          authBasicSection.style.display = authType === 'basic' ? 'block' : 'none';
          authApiKeySection.style.display = authType === 'apiKey' ? 'block' : 'none';
        };

        executeTypeSelect.value = node.data.executeType || 'script';

        // Wire up action name field
        const actionNameField = document.getElementById('prop-action-name');
        const aliasDisplay = document.getElementById('prop-alias-display');
        actionNameField.value = node.data.actionName || '';
        if (aliasDisplay) aliasDisplay.textContent = node.data.alias ? `#{nodes.${node.data.alias}.*}` : '';
        actionNameField.oninput = function() {
          node.data.actionName = this.value.trim();
          // Regenerate alias from name, ensure uniqueness, rename-refactor references
          if (node.data.actionName) {
            const newAlias = ensureUniqueAlias(normalizeAlias(node.data.actionName), selectedNode);
            if (newAlias && newAlias !== node.data.alias) {
              renameAliasReferences(node.data.alias, newAlias);
              node.data.alias = newAlias;
            }
          }
          if (aliasDisplay) aliasDisplay.textContent = node.data.alias ? `#{nodes.${node.data.alias}.*}` : '';
          updateExecuteNodeLabelIcon();
        };
        actionNameField.onchange = function() {
          node.data.actionName = this.value.trim();
          updateExecuteNodeLabelIcon();
        };
        
        // Build script select options
        scriptSelect.innerHTML = '<option value="">Select a script...</option>';
        availableScripts.forEach(script => {
          const option = document.createElement('option');
          // Handle both object format {id, name, filename} and string format
          const scriptId = typeof script === 'string' ? script : (script.filename || script.id);
          const scriptDisplay = typeof script === 'string' ? script : (script.name || script.filename || script.id);
          option.value = scriptId;
          option.textContent = scriptDisplay;
          if (node.data.script === scriptId) option.selected = true;
          
          // Add hover behavior to preview script info in the panel
          option.addEventListener('mouseenter', function() {
            if (scriptId) {
              fetchScriptInfo(scriptId);
            }
          });
          
          scriptSelect.appendChild(option);
        });
        
        // Set parameters value
        paramsField.value = node.data.parameters || '';

        // Populate HTTP fields
        httpMethod.value = (node.data.httpMethod || 'GET').toUpperCase();
        httpUrl.value = node.data.httpUrl || '';
        httpTimeout.value = node.data.httpTimeoutMs || 30000;
        httpHeaders.value = headersToMultiline(node.data.httpHeaders || []);
        httpBody.value = node.data.httpBody || '';
        httpAuthType.value = node.data.httpAuthType || 'none';
        httpAuthBearer.value = node.data.httpAuthBearerToken || '';
        httpAuthUser.value = node.data.httpAuthUsername || '';
        httpAuthPass.value = node.data.httpAuthPassword || '';
        httpAuthApiKeyHeader.value = node.data.httpAuthApiKeyHeader || 'X-API-Key';
        httpAuthApiKeyValue.value = node.data.httpAuthApiKeyValue || '';
        updateHttpAuthVisibility();
        
        // Show current script info on focus
        scriptSelect.addEventListener('focus', function() {
          if (node.data.script) {
            fetchScriptInfo(node.data.script);
          }
        });
        
        scriptSelect.onchange = function() {
          node.data.script = this.value;
          updateExecuteNodeLabelIcon();
          if (this.value) {
            fetchScriptInfo(this.value);
          } else {
            document.getElementById('prop-script-info-detail').innerHTML = '';
          }
        };
        
        // Fetch and display info for currently selected script
        if ((node.data.executeType || 'script') === 'script' && node.data.script) {
          fetchScriptInfo(node.data.script);
        }
        
        paramsField.onchange = function() {
          node.data.parameters = this.value;
        };
        
        paramsField.oninput = function() {
          node.data.parameters = this.value;
        };

        // Script timeout handling
        const scriptTimeoutField = document.getElementById('prop-script-timeout');
        scriptTimeoutField.value = node.data.scriptTimeoutMs || 3600000;
        
        scriptTimeoutField.onchange = function() {
          const timeoutValue = parseInt(this.value, 10);
          node.data.scriptTimeoutMs = Number.isFinite(timeoutValue) && timeoutValue > 0 ? timeoutValue : 3600000;
        };
        
        scriptTimeoutField.oninput = function() {
          const timeoutValue = parseInt(this.value, 10);
          node.data.scriptTimeoutMs = Number.isFinite(timeoutValue) && timeoutValue > 0 ? timeoutValue : 3600000;
        };

        executeTypeSelect.onchange = updateExecuteTypeVisibility;

        httpMethod.onchange = function() {
          node.data.httpMethod = this.value;
          updateExecuteNodeLabelIcon();
        };
        httpUrl.onchange = function() {
          node.data.httpUrl = this.value;
          updateExecuteNodeLabelIcon();
        };
        httpUrl.oninput = function() {
          node.data.httpUrl = this.value;
        };
        httpTimeout.onchange = function() {
          const timeoutValue = parseInt(this.value, 10);
          node.data.httpTimeoutMs = Number.isFinite(timeoutValue) && timeoutValue > 0 ? timeoutValue : 30000;
        };
        httpHeaders.onchange = function() {
          node.data.httpHeaders = parseHeadersMultiline(this.value);
        };
        httpHeaders.oninput = function() {
          node.data.httpHeaders = parseHeadersMultiline(this.value);
        };
        httpBody.onchange = function() {
          node.data.httpBody = this.value;
        };
        httpBody.oninput = function() {
          node.data.httpBody = this.value;
        };
        httpAuthType.onchange = function() {
          node.data.httpAuthType = this.value;
          updateHttpAuthVisibility();
        };
        httpAuthBearer.onchange = function() {
          node.data.httpAuthBearerToken = this.value;
        };
        httpAuthBearer.oninput = function() {
          node.data.httpAuthBearerToken = this.value;
        };
        httpAuthUser.onchange = function() {
          node.data.httpAuthUsername = this.value;
        };
        httpAuthUser.oninput = function() {
          node.data.httpAuthUsername = this.value;
        };
        httpAuthPass.onchange = function() {
          node.data.httpAuthPassword = this.value;
        };
        httpAuthPass.oninput = function() {
          node.data.httpAuthPassword = this.value;
        };
        httpAuthApiKeyHeader.onchange = function() {
          node.data.httpAuthApiKeyHeader = this.value;
        };
        httpAuthApiKeyHeader.oninput = function() {
          node.data.httpAuthApiKeyHeader = this.value;
        };
        httpAuthApiKeyValue.onchange = function() {
          node.data.httpAuthApiKeyValue = this.value;
        };
        httpAuthApiKeyValue.oninput = function() {
          node.data.httpAuthApiKeyValue = this.value;
        };
        
        // Setup agent selection
        const agentSelect = document.getElementById('prop-agent');
        agentSelect.innerHTML = '<option value="">Select a target agent...</option>';
        availableAgents.forEach(agent => {
          const option = document.createElement('option');
          option.value = agent.id;
          const statusIcon = agent.status === 'online' ? '✅' : '❌';
          option.textContent = `${statusIcon} ${agent.name}`;
          if (node.data.agent === agent.id) option.selected = true;
          agentSelect.appendChild(option);
        });
        
        agentSelect.onchange = function() {
          node.data.agent = this.value;
        };

        updateExecuteTypeVisibility();
      } else if (node.type === 'wait') {
        ensureWaitDefaults(node);

        const waitSecondsField = document.getElementById('prop-wait-seconds');
        const updateWaitLabel = () => {
          const seconds = parseFloat(node.data.waitSeconds);
          const safeSeconds = Number.isFinite(seconds) && seconds > 0 ? seconds : 5;
          node.data.waitSeconds = safeSeconds;
          node.icon = 'schedule';
          node.label = node.data.actionName || `Wait ${safeSeconds}s`;
          renderNode(selectedNode);
        };

        // Wire up action name field
        const actionNameField = document.getElementById('prop-action-name');
        const aliasDisplay = document.getElementById('prop-alias-display');
        actionNameField.value = node.data.actionName || '';
        if (aliasDisplay) aliasDisplay.textContent = node.data.alias ? `#{nodes.${node.data.alias}.*}` : '';
        actionNameField.oninput = function() {
          node.data.actionName = this.value.trim();
          if (node.data.actionName) {
            const newAlias = ensureUniqueAlias(normalizeAlias(node.data.actionName), selectedNode);
            if (newAlias && newAlias !== node.data.alias) {
              renameAliasReferences(node.data.alias, newAlias);
              node.data.alias = newAlias;
            }
          }
          if (aliasDisplay) aliasDisplay.textContent = node.data.alias ? `#{nodes.${node.data.alias}.*}` : '';
          updateWaitLabel();
        };
        actionNameField.onchange = function() {
          node.data.actionName = this.value.trim();
          updateWaitLabel();
        };

        waitSecondsField.value = node.data.waitSeconds || 5;
        waitSecondsField.onchange = function() {
          node.data.waitSeconds = this.value;
          updateWaitLabel();
        };
        waitSecondsField.oninput = function() {
          node.data.waitSeconds = this.value;
          updateWaitLabel();
        };
      } else if (node.type === 'notify') {
        ensureNotifyDefaults(node);

        const notifyTypeField = document.getElementById('prop-notify-type');
        const notifyTitleField = document.getElementById('prop-notify-title');
        const notifyBodyField = document.getElementById('prop-notify-body');
        const notifyUrlField = document.getElementById('prop-notify-url');

        const updateNotifyLabel = () => {
          node.icon = 'notifications';
          const title = (node.data.notifyTitle || '').trim();
          node.label = node.data.actionName || (title ? `Notify: ${title}` : 'Notify');
          renderNode(selectedNode);
        };

        // Wire up action name field
        const actionNameField = document.getElementById('prop-action-name');
        const aliasDisplay = document.getElementById('prop-alias-display');
        actionNameField.value = node.data.actionName || '';
        if (aliasDisplay) aliasDisplay.textContent = node.data.alias ? `#{nodes.${node.data.alias}.*}` : '';
        actionNameField.oninput = function() {
          node.data.actionName = this.value.trim();
          if (node.data.actionName) {
            const newAlias = ensureUniqueAlias(normalizeAlias(node.data.actionName), selectedNode);
            if (newAlias && newAlias !== node.data.alias) {
              renameAliasReferences(node.data.alias, newAlias);
              node.data.alias = newAlias;
            }
          }
          if (aliasDisplay) aliasDisplay.textContent = node.data.alias ? `#{nodes.${node.data.alias}.*}` : '';
          updateNotifyLabel();
        };
        actionNameField.onchange = function() {
          node.data.actionName = this.value.trim();
          updateNotifyLabel();
        };

        notifyTypeField.value = node.data.notifyType || 'INFORMATION';
        notifyTitleField.value = node.data.notifyTitle || '';
        notifyBodyField.value = node.data.notifyBody || '';
        notifyUrlField.value = node.data.notifyUrl || '';

        notifyTypeField.onchange = function() {
          node.data.notifyType = this.value;
        };
        notifyTitleField.onchange = function() {
          node.data.notifyTitle = this.value;
          updateNotifyLabel();
        };
        notifyTitleField.oninput = function() {
          node.data.notifyTitle = this.value;
          updateNotifyLabel();
        };
        notifyBodyField.onchange = function() {
          node.data.notifyBody = this.value;
        };
        notifyBodyField.oninput = function() {
          node.data.notifyBody = this.value;
        };
        notifyUrlField.onchange = function() {
          node.data.notifyUrl = this.value;
        };
        notifyUrlField.oninput = function() {
          node.data.notifyUrl = this.value;
        };
      } else if (node.type === 'condition') {
        // Populate source alias select with action nodes in workflow
        const sourceAliasSelect = document.getElementById('prop-condition-source-alias');
        if (sourceAliasSelect) {
          sourceAliasSelect.innerHTML = '<option value="">Previous node (default)</option>';
          Object.values(nodes()).forEach(n => {
            if ((n.type === 'execute' || n.type === 'wait' || n.type === 'notify' || n.type === 'plugin') && n.data && n.data.alias) {
              const opt = document.createElement('option');
              opt.value = n.data.alias;
              opt.textContent = (n.data.actionName || n.label || n.data.alias) + ' [' + n.data.alias + ']';
              if (n.data.alias === node.data.sourceNodeAlias) opt.selected = true;
              sourceAliasSelect.appendChild(opt);
            }
          });
          sourceAliasSelect.onchange = function() {
            node.data.sourceNodeAlias = this.value || undefined;
          };
        }

        const conditionPathSection = document.getElementById('prop-condition-path-section');
        const conditionPathField = document.getElementById('prop-condition-path');
        if (conditionPathField) conditionPathField.value = node.data.conditionPath || '';

        const updateConditionPathVisibility = () => {
          const isJsonValue = (document.getElementById('prop-condition-type').value) === 'json_value';
          if (conditionPathSection) conditionPathSection.style.display = isJsonValue ? 'block' : 'none';
        };

        document.getElementById('prop-condition-type').value = node.data.conditionType || 'return_code';
        document.getElementById('prop-condition-operator').value = node.data.operator || '==';
        document.getElementById('prop-condition-value').value = node.data.conditionValue || '';
        updateConditionPathVisibility();

        document.getElementById('prop-condition-type').onchange = function() {
          node.data.conditionType = this.value;
          updateConditionPathVisibility();
        };
        document.getElementById('prop-condition-operator').onchange = function() {
          node.data.operator = this.value;
        };
        document.getElementById('prop-condition-value').onchange = function() {
          node.data.conditionValue = this.value;
        };
        document.getElementById('prop-condition-value').oninput = function() {
          node.data.conditionValue = this.value;
        };
        if (conditionPathField) {
          conditionPathField.onchange = function() { node.data.conditionPath = this.value; };
          conditionPathField.oninput = function() { node.data.conditionPath = this.value; };
        }
      } else if (node.type === 'split-join') {
        if (!node.data) node.data = {};
        if (!node.data.mode) node.data.mode = 'split';
        if (!node.data.joinStrategy) node.data.joinStrategy = 'waitAll';
        if (!node.data.errorPolicy) node.data.errorPolicy = 'waitForAll';

        const modeSelect = document.getElementById('prop-sj-mode');
        const joinStrategySelect = document.getElementById('prop-sj-join-strategy');
        const errorPolicySelect = document.getElementById('prop-sj-error-policy');
        const joinStrategySection = document.getElementById('prop-sj-join-strategy-section');
        const errorPolicySection = document.getElementById('prop-sj-error-policy-section');

        modeSelect.value = node.data.mode;
        joinStrategySelect.value = node.data.joinStrategy;
        errorPolicySelect.value = node.data.errorPolicy;

        const updateSplitJoinPropertyVisibility = () => {
          const isJoin = modeSelect.value === 'join';
          // Join strategy applies to join barrier; error policy (fail-fast) applies at split
          joinStrategySection.style.display = isJoin ? 'block' : 'none';
          errorPolicySection.style.display = isJoin ? 'none' : 'block';
        };

        updateSplitJoinPropertyVisibility();

        const updateSplitJoinNode = () => {
          node.data.mode = modeSelect.value;
          node.data.joinStrategy = joinStrategySelect.value;
          node.data.errorPolicy = errorPolicySelect.value;
          updateSplitJoinPropertyVisibility();
          // Update label to reflect mode
          if (!node.data.actionName) {
            node.label = modeSelect.value === 'join' ? 'Join' : 'Split';
          }
          renderNode(selectedNode);
          if (global.OrchEditor && typeof global.OrchEditor.onSplitJoinModeChanged === 'function') {
            global.OrchEditor.onSplitJoinModeChanged(selectedNode);
          }
        };

        modeSelect.onchange = updateSplitJoinNode;
        joinStrategySelect.onchange = updateSplitJoinNode;
        errorPolicySelect.onchange = updateSplitJoinNode;
      } else if (node.type === 'end-success') {
        document.getElementById('prop-end-type-display').textContent = 'Success';
      } else if (node.type === 'end-failure') {
        document.getElementById('prop-end-type-display').textContent = 'Failure';
      } else if (node.type === 'start') {
        const terminateChk = document.getElementById('prop-start-terminate-on-error');
        terminateChk.checked = !!node.data?.terminateOnError;
        terminateChk.onchange = function() {
          if (!node.data) node.data = {};
          node.data.terminateOnError = terminateChk.checked;
        };
      } else if (node.type === 'plugin') {
        if (!node.data) node.data = {};
        if (!node.data.pluginTimeoutMs) node.data.pluginTimeoutMs = 300000;
        
        const pluginName = node.data?.pluginName || '';
        document.getElementById('prop-plugin-name-display').textContent = pluginName;

        // --- PATCH: Update label when custom name is changed ---
        const actionNameField = document.getElementById('prop-action-name');
        const aliasDisplay = document.getElementById('prop-alias-display');
        actionNameField.value = node.data.actionName || '';
        if (aliasDisplay) aliasDisplay.textContent = node.data.alias ? `#{nodes.${node.data.alias}.*}` : '';
        actionNameField.oninput = function() {
          node.data.actionName = this.value.trim();
          if (node.data.actionName) {
            const newAlias = ensureUniqueAlias(normalizeAlias(node.data.actionName), selectedNode);
            if (newAlias && newAlias !== node.data.alias) {
              renameAliasReferences(node.data.alias, newAlias);
              node.data.alias = newAlias;
            }
          }
          if (aliasDisplay) aliasDisplay.textContent = node.data.alias ? `#{nodes.${node.data.alias}.*}` : '';
          // Update label for plugin node
          node.label = node.data.actionName || pluginName || 'Plugin';
          renderNode(selectedNode);
        };
        actionNameField.onchange = function() {
          node.data.actionName = this.value.trim();
          node.label = node.data.actionName || pluginName || 'Plugin';
          renderNode(selectedNode);
        };

        // Populate agent selector
        const agentSel = document.getElementById('prop-plugin-agent');
        agentSel.innerHTML = '<option value="">-- Select Agent --</option>';
        availableAgents.forEach(function(a) {
          const opt = document.createElement('option');
          opt.value = a.id || a.name;
          opt.textContent = a.name;
          if ((node.data?.agent) === opt.value) opt.selected = true;
          agentSel.appendChild(opt);
        });
        agentSel.onchange = function() { node.data.agent = agentSel.value; };

        // Plugin timeout handling
        const pluginTimeoutField = document.getElementById('prop-plugin-timeout');
        pluginTimeoutField.value = node.data.pluginTimeoutMs || 300000;
        
        pluginTimeoutField.onchange = function() {
          const timeoutValue = parseInt(this.value, 10);
          node.data.pluginTimeoutMs = Number.isFinite(timeoutValue) && timeoutValue > 0 ? timeoutValue : 300000;
        };
        
        pluginTimeoutField.oninput = function() {
          const timeoutValue = parseInt(this.value, 10);
          node.data.pluginTimeoutMs = Number.isFinite(timeoutValue) && timeoutValue > 0 ? timeoutValue : 300000;
        };

        // Build dynamic input fields
        // Helper: check if a field should be visible based on visibleWhen
        function isFieldVisible(inputDef, nodeData) {
          if (!inputDef.visibleWhen) return true;
          // visibleWhen is { fieldName: [allowedValues] }
          // Field is visible if any of its referenced fields have one of the allowed values
          for (const [refFieldName, allowedValues] of Object.entries(inputDef.visibleWhen)) {
            const refKey = 'plugin_input_' + refFieldName;
            const currentValue = nodeData?.[refKey];
            if (allowedValues.includes(currentValue)) {
              return true;
            }
          }
          return false;
        }

        // Helper: re-evaluate visibility of all fields and call callback
        function updatePluginFieldVisibility() {
          if (!plugin) return;
          plugin.inputs.forEach(function(inputDef) {
            const section = document.getElementById('prop-plugin-section-' + inputDef.name);
            if (!section) return;
            const isVisible = isFieldVisible(inputDef, node.data);
            section.style.display = isVisible ? 'block' : 'none';
            // Clear value if hiding
            if (!isVisible) {
              const key = 'plugin_input_' + inputDef.name;
              node.data[key] = '';
            }
          });
        }

        const fieldsContainer = document.getElementById('prop-plugin-fields');
        fieldsContainer.innerHTML = '';
        const plugin = availablePlugins.find(function(p) { return p.name === pluginName; });
        
        // Ensure node.data exists
        if (!node.data) node.data = {};
        
        if (plugin && plugin.inputs) {
          // First pass: populate node.data with current field values (from storage or defaults)
          plugin.inputs.forEach(function(inputDef) {
            const key = 'plugin_input_' + inputDef.name;
            if (node.data?.[key] === undefined) {
              // Field not in node.data, populate it with default or empty
              if (inputDef.type === 'boolean') {
                node.data[key] = inputDef.default === true || inputDef.default === 'true';
              } else {
                node.data[key] = inputDef.default ?? '';
              }
            }
          });
          
          // Second pass: render fields with updated visibility
          plugin.inputs.forEach(function(inputDef) {
            const key = 'plugin_input_' + inputDef.name;
            const section = document.createElement('div');
            section.id = 'prop-plugin-section-' + inputDef.name;
            section.className = 'properties-section';
            // Set initial visibility based on current node.data values
            const isVisible = isFieldVisible(inputDef, node.data);
            section.style.display = isVisible ? 'block' : 'none';
            
            const lbl = document.createElement('label');
            lbl.textContent = inputDef.label || inputDef.name;
            if (inputDef.required) { const req = document.createElement('span'); req.textContent = ' *'; req.style.color = '#f44336'; lbl.appendChild(req); }
            section.appendChild(lbl);
            let input;
            if (inputDef.type === 'select' && inputDef.options) {
              input = document.createElement('select');
              inputDef.options.forEach(function(opt) {
                const o = document.createElement('option');
                o.value = opt; o.textContent = opt;
                if ((node.data?.[key] || inputDef.default || '') === opt) o.selected = true;
                input.appendChild(o);
              });
            } else if (inputDef.type === 'boolean') {
              input = document.createElement('input');
              input.type = 'checkbox';
              input.checked = node.data?.[key] === true || node.data?.[key] === 'true';
            } else if (inputDef.type === 'number') {
              input = document.createElement('input');
              input.type = 'number';
              input.value = node.data?.[key] ?? inputDef.default ?? '';
              if (inputDef.placeholder) input.placeholder = inputDef.placeholder;
            } else if (inputDef.type === 'json' || inputDef.type === 'list') {
              input = document.createElement('textarea');
              input.value = node.data?.[key] ?? inputDef.default ?? '';
              if (inputDef.placeholder) input.placeholder = inputDef.placeholder;
            } else {
              input = document.createElement('input');
              input.type = inputDef.type === 'secret' ? 'password' : 'text';
              input.value = node.data?.[key] ?? inputDef.default ?? '';
              if (inputDef.placeholder) input.placeholder = inputDef.placeholder;
            }
            input.id = 'prop-plugin-input-' + inputDef.name;
            if (inputDef.description) { const sm = document.createElement('small'); sm.textContent = inputDef.description; sm.style.color = '#999'; sm.style.display = 'block'; sm.style.marginTop = '3px'; sm.style.whiteSpace = 'pre-wrap'; section.appendChild(sm); }
            const onChange = function() {
              if (inputDef.type === 'boolean') { node.data[key] = input.checked; }
              else { node.data[key] = input.value; }
              // Re-evaluate visibility of all fields when this field changes
              updatePluginFieldVisibility();
            };
            input.oninput = onChange; input.onchange = onChange;
            section.insertBefore(input, section.querySelector('small'));
            fieldsContainer.appendChild(section);
          });
        }

        // Docs button
        _currentPluginDocs = (plugin && plugin.docsMd) ? plugin.docsMd : null;
        _currentPluginLabel = (plugin && plugin.label) ? plugin.label : pluginName;
        var docsSection = document.getElementById('prop-plugin-docs-section');
        docsSection.style.display = _currentPluginDocs ? 'block' : 'none';
      }
    }
    
    // ── Plugin Documentation ──────────────────────────────────────────────────
    var _currentPluginDocs = null;
    var _currentPluginLabel = '';


    function renderMarkdown(md) {
      if (!md) return '';
      var lines = md.split('\n');
      var html = '';
      var inFence = false;
      var fenceLang = '';
      var fenceLines = [];
      var inTable = false;
      var tableHeader = false;

      function flushTable() {
        if (!inTable) return;
        html += '</tbody></table>';
        inTable = false;
        tableHeader = false;
      }

      function escapeHtml(s) {
        return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
      }

      function inlineMarkdown(s) {
        // code first to prevent inner escaping
        s = s.replace(/`([^`]+)`/g, function(_, c) { return '<code>' + escapeHtml(c) + '</code>'; });
        s = s.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
        s = s.replace(/\*([^*]+)\*/g, '<em>$1</em>');
        s = s.replace(/~~([^~]+)~~/g, '<del>$1</del>');
        s = s.replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank" rel="noopener">$1</a>');
        return s;
      }

      for (var i = 0; i < lines.length; i++) {
        var line = lines[i];

        // Fenced code blocks
        if (/^```/.test(line)) {
          if (inFence) {
            html += '<pre><code' + (fenceLang ? ' class="language-' + escapeHtml(fenceLang) + '"' : '') + '>' + escapeHtml(fenceLines.join('\n')) + '</code></pre>';
            inFence = false; fenceLines = []; fenceLang = '';
          } else {
            flushTable();
            inFence = true;
            fenceLang = line.replace(/^```/, '').trim();
          }
          continue;
        }
        if (inFence) { fenceLines.push(line); continue; }

        // Horizontal rule
        if (/^---+$/.test(line.trim())) { flushTable(); html += '<hr>'; continue; }

        // Table rows
        if (/^\|/.test(line)) {
          if (/^\|[\s\-|:]+\|$/.test(line)) {
            // separator row — already handled
            continue;
          }
          var cells = line.split('|').filter(function(c, idx, arr) { return idx > 0 && idx < arr.length - 1; });
          if (!inTable) {
            html += '<table><thead><tr>';
            cells.forEach(function(c) { html += '<th>' + inlineMarkdown(c.trim()) + '</th>'; });
            html += '</tr></thead><tbody>';
            inTable = true; tableHeader = true;
          } else {
            html += '<tr>';
            cells.forEach(function(c) { html += '<td>' + inlineMarkdown(c.trim()) + '</td>'; });
            html += '</tr>';
          }
          continue;
        }
        flushTable();

        // Headings
        var hm = line.match(/^(#{1,6})\s+(.*)/);
        if (hm) { html += '<h' + hm[1].length + '>' + inlineMarkdown(hm[2]) + '</h' + hm[1].length + '>'; continue; }

        // Blockquote
        if (/^> /.test(line)) { html += '<blockquote>' + inlineMarkdown(line.slice(2)) + '</blockquote>'; continue; }

        // List item
        var lim = line.match(/^(\s*[-*+]|\s*\d+\.)\s+(.*)/);
        if (lim) { html += '<li>' + inlineMarkdown(lim[2]) + '</li>'; continue; }

        // Blank line
        if (line.trim() === '') { html += '<p style="margin:4px 0;"></p>'; continue; }

        // Paragraph
        html += '<p>' + inlineMarkdown(line) + '</p>';
      }
      flushTable();
      if (inFence) html += '<pre><code>' + escapeHtml(fenceLines.join('\n')) + '</code></pre>';
      return html;
    }


    function openPluginDocs() {
      if (!_currentPluginDocs) return;
      var modal = document.getElementById('plugin-docs-modal');
      var titleSpan = modal.querySelector('#plugin-docs-modal-title span');
      var body = document.getElementById('plugin-docs-modal-body');
      titleSpan.textContent = _currentPluginLabel + ' — Documentation';
      body.innerHTML = renderMarkdown(_currentPluginDocs);
      (M.Modal.getInstance(modal) || M.Modal.init(modal)).open();
    }


    function extractHTMLParams(data) {
      const startTag = '#start-params';
      const endTag = '#end-params';
      
      // Find the positions of the start and end tags
      const startIndex = data.indexOf(startTag);
      const endIndex = data.indexOf(endTag);
      
      // If both tags are found and the start tag comes before the end tag
      if (startIndex !== -1 && endIndex !== -1 && startIndex < endIndex) {
        // Extract the HTML content between the tags
        let htmlContent = data.substring(startIndex + startTag.length, endIndex);
        // Remove the hash symbols at the beginning of each line specifically
        htmlContent = htmlContent.replace(/^#/gm, '');
        return htmlContent;
      } else {
        // Return an empty string or some other value to indicate that the tags were not found
        return '';
      }
    }
    

    function fetchScriptInfo(scriptPath) {
      const url = '/rest/script/' + scriptPath;
      fetch(url)
        .then(response => {
          if (!response.ok) {
            throw new Error('Network response was not ok');
          }
          return response.text();
        })
        .then(data => {
          document.getElementById('prop-script-info-detail').innerHTML = extractHTMLParams(data);
        })
        .catch(error => {
          console.error('Error fetching script info:', error);
          document.getElementById('prop-script-info-detail').innerHTML = '';
        });
    }
    

    function loadScripts() {
      $.ajax({
        url: '/rest/orchestration/scripts',
        type: 'GET',
        success: function(scripts) {
          availableScripts = scripts || [];
          if (selectedNode) updatePropertiesPanel();
        }
      });
    }
    

    function loadAgents() {
      $.ajax({
        url: '/rest/orchestration/agents',
        type: 'GET',
        success: function(agents) {
          availableAgents = agents || [];
          if (selectedNode) updatePropertiesPanel();
        }
      });
    }


    function loadPlugins() {
      $.ajax({
        url: '/rest/orchestration/plugins',
        type: 'GET',
        success: function(plugins) {
          availablePlugins = plugins || [];
          populatePluginPalette();
        }
      });
    }



  function openPropertiesForNode(node) {
    if (!node) return;
    selectedNode = node.id;
    var panel = document.getElementById('orchestration-properties');
    if (panel) panel.classList.add('show');
    var zc = document.getElementById('zoom-controls');
    if (zc) zc.classList.add('shift-left');
    var empty = document.getElementById('properties-empty');
    var content = document.getElementById('properties-content');
    if (empty) empty.style.display = 'none';
    if (content) content.style.display = 'block';
    try {
      forceAllPropertySelectsNative();
      updatePropertiesPanel();
    } catch (err) {
      console.error('updatePropertiesPanel failed', err);
      if (typeof M !== 'undefined') M.toast({ html: 'Properties panel error: ' + (err.message || err) });
    }
  }

  function closePropertiesPanel() {
    selectedNode = null;
    var panel = document.getElementById('orchestration-properties');
    if (panel) panel.classList.remove('show');
    var zc = document.getElementById('zoom-controls');
    if (zc) zc.classList.remove('shift-left');
    var empty = document.getElementById('properties-empty');
    var content = document.getElementById('properties-content');
    if (empty) empty.style.display = '';
    if (content) content.style.display = 'none';
  }

  function deleteSelectedNode() {
    if (global.OrchEditor) global.OrchEditor.deleteSelected();
    closePropertiesPanel();
  }

  function bootPropertiesData() {
    loadScripts();
    loadAgents();
    loadPlugins();
  }

  global.OrchProperties = {
    open: openPropertiesForNode,
    close: closePropertiesPanel,
    update: updatePropertiesPanel,
    boot: bootPropertiesData,
    deleteSelectedNode: deleteSelectedNode
  };

  global.deleteSelectedNode = deleteSelectedNode;
  global.openPluginDocs = openPluginDocs;
  global.closeDetailsPanel = closePropertiesPanel;
  global.selectNodeForProperties = openPropertiesForNode;

})(typeof window !== 'undefined' ? window : global);
