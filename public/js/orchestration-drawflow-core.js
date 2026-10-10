/**
 * Orchelium ↔ Drawflow conversion layer
 *
 * Native Orchelium format (API / engine):
 *   nodes: [{ id, label, type, icon, x, y, data }]
 *   edges: [{ id, from, fromPort, to, label, color }]
 *
 * Drawflow export format:
 *   { drawflow: { Home: { data: { [id]: { id, name, data, class, html, inputs, outputs, pos_x, pos_y } } } } }
 *
 * Port mapping:
 *   - Most nodes: 1 input (input_1), 1 output (output_1 = "out")
 *   - condition:  1 input, 2 outputs (output_1 = "true", output_2 = "false")
 *   - start:      0 inputs, 1 output (output_1 = "out")
 *   - end-*:      1 input, 0 outputs
 *   - split-join: 1 input, 1 output (multiple connections allowed on output)
 */

(function (global) {
  'use strict';

  const PORT_MAP = {
    start:        { inputs: 0, outputs: 1, outputIds: ['out'], multiIn: false, multiOut: false },
    execute:      { inputs: 1, outputs: 1, outputIds: ['out'], multiIn: false, multiOut: false },
    wait:         { inputs: 1, outputs: 1, outputIds: ['out'], multiIn: false, multiOut: false },
    notify:       { inputs: 1, outputs: 1, outputIds: ['out'], multiIn: false, multiOut: false },
    plugin:       { inputs: 1, outputs: 1, outputIds: ['out'], multiIn: false, multiOut: false },
    condition:    { inputs: 1, outputs: 2, outputIds: ['true', 'false'], multiIn: false, multiOut: false },
    'split-join': { inputs: 1, outputs: 1, outputIds: ['out'], multiIn: false, multiOut: true }, // default split
    'end-success':{ inputs: 1, outputs: 0, outputIds: [], multiIn: false, multiOut: false },
    'end-failure':{ inputs: 1, outputs: 0, outputIds: [], multiIn: false, multiOut: false }
  };

  const ICON_MAP = {
    start: 'play_arrow',
    execute: 'settings',
    wait: 'schedule',
    notify: 'notifications',
    condition: 'call_split',
    'split-join': 'account_tree',
    'end-success': 'check_circle',
    'end-failure': 'error',
    plugin: 'extension'
  };

  /** pluginName → iconSvg (from /rest/orchestration/plugins) */
  let pluginIconByName = {};
  /** pluginName → { name, label, iconSvg, ... } */
  let pluginMetaByName = {};

  function setPluginCatalog(plugins) {
    pluginIconByName = {};
    pluginMetaByName = {};
    (plugins || []).forEach(function (p) {
      if (!p || !p.name) return;
      pluginMetaByName[p.name] = p;
      if (p.iconSvg) pluginIconByName[p.name] = p.iconSvg;
    });
  }

  function getPluginMeta(name) {
    if (!name) return null;
    return pluginMetaByName[name] || null;
  }

  function resolvePluginIconSvg(node) {
    if (!node) return null;
    if (node.data && node.data.iconSvg) return node.data.iconSvg;
    const name = node.data && node.data.pluginName;
    if (name && pluginIconByName[name]) return pluginIconByName[name];
    return null;
  }

  /**
   * Port geometry + multiplicity rules.
   * Accepts a type string or a full node object (for split-join mode).
   */
  function portSpec(typeOrNode) {
    const isObj = typeOrNode && typeof typeOrNode === 'object';
    const type = isObj ? typeOrNode.type : typeOrNode;
    const base = PORT_MAP[type] || { inputs: 1, outputs: 1, outputIds: ['out'], multiIn: false, multiOut: false };
    if (type === 'split-join') {
      const mode = (isObj && typeOrNode.data && typeOrNode.data.mode) || 'split';
      if (mode === 'join') {
        return { inputs: 1, outputs: 1, outputIds: ['out'], multiIn: true, multiOut: false, mode: 'join' };
      }
      return { inputs: 1, outputs: 1, outputIds: ['out'], multiIn: false, multiOut: true, mode: 'split' };
    }
    return Object.assign({}, base);
  }

  function iconFor(node) {
    if (node.icon && node.icon !== 'extension') return node.icon;
    if (node.type === 'execute' && node.data && node.data.executeType === 'http') return 'http';
    if (node.type === 'plugin') return 'extension';
    return ICON_MAP[node.type] || 'device_hub';
  }

  function iconHtmlFor(node) {
    // Plugin nodes: prefer custom SVG (same as classic builder)
    if (node.type === 'plugin') {
      const svg = resolvePluginIconSvg(node);
      if (svg) {
        return '<img class="orch-plugin-icon" src="data:image/svg+xml,' +
          encodeURIComponent(svg) +
          '" width="18" height="18" alt="" draggable="false">';
      }
    }
    const icon = iconFor(node);
    return '<i class="material-icons">' + escapeHtml(icon) + '</i>';
  }

  function nodeHtml(node) {
    const label = escapeHtml(node.label || node.type || 'Node');
    const typeClass = (node.type || 'unknown').replace(/[^a-z0-9-]/gi, '');
    const sjModeClass = (node.type === 'split-join')
      ? (' orch-df-sj-' + ((node.data && node.data.mode) || 'split'))
      : '';
    let sub = '';
    if (node.type === 'execute' && node.data) {
      if (node.data.executeType === 'http') {
        sub = escapeHtml((node.data.httpMethod || 'GET') + ' ' + (node.data.httpUrl || ''));
      } else if (node.data.script) {
        sub = escapeHtml(String(node.data.script).split('/').pop());
      }
    } else if (node.type === 'wait' && node.data) {
      sub = escapeHtml((node.data.waitSeconds || 5) + 's');
    } else if (node.type === 'plugin' && node.data) {
      const meta = getPluginMeta(node.data.pluginName);
      sub = escapeHtml((meta && meta.label) || node.data.pluginName || '');
    } else if (node.type === 'condition' && node.data) {
      sub = escapeHtml((node.data.testType || 'returnCode') + ' ' + (node.data.operator || '==') + ' ' + (node.data.value != null ? node.data.value : ''));
    } else if (node.type === 'split-join') {
      const mode = (node.data && node.data.mode) || 'split';
      sub = mode === 'join' ? 'Join · N inputs → 1' : 'Split · 1 → N outputs';
    }

    return (
      '<div class="orch-df-node orch-df-' + typeClass + sjModeClass + '">' +
        '<div class="orch-df-title">' +
          iconHtmlFor(node) +
          '<span class="orch-df-label">' + label + '</span>' +
        '</div>' +
        (sub ? '<div class="orch-df-sub">' + sub + '</div>' : '') +
      '</div>'
    );
  }

  function escapeHtml(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  /**
   * Convert Orchelium nodes[] + edges[] → Drawflow import object
   */
  function toDrawflow(nodes, edges) {
    const data = {};
    const idMap = {}; // orch id → numeric drawflow id
    let nextId = 1;

    const nodeList = Array.isArray(nodes) ? nodes : Object.values(nodes || {});
    nodeList.forEach(function (n) {
      const dfId = nextId++;
      idMap[n.id] = dfId;
      const spec = portSpec(n.type);
      const inputs = {};
      const outputs = {};
      for (let i = 1; i <= spec.inputs; i++) {
        inputs['input_' + i] = { connections: [] };
      }
      for (let i = 1; i <= spec.outputs; i++) {
        outputs['output_' + i] = { connections: [] };
      }

      data[String(dfId)] = {
        id: dfId,
        name: n.type,
        data: Object.assign({}, n.data || {}, {
          _orchId: n.id,
          _label: n.label,
          _icon: n.icon,
          _type: n.type
        }),
        class: n.type,
        html: nodeHtml(n),
        typenode: false,
        inputs: inputs,
        outputs: outputs,
        pos_x: typeof n.x === 'number' ? n.x : 100,
        pos_y: typeof n.y === 'number' ? n.y : 100
      };
    });

    // Wire connections
    (edges || []).forEach(function (e) {
      const fromDf = idMap[e.from];
      const toDf = idMap[e.to];
      if (fromDf == null || toDf == null) return;

      const fromNode = data[String(fromDf)];
      const toNode = data[String(toDf)];
      if (!fromNode || !toNode) return;

      const spec = portSpec(fromNode.name);
      let outputIndex = 1;
      const portId = e.fromPort || 'out';
      const idx = (spec.outputIds || []).indexOf(portId);
      if (idx >= 0) outputIndex = idx + 1;

      const outKey = 'output_' + outputIndex;
      const inKey = 'input_1';

      if (!fromNode.outputs[outKey]) fromNode.outputs[outKey] = { connections: [] };
      if (!toNode.inputs[inKey]) toNode.inputs[inKey] = { connections: [] };

      fromNode.outputs[outKey].connections.push({
        node: String(toDf),
        output: inKey
      });
      toNode.inputs[inKey].connections.push({
        node: String(fromDf),
        input: outKey
      });
    });

    return {
      drawflow: {
        Home: {
          data: data
        }
      }
    };
  }

  /**
   * Convert Drawflow export → Orchelium { nodes, edges }
   */
  function fromDrawflow(exported) {
    const home = exported && exported.drawflow && exported.drawflow.Home;
    const data = (home && home.data) || {};
    const nodes = [];
    const edges = [];
    let edgeCounter = 0;

    const dfToOrch = {};

    Object.keys(data).forEach(function (key) {
      const n = data[key];
      const orchId = (n.data && n.data._orchId) || ('node_' + n.id);
      dfToOrch[String(n.id)] = orchId;

      const type = (n.data && n.data._type) || n.name || n.class || 'execute';
      const label = (n.data && n.data._label) || n.name || type;
      const icon = (n.data && n.data._icon) || ICON_MAP[type] || 'device_hub';

      // Strip internal keys from data
      const cleanData = Object.assign({}, n.data || {});
      delete cleanData._orchId;
      delete cleanData._label;
      delete cleanData._icon;
      delete cleanData._type;

      nodes.push({
        id: orchId,
        label: label,
        type: type,
        icon: icon,
        x: n.pos_x,
        y: n.pos_y,
        data: cleanData
      });
    });

    Object.keys(data).forEach(function (key) {
      const n = data[key];
      const fromOrch = dfToOrch[String(n.id)];
      const spec = portSpec((n.data && n.data._type) || n.name);

      Object.keys(n.outputs || {}).forEach(function (outKey) {
        const outIdx = parseInt(outKey.replace('output_', ''), 10) || 1;
        const fromPort = (spec.outputIds && spec.outputIds[outIdx - 1]) || 'out';
        const conns = (n.outputs[outKey] && n.outputs[outKey].connections) || [];
        conns.forEach(function (c) {
          const toOrch = dfToOrch[String(c.node)];
          if (!toOrch) return;
          edgeCounter++;
          const color =
            fromPort === 'true' ? '#4caf50' :
            fromPort === 'false' ? '#f44336' : '#2196f3';
          edges.push({
            id: 'edge_' + edgeCounter,
            from: fromOrch,
            fromPort: fromPort,
            to: toOrch,
            label: fromPort === 'out' ? 'next' : fromPort,
            color: color
          });
        });
      });
    });

    return { nodes: nodes, edges: edges };
  }

  /**
   * Build default node payload when creating from palette
   */
  function defaultNode(type, extra) {
    extra = extra || {};
    const base = {
      label: 'Node',
      icon: ICON_MAP[type] || 'device_hub',
      data: {}
    };

    switch (type) {
      case 'start':
        base.label = 'Start';
        base.icon = 'play_arrow';
        break;
      case 'execute':
        base.label = 'Execute Script';
        base.icon = 'settings';
        base.data = { executeType: 'script', scriptTimeoutMs: 3600000 };
        break;
      case 'execute-http':
        base.label = 'HTTP GET';
        base.icon = 'http';
        base.data = {
          executeType: 'http',
          httpMethod: 'GET',
          httpUrl: '',
          httpTimeoutMs: 30000,
          httpHeaders: [],
          httpAuthType: 'none',
          httpBody: ''
        };
        // stored type is still "execute"
        break;
      case 'wait':
        base.label = 'Wait 5s';
        base.icon = 'schedule';
        base.data = { waitSeconds: 5 };
        break;
      case 'notify':
        base.label = 'Notify';
        base.icon = 'notifications';
        base.data = {
          notifyType: 'INFORMATION',
          notifyTitle: '',
          notifyBody: '',
          notifyUrl: ''
        };
        break;
      case 'condition':
        base.label = 'if';
        base.icon = 'call_split';
        base.data = { testType: 'returnCode', operator: '==', value: '0' };
        break;
      case 'split-join':
        base.label = 'Split/Join';
        base.icon = 'account_tree';
        base.data = { mode: 'split', joinStrategy: 'waitAll', errorPolicy: 'waitForAll' };
        break;
      case 'end-success':
        base.label = 'Success';
        base.icon = 'check_circle';
        break;
      case 'end-failure':
        base.label = 'Failure';
        base.icon = 'error';
        break;
      case 'plugin': {
        const pName = extra.pluginName || '';
        const meta = getPluginMeta(pName) || {};
        base.label = meta.label || pName || 'Plugin';
        base.icon = 'extension';
        base.data = {
          pluginName: pName,
          pluginTimeoutMs: 300000
        };
        if (meta.iconSvg) base.data.iconSvg = meta.iconSvg;
        break;
      }
      default:
        break;
    }

    if (extra.label) base.label = extra.label;
    if (extra.data) base.data = Object.assign(base.data, extra.data);
    return base;
  }

  function resolvedType(paletteType) {
    if (paletteType === 'execute-http') return 'execute';
    if (paletteType && paletteType.indexOf('plugin:') === 0) return 'plugin';
    return paletteType;
  }

  /**
   * Update Drawflow node HTML after property changes
   */
  function refreshNodeHtml(editor, dfId, orchNode) {
    if (!editor || dfId == null || !orchNode) return;
    const html = nodeHtml(orchNode);
    try {
      const el = editor.container.querySelector('#node-' + dfId);
      if (el) {
        const content = el.querySelector('.drawflow_content_node');
        if (content) content.innerHTML = html;
      }
      // also update stored data snapshot
      const node = editor.getNodeFromId(dfId);
      if (node) {
        node.html = html;
        node.data = Object.assign({}, node.data, {
          _label: orchNode.label,
          _icon: orchNode.icon,
          _type: orchNode.type
        }, orchNode.data || {});
      }
    } catch (err) {
      console.warn('refreshNodeHtml', err);
    }
  }

  /**
   * Apply execution status classes to Drawflow nodes (viewer)
   * statusMap: { [orchNodeId]: 'in-progress' | 'completed' | 'failed' | 'pending' }
   */
  function ensureAntsRing(el) {
    if (!el || el.querySelector('.orch-ants-ring')) return;
    const ns = 'http://www.w3.org/2000/svg';
    const svg = document.createElementNS(ns, 'svg');
    svg.setAttribute('class', 'orch-ants-ring');
    svg.setAttribute('width', '100%');
    svg.setAttribute('height', '100%');
    // Use percentage so it scales with the node
    const rect = document.createElementNS(ns, 'rect');
    rect.setAttribute('x', '1.5');
    rect.setAttribute('y', '1.5');
    rect.setAttribute('width', 'calc(100% - 3px)');
    rect.setAttribute('height', 'calc(100% - 3px)');
    // SVG doesn't love calc in attributes in all browsers — size via JS
    const w = el.offsetWidth || 160;
    const h = el.offsetHeight || 48;
    const pad = 3;
    svg.setAttribute('viewBox', '0 0 ' + (w + pad * 2) + ' ' + (h + pad * 2));
    svg.setAttribute('width', String(w + pad * 2));
    svg.setAttribute('height', String(h + pad * 2));
    rect.setAttribute('x', String(pad));
    rect.setAttribute('y', String(pad));
    rect.setAttribute('width', String(w));
    rect.setAttribute('height', String(h));
    const isRound = el.classList.contains('start') ||
      el.classList.contains('end-success') ||
      el.classList.contains('end-failure');
    const rx = isRound ? Math.min(w, h) / 2 : 8;
    rect.setAttribute('rx', String(rx));
    rect.setAttribute('ry', String(rx));
    svg.appendChild(rect);
    el.appendChild(svg);
    el.classList.add('has-ants-ring');
  }

  function removeAntsRing(el) {
    if (!el) return;
    const ring = el.querySelector('.orch-ants-ring');
    if (ring) ring.remove();
    el.classList.remove('has-ants-ring');
  }

  function applyStatusClasses(editor, statusMap, idMap) {
    // Only change the class when the status *actually* changes — re-adding
    // the same class restarts CSS animations (jerky marching ants).
    if (!editor || !statusMap) return;
    Object.keys(statusMap).forEach(function (orchId) {
      const dfId = idMap ? idMap[orchId] : orchId;
      const el = editor.container.querySelector('#node-' + dfId);
      if (!el) return;
      const st = statusMap[orchId];
      let next = 'node-pending';
      if (st === 'in-progress' || st === 'running') next = 'node-in-progress';
      else if (st === 'completed' || st === 'success' || st === 'executed') next = 'node-completed';
      else if (st === 'failed' || st === 'failure' || st === 'error') next = 'node-failed';

      if (el.classList.contains(next)) {
        // Keep existing SVG ring animating; do nothing
        if (next === 'node-in-progress') ensureAntsRing(el);
        return;
      }
      el.classList.remove('node-in-progress', 'node-completed', 'node-failed', 'node-pending');
      el.classList.add(next);
      if (next === 'node-in-progress') {
        ensureAntsRing(el);
      } else {
        removeAntsRing(el);
      }
    });
  }

  global.OrchDrawflow = {
    toDrawflow: toDrawflow,
    fromDrawflow: fromDrawflow,
    defaultNode: defaultNode,
    resolvedType: resolvedType,
    portSpec: portSpec,
    nodeHtml: nodeHtml,
    refreshNodeHtml: refreshNodeHtml,
    applyStatusClasses: applyStatusClasses,
    setPluginCatalog: setPluginCatalog,
    getPluginMeta: getPluginMeta,
    resolvePluginIconSvg: resolvePluginIconSvg,
    ICON_MAP: ICON_MAP,
    PORT_MAP: PORT_MAP
  };
})(typeof window !== 'undefined' ? window : global);
