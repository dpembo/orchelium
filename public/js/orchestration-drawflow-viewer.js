/**
 * Orchelium Orchestration Monitor – Drawflow view mode
 */
(function (global) {
  'use strict';

  let editor = null;
  let dfIdByOrch = {};
  let orchIdByDf = {};
  let currentNodes = [];
  let currentEdges = [];
  let selectedDfId = null;

  function init(containerId) {
    const container = document.getElementById(containerId || 'drawflow-viewer');
    if (!container || typeof Drawflow === 'undefined') {
      console.error('Drawflow viewer container / library missing');
      return null;
    }

    editor = new Drawflow(container);
    editor.reroute = true;
    editor.editor_mode = 'fixed';
    editor.zoom_min = 0.1;
    editor.zoom_max = 1.6;
    editor.start();
    if (editor.precanvas) {
      editor.precanvas.style.transformOrigin = '0 0';
    }

    editor.on('nodeSelected', function (dfId) {
      selectDfNode(dfId);
    });

    container.addEventListener('click', function (ev) {
      const nodeEl = ev.target.closest && ev.target.closest('.drawflow-node');
      if (!nodeEl) {
        clearNodeSelection();
        if (typeof global.onViewerNodeDeselected === 'function') {
          global.onViewerNodeDeselected();
        }
        return;
      }
      const dfId = String(nodeEl.id || '').replace(/^node-/, '');
      if (dfId) selectDfNode(dfId);
    });

    return editor;
  }

  function selectDfNode(dfId) {
    const orchId = orchIdByDf[String(dfId)];
    if (!orchId) return;
    selectedDfId = String(dfId);
    try {
      editor.container.querySelectorAll('.drawflow-node.selected').forEach(function (el) {
        el.classList.remove('selected');
      });
      const el = editor.container.querySelector('#node-' + dfId);
      if (el) {
        el.classList.add('selected');
        editor.node_selected = el;
      }
    } catch (e) { /* ignore */ }
    if (typeof global.onViewerNodeSelected === 'function') {
      global.onViewerNodeSelected(orchId);
    }
  }

  function clearNodeSelection() {
    selectedDfId = null;
    try {
      editor.container.querySelectorAll('.drawflow-node.selected').forEach(function (el) {
        el.classList.remove('selected');
      });
      editor.node_selected = null;
    } catch (e) { /* ignore */ }
  }

  function num(v, fallback) {
    const n = Number(v);
    return Number.isFinite(n) ? n : fallback;
  }

  function loadGraph(nodes, edges) {
    if (!editor) {
      console.error('[OrchViewer] loadGraph called before init');
      return;
    }
    currentNodes = nodes || [];
    currentEdges = edges || [];
    dfIdByOrch = {};
    orchIdByDf = {};
    selectedDfId = null;

    editor.clear();
    if (editor.precanvas) {
      editor.precanvas.style.transformOrigin = '0 0';
    }

    const list = currentNodes.slice();
    console.log('[OrchViewer] loadGraph nodes=', list.length, 'edges=', (currentEdges || []).length);

    list.forEach(function (node, i) {
      if (!node || !node.id) return;
      const type = node.type || 'execute';
      const spec = OrchDrawflow.portSpec(node);
      const x = num(node.x, 80 + i * 220);
      const y = num(node.y, 120);
      // persist coerced coords for fit
      node.x = x;
      node.y = y;
      try {
        const dfId = editor.addNode(
          type,
          spec.inputs,
          spec.outputs,
          x,
          y,
          String(type).replace(/[^a-z0-9_-]/gi, ''),
          Object.assign({}, node.data || {}, {
            _orchId: node.id,
            _label: node.label,
            _icon: node.icon,
            _type: type
          }),
          OrchDrawflow.nodeHtml(node)
        );
        dfIdByOrch[node.id] = dfId;
        orchIdByDf[String(dfId)] = node.id;
        console.log('[OrchViewer] addNode', node.id, type, 'dfId=', dfId, 'at', x, y);
      } catch (err) {
        console.error('[OrchViewer] addNode failed', node.id, err);
      }
    });

    (currentEdges || []).forEach(function (e) {
      const fromId = e.from || e.source;
      const toId = e.to || e.target;
      const fromDf = dfIdByOrch[fromId];
      const toDf = dfIdByOrch[toId];
      if (fromDf == null || toDf == null) {
        console.warn('[OrchViewer] skip edge', e);
        return;
      }
      const fromNode = list.find(function (n) { return n.id === fromId; });
      const spec = OrchDrawflow.portSpec(fromNode || { type: 'execute' });
      let outputIndex = 1;
      const port = e.fromPort || e.port || 'out';
      const idx = (spec.outputIds || []).indexOf(port);
      if (idx >= 0) outputIndex = idx + 1;
      try {
        editor.addConnection(fromDf, toDf, 'output_' + outputIndex, 'input_1');
      } catch (err) {
        console.warn('[OrchViewer] addConnection failed', err);
      }
    });

    editor.editor_mode = 'fixed';

    const domCount = editor.container.querySelectorAll('.drawflow-node').length;
    console.log('[OrchViewer] DOM nodes after load=', domCount);

    Object.keys(dfIdByOrch).forEach(function (id) {
      const dfId = dfIdByOrch[id];
      if (dfId != null && typeof editor.updateConnectionNodes === 'function') {
        try { editor.updateConnectionNodes('node-' + dfId); } catch (e) {}
      }
    });

    focusGraphInView();
  }

  function expandCanvasExtents() {
    if (!editor || !editor.precanvas) return;
    // Large fixed surface so absolute nodes are never clipped by a 100%x100% precanvas
    const pw = 4000;
    const ph = 3000;
    const pc = editor.precanvas;
    pc.style.minWidth = pw + 'px';
    pc.style.minHeight = ph + 'px';
    pc.style.width = pw + 'px';
    pc.style.height = ph + 'px';
    pc.style.overflow = 'visible';
    pc.style.transformOrigin = '0 0';
    pc.style.position = 'absolute';
    pc.style.top = '0';
    pc.style.left = '0';
  }

  function fitView() {
    if (!editor) return false;
    expandCanvasExtents();

    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    let counted = 0;
    Object.keys(dfIdByOrch).forEach(function (orchId) {
      const dfId = dfIdByOrch[orchId];
      const el = editor.container.querySelector('#node-' + dfId);
      if (!el) return;
      const x = parseFloat(el.style.left);
      const y = parseFloat(el.style.top);
      const left = Number.isFinite(x) ? x : 0;
      const top = Number.isFinite(y) ? y : 0;
      const w = el.offsetWidth || 160;
      const h = el.offsetHeight || 48;
      minX = Math.min(minX, left);
      minY = Math.min(minY, top);
      maxX = Math.max(maxX, left + w);
      maxY = Math.max(maxY, top + h);
      counted++;
    });
    if (!counted) {
      (currentNodes || []).forEach(function (n) {
        const x = num(n.x, 0), y = num(n.y, 0);
        minX = Math.min(minX, x);
        minY = Math.min(minY, y);
        maxX = Math.max(maxX, x + 200);
        maxY = Math.max(maxY, y + 80);
        counted++;
      });
    }
    if (!counted) return false;

    const container = editor.container;
    const vw = container.clientWidth || 0;
    const vh = container.clientHeight || 0;
    if (vw < 40 || vh < 40) return false;

    const graphW = Math.max(maxX - minX, 40);
    const graphH = Math.max(maxY - minY, 40);
    const pad = 48;
    let zoom = Math.min((vw - pad * 2) / graphW, (vh - pad * 2) / graphH, 1.0);
    zoom = Math.max(0.15, Math.min(zoom, 1.0));

    // Place top-left of graph at (pad, pad) in the viewport — simpler than center
    const canvasX = pad - minX * zoom;
    const canvasY = pad - minY * zoom;

    editor.zoom = zoom;
    editor.zoom_last_value = zoom;
    editor.canvas_x = canvasX;
    editor.canvas_y = canvasY;
    if (editor.precanvas) {
      editor.precanvas.style.transformOrigin = '0 0';
      editor.precanvas.style.transform =
        'translate(' + canvasX + 'px, ' + canvasY + 'px) scale(' + zoom + ')';
    }
    const zlabel = document.getElementById('zoom-level');
    if (zlabel) zlabel.textContent = Math.round(zoom * 100) + '%';

    console.log('[OrchViewer] fitView bounds=', minX.toFixed(0), minY.toFixed(0), maxX.toFixed(0), maxY.toFixed(0),
      'view=', vw, 'x', vh, 'zoom=', zoom.toFixed(3), 'canvas=', canvasX.toFixed(0), canvasY.toFixed(0), 'nodes=', counted);
    return true;
  }

  function focusGraphInView() {
    let attempts = 0;
    function tryFit() {
      attempts += 1;
      const ok = fitView();
      if (!ok && attempts < 30) {
        setTimeout(tryFit, 50);
        return;
      }
      setTimeout(fitView, 100);
      setTimeout(fitView, 300);
      setTimeout(fitView, 600);
    }
    requestAnimationFrame(function () { setTimeout(tryFit, 0); });
  }

  function updateStatuses(statusMap) {
    if (!editor || !statusMap) return;
    OrchDrawflow.applyStatusClasses(editor, statusMap, dfIdByOrch);
  }

  function statusesFromExecution(executionData, inProgressSet) {
    const map = {};
    if (!executionData || !executionData.nodes) return map;

    const visitedList = executionData.visitedNodes || [];
    const visited = new Set(visitedList);
    const outputs = executionData.nodeScriptOutputs || executionData.scriptOutputs || {};
    const errorsRaw = executionData.errors || {};
    const execMeta = executionData.execution || {};
    const stRaw = String(execMeta.status || '').toLowerCase();
    const finalRaw = String(execMeta.finalStatus || '').toLowerCase();
    const finished = (
      stRaw === 'completed' || stRaw === 'failed' || stRaw === 'error' ||
      finalRaw === 'success' || finalRaw === 'failure' || finalRaw === 'error' ||
      executionData.isInProgress === false
    );
    const isRunning = !finished && !!(
      executionData.isInProgress ||
      stRaw === 'running' || stRaw === 'in-progress'
    );

    const errorNodes = {};
    if (Array.isArray(errorsRaw)) {
      errorsRaw.forEach(function (e) {
        if (!e) return;
        const id = e.nodeId || e.id || e.node;
        if (id) errorNodes[id] = e.message || e.error || true;
      });
    } else if (errorsRaw && typeof errorsRaw === 'object') {
      Object.keys(errorsRaw).forEach(function (k) {
        errorNodes[k] = errorsRaw[k];
      });
    }

    let currentId = null;
    if (isRunning) {
      if (inProgressSet && inProgressSet.size) {
        inProgressSet.forEach(function (id) { currentId = id; });
      }
      if (!currentId && execMeta.currentNode) {
        currentId = execMeta.currentNode;
      }
      if (!currentId && executionData.currentNode) {
        currentId = executionData.currentNode;
      }
      if (!currentId && visitedList.length) {
        currentId = visitedList[visitedList.length - 1];
      }
    }

    executionData.nodes.forEach(function (n) {
      const id = n.id;
      const out = outputs[id];

      // Never keep marching ants after the run has finished
      if (isRunning && ((inProgressSet && inProgressSet.has(id)) || id === currentId)) {
        if (out && out.status && out.status !== 'in-progress' && out.status !== 'running') {
          /* finished */
        } else if (!errorNodes[id]) {
          map[id] = 'in-progress';
          return;
        }
      }
      if (isRunning && (n.status === 'in-progress' || n.inProgress ||
          (out && (out.status === 'in-progress' || out.status === 'running')))) {
        map[id] = 'in-progress';
        return;
      }
      if (errorNodes[id] || n.status === 'failed' || (out && (out.status === 'failed' || out.status === 'error'))) {
        map[id] = 'failed';
        return;
      }
      if (out && out.status !== 'in-progress' && out.status !== 'running') {
        map[id] = 'completed';
        return;
      }
      if (visited.has(id)) {
        map[id] = (isRunning && id === currentId) ? 'in-progress' : 'completed';
        return;
      }
      if (n.status === 'completed' || n.status === 'executed' || n.executed) {
        map[id] = 'completed';
        return;
      }
      map[id] = 'pending';
    });

    return map;
  }

  /** After plugin catalog loads, re-render plugin node HTML with SVG icons */
  function refreshPluginIcons() {
    if (!editor || !currentNodes.length) return;
    currentNodes.forEach(function (node) {
      if (!node || node.type !== 'plugin') return;
      const name = node.data && node.data.pluginName;
      if (!name) return;
      const meta = OrchDrawflow.getPluginMeta && OrchDrawflow.getPluginMeta(name);
      if (meta && meta.iconSvg) {
        if (!node.data) node.data = {};
        node.data.iconSvg = meta.iconSvg;
        if (meta.label && (!node.label || node.label === name || node.label === 'Plugin')) {
          node.label = meta.label;
        }
      }
      const dfId = dfIdByOrch[node.id];
      if (dfId != null) {
        OrchDrawflow.refreshNodeHtml(editor, dfId, node);
      }
    });
  }

  global.OrchViewer = {
    init: init,
    loadGraph: loadGraph,
    updateStatuses: updateStatuses,
    statusesFromExecution: statusesFromExecution,
    fitView: fitView,
    focusGraphInView: focusGraphInView,
    expandCanvasExtents: expandCanvasExtents,
    refreshPluginIcons: refreshPluginIcons,
    getEditor: function () { return editor; },
    getNodeCount: function () { return Object.keys(dfIdByOrch).length; },
    getOrchId: function (dfId) { return orchIdByDf[String(dfId)]; },
    getDfId: function (orchId) { return dfIdByOrch[orchId]; }
  };
})(typeof window !== 'undefined' ? window : global);
