/**
 * Orchelium Orchestration Builder – Drawflow backend
 *
 * Expects global OrchDrawflow (core) and Drawflow class.
 * Integrates with existing palette, properties panel, and REST endpoints
 * while replacing the SVG canvas with Drawflow.
 *
 * Public API exposed on window.OrchEditor
 */
(function (global) {
  'use strict';

  let editor = null;
  let nodeIdCounter = 0;
  let edgeIdCounter = 0;

  // Parallel model kept in Orchelium format for save / properties
  let nodes = {};   // id → node
  let edges = [];   // edge objects
  let dfIdByOrch = {}; // orchId → drawflow numeric id
  let orchIdByDf = {}; // drawflow id → orchId

  let selectedOrchId = null;
  let orchestrationId = null;
  let isLoading = false;
  let suppressEvents = false;

  // Defer properties panel until mouseup if the user was dragging
  let pendingSelectOrchId = null;
  let nodeDragMoved = false;
  let pointerDownOnNode = false;
  let selectPointerStart = null;
  let clipboardNode = null; // deep-cloned node data for copy/paste
  let edgePanRaf = null;
  let edgePanActive = false;
  let lastDragDfId = null;

  // Connection-rule helpers (assigned in wireEvents once editor exists)
  let removeOtherOutputConnections = function () {};
  let removeOtherInputConnections = function () {};
  let refreshPortMultiClasses = function () {};
  let refreshAllPortMultiClasses = function () {};

  const MAX_HISTORY = 80;
  let undoStack = [];
  let redoStack = [];
  /** Last known camera — updated on user pan/zoom; undo/redo always restores this */
  let viewState = { x: 0, y: 0, z: 1 };
  let autoFitOnLoad = true; // only the first load may auto-fit
  let historyTimer = null;

  // ── Init ──────────────────────────────────────────────────────────────

  function init(options) {
    options = options || {};
    const container = document.getElementById(options.containerId || 'drawflow');
    if (!container) {
      console.error('Drawflow container not found');
      return null;
    }
    if (typeof Drawflow === 'undefined') {
      console.error('Drawflow library not loaded');
      return null;
    }

    orchestrationId = options.orchestrationId || null;

    editor = new Drawflow(container);
    editor.reroute = true;
    editor.reroute_fix_fix = 8;
    editor.force_first_input = false;
    editor.draggable_inputs = false;
    editor.editor_mode = 'edit';
    editor.zoom_min = 0.1;
    editor.zoom_max = 1.6;
    editor.start();

    // Drawflow pan/zoom math requires transform-origin top-left
    if (editor.precanvas) {
      editor.precanvas.style.transformOrigin = '0 0';
    }

    // Keep viewState in sync with user pan/zoom so undo never jumps the camera
    try {
      editor.on('zoom', function () { captureViewState(); });
      editor.on('translate', function () { captureViewState(); });
    } catch (e) { /* older drawflow may lack translate event */ }
    editor.container.addEventListener('mouseup', function () {
      captureViewState();
    });
    editor.container.addEventListener('wheel', function () {
      setTimeout(captureViewState, 0);
    }, { passive: true });

    wireEvents();
    setupPaletteDrop(container);
    setupZoomButtons();

    // If layout is still settling, re-fit when the window is ready
    window.addEventListener('resize', function () {
      if (Object.keys(nodes).length) {
        clearTimeout(window.__orchFitTimer);
        window.__orchFitTimer = setTimeout(function () {
          fitViewToNodes();
        }, 100);
      }
    });

    return editor;
  }

  function wireEvents() {
    editor.on('nodeCreated', function (dfId) {
      if (suppressEvents) return;
      // Node created via addNode – already registered in addNodeFromPalette
    });

    editor.on('nodeRemoved', function (dfId) {
      if (suppressEvents) return;
      const orchId = orchIdByDf[String(dfId)];
      if (!orchId) return;
      delete nodes[orchId];
      edges = edges.filter(function (e) {
        return e.from !== orchId && e.to !== orchId;
      });
      delete dfIdByOrch[orchId];
      delete orchIdByDf[String(dfId)];
      if (selectedOrchId === orchId) {
        selectedOrchId = null;
        if (typeof global.closeDetailsPanel === 'function') global.closeDetailsPanel();
        else hideProperties();
      }
      scheduleHistory();
      updateStats();
    });

    editor.on('nodeSelected', function (dfId) {
      if (suppressEvents) return;
      const orchId = orchIdByDf[String(dfId)];
      selectedOrchId = orchId || null;
      pendingSelectOrchId = orchId || null;
      nodeDragMoved = false;
      pointerDownOnNode = true;
      selectPointerStart = null;
      try {
        const n = editor.getNodeFromId(dfId);
        selectPointerStart = { x: n.pos_x, y: n.pos_y };
      } catch (e) { /* ignore */ }
    });

    editor.on('nodeUnselected', function () {
      if (suppressEvents) return;
      selectedOrchId = null;
      pendingSelectOrchId = null;
      pointerDownOnNode = false;
      hideProperties();
    });

    editor.on('nodeMoved', function (dfId) {
      if (suppressEvents) return;
      const orchId = orchIdByDf[String(dfId)];
      if (!orchId || !nodes[orchId]) return;
      try {
        const n = editor.getNodeFromId(dfId);
        nodes[orchId].x = n.pos_x;
        nodes[orchId].y = n.pos_y;
        if (selectPointerStart) {
          const dx = Math.abs(n.pos_x - selectPointerStart.x);
          const dy = Math.abs(n.pos_y - selectPointerStart.y);
          if (dx > 6 || dy > 6) nodeDragMoved = true;
        }
        lastDragDfId = dfId;
        // Grow graph surface in all directions (including left/up via origin shift)
        expandCanvasExtents();
        normalizeGraphOrigin();
        startEdgePan();
        panToKeepNodeVisible(dfId);
      } catch (e) { /* ignore */ }
      scheduleHistory();
    });

    function openPropsForOrchId(orchId) {
      if (!orchId || !nodes[orchId]) return;
      showProperties(nodes[orchId]);
    }

    function clearDrawflowSelection() {
      // Reset Drawflow internal selection so the same node can be re-selected
      try {
        if (editor.node_selected) {
          editor.node_selected.classList.remove('selected');
        }
      } catch (e) { /* ignore */ }
      editor.node_selected = null;
      try {
        editor.container.querySelectorAll('.drawflow-node.selected').forEach(function (el) {
          el.classList.remove('selected');
        });
      } catch (e) { /* ignore */ }
    }

    function clearSelection() {
      selectedOrchId = null;
      pendingSelectOrchId = null;
      pointerDownOnNode = false;
      selectPointerStart = null;
      clearDrawflowSelection();
      hideProperties();
    }

    function isCanvasBackgroundTarget(t) {
      if (!t || !t.closest) return true;
      if (t.closest('.drawflow-node')) return false;
      if (t.closest('.connection') || t.closest('.main-path')) return false;
      if (t.closest('.output') || t.closest('.input') || t.closest('.point')) return false;
      return true;
    }

    // Single mouseup handler on the Drawflow container:
    //  - node / connector → open properties for that node
    //  - empty canvas → deselect
    editor.container.addEventListener('mouseup', function (ev) {
      if (suppressEvents) return;

      const nodeEl = ev.target && ev.target.closest && ev.target.closest('.drawflow-node');
      if (nodeEl) {
        // id is "node-N"
        const dfId = String(nodeEl.id || '').replace(/^node-/, '');
        const orchId = orchIdByDf[dfId] || selectedOrchId || pendingSelectOrchId;
        pointerDownOnNode = false;
        pendingSelectOrchId = null;
        selectPointerStart = null;
        if (orchId) {
          selectedOrchId = orchId;
          // Ensure Drawflow selection class is present
          try {
            editor.container.querySelectorAll('.drawflow-node.selected').forEach(function (el) {
              if (el !== nodeEl) el.classList.remove('selected');
            });
            nodeEl.classList.add('selected');
            editor.node_selected = nodeEl;
          } catch (e) { /* ignore */ }
          openPropsForOrchId(orchId);
        }
        return;
      }

      if (isCanvasBackgroundTarget(ev.target)) {
        // Only deselect if we weren't mid-node interaction
        if (!pointerDownOnNode) {
          clearSelection();
        } else {
          pointerDownOnNode = false;
        }
      }
    });


    // ── Edge auto-pan while dragging a node near the viewport edge ──
    function startEdgePan() {
      if (edgePanActive) return;
      edgePanActive = true;
      function tick() {
        if (!edgePanActive || lastDragDfId == null) {
          edgePanRaf = null;
          return;
        }
        try { panIfNodeNearEdge(lastDragDfId); } catch (e) { /* ignore */ }
        edgePanRaf = requestAnimationFrame(tick);
      }
      edgePanRaf = requestAnimationFrame(tick);
    }

    function stopEdgePan() {
      edgePanActive = false;
      lastDragDfId = null;
      if (edgePanRaf) {
        cancelAnimationFrame(edgePanRaf);
        edgePanRaf = null;
      }
    }

    document.addEventListener('mouseup', stopEdgePan);
    document.addEventListener('touchend', stopEdgePan);

    function panIfNodeNearEdge(dfId) {
      const orchId = orchIdByDf[String(dfId)];
      if (!orchId || !nodes[orchId]) return;
      const node = nodes[orchId];
      const container = editor.container;
      const w = container.clientWidth;
      const h = container.clientHeight;
      if (!w || !h) return;

      const zoom = editor.zoom || 1;
      const margin = 72;
      let nodeW = 180;
      let nodeH = 60;
      try {
        const el = editor.container.querySelector('#node-' + dfId);
        if (el) {
          nodeW = el.offsetWidth || nodeW;
          nodeH = el.offsetHeight || nodeH;
        }
      } catch (e) {}

      const sx = node.x * zoom + editor.canvas_x;
      const sy = node.y * zoom + editor.canvas_y;

      let dx = 0;
      let dy = 0;
      // Speed scales up the closer you are to the edge
      function edgeSpeed(dist, limit) {
        if (dist >= limit) return 0;
        return Math.max(8, Math.round(22 * (1 - dist / limit)));
      }

      if (sx + nodeW > w - margin) dx = -edgeSpeed(w - (sx + nodeW), margin);
      else if (sx < margin) dx = edgeSpeed(sx, margin);
      if (sy + nodeH > h - margin) dy = -edgeSpeed(h - (sy + nodeH), margin);
      else if (sy < margin) dy = edgeSpeed(sy, margin);

      if (!dx && !dy) return;

      editor.canvas_x += dx;
      editor.canvas_y += dy;
      if (editor.precanvas) {
        editor.precanvas.style.transform =
          'translate(' + editor.canvas_x + 'px, ' + editor.canvas_y + 'px) scale(' + zoom + ')';
      }

      const graphDx = -dx / zoom;
      const graphDy = -dy / zoom;
      node.x += graphDx;
      node.y += graphDy;
      try {
        const el = editor.container.querySelector('#node-' + dfId);
        if (el) {
          el.style.left = node.x + 'px';
          el.style.top = node.y + 'px';
        }
        const dn = editor.getNodeFromId(dfId);
        if (dn) {
          dn.pos_x = node.x;
          dn.pos_y = node.y;
        }
        if (typeof editor.updateConnectionNodes === 'function') {
          editor.updateConnectionNodes('node-' + dfId);
        }
      } catch (e) { /* ignore */ }

      // Grow canvas if the node is pushing the graph extents
      expandCanvasExtents();
      normalizeGraphOrigin();
    }

    /** If a dragged node is outside the visible viewport, pan toward it */
    function panToKeepNodeVisible(dfId) {
      const orchId = orchIdByDf[String(dfId)];
      if (!orchId || !nodes[orchId]) return;
      const node = nodes[orchId];
      const container = editor.container;
      const w = container.clientWidth;
      const h = container.clientHeight;
      if (!w || !h) return;

      const zoom = editor.zoom || 1;
      let nodeW = 180, nodeH = 60;
      try {
        const el = editor.container.querySelector('#node-' + dfId);
        if (el) {
          nodeW = el.offsetWidth || nodeW;
          nodeH = el.offsetHeight || nodeH;
        }
      } catch (e) {}

      const sx = node.x * zoom + editor.canvas_x;
      const sy = node.y * zoom + editor.canvas_y;
      const margin = 40;
      let dx = 0, dy = 0;

      if (sx + nodeW < 0) dx = -sx + margin;
      else if (sx > w) dx = w - sx - margin;
      if (sy + nodeH < 0) dy = -sy + margin;
      else if (sy > h) dy = h - sy - margin;

      if (!dx && !dy) return;

      editor.canvas_x += dx;
      editor.canvas_y += dy;
      if (editor.precanvas) {
        editor.precanvas.style.transform =
          'translate(' + editor.canvas_x + 'px, ' + editor.canvas_y + 'px) scale(' + zoom + ')';
      }
    }

    setupContextMenu();

    editor.on('connectionCreated', function (conn) {
      if (suppressEvents) return;
      // conn: { output_id, input_id, output_class, input_class }
      const fromDf = conn.output_id;
      const toDf = conn.input_id;
      const outClass = conn.output_class || 'output_1';
      const inClass = conn.input_class || 'input_1';
      const fromOrch = orchIdByDf[String(fromDf)];
      const toOrch = orchIdByDf[String(toDf)];
      if (!fromOrch || !toOrch) {
        // Unmapped — remove
        try { editor.removeSingleConnection(fromDf, toDf, outClass, inClass); } catch (e) {}
        return;
      }

      const fromNode = nodes[fromOrch];
      const toNode = nodes[toOrch];
      const fromSpec = OrchDrawflow.portSpec(fromNode || { type: 'execute' });
      const toSpec = OrchDrawflow.portSpec(toNode || { type: 'execute' });

      // Resolve logical output port id (out / true / false)
      let fromPort = 'out';
      const outIdx = parseInt(String(outClass).replace('output_', ''), 10) || 1;
      if (fromSpec.outputIds && fromSpec.outputIds[outIdx - 1]) {
        fromPort = fromSpec.outputIds[outIdx - 1];
      }

      // Enforce 1-out (unless multiOut, e.g. split)
      if (!fromSpec.multiOut) {
        removeOtherOutputConnections(fromDf, outClass, toDf, inClass);
        // Keep at most one edge from this orch port in our model
        edges = edges.filter(function (e) {
          return !(e.from === fromOrch && e.fromPort === fromPort);
        });
      }

      // Enforce 1-in (unless multiIn, e.g. join)
      if (!toSpec.multiIn) {
        removeOtherInputConnections(toDf, inClass, fromDf, outClass);
        edges = edges.filter(function (e) {
          return !(e.to === toOrch);
        });
      }

      // Avoid duplicate edge in model
      edges = edges.filter(function (e) {
        return !(e.from === fromOrch && e.to === toOrch && e.fromPort === fromPort);
      });

      edgeIdCounter++;
      edges.push({
        id: 'edge_' + edgeIdCounter,
        from: fromOrch,
        fromPort: fromPort,
        to: toOrch,
        label: fromPort === 'out' ? 'next' : fromPort,
        color: fromPort === 'true' ? '#4caf50' : (fromPort === 'false' ? '#f44336' : '#2196f3')
      });

      // Refresh multi-port visual markers
      refreshPortMultiClasses(fromDf);
      refreshPortMultiClasses(toDf);

      scheduleHistory();
      updateStats();
    });

    /**
     * Remove all connections from an output port except the one just made to (keepToDf, keepInClass).
     */
    removeOtherOutputConnections = function removeOtherOutputConnections(fromDf, outClass, keepToDf, keepInClass) {
      let dfNode;
      try { dfNode = editor.getNodeFromId(fromDf); } catch (e) { return; }
      const conns = (dfNode.outputs[outClass] && dfNode.outputs[outClass].connections) || [];
      // Copy list — we'll mutate by removing
      const toRemove = conns.filter(function (c) {
        return !(String(c.node) === String(keepToDf) && c.output === keepInClass);
      });
      suppressEvents = true;
      toRemove.forEach(function (c) {
        try {
          editor.removeSingleConnection(fromDf, c.node, outClass, c.output);
        } catch (e) { console.warn('remove out conn', e); }
      });
      suppressEvents = false;
    }

    /**
     * Remove all connections into an input port except the one from (keepFromDf, keepOutClass).
     */
    removeOtherInputConnections = function removeOtherInputConnections(toDf, inClass, keepFromDf, keepOutClass) {
      let dfNode;
      try { dfNode = editor.getNodeFromId(toDf); } catch (e) { return; }
      const conns = (dfNode.inputs[inClass] && dfNode.inputs[inClass].connections) || [];
      const toRemove = conns.filter(function (c) {
        return !(String(c.node) === String(keepFromDf) && c.input === keepOutClass);
      });
      suppressEvents = true;
      toRemove.forEach(function (c) {
        try {
          editor.removeSingleConnection(c.node, toDf, c.input, inClass);
        } catch (e) { console.warn('remove in conn', e); }
      });
      suppressEvents = false;
    }

    /** Mark input/output circles that allow multiple connections */
    refreshPortMultiClasses = function refreshPortMultiClasses(dfId) {
      const orchId = orchIdByDf[String(dfId)];
      const node = orchId ? nodes[orchId] : null;
      if (!node) return;
      const spec = OrchDrawflow.portSpec(node);
      const el = editor.container.querySelector('#node-' + dfId);
      if (!el) return;
      el.classList.toggle('orch-multi-out', !!spec.multiOut);
      el.classList.toggle('orch-multi-in', !!spec.multiIn);
      el.classList.toggle('orch-sj-split', node.type === 'split-join' && !spec.multiIn);
      el.classList.toggle('orch-sj-join', node.type === 'split-join' && !!spec.multiIn);
    }

    refreshAllPortMultiClasses = function refreshAllPortMultiClasses() {
      Object.keys(orchIdByDf).forEach(function (dfId) {
        refreshPortMultiClasses(dfId);
      });
    }

    editor.on('connectionRemoved', function (conn) {
      if (suppressEvents) return;
      const fromOrch = orchIdByDf[String(conn.output_id)];
      const toOrch = orchIdByDf[String(conn.input_id)];
      if (!fromOrch || !toOrch) return;
      edges = edges.filter(function (e) {
        return !(e.from === fromOrch && e.to === toOrch);
      });
      scheduleHistory();
      updateStats();
    });
  }

  // ── Palette drop ──────────────────────────────────────────────────────

  function setupPaletteDrop(container) {
    document.querySelectorAll('.palette-item[draggable="true"]').forEach(function (item) {
      item.addEventListener('dragstart', function (e) {
        e.dataTransfer.setData('nodeType', item.dataset.nodeType || '');
        if (item.dataset.pluginName) {
          e.dataTransfer.setData('pluginName', item.dataset.pluginName);
        }
      });
    });

    container.addEventListener('dragover', function (e) {
      e.preventDefault();
    });

    container.addEventListener('drop', function (e) {
      e.preventDefault();
      const type = e.dataTransfer.getData('nodeType');
      if (!type) return;

      // Only one start node
      if (type === 'start' && Object.values(nodes).some(function (n) { return n.type === 'start'; })) {
        if (typeof M !== 'undefined') M.toast({ html: 'Only one Start node is allowed' });
        return;
      }

      const rect = container.getBoundingClientRect();
      // Account for Drawflow zoom / translate
      const zoom = editor.zoom || 1;
      const canvasX = (e.clientX - rect.left - (editor.canvas_x || 0)) / zoom;
      const canvasY = (e.clientY - rect.top - (editor.canvas_y || 0)) / zoom;

      const pluginName = e.dataTransfer.getData('pluginName') || null;
      addNodeFromPalette(type, canvasX, canvasY, pluginName ? { pluginName: pluginName } : {});
    });
  }

  function addNodeFromPalette(paletteType, x, y, extra) {
    nodeIdCounter++;
    const orchId = 'node_' + nodeIdCounter;
    const resolved = OrchDrawflow.resolvedType(paletteType);
    const defaults = OrchDrawflow.defaultNode(
      paletteType === 'execute-http' ? 'execute-http' : resolved,
      extra || {}
    );

    const node = {
      id: orchId,
      type: resolved,
      label: defaults.label,
      icon: defaults.icon,
      x: x,
      y: y,
      data: defaults.data || {}
    };
    nodes[orchId] = node;

    const spec = OrchDrawflow.portSpec(resolved);
    suppressEvents = true;
    const dfId = editor.addNode(
      resolved,           // name
      spec.inputs,        // inputs
      spec.outputs,       // outputs
      x,
      y,
      resolved,           // class
      Object.assign({}, node.data, {
        _orchId: orchId,
        _label: node.label,
        _icon: node.icon,
        _type: node.type
      }),
      OrchDrawflow.nodeHtml(node)
    );
    suppressEvents = false;

    dfIdByOrch[orchId] = dfId;
    orchIdByDf[String(dfId)] = orchId;
    refreshPortMultiClasses(dfId);
    expandCanvasExtents();

    // Disable start in palette if added
    if (resolved === 'start') {
      const startItem = document.querySelector('[data-node-type="start"]');
      if (startItem) {
        startItem.setAttribute('draggable', 'false');
        startItem.style.opacity = '0.4';
      }
    }

    scheduleHistory();
    updateStats();
    return orchId;
  }

  // ── Load / Save ───────────────────────────────────────────────────────

  function loadOrchestration(orch) {
    if (!orch) return;
    isLoading = true;
    suppressEvents = true;

    nodes = {};
    edges = [];
    dfIdByOrch = {};
    orchIdByDf = {};
    nodeIdCounter = 0;
    edgeIdCounter = 0;

    (orch.nodes || []).forEach(function (n) {
      const num = parseInt(String(n.id).replace(/\D/g, ''), 10) || 0;
      if (num > nodeIdCounter) nodeIdCounter = num;
      // Coerce coords (API may send strings); classic SVG used center-ish coords
      const nx = Number(n.x);
      const ny = Number(n.y);
      nodes[n.id] = {
        id: n.id,
        label: n.label || n.type || 'Node',
        type: n.type,
        icon: n.icon,
        x: Number.isFinite(nx) ? nx : 100,
        y: Number.isFinite(ny) ? ny : 100,
        data: n.data || {}
      };
    });
    (orch.edges || []).forEach(function (e) {
      const num = parseInt(String(e.id).replace(/\D/g, ''), 10) || 0;
      if (num > edgeIdCounter) edgeIdCounter = num;
      edges.push(e);
    });


    // If multiple nodes share nearly the same position, spread them so they are visible
    (function spreadOverlapping() {
      const list = Object.values(nodes);
      const seen = {};
      list.forEach(function (n, i) {
        const key = Math.round(n.x / 20) + ',' + Math.round(n.y / 20);
        if (seen[key] == null) {
          seen[key] = 0;
        } else {
          seen[key] += 1;
          n.x += seen[key] * 40;
          n.y += seen[key] * 40;
        }
      });
    })();

    // Prefer imperative addNode/addConnection over import() — more reliable with custom HTML nodes
    editor.clear();

    Object.keys(nodes).forEach(function (orchId) {
      const node = nodes[orchId];
      const spec = OrchDrawflow.portSpec(node.type);
      const x = node.x;
      const y = node.y;
      let dfId;
      try {
        const html = OrchDrawflow.nodeHtml(node);
        dfId = editor.addNode(
          node.type || 'execute',
          spec.inputs,
          spec.outputs,
          x,
          y,
          (node.type || 'execute').replace(/[^a-z0-9_-]/gi, ''),
          Object.assign({}, node.data, {
            _orchId: orchId,
            _label: node.label,
            _icon: node.icon,
            _type: node.type
          }),
          html
        );
        console.log('[OrchEditor] addNode', orchId, 'type=', node.type, 'dfId=', dfId, 'at', x, y);
      } catch (err) {
        console.error('[OrchEditor] Failed to add node', orchId, node, err);
        return;
      }
      if (dfId == null) {
        console.error('[OrchEditor] addNode returned null/undefined for', orchId);
        return;
      }
      dfIdByOrch[orchId] = dfId;
      orchIdByDf[String(dfId)] = orchId;
    });

    edges.forEach(function (e) {
      const fromDf = dfIdByOrch[e.from];
      const toDf = dfIdByOrch[e.to];
      if (fromDf == null || toDf == null) {
        console.warn('Skipping edge, missing node map', e);
        return;
      }
      const fromNode = nodes[e.from];
      const spec = OrchDrawflow.portSpec(fromNode ? fromNode.type : 'execute');
      let outputIndex = 1;
      const portId = e.fromPort || 'out';
      const idx = (spec.outputIds || []).indexOf(portId);
      if (idx >= 0) outputIndex = idx + 1;
      const outClass = 'output_' + outputIndex;
      const inClass = 'input_1';
      try {
        editor.addConnection(fromDf, toDf, outClass, inClass);
      } catch (err) {
        console.error('Failed to add connection', e, err);
      }
    });

    // Verify DOM nodes match model
    const domCount = editor.container.querySelectorAll('.drawflow-node').length;
    const modelCount = Object.keys(nodes).length;
    console.log('[OrchEditor] loaded model nodes=', modelCount,
      'dom nodes=', domCount,
      'ids=', Object.keys(nodes),
      'dfMap=', JSON.parse(JSON.stringify(dfIdByOrch)));
    if (domCount < modelCount) {
      console.warn('[OrchEditor] some nodes failed to render in Drawflow');
    }

    suppressEvents = false;
    isLoading = false;
    resetHistory();
    updateStats();
    // Re-draw connections after load
    Object.keys(dfIdByOrch).forEach(function (id) {
      const dfId = dfIdByOrch[id];
      if (dfId != null && typeof editor.updateConnectionNodes === 'function') {
        try { editor.updateConnectionNodes('node-' + dfId); } catch (e) {}
      }
    });
    refreshAllPortMultiClasses();
    // Auto-fit only once per editor session so undo never jumps back to a fit view
    if (autoFitOnLoad) {
      autoFitOnLoad = false;
      focusGraphInView();
      setTimeout(function () { captureViewState(); }, 350);
      setTimeout(function () { captureViewState(); }, 700);
    } else {
      applyViewState(viewState);
      requestAnimationFrame(function () { applyViewState(viewState); });
    }

    // Fix start palette state
    const hasStart = Object.values(nodes).some(function (n) { return n.type === 'start'; });
    const startItem = document.querySelector('[data-node-type="start"]');
    if (startItem) {
      startItem.setAttribute('draggable', hasStart ? 'false' : 'true');
      startItem.style.opacity = hasStart ? '0.4' : '1';
    }
  }

  /** Pan/zoom so all nodes are visible in the current viewport */
  /**
   * Grow the Drawflow precanvas so all nodes (+ padding) fit in graph space.
   * Supports content extending in any direction (including negative coords).
   */

  /**
   * Reliably center/fit the graph after load or layout.
   * Retries until the container has real dimensions (flex layout can be 0 at first paint).
   */
  function focusGraphInView() {
    let attempts = 0;
    function tryFit() {
      attempts += 1;
      const container = editor && editor.container;
      const w = container ? container.clientWidth : 0;
      const h = container ? container.clientHeight : 0;
      if ((!w || !h || w < 40 || h < 40) && attempts < 30) {
        setTimeout(tryFit, 50);
        return;
      }
      expandCanvasExtents();
      const ok = fitViewToNodes();
      if (!ok && attempts < 30) {
        setTimeout(tryFit, 50);
        return;
      }
      // Second / third pass after fonts & node sizes settle
      setTimeout(function () {
        expandCanvasExtents();
        fitViewToNodes();
      }, 100);
      setTimeout(function () {
        fitViewToNodes();
      }, 300);
      setTimeout(function () {
        fitViewToNodes();
      }, 600);
    }
    // Kick off after current frame
    requestAnimationFrame(function () { setTimeout(tryFit, 0); });
  }

  function expandCanvasExtents() {
    if (!editor || !editor.precanvas) return;
    const list = Object.values(nodes);
    const PAD = 400; // free space on each side for future drops / pan room

    let minX = 0, minY = 0, maxX = 1200, maxY = 800;
    if (list.length) {
      minX = Infinity;
      minY = Infinity;
      maxX = -Infinity;
      maxY = -Infinity;
      list.forEach(function (n) {
        const x = typeof n.x === 'number' ? n.x : 0;
        const y = typeof n.y === 'number' ? n.y : 0;
        let w = 200;
        let h = 70;
        const dfId = dfIdByOrch[n.id];
        if (dfId != null) {
          const el = editor.container.querySelector('#node-' + dfId);
          if (el) {
            w = el.offsetWidth || w;
            h = el.offsetHeight || h;
          }
        }
        minX = Math.min(minX, x);
        minY = Math.min(minY, y);
        maxX = Math.max(maxX, x + w);
        maxY = Math.max(maxY, y + h);
      });
    }

    const width = Math.max(maxX - minX + PAD * 2, 2000);
    const height = Math.max(maxY - minY + PAD * 2, 1200);
    editor.precanvas.style.minWidth = width + 'px';
    editor.precanvas.style.minHeight = height + 'px';
    editor.precanvas.style.width = width + 'px';
    editor.precanvas.style.height = height + 'px';
    editor.precanvas.style.overflow = 'visible';
    editor.precanvas.style.transformOrigin = '0 0';
    const svg = editor.precanvas.querySelector('svg');
    if (svg) {
      svg.style.overflow = 'visible';
      svg.setAttribute('overflow', 'visible');
    }
  }


  /**
   * Keep a padding margin on the left/top of the graph.
   * When nodes are dragged past that margin, shift the whole graph
   * so there is always room to expand left/up (Drawflow clips at 0,0 otherwise).
   */
  function normalizeGraphOrigin() {
    // Intentionally does NOT shift node coordinates.
    // Shifting all nodes + compensating camera made undo restore old positions
    // against a compensated camera and the diagram vanished off-screen.
    // We only grow the precanvas so content can extend in any direction.
    if (!editor || suppressEvents || isLoading) return;
    expandCanvasExtents();
  }


  function getGraphBounds() {
    const list = Object.values(nodes);
    if (!list.length) {
      return { minX: 0, minY: 0, maxX: 400, maxY: 300 };
    }
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    list.forEach(function (n) {
      const x = typeof n.x === 'number' ? n.x : 0;
      const y = typeof n.y === 'number' ? n.y : 0;
      let w = 200;
      let h = 70;
      const dfId = dfIdByOrch[n.id];
      if (dfId != null && editor) {
        const el = editor.container.querySelector('#node-' + dfId);
        if (el) {
          w = Math.max(el.offsetWidth || 0, 120);
          h = Math.max(el.offsetHeight || 0, 40);
        }
      }
      minX = Math.min(minX, x);
      minY = Math.min(minY, y);
      maxX = Math.max(maxX, x + w);
      maxY = Math.max(maxY, y + h);
    });
    return { minX: minX, minY: minY, maxX: maxX, maxY: maxY };
  }

  function fitViewToNodes() {
    if (!editor) return;
    expandCanvasExtents();

    // Prefer live DOM positions (authoritative after render)
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    let counted = 0;
    Object.keys(dfIdByOrch).forEach(function (orchId) {
      const dfId = dfIdByOrch[orchId];
      const el = editor.container.querySelector('#node-' + dfId);
      if (!el) return;
      const x = parseFloat(el.style.left) || 0;
      const y = parseFloat(el.style.top) || 0;
      const w = el.offsetWidth || 160;
      const h = el.offsetHeight || 48;
      minX = Math.min(minX, x);
      minY = Math.min(minY, y);
      maxX = Math.max(maxX, x + w);
      maxY = Math.max(maxY, y + h);
      counted++;
    });

    if (!counted) {
      const bounds = getGraphBounds();
      minX = bounds.minX;
      minY = bounds.minY;
      maxX = bounds.maxX;
      maxY = bounds.maxY;
    }

    // Prefer the canvas host size (stable after header resize)
    const container = editor.container;
    const host = document.getElementById('orchestration-canvas') || container;
    const vw = (host && host.clientWidth) || container.clientWidth || 0;
    const vh = (host && host.clientHeight) || container.clientHeight || 0;
    if (vw < 40 || vh < 40) {
      // Container not laid out yet — caller should retry
      return false;
    }

    const graphW = Math.max(maxX - minX, 40);
    const graphH = Math.max(maxY - minY, 40);
    const padX = 48;
    const padY = 48;

    let zoom = Math.min((vw - padX * 2) / graphW, (vh - padY * 2) / graphH, 1.0);
    zoom = Math.max(0.1, Math.min(zoom, 1.0));

    // True center of graph bounds in the viewport
    const cx = (minX + maxX) / 2;
    const cy = (minY + maxY) / 2;
    const canvasX = (vw / 2) - (cx * zoom);
    const canvasY = (vh / 2) - (cy * zoom);

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

    console.log('[OrchEditor] fitView', {
      bounds: [minX, minY, maxX, maxY],
      view: [vw, vh],
      zoom: zoom,
      canvas: [canvasX, canvasY],
      nodes: counted
    });
    return true;
  }

  function getOrchestrationPayload(meta) {
    meta = meta || {};
    return {
      jobId: meta.jobId || orchestrationId || ('orch_' + Date.now()),
      name: meta.name || '',
      description: meta.description || '',
      icon: meta.icon || 'account_tree',
      color: meta.color || '#ff9800',
      nodes: Object.values(nodes).map(function (n) {
        return {
          id: n.id,
          label: n.label,
          type: n.type,
          icon: n.icon,
          x: n.x,
          y: n.y,
          data: n.data
        };
      }),
      edges: edges.map(function (e) {
        return {
          id: e.id,
          from: e.from,
          fromPort: e.fromPort,
          to: e.to,
          label: e.label,
          color: e.color
        };
      })
    };
  }

  function validate() {
    const errors = [];
    const list = Object.values(nodes);
    const labelOf = function (n) {
      return (n && (n.label || n.data && n.data.alias || n.id)) || 'node';
    };

    if (!list.some(function (n) { return n.type === 'start'; })) {
      errors.push('Orchestration must have a Start node.');
    }
    if (!list.some(function (n) {
      return n.type === 'end-success' || n.type === 'end-failure';
    })) {
      errors.push('Orchestration must have at least one End node (Success or Failure).');
    }

    list.forEach(function (node) {
      const label = labelOf(node);
      const data = node.data || {};

      if (node.type === 'execute') {
        const executeType = data.executeType || 'script';
        if (executeType === 'http') {
          if (!data.httpUrl || !String(data.httpUrl).trim()) {
            errors.push('HTTP node "' + label + '" is missing a URL.');
          }
          const authType = data.httpAuthType || 'none';
          if (authType === 'bearer' && !String(data.httpAuthBearerToken || '').trim()) {
            errors.push('HTTP node "' + label + '" is using Bearer auth but has no token.');
          }
          if (authType === 'basic') {
            if (!String(data.httpAuthUsername || '').trim() || !String(data.httpAuthPassword || '').trim()) {
              errors.push('HTTP node "' + label + '" is using Basic auth but username/password are incomplete.');
            }
          }
          if (authType === 'apiKey') {
            if (!String(data.httpAuthApiKeyHeader || '').trim() || !String(data.httpAuthApiKeyValue || '').trim()) {
              errors.push('HTTP node "' + label + '" is using API key auth but header/value are incomplete.');
            }
          }
        } else {
          if (!data.script || !String(data.script).trim()) {
            errors.push('Execute node "' + label + '" is missing a Script selection.');
          }
          if (!data.agent || !String(data.agent).trim()) {
            errors.push('Execute node "' + label + '" is missing a Target Agent selection.');
          }
        }
      } else if (node.type === 'wait') {
        const waitSeconds = parseFloat(data.waitSeconds);
        if (!Number.isFinite(waitSeconds) || waitSeconds <= 0) {
          errors.push('Wait node "' + label + '" must have a delay greater than 0 seconds.');
        }
      } else if (node.type === 'notify') {
        if (!String(data.notifyTitle || '').trim()) {
          errors.push('Notification node "' + label + '" is missing a title.');
        }
        if (!String(data.notifyBody || data.notifyMessage || '').trim()) {
          errors.push('Notification node "' + label + '" is missing a message.');
        }
      } else if (node.type === 'condition') {
        const trueEdges = edges.filter(function (e) { return e.from === node.id && e.fromPort === 'true'; });
        const falseEdges = edges.filter(function (e) { return e.from === node.id && e.fromPort === 'false'; });
        if (trueEdges.length === 0) {
          errors.push('Condition node "' + label + '" is missing a True path.');
        }
        if (falseEdges.length === 0) {
          errors.push('Condition node "' + label + '" is missing a False path.');
        }
      } else if (node.type === 'plugin') {
        if (!data.agent || !String(data.agent).trim()) {
          errors.push('Plugin node "' + label + '" is missing a Target Agent selection.');
        }
        if (!data.pluginName || !String(data.pluginName).trim()) {
          errors.push('Plugin node "' + label + '" is missing a plugin selection.');
        } else if (typeof global.availablePlugins !== 'undefined' && Array.isArray(global.availablePlugins)) {
          const plugin = global.availablePlugins.find(function (p) {
            return p.name === data.pluginName || p.id === data.pluginName;
          });
          if (plugin && plugin.inputs) {
            plugin.inputs.forEach(function (inputDef) {
              let isVisible = true;
              if (inputDef.visibleWhen) {
                isVisible = false;
                Object.keys(inputDef.visibleWhen).forEach(function (refFieldName) {
                  const allowed = inputDef.visibleWhen[refFieldName] || [];
                  const currentValue = data['plugin_input_' + refFieldName];
                  if (allowed.indexOf(currentValue) >= 0) isVisible = true;
                });
              }
              if (isVisible && inputDef.required) {
                const value = data['plugin_input_' + inputDef.name];
                if (!value || String(value).trim() === '') {
                  errors.push(
                    'Plugin node "' + label + '" is missing required field "' +
                    (inputDef.label || inputDef.name) + '".'
                  );
                }
              }
            });
          }
        }
      } else if (node.type === 'split-join') {
        const mode = (data.mode || 'split').toLowerCase();
        const outgoing = edges.filter(function (e) { return e.from === node.id; });
        const incoming = edges.filter(function (e) { return e.to === node.id; });
        if (mode === 'split' && outgoing.length < 2) {
          errors.push('Split/Join node "' + label + '" in split mode must have at least 2 outgoing paths.');
        }
        if (mode === 'join') {
          if (incoming.length < 2) {
            errors.push('Split/Join node "' + label + '" in join mode must have at least 2 incoming paths.');
          }
          if (outgoing.length === 0) {
            errors.push('Split/Join node "' + label + '" in join mode must have an outgoing path.');
          }
        }
      }

      // Dangling exits (action nodes need an exit)
      if (node.type !== 'start' && node.type !== 'end-success' && node.type !== 'end-failure' &&
          node.type !== 'condition') {
        const outgoing = edges.filter(function (e) { return e.from === node.id; });
        if (outgoing.length === 0) {
          errors.push('Node "' + label + '" has no exit path. It must connect to another node or an end node.');
        }
      }
      if (node.type === 'end-success' || node.type === 'end-failure') {
        const hasOut = edges.some(function (e) { return e.from === node.id; });
        if (hasOut) {
          errors.push('End node "' + label + '" should not have outgoing connections.');
        }
      }
    });

    return errors;
  }

  // ── Properties bridge ─────────────────────────────────────────────────

  function showProperties(node) {
    // Prefer existing global handlers from the original page if present
    if (typeof global.selectNodeForProperties === 'function') {
      global.selectNodeForProperties(node);
      return;
    }
    const panel = document.getElementById('orchestration-properties');
    if (panel) panel.classList.add('show');
    // Minimal fallback: fill common fields
    const typeEl = document.getElementById('prop-node-type');
    if (typeEl) typeEl.textContent = node.type;
    const labelEl = document.getElementById('prop-node-label');
    if (labelEl) labelEl.value = node.label || '';
  }

  function hideProperties() {
    if (typeof global.closeDetailsPanel === 'function') {
      global.closeDetailsPanel();
      return;
    }
    const panel = document.getElementById('orchestration-properties');
    if (panel) panel.classList.remove('show');
  }

  /**
   * Call after the properties panel mutates a node so Drawflow HTML stays in sync.
   */
  function syncNodeFromProperties(orchId) {
    const node = nodes[orchId];
    if (!node) return;
    const dfId = dfIdByOrch[orchId];
    if (dfId == null) return;
    OrchDrawflow.refreshNodeHtml(editor, dfId, node);
    // keep position
    try {
      const dn = editor.getNodeFromId(dfId);
      node.x = dn.pos_x;
      node.y = dn.pos_y;
    } catch (e) { /* ignore */ }
    scheduleHistory();
  }

  function deleteSelected() {
    if (!selectedOrchId) return;
    const dfId = dfIdByOrch[selectedOrchId];
    if (dfId != null) {
      editor.removeNodeId('node-' + dfId);
    }
  }

  // ── History ───────────────────────────────────────────────────────────

  function snapshot() {
    return JSON.stringify({
      nodes: nodes,
      edges: edges,
      nodeIdCounter: nodeIdCounter,
      edgeIdCounter: edgeIdCounter
    });
  }


  function captureViewState() {
    if (!editor) return viewState;
    const z = editor.zoom || 1;
    const x = typeof editor.canvas_x === 'number' ? editor.canvas_x : 0;
    const y = typeof editor.canvas_y === 'number' ? editor.canvas_y : 0;
    // Prefer the live CSS transform — more reliable than Drawflow internals after edits
    let transform = null;
    if (editor.precanvas && editor.precanvas.style && editor.precanvas.style.transform) {
      transform = editor.precanvas.style.transform;
    } else {
      transform = 'translate(' + x + 'px, ' + y + 'px) scale(' + z + ')';
    }
    viewState = { x: x, y: y, z: z, transform: transform };
    return viewState;
  }

  function applyViewState(vs) {
    if (!editor || !vs) return;
    const z = (typeof vs.z === 'number' && vs.z > 0) ? vs.z : 1;
    const x = typeof vs.x === 'number' ? vs.x : 0;
    const y = typeof vs.y === 'number' ? vs.y : 0;
    editor.canvas_x = x;
    editor.canvas_y = y;
    editor.zoom = z;
    editor.zoom_last_value = z;
    if (editor.precanvas) {
      editor.precanvas.style.transformOrigin = '0 0';
      editor.precanvas.style.transform = vs.transform ||
        ('translate(' + x + 'px, ' + y + 'px) scale(' + z + ')');
    }
    const zlabel = document.getElementById('zoom-level');
    if (zlabel) zlabel.textContent = Math.round(z * 100) + '%';
  }

  /** Re-assert camera for a few frames so nothing can steal the viewport */
  function lockViewState(vs, ms) {
    if (!vs) return;
    applyViewState(vs);
    const until = Date.now() + (ms || 200);
    function tick() {
      applyViewState(vs);
      if (Date.now() < until) requestAnimationFrame(tick);
    }
    requestAnimationFrame(tick);
  }

  /**
   * Apply a history snapshot WITHOUT editor.clear().
   * Clearing the canvas resets Drawflow's camera and is what made undo
   * jump/vanish the diagram. Instead we diff nodes & edges in place.
   */
  function applySnapshot(json) {
    const state = JSON.parse(json);
    // Freeze exact viewport BEFORE any DOM mutation
    const savedCam = captureViewState();
    const frozenTransform = savedCam && savedCam.transform;
    isLoading = true;
    suppressEvents = true;

    const nextNodes = state.nodes || {};
    const nextEdges = state.edges || [];
    nodeIdCounter = state.nodeIdCounter || 0;
    edgeIdCounter = state.edgeIdCounter || 0;

    // ── 1. Remove nodes that no longer exist ──────────────────────────
    Object.keys(nodes).forEach(function (orchId) {
      if (nextNodes[orchId]) return;
      const dfId = dfIdByOrch[orchId];
      if (dfId != null) {
        try { editor.removeNodeId('node-' + dfId); } catch (e) {}
        delete orchIdByDf[String(dfId)];
        delete dfIdByOrch[orchId];
      }
    });

    // ── 2. Add or update nodes ────────────────────────────────────────
    Object.keys(nextNodes).forEach(function (orchId) {
      const node = nextNodes[orchId];
      const x = typeof node.x === 'number' ? node.x : 100;
      const y = typeof node.y === 'number' ? node.y : 100;
      node.x = x;
      node.y = y;

      if (dfIdByOrch[orchId] != null) {
        // Existing — move + refresh content
        const dfId = dfIdByOrch[orchId];
        try {
          const el = editor.container.querySelector('#node-' + dfId);
          if (el) {
            el.style.left = x + 'px';
            el.style.top = y + 'px';
          }
          const dn = editor.getNodeFromId(dfId);
          if (dn) {
            dn.pos_x = x;
            dn.pos_y = y;
            dn.data = Object.assign({}, node.data || {}, {
              _orchId: orchId,
              _label: node.label,
              _icon: node.icon,
              _type: node.type
            });
          }
          OrchDrawflow.refreshNodeHtml(editor, dfId, node);
          if (typeof editor.updateConnectionNodes === 'function') {
            editor.updateConnectionNodes('node-' + dfId);
          }
        } catch (e) {
          console.warn('applySnapshot update', orchId, e);
        }
      } else {
        // New node
        const spec = OrchDrawflow.portSpec(node);
        try {
          const dfId = editor.addNode(
            node.type || 'execute',
            spec.inputs,
            spec.outputs,
            x,
            y,
            (node.type || 'execute').replace(/[^a-z0-9_-]/gi, ''),
            Object.assign({}, node.data || {}, {
              _orchId: orchId,
              _label: node.label,
              _icon: node.icon,
              _type: node.type
            }),
            OrchDrawflow.nodeHtml(node)
          );
          dfIdByOrch[orchId] = dfId;
          orchIdByDf[String(dfId)] = orchId;
        } catch (err) {
          console.error('applySnapshot addNode', orchId, err);
        }
      }
    });

    nodes = nextNodes;

    // ── 3. Rebuild connections ────────────────────────────────────────
    // Strip every connection, then re-add from the snapshot edge list.
    Object.keys(dfIdByOrch).forEach(function (orchId) {
      const dfId = dfIdByOrch[orchId];
      if (dfId == null) return;
      try {
        if (typeof editor.removeConnectionNodeId === 'function') {
          editor.removeConnectionNodeId('node-' + dfId);
        }
      } catch (e) {}
    });

    edges = nextEdges;
    edges.forEach(function (e) {
      const fromDf = dfIdByOrch[e.from];
      const toDf = dfIdByOrch[e.to];
      if (fromDf == null || toDf == null) return;
      const fromNode = nodes[e.from];
      const spec = OrchDrawflow.portSpec(fromNode || { type: 'execute' });
      let outputIndex = 1;
      const idx = (spec.outputIds || []).indexOf(e.fromPort || 'out');
      if (idx >= 0) outputIndex = idx + 1;
      try {
        editor.addConnection(fromDf, toDf, 'output_' + outputIndex, 'input_1');
      } catch (err) {}
    });

    suppressEvents = false;
    isLoading = false;
    updateStats();
    refreshAllPortMultiClasses();

    // Hard-lock the viewport for a few frames. Never call fitView here.
    if (frozenTransform) savedCam.transform = frozenTransform;
    lockViewState(savedCam, 300);
  }

  function scheduleHistory() {
    if (isLoading) return;
    clearTimeout(historyTimer);
    historyTimer = setTimeout(function () {
      undoStack.push(snapshot());
      if (undoStack.length > MAX_HISTORY) undoStack.shift();
      redoStack = [];
      updateUndoButtons();
    }, 200);
  }

  function resetHistory() {
    undoStack = [snapshot()];
    redoStack = [];
    updateUndoButtons();
  }

  function undo() {
    if (undoStack.length < 2) return;
    redoStack.push(undoStack.pop());
    applySnapshot(undoStack[undoStack.length - 1]);
    updateUndoButtons();
  }

  function redo() {
    if (!redoStack.length) return;
    const s = redoStack.pop();
    undoStack.push(s);
    applySnapshot(s);
    updateUndoButtons();
  }

  function updateUndoButtons() {
    const u = document.getElementById('undo-btn');
    const r = document.getElementById('redo-btn');
    if (u) u.disabled = undoStack.length < 2;
    if (r) r.disabled = redoStack.length === 0;
  }

  // ── Zoom ──────────────────────────────────────────────────────────────

  function setupZoomButtons() {
    const zin = document.getElementById('zoom-in');
    const zout = document.getElementById('zoom-out');
    const zreset = document.getElementById('zoom-reset');
    const zlabel = document.getElementById('zoom-level');
    if (zin) zin.onclick = function () {
      editor.zoom_in();
      if (zlabel) zlabel.textContent = Math.round(editor.zoom * 100) + '%';
    };
    if (zout) zout.onclick = function () {
      editor.zoom_out();
      if (zlabel) zlabel.textContent = Math.round(editor.zoom * 100) + '%';
    };
    if (zreset) zreset.onclick = function () {
      editor.zoom_reset();
      if (zlabel) zlabel.textContent = '100%';
    };
  }

  function updateStats() {
    const nc = document.getElementById('node-count');
    const ec = document.getElementById('edge-count');
    if (nc) nc.textContent = Object.keys(nodes).length;
    if (ec) ec.textContent = edges.length;
  }

  // ── Public API ────────────────────────────────────────────────────────


  // ── Context menu / clipboard ──────────────────────────────────────

  let _ctxMenuOrchId = null;
  let _ctxPasteX = null;
  let _ctxPasteY = null;

  function setupContextMenu() {
    const menu = document.getElementById('canvas-context-menu');
    if (!menu || !editor) return;

    function hideMenu() {
      menu.style.display = 'none';
      _ctxMenuOrchId = null;
    }

    editor.container.addEventListener('contextmenu', function (e) {
      e.preventDefault();
      e.stopPropagation();

      _ctxMenuOrchId = null;
      const nodeEl = e.target.closest && e.target.closest('.drawflow-node');
      if (nodeEl) {
        const dfId = String(nodeEl.id || '').replace(/^node-/, '');
        _ctxMenuOrchId = orchIdByDf[dfId] || null;
        if (_ctxMenuOrchId) {
          selectedOrchId = _ctxMenuOrchId;
          try {
            editor.container.querySelectorAll('.drawflow-node.selected').forEach(function (el) {
              el.classList.remove('selected');
            });
            nodeEl.classList.add('selected');
            editor.node_selected = nodeEl;
          } catch (err) {}
        }
      }

      // Enable/disable items
      const hasNode = !!_ctxMenuOrchId;
      const node = hasNode ? nodes[_ctxMenuOrchId] : null;
      const canClone = hasNode && node && node.type !== 'start';
      setCtxEnabled('ctx-delete', hasNode);
      setCtxEnabled('ctx-clone', canClone);
      setCtxEnabled('ctx-copy', canClone);
      setCtxEnabled('ctx-paste', !!clipboardNode);
      setCtxEnabled('ctx-undo', undoStack.length >= 2);
      setCtxEnabled('ctx-redo', redoStack.length > 0);

      // Remember graph coords under the cursor for paste
      try {
        const rect = editor.container.getBoundingClientRect();
        const zoom = editor.zoom || 1;
        _ctxPasteX = (e.clientX - rect.left - (editor.canvas_x || 0)) / zoom;
        _ctxPasteY = (e.clientY - rect.top - (editor.canvas_y || 0)) / zoom;
      } catch (err) {
        _ctxPasteX = null;
        _ctxPasteY = null;
      }

      menu.style.display = 'block';
      const mw = menu.offsetWidth || 180;
      const mh = menu.offsetHeight || 280;
      let x = e.clientX;
      let y = e.clientY;
      if (x + mw > window.innerWidth) x = window.innerWidth - mw - 8;
      if (y + mh > window.innerHeight) y = window.innerHeight - mh - 8;
      menu.style.left = x + 'px';
      menu.style.top = y + 'px';
    });

    menu.addEventListener('click', function (e) {
      e.preventDefault();
      e.stopPropagation();
      const item = e.target.closest && e.target.closest('[data-action]');
      if (!item || item.classList.contains('ctx-disabled')) return;
      const action = item.getAttribute('data-action');
      // Capture target BEFORE hideMenu() clears _ctxMenuOrchId
      const targetId = _ctxMenuOrchId;
      hideMenu();
      runContextAction(action, targetId);
    });

    document.addEventListener('click', function (e) {
      if (!menu.contains(e.target)) hideMenu();
    });
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') hideMenu();
    });
  }

  function setCtxEnabled(id, enabled) {
    const el = document.getElementById(id);
    if (!el) return;
    if (enabled) el.classList.remove('ctx-disabled');
    else el.classList.add('ctx-disabled');
  }

  function runContextAction(action, targetId) {
    const orchId = targetId || _ctxMenuOrchId || selectedOrchId;
    switch (action) {
      case 'delete':
        if (orchId) {
          selectedOrchId = orchId;
          deleteSelected();
        }
        break;
      case 'clone':
        if (orchId) cloneNode(orchId);
        break;
      case 'copy':
        if (orchId) copyNode(orchId);
        break;
      case 'paste':
        if (typeof _ctxPasteX === 'number' && typeof _ctxPasteY === 'number') {
          pasteNode(_ctxPasteX, _ctxPasteY);
        } else {
          pasteNode();
        }
        break;
      case 'undo':
        undo();
        break;
      case 'redo':
        redo();
        break;
      case 'zoom-in':
        if (editor) { editor.zoom_in(); syncZoomLabel(); }
        break;
      case 'zoom-out':
        if (editor) { editor.zoom_out(); syncZoomLabel(); }
        break;
      case 'zoom-reset':
        if (editor) { editor.zoom_reset(); syncZoomLabel(); }
        break;
      case 'fit':
        fitViewToNodes();
        break;
      case 'auto-layout':
        autoLayout();
        break;
    }
  }

  function syncZoomLabel() {
    const zlabel = document.getElementById('zoom-level');
    if (zlabel && editor) zlabel.textContent = Math.round((editor.zoom || 1) * 100) + '%';
    captureViewState();
  }

  function cloneNode(orchId) {
    const source = nodes[orchId];
    if (!source || source.type === 'start') return null;
    copyNode(orchId);
    return pasteNode(source.x + 60, source.y + 60);
  }

  function copyNode(orchId) {
    const source = nodes[orchId];
    if (!source || source.type === 'start') return;
    clipboardNode = JSON.parse(JSON.stringify(source));
    delete clipboardNode.id;
    if (window.M) M.toast({ html: 'Copied', displayLength: 1200 });
  }

  function pasteNode(atX, atY) {
    if (!clipboardNode) return null;
    const x = typeof atX === 'number' ? atX : (clipboardNode.x || 100) + 40;
    const y = typeof atY === 'number' ? atY : (clipboardNode.y || 100) + 40;
    const type = clipboardNode.type;
    // Use palette add path then overwrite data
    const extra = Object.assign({}, clipboardNode.data || {});
    if (type === 'plugin' && extra.pluginName) {
      // ok
    }
    const newId = addNodeFromPalette(
      type === 'execute' && extra.executeType === 'http' ? 'execute-http' : type,
      x,
      y,
      type === 'plugin' ? { pluginName: extra.pluginName } : {}
    );
    if (!newId || !nodes[newId]) return null;
    // Restore full data / label from clipboard
    nodes[newId].label = clipboardNode.label || nodes[newId].label;
    nodes[newId].icon = clipboardNode.icon || nodes[newId].icon;
    nodes[newId].data = Object.assign({}, clipboardNode.data || {});
    // New alias for action nodes
    if (nodes[newId].data.alias) {
      delete nodes[newId].data.alias;
    }
    const dfId = dfIdByOrch[newId];
    if (dfId != null) {
      OrchDrawflow.refreshNodeHtml(editor, dfId, nodes[newId]);
      refreshPortMultiClasses(dfId);
    }
    selectedOrchId = newId;
    if (window.M) M.toast({ html: 'Pasted', displayLength: 1200 });
    scheduleHistory();
    updateStats();
    return newId;
  }

  // Keyboard shortcuts
  document.addEventListener('keydown', function (e) {
    const tag = (e.target && e.target.tagName) || '';
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || e.target.isContentEditable) return;

    const mod = e.ctrlKey || e.metaKey;
    if (mod && e.key === 'z' && !e.shiftKey) {
      e.preventDefault();
      undo();
    } else if (mod && (e.key === 'y' || (e.key === 'z' && e.shiftKey))) {
      e.preventDefault();
      redo();
    } else if (mod && e.key === 'c') {
      if (selectedOrchId) {
        e.preventDefault();
        copyNode(selectedOrchId);
      }
    } else if (mod && e.key === 'v') {
      if (clipboardNode) {
        e.preventDefault();
        pasteNode();
      }
    } else if (mod && e.key === 'd') {
      if (selectedOrchId) {
        e.preventDefault();
        cloneNode(selectedOrchId);
      }
    } else if (e.key === 'Delete' || e.key === 'Backspace') {
      if (selectedOrchId) {
        e.preventDefault();
        deleteSelected();
      }
    }
  });



  /**
   * Horizontal layered auto-layout (left → right).
   * Depth from graph roots determines X; rank within layer determines Y.
   * Condition true branches prefer above parent, false below.
   */
  function autoLayout() {
    const nodeIds = Object.keys(nodes);
    if (!nodeIds.length) return;

    const incoming = {};
    const outgoing = {};
    const incomingMeta = {};
    const indegree = {};
    const depth = {};

    nodeIds.forEach(function (id) {
      incoming[id] = [];
      outgoing[id] = [];
      incomingMeta[id] = [];
      indegree[id] = 0;
      depth[id] = 0;
    });

    edges.forEach(function (edge) {
      if (!nodes[edge.from] || !nodes[edge.to]) return;
      outgoing[edge.from].push(edge.to);
      incoming[edge.to].push(edge.from);
      incomingMeta[edge.to].push({ parentId: edge.from, fromPort: edge.fromPort || 'out' });
      indegree[edge.to] += 1;
    });

    // Prefer starting from Start nodes
    const roots = nodeIds.filter(function (id) {
      return indegree[id] === 0 || nodes[id].type === 'start';
    });
    // Unique roots
    const queue = [];
    const seenQ = {};
    roots.forEach(function (id) {
      if (!seenQ[id]) { seenQ[id] = true; queue.push(id); }
    });
    // If no roots, pick arbitrary
    if (!queue.length && nodeIds.length) queue.push(nodeIds[0]);

    const indegreeWork = Object.assign({}, indegree);
    const topo = [];
    const inTopo = {};

    while (queue.length) {
      const current = queue.shift();
      if (inTopo[current]) continue;
      inTopo[current] = true;
      topo.push(current);
      (outgoing[current] || []).forEach(function (next) {
        depth[next] = Math.max(depth[next] || 0, (depth[current] || 0) + 1);
        indegreeWork[next] = (indegreeWork[next] || 1) - 1;
        if (indegreeWork[next] <= 0) queue.push(next);
      });
    }

    // Cycles / remaining
    nodeIds.forEach(function (id) {
      if (inTopo[id]) return;
      const parentDepths = (incoming[id] || []).map(function (p) { return depth[p] || 0; });
      depth[id] = parentDepths.length ? Math.max.apply(null, parentDepths) + 1 : 0;
      topo.push(id);
      inTopo[id] = true;
    });

    // Layers by depth
    const layers = {};
    topo.forEach(function (id) {
      const d = depth[id] || 0;
      if (!layers[d]) layers[d] = [];
      layers[d].push(id);
    });
    const layerIds = Object.keys(layers).map(Number).sort(function (a, b) { return a - b; });

    // Order within each layer: barycenter of parent ranks, with condition bias
    const rank = {};
    layerIds.forEach(function (layerId, li) {
      const ids = layers[layerId];
      if (li === 0) {
        // Start-like nodes first
        ids.sort(function (a, b) {
          const sa = nodes[a].type === 'start' ? 0 : 1;
          const sb = nodes[b].type === 'start' ? 0 : 1;
          return sa - sb || String(a).localeCompare(String(b));
        });
      } else {
        ids.sort(function (a, b) {
          function score(id) {
            const metas = incomingMeta[id] || [];
            if (!metas.length) return 0;
            let sum = 0;
            metas.forEach(function (m) {
              let r = rank[m.parentId] != null ? rank[m.parentId] : 0;
              const parent = nodes[m.parentId];
              if (parent && parent.type === 'condition') {
                if (m.fromPort === 'true') r -= 0.4;
                else if (m.fromPort === 'false') r += 0.4;
              }
              sum += r;
            });
            return sum / metas.length;
          }
          return score(a) - score(b) || String(a).localeCompare(String(b));
        });
      }
      ids.forEach(function (id, idx) {
        rank[id] = idx;
      });
    });

    // Layout metrics (horizontal flow) — compact so connectors stay short at 100% zoom
    const COL_GAP = 36;   // was 100 — horizontal gap between node columns
    const ROW_GAP = 24;   // was 48 — vertical gap within a column
    const NODE_W = 168;   // nominal node width for column pitch
    const DEFAULT_H = 52;
    const ORIGIN_X = 48;
    const ORIGIN_Y = 64;

    function measureHeight(id) {
      const dfId = dfIdByOrch[id];
      if (dfId != null && editor) {
        const el = editor.container.querySelector('#node-' + dfId);
        if (el && el.offsetHeight) return el.offsetHeight;
      }
      const t = nodes[id] && nodes[id].type;
      if (t === 'start' || t === 'end-success' || t === 'end-failure') return 48;
      if (t === 'condition') return 64;
      return DEFAULT_H;
    }

    // Port center Y (Drawflow ports sit mid-height on the node)
    function portCenterY(id) {
      return nodes[id].y + measureHeight(id) / 2;
    }

    function setByPortCenterY(id, centerY) {
      nodes[id].y = centerY - measureHeight(id) / 2;
    }

    function measureWidth(id) {
      const dfId = dfIdByOrch[id];
      if (dfId != null && editor) {
        const el = editor.container.querySelector('#node-' + dfId);
        if (el && el.offsetWidth) return el.offsetWidth;
      }
      return NODE_W;
    }

    // Column x: use max measured width in previous layers + small gap
    const colX = {};
    let nextX = ORIGIN_X;
    layerIds.forEach(function (layerId, i) {
      colX[layerId] = nextX;
      const ids = layers[layerId] || [];
      let maxW = NODE_W;
      ids.forEach(function (id) {
        maxW = Math.max(maxW, measureWidth(id));
      });
      nextX += maxW + COL_GAP;
    });

    // Initial placement: columns by depth, stacked rows
    layerIds.forEach(function (layerId) {
      const ids = layers[layerId];
      let y = ORIGIN_Y;
      ids.forEach(function (id) {
        const n = nodes[id];
        n.x = colX[layerId];
        n.y = y;
        y += measureHeight(id) + ROW_GAP;
      });
    });

    // Straighten pass: align child port centers to parent port centers
    // so horizontal connectors are as straight as possible.
    // Process in topological order so parents are settled first.
    topo.forEach(function (id) {
      const parents = incomingMeta[id] || [];
      if (!parents.length) return;

      // Single parent → match that parent's port center (straight line)
      if (parents.length === 1) {
        const p = parents[0];
        const parent = nodes[p.parentId];
        if (!parent) return;

        // Condition branches: offset true above / false below
        if (parent.type === 'condition') {
          const parentCy = portCenterY(p.parentId);
          const gap = measureHeight(id) / 2 + ROW_GAP + 8;
          if (p.fromPort === 'true') {
            setByPortCenterY(id, parentCy - gap);
          } else if (p.fromPort === 'false') {
            setByPortCenterY(id, parentCy + gap);
          } else {
            setByPortCenterY(id, parentCy);
          }
          return;
        }

        // Split with multiple children: leave vertical packing for later;
        // single-outgoing still aligns.
        const outCount = (outgoing[p.parentId] || []).length;
        if (outCount <= 1) {
          setByPortCenterY(id, portCenterY(p.parentId));
        }
        return;
      }

      // Multiple parents (join): barycenter of parent port centers
      let sum = 0;
      parents.forEach(function (p) {
        if (nodes[p.parentId]) sum += portCenterY(p.parentId);
      });
      setByPortCenterY(id, sum / parents.length);
    });

    // Split fan-out: distribute children evenly around the split's port center
    nodeIds.forEach(function (id) {
      const n = nodes[id];
      if (!n || n.type !== 'split-join') return;
      const mode = (n.data && n.data.mode) || 'split';
      if (mode === 'join') return;
      const childIds = outgoing[id] || [];
      if (childIds.length <= 1) {
        if (childIds.length === 1 && nodes[childIds[0]]) {
          setByPortCenterY(childIds[0], portCenterY(id));
        }
        return;
      }
      const parentCy = portCenterY(id);
      const totalH = childIds.reduce(function (acc, cid) {
        return acc + measureHeight(cid);
      }, 0) + (childIds.length - 1) * ROW_GAP;
      let y = parentCy - totalH / 2;
      childIds.forEach(function (cid) {
        if (!nodes[cid]) return;
        nodes[cid].y = y;
        // Prefer aligning each child's center would be y + h/2 — already top-based
        y += measureHeight(cid) + ROW_GAP;
      });
    });

    // Resolve vertical overlaps within each layer (keep relative order)
    layerIds.forEach(function (layerId) {
      const ids = layers[layerId].slice().sort(function (a, b) {
        return nodes[a].y - nodes[b].y;
      });
      for (let i = 1; i < ids.length; i++) {
        const prev = ids[i - 1];
        const cur = ids[i];
        const minY = nodes[prev].y + measureHeight(prev) + ROW_GAP;
        if (nodes[cur].y < minY) {
          nodes[cur].y = minY;
        }
      }
    });

    // Final chain straighten: one more pass for pure 1-in-1-out sequences
    // (fixes start/end vs taller execute nodes after overlap resolution)
    topo.forEach(function (id) {
      const outs = (outgoing[id] || []);
      const ins = (incoming[id] || []);
      if (outs.length === 1 && ins.length <= 1) {
        const child = outs[0];
        const childIns = incoming[child] || [];
        if (childIns.length === 1 && childIns[0] === id) {
          // Only straighten if parent is not a condition/split multi-out
          const parentType = nodes[id].type;
          const multiOut = parentType === 'condition' ||
            (parentType === 'split-join' && ((nodes[id].data && nodes[id].data.mode) || 'split') === 'split');
          if (!multiOut) {
            setByPortCenterY(child, portCenterY(id));
          }
        }
      }
    });

    // Apply positions to Drawflow DOM + model
    suppressEvents = true;
    nodeIds.forEach(function (id) {
      const n = nodes[id];
      const dfId = dfIdByOrch[id];
      if (dfId == null) return;
      try {
        const el = editor.container.querySelector('#node-' + dfId);
        if (el) {
          el.style.left = n.x + 'px';
          el.style.top = n.y + 'px';
        }
        const dn = editor.getNodeFromId(dfId);
        if (dn) {
          dn.pos_x = n.x;
          dn.pos_y = n.y;
        }
        if (typeof editor.updateConnectionNodes === 'function') {
          editor.updateConnectionNodes('node-' + dfId);
        }
      } catch (err) {
        console.warn('autoLayout position', id, err);
      }
    });
    suppressEvents = false;

    scheduleHistory();
    focusGraphInView();
    setTimeout(function () { captureViewState(); }, 400);
    if (window.M) M.toast({ html: 'Auto layout applied', displayLength: 1500 });
  }

  global.OrchEditor = {
    init: init,
    loadOrchestration: loadOrchestration,
    getOrchestrationPayload: getOrchestrationPayload,
    validate: validate,
    getNodes: function () { return nodes; },
    getEdges: function () { return edges; },
    getSelectedId: function () { return selectedOrchId; },
    getNode: function (id) { return nodes[id]; },
    setNodeData: function (id, patch) {
      if (!nodes[id]) return;
      Object.assign(nodes[id], patch);
      if (patch.data) nodes[id].data = Object.assign({}, nodes[id].data, patch.data);
      syncNodeFromProperties(id);
    },
    syncNodeFromProperties: syncNodeFromProperties,
    deleteSelected: deleteSelected,
    cloneNode: cloneNode,
    copyNode: copyNode,
    pasteNode: pasteNode,
    undo: undo,
    redo: redo,
    getEditor: function () { return editor; },
    addNodeFromPalette: addNodeFromPalette,
    fitViewToNodes: fitViewToNodes,
    focusGraphInView: focusGraphInView,
    expandCanvasExtents: expandCanvasExtents,
    normalizeGraphOrigin: normalizeGraphOrigin,
    autoLayout: autoLayout,
    refreshPortClasses: refreshAllPortMultiClasses,
    onSplitJoinModeChanged: onSplitJoinModeChanged
  };

  /**
   * Called when properties panel changes split-join mode.
   * Updates visuals and prunes connections that are no longer legal.
   */
  function onSplitJoinModeChanged(orchId) {
    const node = nodes[orchId];
    if (!node || node.type !== 'split-join') return;
    const dfId = dfIdByOrch[orchId];
    if (dfId == null) return;
    const spec = OrchDrawflow.portSpec(node);

    // Prune model edges that violate new multiplicity
    if (!spec.multiOut) {
      // keep only first outgoing
      let seenOut = false;
      edges = edges.filter(function (e) {
        if (e.from !== orchId) return true;
        if (!seenOut) { seenOut = true; return true; }
        return false;
      });
    }
    if (!spec.multiIn) {
      let seenIn = false;
      edges = edges.filter(function (e) {
        if (e.to !== orchId) return true;
        if (!seenIn) { seenIn = true; return true; }
        return false;
      });
    }

    // Rebuild drawflow connections for this node from model
    suppressEvents = true;
    try {
      const dfNode = editor.getNodeFromId(dfId);
      // Remove all current connections involving this node
      Object.keys(dfNode.outputs || {}).forEach(function (ok) {
        const conns = (dfNode.outputs[ok].connections || []).slice();
        conns.forEach(function (c) {
          try { editor.removeSingleConnection(dfId, c.node, ok, c.output); } catch (e) {}
        });
      });
      Object.keys(dfNode.inputs || {}).forEach(function (ik) {
        const conns = (dfNode.inputs[ik].connections || []).slice();
        conns.forEach(function (c) {
          try { editor.removeSingleConnection(c.node, dfId, c.input, ik); } catch (e) {}
        });
      });
      // Re-add allowed edges from model
      edges.forEach(function (e) {
        if (e.from !== orchId && e.to !== orchId) return;
        const fromDf = dfIdByOrch[e.from];
        const toDf = dfIdByOrch[e.to];
        if (fromDf == null || toDf == null) return;
        const fromN = nodes[e.from];
        const fSpec = OrchDrawflow.portSpec(fromN || { type: 'execute' });
        let outputIndex = 1;
        const idx = (fSpec.outputIds || []).indexOf(e.fromPort || 'out');
        if (idx >= 0) outputIndex = idx + 1;
        try {
          editor.addConnection(fromDf, toDf, 'output_' + outputIndex, 'input_1');
        } catch (err) {}
      });
    } catch (err) {
      console.warn('onSplitJoinModeChanged', err);
    }
    suppressEvents = false;

    syncNodeFromProperties(orchId);
    refreshPortMultiClasses(dfId);
    updateStats();
    scheduleHistory();
  }
})(typeof window !== 'undefined' ? window : global);
