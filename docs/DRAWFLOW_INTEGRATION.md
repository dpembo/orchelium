# Drawflow Integration (first complete pass)

This branch adds a **Drawflow-based orchestration editor and monitor** alongside the existing SVG builder. The native API contract (`nodes[]` + `edges[]`) is unchanged so the engine, scheduler, and history keep working.

## Why Drawflow

- Vanilla JS, no framework dependency
- npm package (`drawflow`)
- Built-in pan/zoom, connections, import/export, edit vs view mode
- Same library for **builder** and **live monitor** → consistent UX

## Files added

| Path | Role |
|------|------|
| `package.json` | dependency `drawflow@^0.0.60` |
| `public/js/drawflow/drawflow.min.js` | vendored dist (also available via `node_modules` after `npm i`) |
| `public/css/drawflow.min.css` | base Drawflow styles |
| `public/css/orchestration-drawflow.css` | Orchelium theme (light/dark, status colours) |
| `public/js/orchestration-drawflow-core.js` | convert Orchelium ↔ Drawflow JSON |
| `public/js/orchestration-drawflow-editor.js` | builder controller |
| `public/js/orchestration-drawflow-viewer.js` | monitor (view mode + status) |
| `views/orchestrationBuilderDrawflow.ejs` | full builder UI |
| `views/orchestrationMonitorDrawflow.ejs` | full monitor UI |

## Install

```bash
npm install
# ensures node_modules/drawflow is present
# optional: copy dist into public if you prefer not to serve from node_modules
cp node_modules/drawflow/dist/drawflow.min.js public/js/drawflow/
cp node_modules/drawflow/dist/drawflow.min.css public/css/
```

## Wire routes (server.js)

Add routes parallel to the existing builder/monitor (names may differ slightly in your tree):

```js
app.get('/orchestrationBuilderDrawflow.html', ensureAuth, (req, res) => {
  res.render('orchestrationBuilderDrawflow', {
    jobId: req.query.id || '',
    jobName: '',
    jobDescription: ''
  });
});

app.get('/orchestrationMonitorDrawflow.html', ensureAuth, (req, res) => {
  res.render('orchestrationMonitorDrawflow', {});
});
```

Serve static assets as usual (`express.static('public')`).

## Data contract (unchanged)

```js
{
  jobId, name, description, icon, color,
  nodes: [{ id, label, type, icon, x, y, data }],
  edges: [{ id, from, fromPort, to, label, color }]
}
```

### Port mapping

| Node type     | Inputs | Outputs                         |
|---------------|--------|---------------------------------|
| start         | 0      | `out`                           |
| execute / wait / notify / plugin | 1 | `out`                    |
| condition     | 1      | `true`, `false`                 |
| split-join    | 1      | `out` (multi-connection OK)     |
| end-success / end-failure | 1 | none                     |

Conversion lives in `OrchDrawflow.toDrawflow` / `fromDrawflow`.

## How the editor works

1. Palette drag → `OrchEditor.addNodeFromPalette`
2. Drawflow `connectionCreated` / `connectionRemoved` keep the parallel `edges[]` model in sync
3. Selection opens the properties panel; edits call `OrchEditor.syncNodeFromProperties`
4. Save posts the same JSON the classic builder uses
5. Undo/redo snapshots the Orchelium model and re-imports into Drawflow

## How the viewer works

1. Load execution payload (`nodes`, `edges`, `visitedNodes`, `nodeScriptOutputs`, …)
2. `OrchViewer.loadGraph` in **view** mode
3. Poll / socket updates → `statusesFromExecution` → CSS classes:
   - `node-in-progress` (pulse)
   - `node-completed`
   - `node-failed`
   - `node-pending` (dimmed)
4. Click node → details side panel (exit code, stdout, errors)

## Migration path from classic SVG builder

1. Ship Drawflow routes **alongside** classic ones (done with `*Drawflow` pages).
2. Validate save/load/run against existing jobs.
3. Optionally add a UI toggle “Classic / Drawflow”.
4. When confident, point the main “Create orchestration” link at the Drawflow builder and retire SVG drawing code.

Existing saved orchestrations load without migration: conversion runs on every load.

## Known first-pass limitations

- Properties panel is simplified (script/agent dropdowns are free-text; wire to your existing script/agent lists like the classic builder).
- Multi-select / box-select not ported (Drawflow is primarily single-select).
- Condition TRUE/FALSE ports are positional (`output_1` / `output_2`); labels on the connection path are not drawn yet (can be added with custom CSS or edge labels).
- Auto-layout from the classic context menu is not reimplemented (Drawflow keeps manual positions).
- Plugin parameter forms are minimal (plugin name only).

## Next increments

1. Reuse classic builder’s full properties HTML (script select, agent select, HTTP headers editor, plugin docs modal).
2. Connection colouring by `fromPort` (green true / red false).
3. Replace classic builder canvas in-place once parity is reached.
4. Serve Drawflow from `node_modules` via a dedicated static mount if you prefer not to vendor the min files.
