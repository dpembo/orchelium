'use strict';

const axios      = require('axios');
const path       = require('path');
const fs         = require('fs');
const fsPromises = require('fs').promises;
const unzipper   = require('unzipper');
const { pipeline } = require('stream/promises');

const { PLUGINS_DIR } = require('./pluginRegistry');

const PLUGIN_NAME_REGEX = /^[a-z0-9][a-z0-9-]*$/;

// ── Local cache ───────────────────────────────────────────────────────────────

const CACHE_DIR           = path.join(PLUGINS_DIR, '.cache');
const CACHE_PLUGINS_FILE  = path.join(CACHE_DIR, 'plugins.json');
const CACHE_VALIDATION_FILE = path.join(CACHE_DIR, 'validation-report.json');
const CACHE_ICONS_DIR     = path.join(CACHE_DIR, 'icons');
const CACHE_META_FILE     = path.join(CACHE_DIR, 'meta.json');  // stores last known hash

const SVG_MAX_BYTES = 50 * 1024;

// ── Registry URL helpers ──────────────────────────────────────────────────────

function getRegistryBase() {
  return (serverConfig.pluginRegistry.url || 'https://orchelium.com/plugins/registry/').replace(/\/$/, '');
}

function getRegistryUrl() {
  const base = getRegistryBase();
  const pluginsPath = (serverConfig.pluginRegistry.plugins || 'plugins.json').replace(/\/$/, '');
  const normalised = pluginsPath.startsWith('/') ? pluginsPath : `/${pluginsPath}`;
  return `${base}${normalised}`;
}

function getValidationUrl() {
  const base = getRegistryBase();
  const validationPath = (serverConfig.pluginRegistry.validation || 'validation-report.json').replace(/\/$/, '');
  const normalised = validationPath.startsWith('/') ? validationPath : `/${validationPath}`;
  return `${base}${normalised}`;
}

function getHashUrl() {
  const base = getRegistryBase();
  const hashPath = (serverConfig.pluginRegistry.hash || 'registry-hash.php').replace(/\/$/, '');
  const normalised = hashPath.startsWith('/') ? hashPath : `/${hashPath}`;
  return `${base}${normalised}`;
}

function getIconUrl(reponame) {
  const base = getRegistryBase();
  const iconsPath = (serverConfig.pluginRegistry.icons || 'icons/').replace(/\/$/, '');
  const normalised = iconsPath.startsWith('/') ? iconsPath : `/${iconsPath}`;
  return `${base}${normalised}/${reponame}.svg`;
}

function getCommunityZipUrl(reponame) {
  const base = getRegistryBase();
  const installsPath = (serverConfig.pluginRegistry.installs || 'installs/').replace(/\/$/, '');
  const normalised = installsPath.startsWith('/') ? installsPath : `/${installsPath}`;
  return `${base}${normalised}/${reponame}.zip`;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function validatePluginName(name) {
  if (!name || typeof name !== 'string' || !PLUGIN_NAME_REGEX.test(name)) {
    throw new Error(`Invalid plugin name '${name}'. Names must match [a-z0-9][a-z0-9-]*.`);
  }
}

function safePluginPath(name) {
  validatePluginName(name);
  const resolved = path.resolve(PLUGINS_DIR, name);
  if (!resolved.startsWith(path.resolve(PLUGINS_DIR))) {
    throw new Error('Plugin path traversal detected');
  }
  return resolved;
}

// ── Cache helpers ─────────────────────────────────────────────────────────────

async function ensureCacheDirs() {
  await fsPromises.mkdir(CACHE_DIR,       { recursive: true });
  await fsPromises.mkdir(CACHE_ICONS_DIR, { recursive: true });
  logger.debug(`[PLUGIN:CACHE] Cache directories ready: ${CACHE_DIR}`);
}

async function readCacheMeta() {
  try {
    const raw  = await fsPromises.readFile(CACHE_META_FILE, 'utf8');
    const meta = JSON.parse(raw);
    const age  = meta.cachedAt ? Math.round((Date.now() - meta.cachedAt) / 1000) : null;
    logger.debug(`[PLUGIN:CACHE] Meta read — hash: ${meta.hash ?? '(none)'}${age !== null ? `, cached ${age}s ago` : ''}`);
    return meta;
  } catch {
    logger.debug('[PLUGIN:CACHE] No meta file found — treating as empty cache');
    return {};
  }
}

async function writeCacheMeta(meta) {
  await fsPromises.writeFile(CACHE_META_FILE, JSON.stringify(meta, null, 2), 'utf8');
  logger.debug(`[PLUGIN:CACHE] Meta written — hash: ${meta.hash}`);
}

async function clearCache() {
  logger.info('[PLUGIN:CACHE] Clearing local registry cache (hash changed)');
  try {
    await fsPromises.rm(CACHE_DIR, { recursive: true, force: true });
    logger.debug(`[PLUGIN:CACHE] Cache directory removed: ${CACHE_DIR}`);
  } catch (err) {
    logger.warn(`[PLUGIN:CACHE] Could not clear cache: ${err.message}`);
  }
  await ensureCacheDirs();
}

// ── Remote hash check ─────────────────────────────────────────────────────────

/**
 * Fetch the remote hash and compare against the locally stored hash.
 * Returns { changed: boolean, remoteHash: string }.
 */
async function checkRemoteHash() {
  const hashUrl = getHashUrl();
  logger.debug(`[PLUGIN:CACHE] Fetching registry hash from ${hashUrl}`);

  const resp       = await axios.get(hashUrl, { timeout: 10000 });
  const remoteHash = resp.data?.hash;
  const remoteCount = resp.data?.count ?? '?';
  const remoteUpdated = resp.data?.updated ? new Date(resp.data.updated * 1000).toISOString() : '?';

  if (!remoteHash || typeof remoteHash !== 'string') {
    throw new Error('Hash endpoint returned an unexpected response');
  }

  const meta    = await readCacheMeta();
  const changed = meta.hash !== remoteHash;

  if (changed) {
    logger.info(`[PLUGIN:CACHE] Hash changed — remote: ${remoteHash} | cached: ${meta.hash ?? '(none)'} | plugins: ${remoteCount} | updated: ${remoteUpdated}`);
  } else {
    logger.info(`[PLUGIN:CACHE] Hash unchanged (${remoteHash}) — cache is current`);
  }

  return { changed, remoteHash };
}

// ── Icon caching ──────────────────────────────────────────────────────────────

/**
 * Download and cache a single SVG icon.
 * Returns the local cache path on success, or null on failure.
 */
async function cacheIcon(reponame) {
  const iconUrl   = getIconUrl(reponame);
  const localPath = path.join(CACHE_ICONS_DIR, `${reponame}.svg`);

  try {
    const resp = await axios.get(iconUrl, { timeout: 10000, responseType: 'arraybuffer' });
    const buf  = Buffer.from(resp.data);

    if (buf.length > SVG_MAX_BYTES) {
      logger.warn(`[PLUGIN:CACHE] Icon for '${reponame}' exceeds size limit (${buf.length} bytes), skipping`);
      return null;
    }

    await fsPromises.writeFile(localPath, buf);
    logger.debug(`[PLUGIN:CACHE] Icon cached: ${reponame}.svg (${buf.length} bytes)`);
    return localPath;
  } catch (err) {
    logger.warn(`[PLUGIN:CACHE] Could not cache icon for '${reponame}': ${err.message}`);
    return null;
  }
}

// ── Full registry refresh ─────────────────────────────────────────────────────

/**
 * Fetch the registry and validation report from the remote server,
 * write them to the local cache, and download all icons.
 */
async function refreshCache(remoteHash) {
  const registryUrl   = getRegistryUrl();
  const validationUrl = getValidationUrl();

  logger.info(`[PLUGIN:CACHE] Starting full cache refresh from ${registryUrl}`);
  const refreshStart = Date.now();

  const [registryResp, validationResp] = await Promise.allSettled([
    axios.get(registryUrl,   { timeout: 15000 }),
    axios.get(validationUrl, { timeout: 15000 }),
  ]);

  if (registryResp.status === 'rejected') throw registryResp.reason;

  const data = registryResp.value.data;
  if (!data || !Array.isArray(data.plugins)) {
    throw new Error('Remote registry is malformed — expected { plugins: [...] }');
  }

  logger.debug(`[PLUGIN:CACHE] Registry fetched: ${data.plugins.length} plugins (${data.official ?? 0} official, ${data.community ?? 0} community)`);

  const validationReport = validationResp.status === 'fulfilled'
    ? validationResp.value.data
    : {};

  if (validationResp.status === 'rejected') {
    logger.warn('[PLUGIN:CACHE] Could not fetch validation report — warnings will not be shown');
  } else {
    logger.debug(`[PLUGIN:CACHE] Validation report fetched: ${Object.keys(validationReport).length} entries`);
  }

  // Write plugins.json and validation-report.json to cache
  await fsPromises.writeFile(CACHE_PLUGINS_FILE,    JSON.stringify(data,             null, 2), 'utf8');
  await fsPromises.writeFile(CACHE_VALIDATION_FILE, JSON.stringify(validationReport, null, 2), 'utf8');
  logger.debug('[PLUGIN:CACHE] plugins.json and validation-report.json written to cache');

  // Download all icons in parallel (best-effort)
  const iconJobs = data.plugins.map(p => p.reponame).filter(Boolean);
  logger.info(`[PLUGIN:CACHE] Downloading ${iconJobs.length} icons…`);
  const iconResults = await Promise.allSettled(iconJobs.map(reponame => cacheIcon(reponame)));
  const iconOk   = iconResults.filter(r => r.status === 'fulfilled' && r.value).length;
  const iconFail = iconResults.length - iconOk;
  logger.info(`[PLUGIN:CACHE] Icons cached: ${iconOk} ok${iconFail > 0 ? `, ${iconFail} failed` : ''}`);

  // Persist the new hash so subsequent calls can short-circuit
  await writeCacheMeta({ hash: remoteHash, cachedAt: Date.now() });

  const elapsed = Date.now() - refreshStart;
  logger.info(`[PLUGIN:CACHE] Cache refresh complete in ${elapsed}ms — ${data.plugins.length} plugins, hash ${remoteHash}`);

  return { data, validationReport };
}

// ── Load from local cache ─────────────────────────────────────────────────────

async function loadFromCache() {
  logger.debug('[PLUGIN:CACHE] Reading registry from local cache');
  const [pluginsRaw, validationRaw] = await Promise.all([
    fsPromises.readFile(CACHE_PLUGINS_FILE,    'utf8'),
    fsPromises.readFile(CACHE_VALIDATION_FILE, 'utf8').catch(() => '{}'),
  ]);

  const data             = JSON.parse(pluginsRaw);
  const validationReport = JSON.parse(validationRaw);
  logger.debug(`[PLUGIN:CACHE] Loaded from cache: ${data.plugins?.length ?? 0} plugins, ${Object.keys(validationReport).length} validation entries`);

  return { data, validationReport };
}

// ── Public: fetchRemoteRegistry ───────────────────────────────────────────────

/**
 * Main entry point called by the rest of the hub.
 *
 * Flow:
 *   1. Call hash endpoint (one cheap request).
 *   2. If hash matches cached hash → load from disk, return immediately.
 *   3. If hash differs (or no cache) → clear cache, re-download everything,
 *      persist new files, update stored hash.
 *
 * Decorates each plugin with:
 *   - iconUrl  → /plugin-icons/<reponame>.svg (served as static files by Express)
 *   - validationWarnings
 */
async function fetchRemoteRegistry() {
  await ensureCacheDirs();

  // Step 1: check hash
  let changed, remoteHash;
  try {
    ({ changed, remoteHash } = await checkRemoteHash());
  } catch (err) {
    logger.warn(`[PLUGIN:CACHE] Hash check failed (${err.message}) — attempting to serve from local cache`);
    try {
      const payload = await loadFromCache();
      logger.info('[PLUGIN:CACHE] Serving stale cache due to hash endpoint failure');
      return buildRegistryResponse(payload);
    } catch (cacheErr) {
      throw new Error(`Registry hash check failed and no local cache available: ${err.message}`);
    }
  }

  // Step 2: short-circuit if nothing has changed
  let payload;
  if (!changed) {
    payload = await loadFromCache();
  } else {
    // Step 3: full refresh
    await clearCache();
    payload = await refreshCache(remoteHash);
  }

  return buildRegistryResponse(payload);
}

// ── Response builder ──────────────────────────────────────────────────────────

/**
 * Decorate plugins with local icon paths and validation warnings,
 * then return the registry data object the hub expects.
 */
function buildRegistryResponse({ data, validationReport }) {
  let warnCount = 0;
  for (const plugin of data.plugins) {
    plugin.iconUrl = `/plugin-icons/${plugin.reponame}.svg`;

    if (plugin.source === 'community' && !plugin.path && plugin.repository_url) {
      plugin.path = plugin.repository_url.split('/').pop();
    }

    const report = validationReport[plugin.reponame] ?? null;
    plugin.validationWarnings = report?.warnings ?? [];
    if (plugin.validationWarnings.length > 0) warnCount++;
  }

  logger.info(`[PLUGIN] Registry ready: ${data.plugins.length} plugins (${data.official ?? 0} official, ${data.community ?? 0} community)${warnCount > 0 ? `, ${warnCount} with validation warnings` : ''}`);

  return data;
}

// ── Install ───────────────────────────────────────────────────────────────────

async function installPlugin(pluginName, registryEntry) {
  validatePluginName(pluginName);

  if (!registryEntry) {
    throw new Error('registryEntry is required');
  }

  const repoName = registryEntry.reponame;
  if (!repoName) {
    throw new Error(`Plugin '${pluginName}' is missing reponame field`);
  }

  const destDir = safePluginPath(pluginName);
  const zipUrl  = getCommunityZipUrl(repoName);

  logger.info(`[PLUGIN] Installing plugin '${pluginName}' from ${zipUrl}`);

  const zipResp   = await axios.get(zipUrl, { responseType: 'arraybuffer', timeout: 30000 });
  const zipBuffer = Buffer.from(zipResp.data);

  logger.debug(`[PLUGIN] Downloaded zip for '${pluginName}' (${zipBuffer.length} bytes), extracting to ${destDir}`);

  await fsPromises.mkdir(destDir, { recursive: true });
  await extractPluginArchive(zipBuffer, destDir);

  logger.info(`[PLUGIN] Plugin '${pluginName}' installed successfully`);
}

/**
 * Write every archive entry inside destDir, and finish only after the writes
 * and script permission changes have completed.
 */
async function extractPluginArchive(zipBuffer, destDir) {
  const root = path.resolve(destDir);
  const directory = await unzipper.Open.buffer(zipBuffer);
  const writes = [];

  for (const entry of directory.files) {
    const parts = String(entry.path || '').split('/');
    const stripped = parts.slice(1).join('/');
    if (!stripped) continue;

    if (entry.type === 'SymbolicLink' || entry.type === 'Link') {
      throw new Error(`Plugin archive contains a link: ${entry.path}`);
    }

    const destPath = path.resolve(root, stripped);
    if (destPath !== root && !destPath.startsWith(root + path.sep)) {
      throw new Error(`Plugin archive contains an unsafe path: ${entry.path}`);
    }

    if (entry.type === 'Directory' || entry.path.endsWith('/')) {
      writes.push(fsPromises.mkdir(destPath, { recursive: true }));
      continue;
    }

    writes.push((async () => {
      await fsPromises.mkdir(path.dirname(destPath), { recursive: true });
      await pipeline(entry.stream(), fs.createWriteStream(destPath));
      if (destPath.endsWith('.sh')) {
        await fsPromises.chmod(destPath, 0o755);
      }
    })());
  }

  await Promise.all(writes);
}

// ── Uninstall ─────────────────────────────────────────────────────────────────

async function uninstallPlugin(pluginName) {
  validatePluginName(pluginName);

  logger.info(`[PLUGIN] Uninstalling plugin '${pluginName}'`);

  const pluginDir = safePluginPath(pluginName);

  try {
    await fsPromises.access(pluginDir);
  } catch {
    throw new Error(`Plugin '${pluginName}' is not installed`);
  }

  await fsPromises.rm(pluginDir, { recursive: true });

  logger.info(`[PLUGIN] Plugin '${pluginName}' uninstalled successfully`);
}

// ── Exports ───────────────────────────────────────────────────────────────────

module.exports = {
  fetchRemoteRegistry,
  installPlugin,
  uninstallPlugin,
  extractPluginArchive,
};