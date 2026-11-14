module.exports = function register(app) {
app.get(/^\/docs(?:\/.*)?$/, User.isAuthenticated, asyncHandler(async (req, res, next) => {
  const docsPath = req.path.replace(/^\/docs/, '') || '/';
  const relPath = decodeURIComponent(docsPath).replace(/^\/+/, '');

  const resolveIfFile = async (candidatePath) => {
    try {
      const stat = await fs.promises.stat(candidatePath);
      return stat.isFile() ? candidatePath : null;
    } catch (err) {
      return null;
    }
  };

  const safeResolve = (p) => {
    const resolved = path.resolve(docsRoot, p);
    const rel = path.relative(docsRoot, resolved);
    if (rel.startsWith('..') || path.isAbsolute(rel)) return null;
    return resolved;
  };

  let candidates = [];
  if (!relPath || relPath.endsWith('/')) {
    const dirTarget = safeResolve(relPath);
    if (dirTarget) candidates.push(path.join(dirTarget, 'index.md'));
  } else {
    const direct = safeResolve(relPath);
    if (direct) {
      candidates.push(direct);
      if (!path.extname(direct)) {
        candidates.push(`${direct}.md`);
        candidates.push(path.join(direct, 'index.md'));
      }
    }
  }

  let markdownFile = null;
  for (const candidate of candidates) {
    markdownFile = await resolveIfFile(candidate);
    if (markdownFile) break;
  }

  if (!markdownFile || path.extname(markdownFile).toLowerCase() !== '.md') {
    return next();
  }

  const stripHtml = (value) => value.replace(/<[^>]*>/g, '');
  const slugify = (value) => stripHtml(String(value || ''))
    .toLowerCase()
    .trim()
    .replace(/&[a-z0-9#]+;/gi, '')
    .replace(/[^a-z0-9\s-]/g, '')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');

  const addHeadingIds = (html) => {
    const seen = new Map();
    return html.replace(/<h([1-6])>([\s\S]*?)<\/h\1>/gi, (match, level, headingContent) => {
      const base = slugify(headingContent) || 'section';
      const count = seen.get(base) || 0;
      seen.set(base, count + 1);
      const id = count === 0 ? base : `${base}-${count}`;
      return `<h${level} id="${id}">${headingContent}</h${level}>`;
    });
  };

  try {
    const markdown = await fs.promises.readFile(markdownFile, 'utf8');
    const htmlBody = addHeadingIds(marked.parse(markdown));
    const relTitle = path.relative(docsRoot, markdownFile) || 'index.md';

    return res.status(200).send(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Docs - ${relTitle}</title>
  <style>
    html { scroll-behavior: smooth; }
    @media (prefers-reduced-motion: reduce) {
      html { scroll-behavior: auto; }
    }
    body { font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Helvetica, Arial, sans-serif; margin: 0; padding: 0; background: #f8f9fb; color: #1f2328; }
    .wrap { max-width: 980px; margin: 30px auto; background: #fff; border: 1px solid #d0d7de; border-radius: 8px; padding: 24px; }
    .crumbs { margin-bottom: 16px; font-size: 14px; }
    .crumbs a { color: #0969da; text-decoration: none; }
    .crumbs a:hover { text-decoration: underline; }
    .markdown-body h1, .markdown-body h2, .markdown-body h3, .markdown-body h4, .markdown-body h5, .markdown-body h6 { scroll-margin-top: 16px; }
    .markdown-body h1, .markdown-body h2, .markdown-body h3 { margin-top: 1.2em; }
    .markdown-body pre { background: #f6f8fa; padding: 12px; border-radius: 6px; overflow: auto; }
    .markdown-body code { background: #f6f8fa; padding: 0.1em 0.3em; border-radius: 4px; }
    .markdown-body table { border-collapse: collapse; width: 100%; }
    .markdown-body table th, .markdown-body table td { border: 1px solid #d0d7de; padding: 6px 8px; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="crumbs"><a href="/docs/">Docs Home</a> / ${relTitle}</div>
    <article class="markdown-body">${htmlBody}</article>
  </div>
</body>
</html>`);
  } catch (err) {
    logger.error('Error rendering docs markdown', err);
    return res.status(500).send('Unable to render documentation file.');
  }
}));

app.get('/docs.html', User.isAuthenticated, async (req, res) => {
  res.redirect('/docs/');
});
};
