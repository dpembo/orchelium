function safeRedirect(value, fallback = '/') {
  if (typeof value !== 'string') return fallback;
  const trimmed = value.trim();
  if (!trimmed) return fallback;
  if (!trimmed.startsWith('/') || trimmed.startsWith('//') || trimmed.startsWith('/\\')) return fallback;
  if (trimmed.includes('\\') || trimmed.includes('\0') || trimmed.includes('://')) return fallback;
  return trimmed;
}

module.exports = { safeRedirect };
