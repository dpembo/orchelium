const { safeRedirect } = require('../../utils/safeRedirect');

describe('safeRedirect', () => {
  test('keeps a same-site path', () => {
    expect(safeRedirect('/scheduleList.html')).toBe('/scheduleList.html');
    expect(safeRedirect('/history.html?job=a')).toBe('/history.html?job=a');
    expect(safeRedirect('  /settings.html  ')).toBe('/settings.html');
  });

  test('rejects absolute and protocol-relative targets', () => {
    expect(safeRedirect('https://evil.example', '/')).toBe('/');
    expect(safeRedirect('//evil.example', '/')).toBe('/');
    expect(safeRedirect('/\\evil.example', '/')).toBe('/');
    expect(safeRedirect('javascript:alert(1)', '/home')).toBe('/home');
    expect(safeRedirect('', '/home')).toBe('/home');
    expect(safeRedirect(null, '/home')).toBe('/home');
  });
});
