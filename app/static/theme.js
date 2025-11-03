(function () {
  const ThemeKey = 'pref_theme';

  function current() {
    return localStorage.getItem(ThemeKey) || 'light';
  }

  function apply(mode) {
    const root = document.documentElement;
    root.classList.toggle('theme-dark', mode === 'dark');
    if (mode !== 'dark') {
      root.classList.remove('theme-dark');
    }
    return mode;
  }

  function set(mode) {
    localStorage.setItem(ThemeKey, mode);
    return apply(mode);
  }

  function toggle() {
    const next = current() === 'dark' ? 'light' : 'dark';
    return set(next);
  }

  function init() {
    apply(current());
  }

  window.Theme = {
    key: ThemeKey,
    current,
    apply,
    set,
    toggle,
    init,
  };
})();
