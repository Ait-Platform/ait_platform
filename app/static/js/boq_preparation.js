(() => {
  const node = document.getElementById('boq-preparation-config');
  if (!node) return;
  const config = JSON.parse(node.textContent);
  const form = document.getElementById('carpentry-quote-form');
  const field = form?.elements.namedItem('quote_key');
  const tab = crypto.randomUUID();
  const read = () => { try { return localStorage.getItem(config.storage_key); } catch { return null; } };
  const store = key => { try { localStorage.setItem(config.storage_key, key); } catch { /* Storage may be disabled. */ } };
  let key = config.quote_key || read() || field?.value;
  if (!/^[a-f0-9]{32}$/i.test(key || '')) key = crypto.randomUUID().replaceAll('-', '');
  if (field) field.value = key;
  if (!config.quote_key) store(key);
  else if (read() === key) { try { localStorage.removeItem(config.storage_key); } catch {} }
  let tick = 0, active = false, lastInput = Date.now();
  let tokenAt = Date.now(), refreshing = null;
  function refreshToken() {
    if (!refreshing) refreshing = fetch(config.url, {credentials: 'same-origin', cache: 'no-store'})
      .then(response => { if (!response.ok) throw new Error('Unavailable'); return response.json(); })
      .then(data => { config.csrf = data.csrf; tokenAt = Date.now(); })
      .finally(() => { refreshing = null; });
    return refreshing;
  }
  const visible = () => !document.hidden && document.hasFocus() &&
    (!form || !document.getElementById('quote-area-form')?.hidden);
  function send(action, target = key) {
    const sequence = ++tick;
    const post = () => {
      const body = new URLSearchParams({quote_key: target, tab_key: tab, tick: String(sequence), action, csrf_token: config.csrf});
      // keepalive persists the final pause through navigation without blocking it.
      return fetch(config.url, {method: 'POST', body, credentials: 'same-origin', keepalive: true});
    };
    const ready = Date.now()-tokenAt >= 1800000 ? refreshToken() : Promise.resolve();
    ready.then(post).then(response => {
      if (response.status === 400) return refreshToken().then(post);
    }).catch(() => {});
  }
  function update() {
    const next = visible() && Date.now() - lastInput < 60000;
    if (next || active) send(next ? 'active' : 'pause');
    active = next;
  }
  let idleTimeout;
  function interacted() {
    lastInput = Date.now();
    clearTimeout(idleTimeout);
    idleTimeout = setTimeout(update, 60000);
    if (!active) update();
  }
  for (const event of ['pointerdown', 'pointermove', 'keydown', 'input', 'wheel']) {
    document.addEventListener(event, () => {
      interacted();
    }, {passive: true});
  }
  window.addEventListener('blur', () => { if (active) send('pause'); active = false; });
  window.addEventListener('focus', () => { interacted(); update(); });
  document.addEventListener('visibilitychange', () => { if (!document.hidden) interacted(); update(); });
  window.addEventListener('pagehide', () => { if (active) send('pause'); active = false; });
  window.addEventListener('pageshow', () => { interacted(); update(); });
  document.addEventListener('carpentry:new-quote', () => {
    if (active) send('pause');
    key = crypto.randomUUID().replaceAll('-', '');
    field.value = key; store(key); active = false; interacted(); update();
  });
  // The shared hub changes visibility without navigation.
  if (form) new MutationObserver(update).observe(document.getElementById('quote-area-form'), {attributes: true, attributeFilter: ['hidden']});
  setInterval(update, 15000);
  idleTimeout = setTimeout(update, 60000);
  update();
})();
