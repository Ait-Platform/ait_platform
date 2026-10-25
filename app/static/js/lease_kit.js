(function () {
  'use strict';
  document.querySelectorAll('[data-lease-pdf-viewer]').forEach(function (root) {
  if (!root || !root.dataset.pdfUrl) return;
  const byId = name => document.getElementById((root.dataset.viewerPrefix || 'lease-kit') + '-' + name);
  const status = byId('status'), viewport = byId('viewport'), canvas = byId('canvas');
  const pageInput = byId('page');
  const buttons = {};
  ['zoom-out', 'zoom-in', 'previous', 'next'].forEach(name => { buttons[name] = byId(name); });
  let pdf, page = 1, zoom = 1, busy = false, failed = false;
  function update() {
    const unavailable = !pdf || failed;
    Object.values(buttons).forEach(button => { button.disabled = unavailable; });
    pageInput.disabled = unavailable || busy;
    if (unavailable) return;
    buttons.previous.disabled = busy || page === 1;
    buttons.next.disabled = busy || page === pdf.numPages;
    buttons['zoom-out'].disabled = busy || zoom <= 0.5;
    buttons['zoom-in'].disabled = busy || zoom >= 3;
    pageInput.value = page;
    pageInput.max = pdf.numPages;
    byId('page-count').textContent = 'of ' + pdf.numPages;
  }
  async function show() {
    if (busy || !pdf) return;
    busy = true; update();
    try {
      const sheet = await pdf.getPage(page), base = sheet.getViewport({ scale: 1 });
      const fit = Math.max(0.1, Math.min((viewport.clientWidth - 8) / base.width, (viewport.clientHeight - 8) / base.height));
      const view = sheet.getViewport({ scale: fit * zoom });
      const ratio = Math.min(window.devicePixelRatio || 1, 2);
      canvas.width = Math.ceil(view.width * ratio); canvas.height = Math.ceil(view.height * ratio);
      canvas.style.width = view.width + 'px'; canvas.style.height = view.height + 'px';
      await sheet.render({ canvasContext: canvas.getContext('2d'), viewport: view, transform: [ratio, 0, 0, ratio, 0, 0] }).promise;
      canvas.setAttribute('aria-label', 'PDF page ' + page + ' of ' + pdf.numPages);
      viewport.scrollTop = viewport.scrollLeft = 0;
      status.textContent = '';
    } catch (error) { failed = true; status.textContent = 'The PDF could not be displayed. Reload to retry.'; }
    finally { busy = false; update(); }
  }
  function navigate(value) {
    if (busy || !pdf) return;
    const next = Number(value);
    if (!Number.isInteger(next) || next < 1 || next > pdf.numPages) { update(); return; }
    page = next; show();
  }
  buttons.previous.onclick = () => { navigate(page - 1); };
  buttons.next.onclick = () => { navigate(page + 1); };
  pageInput.onchange = () => { const value = pageInput.value; navigate(value); };
  pageInput.onkeydown = event => { if (event.key === 'Enter') { event.preventDefault(); const value = pageInput.value; navigate(value); } };
  buttons['zoom-out'].onclick = () => { if (!busy) { zoom = Math.max(0.5, zoom - 0.25); show(); } };
  buttons['zoom-in'].onclick = () => { if (!busy) { zoom = Math.min(3, zoom + 0.25); show(); } };
  (async () => {
    try {
      if (!window.pdfjsLib) throw new Error('PDF viewer unavailable');
      // Reuse the PDF.js runtime and worker used by AIT's controlled document viewer.
      window.pdfjsLib.GlobalWorkerOptions.workerSrc = 'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/2.16.105/pdf.worker.min.js';
      const response = await fetch(root.dataset.pdfUrl, { credentials: 'same-origin', cache: 'no-store', redirect: 'error' });
      if (!response.ok || !(response.headers.get('Content-Type') || '').includes('application/pdf')) throw new Error('PDF unavailable');
      const bytes = await response.arrayBuffer();
      pdf = await window.pdfjsLib.getDocument({ data: new Uint8Array(bytes), isEvalSupported: false }).promise;
      await show();
    } catch (error) { failed = true; status.textContent = 'The PDF is unavailable. Reload to retry.'; update(); }
  })();
  });
}());
