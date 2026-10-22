(function () {
  'use strict';
  const root = document.getElementById('lease-kit-document');
  if (!root || !root.dataset.pdfUrl) return;
  const byId = name => document.getElementById('lease-kit-' + name);
  const status = byId('status'), viewport = byId('viewport'), canvas = byId('canvas');
  const pageInput = byId('page'), emailForm = byId('email-form'), emailStatus = byId('email-status');
  const buttons = {};
  ['play', 'pause', 'zoom-out', 'zoom-in', 'previous', 'next', 'print', 'download', 'email'].forEach(name => { buttons[name] = byId(name); });
  let pdf, blobUrl, token, page = 1, zoom = 1, busy = false, timer = null, failed = false;
  function pause() { if (timer !== null) clearInterval(timer); timer = null; update(); }
  function update() {
    const unavailable = !pdf || failed;
    Object.values(buttons).forEach(button => { button.disabled = unavailable; });
    pageInput.disabled = unavailable || busy;
    if (unavailable) return;
    buttons.play.disabled = busy || timer !== null || page === pdf.numPages;
    buttons.pause.disabled = timer === null;
    buttons.previous.disabled = busy || page === 1;
    buttons.next.disabled = busy || page === pdf.numPages;
    buttons['zoom-out'].disabled = busy || zoom <= 0.5;
    buttons['zoom-in'].disabled = busy || zoom >= 3;
    buttons.email.disabled = !token;
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
    } catch (error) { failed = true; pause(); status.textContent = 'The PDF could not be displayed. Reload to retry.'; }
    finally { busy = false; update(); }
  }
  function navigate(value) {
    if (busy || !pdf) return;
    const next = Number(value);
    if (!Number.isInteger(next) || next < 1 || next > pdf.numPages) { update(); return; }
    page = next; show();
  }
  buttons.previous.onclick = () => { pause(); navigate(page - 1); };
  buttons.next.onclick = () => { pause(); navigate(page + 1); };
  pageInput.onchange = () => { const value = pageInput.value; pause(); navigate(value); };
  pageInput.onkeydown = event => { if (event.key === 'Enter') { event.preventDefault(); const value = pageInput.value; pause(); navigate(value); } };
  buttons['zoom-out'].onclick = () => { if (!busy) { zoom = Math.max(0.5, zoom - 0.25); show(); } };
  buttons['zoom-in'].onclick = () => { if (!busy) { zoom = Math.min(3, zoom + 0.25); show(); } };
  buttons.play.onclick = () => {
    if (!pdf || timer !== null || page === pdf.numPages) return;
    timer = setInterval(() => { if (!busy) { if (page < pdf.numPages) navigate(page + 1); else pause(); } }, 5000);
    update();
  };
  buttons.pause.onclick = pause;
  buttons.download.onclick = () => {
    if (!blobUrl) return;
    const link = document.createElement('a'); link.href = blobUrl; link.download = root.dataset.filename;
    document.body.appendChild(link); link.click(); link.remove();
  };
  buttons.print.onclick = () => {
    if (!blobUrl) return;
    pause();
    const old = byId('print-frame'); if (old) old.remove();
    const frame = document.createElement('iframe'); frame.id = 'lease-kit-print-frame';
    frame.title = 'Print selected PDF'; frame.style.cssText = 'position:fixed;width:1px;height:1px;left:-10000px;border:0';
    frame.onload = () => { try { frame.contentWindow.focus(); frame.contentWindow.print(); } catch (error) { status.textContent = 'Printing is unavailable in this browser. Download PDF to print the original file.'; } };
    frame.src = blobUrl; document.body.appendChild(frame);
  };
  buttons.email.onclick = () => { pause(); emailForm.hidden = false; emailForm.elements.recipient.focus(); };
  byId('email-cancel').onclick = () => { emailForm.hidden = true; };
  emailForm.onsubmit = async event => {
    event.preventDefault();
    if (!token || !emailForm.reportValidity()) return;
    const send = byId('email-send'); if (send.disabled) return;
    send.disabled = true; emailStatus.textContent = 'Submitting PDF...';
    const data = new FormData(emailForm); data.set('document_token', token);
    try {
      const response = await fetch(root.dataset.emailUrl, { method: 'POST', credentials: 'same-origin', body: data, redirect: 'error' });
      if (!response.ok) { emailStatus.textContent = response.status === 409 ? 'The document changed. Reload before emailing.' : 'Email was not confirmed. Check the recipient and reload before retrying.'; return; }
      const result = await response.json(); emailStatus.textContent = result.message;
    } catch (error) { emailStatus.textContent = 'Email submission was not confirmed. Check before retrying.'; }
    finally { send.disabled = false; }
  };
  window.addEventListener('pagehide', () => { pause(); if (blobUrl) URL.revokeObjectURL(blobUrl); });
  (async () => {
    try {
      if (!window.pdfjsLib) throw new Error('PDF viewer unavailable');
      // Reuse the PDF.js runtime and worker used by AIT's controlled document viewer.
      window.pdfjsLib.GlobalWorkerOptions.workerSrc = 'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/2.16.105/pdf.worker.min.js';
      const response = await fetch(root.dataset.pdfUrl, { credentials: 'same-origin', cache: 'no-store', redirect: 'error' });
      if (!response.ok || !(response.headers.get('Content-Type') || '').includes('application/pdf')) throw new Error('PDF unavailable');
      token = response.headers.get('X-Lease-PDF-Token');
      const bytes = await response.arrayBuffer();
      blobUrl = URL.createObjectURL(new Blob([bytes], { type: 'application/pdf' }));
      pdf = await window.pdfjsLib.getDocument({ data: new Uint8Array(bytes), isEvalSupported: false }).promise;
      await show();
    } catch (error) { failed = true; status.textContent = 'The PDF is unavailable. Reload to retry.'; update(); }
  })();
}());
