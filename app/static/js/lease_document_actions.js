(function () {
  'use strict';
  const root = document.getElementById('lease-document-actions');
  if (!root) return;
  const byId = name => document.getElementById('lease-actions-' + name);
  const status = byId('status'), form = byId('email-form'), emailStatus = byId('email-status');
  const buttons = Array.from(root.querySelectorAll('button[data-action]'));
  let busy = false, selection = null, printFrame = null, printUrl = null;
  function setBusy(value) { busy = value; buttons.forEach(button => { button.disabled = value; }); byId('email-send').disabled = value; }
  async function load(row) {
    const response = await fetch(row.dataset.pdfUrl, { credentials: 'same-origin', cache: 'no-store', redirect: 'error' });
    if (!response.ok || !(response.headers.get('Content-Type') || '').includes('application/pdf')) throw new Error('PDF unavailable');
    return { bytes: await response.arrayBuffer(), token: response.headers.get('X-Lease-PDF-Token') };
  }
  function clearPrint() {
    if (printFrame) printFrame.remove();
    if (printUrl) URL.revokeObjectURL(printUrl);
    printFrame = printUrl = null;
  }
  buttons.forEach(button => {
    button.onclick = async () => {
      if (busy) return;
      const row = button.closest('tr');
      selection = null; form.hidden = true; emailStatus.textContent = '';
      setBusy(true); status.textContent = 'Loading ' + row.dataset.label + '...';
      try {
        const pdf = await load(row);
        if (button.dataset.action === 'print') {
          clearPrint();
          printUrl = URL.createObjectURL(new Blob([pdf.bytes], { type: 'application/pdf' }));
          printFrame = document.createElement('iframe');
          printFrame.title = 'Print ' + row.dataset.label;
          printFrame.style.cssText = 'position:fixed;width:1px;height:1px;left:-10000px;border:0';
          const frame = printFrame;
          printFrame.onload = () => {
            if (printFrame !== frame) return;
            try { frame.contentWindow.focus(); frame.contentWindow.print(); }
            catch (error) { status.textContent = 'Printing is unavailable in this browser. No document was changed.'; }
          };
          printFrame.src = printUrl; document.body.appendChild(printFrame);
        } else {
          if (!pdf.token) throw new Error('Saved draft required');
          selection = { token: pdf.token, emailUrl: row.dataset.emailUrl };
          byId('email-title').textContent = 'Email ' + row.dataset.label;
          form.hidden = false; form.elements.recipient.focus();
        }
        status.textContent = '';
      } catch (error) { status.textContent = 'This PDF is unavailable. Refresh the page before retrying.'; }
      finally { setBusy(false); }
    };
  });
  byId('email-cancel').onclick = () => { if (!busy) { selection = null; form.hidden = true; } };
  form.onsubmit = async event => {
    event.preventDefault();
    if (busy || !selection || !form.reportValidity()) return;
    const chosen = selection;
    const data = new FormData(form); data.set('document_token', chosen.token);
    setBusy(true); emailStatus.textContent = 'Submitting PDF...';
    try {
      const response = await fetch(chosen.emailUrl, { method: 'POST', credentials: 'same-origin', body: data, redirect: 'error' });
      if (!response.ok) { emailStatus.textContent = response.status === 409 ? 'The document changed. Select the row again before emailing.' : 'Email was not confirmed. Check the recipient and select the row again.'; return; }
      const result = await response.json(); emailStatus.textContent = result.message;
    } catch (error) { emailStatus.textContent = 'Email submission was not confirmed. Check before retrying.'; }
    finally { selection = null; setBusy(false); }
  };
  window.addEventListener('pagehide', clearPrint);
}());
