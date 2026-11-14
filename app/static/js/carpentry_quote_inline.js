(() => {
  const register=document.getElementById('debtor-quote-register');
  if(!register) return;
  let editor=null, loading=null, generation=0, saving=false;
  const parse=html=>new DOMParser().parseFromString(html,'text/html');
  function close(focus=false) {
    generation++;loading?.abort();loading=null;
    if(editor) {
      editor.link.setAttribute('aria-expanded','false');
      editor.row.remove();
      if(focus && editor.link.isConnected) editor.link.focus();
      editor=null;
    }
  }
  function error(message) {
    let alert=editor?.row.querySelector('[data-inline-error]');
    if(!editor) return;
    if(!alert) {
      alert=document.createElement('p');alert.dataset.inlineError='';alert.setAttribute('role','alert');
      alert.className='mt-3 text-red-700';editor.cell.append(alert);
    }
    alert.textContent=message;
  }
  function preview(form) {
    const math=window.protradeQuoteAmounts;
    if(!math) return;
    form.querySelector('#quote-item-line-total').textContent=math.money(math.amount(
      form.elements.quantity.value,math.combinedRate(form.elements.material_rate.value,form.elements.labour_rate.value)));
  }
  function render(doc) {
    const source=doc.getElementById('quote-item-edit');
    if(!source) throw new Error('No item form');
    const form=document.importNode(source,true);
    const heading=document.createElement('h3');heading.className='text-lg font-semibold';
    heading.textContent=doc.querySelector('h1').textContent;
    editor.cell.replaceChildren(heading,form);
    form.addEventListener('input',()=>preview(form));
    form.addEventListener('submit',save);
    preview(form);form.elements.quantity.focus();
  }
  async function save(event) {
    event.preventDefault();
    const form=event.currentTarget;
    if(saving || !form.reportValidity()) return;
    saving=true;const state=editor;
    const button=form.querySelector('[type="submit"]');button.disabled=true;
    try {
      const response=await fetch(form.action,{method:'POST',credentials:'same-origin',body:new URLSearchParams(new FormData(form))});
      const doc=parse(await response.text());
      if(!response.ok) {
        if(doc.getElementById('quote-item-edit')) render(doc);
        else error('Unable to save this item. Refresh the register and reopen it.');
        return;
      }
      const updated=doc.getElementById('quote-details-'+state.quoteId);
      const summary=doc.querySelector('[data-quote-summary="'+state.quoteId+'"]');
      const current=document.getElementById('quote-details-'+state.quoteId);
      if(!updated || !summary || !current) throw new Error('No updated quotation');
      const replacement=document.importNode(updated,true);replacement.hidden=false;
      document.querySelector('[data-quote-summary="'+state.quoteId+'"] > td:nth-child(3)').textContent=summary.children[2].textContent;
      close();current.replaceWith(replacement);
      replacement.querySelector('[data-quote-item-row="'+CSS.escape(state.itemId)+'"] [data-edit-quote-item]')?.focus();
    } catch {
      error('Unable to confirm the save. Reopen this item before trying again.');
    } finally {
      saving=false;if(button.isConnected) button.disabled=false;
    }
  }
  register.addEventListener('click',async event=>{
    const cancel=event.target.closest('[data-cancel-quote-item]');
    if(cancel) {event.preventDefault();if(!saving) close(true);return;}
    const link=event.target.closest('[data-edit-quote-item]');
    if(!link) return;
    event.preventDefault();if(saving) return;
    close();
    const parent=link.closest('[data-quote-item-row]');
    const row=document.createElement('tr');row.id='quote-item-inline-row';row.className='quote-inline-editor bg-blue-50';
    const cell=row.insertCell();cell.colSpan=8;cell.className='p-4';cell.textContent='Loading item...';
    parent.after(row);
    editor={row,cell,link,quoteId:link.dataset.editQuoteItem,itemId:parent.dataset.quoteItemRow};
    link.setAttribute('aria-expanded','true');
    loading=new AbortController();const version=generation;
    try {
      const response=await fetch(link.href,{credentials:'same-origin',signal:loading.signal});
      if(!response.ok) throw new Error('Item unavailable');
      const doc=parse(await response.text());
      if(version===generation && editor) render(doc);
    } catch(exception) {
      if(exception.name!=='AbortError' && version===generation) error('Unable to open this item. Refresh the register and try again.');
    }
  });
  document.addEventListener('carpentry:close-inline-item',event=>{
    if(saving) event.preventDefault();else close();
  });
})();
