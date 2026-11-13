(() => {
  const root = document.getElementById('quote-builder-data');
  if (!root) return;
  const data = JSON.parse(root.textContent);
  const search = document.getElementById('quote-work-select');
  const body = document.getElementById('quote-item-lines');
  const add = document.getElementById('quote-add-item');
  const quantity = document.getElementById('quote-work-quantity');
  const message = document.getElementById('quote-work-message');
  const selected = new Map();
  let choice = null;
  const money = cents => `${cents / 100n}.${String(cents % 100n).padStart(2, '0')}`;
  function amount(value, rate) {
    const match = /^(\d+)(?:\.(\d{0,3}))?$/.exec(value);
    if (!match || !rate) return 0n;
    const millis = BigInt(match[1]) * 1000n + BigInt((match[2] || '').padEnd(3, '0'));
    return (millis * BigInt(rate) + 500n) / 1000n;
  }
  function preview() {
    choice = data.choices.find(item => item.label === search.value) || null;
    for (const [field,key] of [['code','code'],['description','description'],['unit','unit'],['material','material_rate'],['labour','labour_rate']]) {
      document.getElementById(`quote-work-${field}`).value = choice?.[key] || '';
    }
    quantity.min = choice?.unit === 'Each' ? '1' : '0.001';
    quantity.step = choice?.unit === 'Each' ? '1' : '0.001';
    const duplicate = choice && selected.has(String(choice.source_id));
    add.disabled = !choice?.source_id || duplicate;
    message.textContent = !choice ? '' : !choice.source_id ? "Save rates for this item in the shop's Price List before adding it." : duplicate ? 'This item is already listed. Use Include or edit its quantity below.' : '';
    document.getElementById('quote-work-total').value = money(choice?.source_id ? amount(quantity.value,choice.rate_cents) : 0n);
  }
  function calculate() {
    let total = 0n;
    let included = 0;
    selected.forEach(({item,row,input,include,identity}) => {
      input.disabled = !include.checked;identity.disabled = !include.checked;
      if (include.checked) included += 1;
      const cents = include.checked ? amount(input.value,item.rate_cents) : 0n;
      row.querySelector('.line-total').textContent = money(cents);
      total += cents;
    });
    document.getElementById('quote-grand-total').value = money(total);
    document.getElementById('quote-save').disabled = included === 0;
    if (!selected.size && !document.getElementById('quote-empty')) {
      const row = body.insertRow();row.id='quote-empty';
      const cell = row.insertCell();cell.colSpan=8;cell.className='p-6 text-center text-slate-600';
      cell.textContent='No items added. Select a work item above.';
    }
    preview();
  }
  function addLine(item,value) {
    const key = String(item.source_id);
    if (selected.has(key)) return;
    document.getElementById('quote-empty')?.remove();
    const row = body.insertRow();row.className='quote-line border-t border-slate-200';row.dataset.sourceId=key;
    const cell = text => { const node=row.insertCell();node.className='p-3';node.textContent=text;return node; };
    const include=document.createElement('input');include.type='checkbox';include.checked=true;
    include.setAttribute('aria-label',`Include ${item.code}`);cell('').append(include);
    const code=cell(item.code), identity=document.createElement('input');
    identity.type='hidden';identity.name='item_id[]';identity.value=key;code.append(identity);
    cell(item.description);cell(item.unit);
    const input=document.createElement('input');input.type='number';input.name=`quantity_${key}`;
    input.value=value;input.required=true;input.min=item.unit==='Each'?'1':'0.001';
    input.step=item.unit==='Each'?'1':'0.001';input.max='99999999999.999';
    input.className='w-28 rounded-lg border border-slate-300 px-3 py-2';input.setAttribute('aria-label',`Quantity ${item.code}`);
    cell('').append(input);cell(item.material_rate);cell(item.labour_rate);cell('0.00').classList.add('line-total');
    const remove=document.createElement('button');remove.type='button';remove.textContent='Remove Item';
    remove.className='quote-remove mt-1 block rounded-lg bg-red-50 px-2 py-1 text-xs';remove.setAttribute('aria-label',`Remove ${item.code}`);
    code.append(remove);
    selected.set(key,{item,row,input,include,identity});
    include.addEventListener('change',calculate);
    input.addEventListener('input',calculate);
    remove.addEventListener('click',()=>{selected.delete(key);row.remove();calculate();});
  }
  search.addEventListener('input',()=>{quantity.value='';preview();});
  search.addEventListener('change',preview);
  quantity.addEventListener('input',preview);
  add.addEventListener('click',()=>{
    preview();
    if (add.disabled) return;
    if (!quantity.value || !quantity.checkValidity()) {
      message.textContent='Enter a positive quantity using the item unit.';quantity.focus();return;
    }
    addLine(choice,quantity.value);search.value='';quantity.value='';calculate();
  });
  function restoreServerState() {
    const options=document.getElementById('quote-work-options');
    options.replaceChildren(...data.choices.map(item=>{
      const option=document.createElement('option');option.value=item.label;
      option.textContent=item.source_id?'Saved shop rates':'Save rates in Price List first';return option;
    }));
    selected.clear();body.replaceChildren();search.value='';quantity.value='';
    data.lines.forEach(line=>addLine(line,String(line.quantity)));calculate();
  }
  document.addEventListener('carpentry:new-quote',()=>{
    data.choices=data.fresh_choices;data.lines=[];
    const form=document.getElementById('carpentry-quote-form');
    form.elements.namedItem('quote_key').value=data.fresh_quote_key;
    form.elements.namedItem('area').value='new';
    const identity=form.elements.namedItem('quote_id');identity.value='';identity.disabled=true;
    document.getElementById('quote-save').textContent='Save Quote';
    document.getElementById('quote-form-title').textContent='New Quotation';
    const reference=document.getElementById('quote-current-reference');if(reference) reference.hidden=true;
    const error=form.querySelector('[role="alert"]');if(error) error.hidden=true;
    restoreServerState();search.focus();
  });
  restoreServerState();
  window.addEventListener('pageshow',restoreServerState);
})();
