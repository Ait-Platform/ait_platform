(() => {
  const root = document.getElementById('quote-builder-data');
  if (!root) return;
  const data = JSON.parse(root.textContent);
  const search = document.getElementById('quote-work-select');
  const body = document.getElementById('quote-item-lines');
  const add = document.getElementById('quote-add-item');
  const quantity = document.getElementById('quote-work-quantity');
  const message = document.getElementById('quote-work-message');
  const material = document.getElementById('quote-work-material');
  const labour = document.getElementById('quote-work-labour');
  const selected = new Map();
  let choice = null;
  const money = cents => `${cents / 100n}.${String(cents % 100n).padStart(2, '0')}`;
  function amount(value, rate) {
    const match = /^(?:(\d+)(?:\.(\d{0,3}))?|\.(\d{1,3}))$/.exec(value);
    if (!match || !rate) return 0n;
    const millis = BigInt(match[1] || '0') * 1000n + BigInt((match[2] || match[3] || '').padEnd(3, '0'));
    return (millis * BigInt(rate) + 500n) / 1000n;
  }
  function rateCents(value) {
    const match = /^(?:(\d+)(?:\.(\d{0,2}))?|\.(\d{1,2}))$/.exec(value);
    return match ? BigInt(match[1] || '0')*100n+BigInt((match[2] || match[3] || '').padEnd(2,'0')) : null;
  }
  function combinedRate(materialValue,labourValue) {
    const m=rateCents(materialValue), l=rateCents(labourValue);
    return m===null || l===null ? '' : String(m+l);
  }
  function validValues(materialInput,labourInput,quantityInput) {
    return [materialInput,labourInput,quantityInput].every(field=>field.value!=='' && field.checkValidity()) &&
      combinedRate(materialInput.value,labourInput.value)!=='' && /^(?:\d+(?:\.\d{0,3})?|\.\d{1,3})$/.test(quantityInput.value);
  }
  const normalize = value => String(value || '').trim().replace(/\s+/g,' ').toLowerCase();
  function preview() {
    const query = normalize(search.value);
    const matches = query ? data.choices.filter(item => [item.label,item.code,item.description,
      item.label.slice(item.label.indexOf('\u2014')+1)].some(value => normalize(value)===query)) : [];
    // Accept a native-list selection or an exact typed code/description, never an ambiguous match.
    const nextChoice = matches.length===1 ? matches[0] : null;
    if (nextChoice !== choice) {
      choice = nextChoice;
    for (const [field,key] of [['code','code'],['description','description'],['unit','unit'],['material','material_rate'],['labour','labour_rate']]) {
      document.getElementById(`quote-work-${field}`).value = choice?.[key] || '';
    }
    }
    quantity.min = '0.001';
    quantity.step = '0.001';
    const duplicate = choice && selected.has(String(choice.source_id));
    add.disabled = !choice || duplicate || !validValues(material,labour,quantity);
    message.textContent = !choice ? (query ? 'Choose a matching item from the list, or enter its full code or description.' : '') : duplicate ? 'This item is already listed. Use Include or edit its quantity below.' : !validValues(material,labour,quantity) ? 'Enter valid non-negative rates and a positive quantity to add this item.' : '';
    document.getElementById('quote-work-total').value = money(choice ? amount(quantity.value,combinedRate(material.value,labour.value)) : 0n);
  }
  function calculate() {
    let total = 0n;
    let included = 0;
    let invalidIncluded = false;
    selected.forEach(({row,input,materialInput,labourInput,include,identity}) => {
      identity.disabled = !include.checked;
      for (const field of [input,materialInput,labourInput]) {field.disabled=false;field.required=include.checked;}
      if (include.checked) {
        if (validValues(materialInput,labourInput,input)) included += 1;
        else invalidIncluded = true;
      }
      const cents = include.checked ? amount(input.value,combinedRate(materialInput.value,labourInput.value)) : 0n;
      row.querySelector('.line-total').textContent = money(cents);
      total += cents;
    });
    document.getElementById('quote-grand-total').value = money(total);
    document.getElementById('quote-save').disabled = included === 0 || invalidIncluded;
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
    input.value=value;input.required=true;input.min='0.001';
    input.step='0.001';input.max='99999999999.999';
    input.className='w-28 rounded-lg border border-slate-300 px-3 py-2';input.setAttribute('aria-label',`Quantity ${item.code}`);
    cell('').append(input);
    const rateInput=(kind,label)=>{
      const field=document.createElement('input');field.type='number';field.name=`${kind}_${key}`;
      field.value=item[kind];field.min='0';field.max='999999999999.99';field.step='0.01';field.required=true;
      field.className=input.className;field.setAttribute('aria-label',`${label} ${item.code}`);cell('').append(field);
      field.addEventListener('input',calculate);field.addEventListener('change',calculate);return field;
    };
    const materialInput=rateInput('material_rate','Material Rate'),labourInput=rateInput('labour_rate','Labour Rate');
    cell('0.00').classList.add('line-total');
    const remove=document.createElement('button');remove.type='button';remove.textContent='Remove Item';
    remove.className='quote-remove mt-1 block rounded-lg bg-red-50 px-2 py-1 text-xs';remove.setAttribute('aria-label',`Remove ${item.code}`);
    code.append(remove);
    selected.set(key,{item,row,input,materialInput,labourInput,include,identity});
    include.addEventListener('change',calculate);
    input.addEventListener('input',calculate);
    input.addEventListener('change',calculate);
    remove.addEventListener('click',()=>{selected.delete(key);row.remove();calculate();});
  }
  search.addEventListener('input',()=>{quantity.value='';preview();});
  search.addEventListener('change',preview);
  quantity.addEventListener('input',preview);
  quantity.addEventListener('change',preview);
  for (const field of [material,labour]) {field.addEventListener('input',preview);field.addEventListener('change',preview);}
  add.addEventListener('click',()=>{
    preview();
    if (add.disabled) return;
    const invalid=[material,labour,quantity].find(field=>!field.value || !field.checkValidity());
    if (invalid) {
      message.textContent='Enter non-negative rates (up to two decimals) and a positive quantity (up to three decimals).';invalid.focus();return;
    }
    addLine({...choice,material_rate:material.value,labour_rate:labour.value},quantity.value);search.value='';quantity.value='';calculate();
  });
  function restoreServerState() {
    const options=document.getElementById('quote-work-options');
    options.replaceChildren(...data.choices.map(item=>{
      const option=document.createElement('option');option.value=item.label;
      option.textContent=item.material_rate!=='' && item.labour_rate!==''?'Default rates available':'Enter quotation rates';return option;
    }));
    selected.clear();body.replaceChildren();search.value='';quantity.value='';
    choice=null;for(const field of ['code','description','unit','material','labour']) document.getElementById(`quote-work-${field}`).value='';
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
  document.getElementById('carpentry-quote-form').addEventListener('submit',event=>{
    for (const {input,materialInput,labourInput,include} of selected.values()) {
      if (!include.checked) continue;
      const invalid=[input,materialInput,labourInput].find(field=>!field.checkValidity());
      if(invalid) {event.preventDefault();invalid.reportValidity();return;}
    }
  });
  restoreServerState();
  // Initial pageshow can arrive after the user starts typing (e.g. slow page assets).
  // Only a genuine back/forward-cache restore should replace their working values.
  window.addEventListener('pageshow',event=>{if(event.persisted) restoreServerState();});
})();
