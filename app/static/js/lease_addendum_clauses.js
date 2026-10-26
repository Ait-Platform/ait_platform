(function(){
 'use strict';
 const root=document.getElementById('addendum-clauses');if(!root)return;
 const get=name=>document.getElementById('addendum-clause-'+name),status=get('status');let state=JSON.parse(get('data').textContent),source='certificate',busy=false;
 const node=(tag,text)=>{const value=document.createElement(tag);if(text!==undefined)value.textContent=text;return value;};
 function choice(entry,checked){const input=node('input');input.type='checkbox';input.checked=checked;input.disabled=busy;input.setAttribute('aria-label','Select '+entry.source_label+' '+entry.reference);input.onchange=()=>change(entry.id,input.checked);return input;}
 function original(entry,expanded=false){const details=node('details');details.open=expanded;details.append(node('summary','Original wording'));const text=node('pre',entry.original);text.style.whiteSpace='pre-wrap';text.style.overflowWrap='anywhere';details.append(text,node('p',entry.provenance));return details;}
 function render(){
  const selected=new Set(state.selected.map(entry=>entry.id)),catalog=get('catalog'),register=get('selected');catalog.replaceChildren();register.replaceChildren();
  root.querySelectorAll('[data-clause-source]').forEach(button=>{const active=button.dataset.clauseSource===source;button.setAttribute('aria-pressed',String(active));button.style.outline=active?'2px solid #2563eb':'';});
  const entries=state.catalog.filter(entry=>entry.source===source);
  for(const entry of entries){const row=node('tr'),cell=node('td');cell.append(choice(entry,selected.has(entry.id)));const clause=node('td',entry.source_label+' '+entry.reference+' — '+entry.description),links=node('td');
   for(const relation of state.relationships){const targetId=relation.left===entry.id?relation.right:relation.right===entry.id?relation.left:null,target=state.catalog.find(item=>item.id===targetId);if(!target)continue;const link=node('a',target.source_label+' '+target.reference);link.href='#'+target.id;link.title=relation.evidence;link.onclick=event=>{event.preventDefault();source=target.source;render();const targetRow=document.getElementById('clause-row-'+target.id);if(targetRow&&targetRow.scrollIntoView)targetRow.scrollIntoView({block:'nearest'});};links.append(link,node('span',' '));}
   row.id='clause-row-'+entry.id;row.append(cell,clause,links);if(selected.has(entry.id))clause.append(original(entry,true));catalog.append(row);}
  if(!entries.length){const row=node('tr'),cell=node('td','No automatically identified provisions in this source.');cell.colSpan=3;row.append(cell);catalog.append(row);}
  for(const entry of state.selected){const row=node('tr'),cell=node('td'),statusCell=node('td','Selected — '+(entry.source_status||'Saved original'));cell.append(choice(entry,true));statusCell.append(original(entry,true));row.append(cell,node('td',entry.source_label),node('td',entry.reference),node('td',entry.description),statusCell);register.append(row);}
  get('count').textContent=state.selected.length+' selected';const related=get('relationships');related.replaceChildren();let count=0;
  for(const relationship of state.relationships){const left=selected.has(relationship.left),right=selected.has(relationship.right);if(!left&&!right)continue;const targetId=left?relationship.right:relationship.left,entry=state.catalog.find(row=>row.id===targetId);if(!entry)continue;
   const line=node('div'),label=node('label');label.append(choice(entry,selected.has(entry.id)),node('span',' '+entry.source_label+' — '+entry.reference+' ('+(selected.has(entry.id)?'Selected':'Suggested only')+')'));line.append(label,node('p',relationship.evidence),original(entry));related.append(line);count++;
  }
  if(!count)related.append(node('p','No verified explicit relationships identified for the selected provisions.'));
  if(root.dispatchEvent&&typeof CustomEvent!=='undefined')root.dispatchEvent(new CustomEvent('addendum-selection-change',{bubbles:true,detail:state}));
 }
 async function save(data){if(busy)return;busy=true;data.set('csrf_token',get('csrf').value);data.set('clause_lease',state.lease_key);data.set('register_version',String(state.version));status.textContent='Saving selection...';render();
  try{const response=await fetch(root.dataset.url,{method:'POST',credentials:'same-origin',body:data,redirect:'error'});let result;try{result=await response.json();}catch(error){throw Error('Selection could not be saved ('+response.status+').');}if(!response.ok)throw Error(result.error||'Selection could not be saved.');state=result;status.textContent='Saved';}
  catch(error){status.textContent=error.message;}finally{busy=false;render();}
 }
 function change(id,selected){const data=new FormData();data.set('action','addendum_clause_selection');data.set('provision_id',id);data.set('selected',selected?'yes':'no');return save(data);}
 root.querySelectorAll('[data-clause-source]').forEach(button=>button.onclick=()=>{source=button.dataset.clauseSource;render();});
 const manual=document.getElementById('addendum-manual-form');if(manual)manual.onsubmit=event=>{event.preventDefault();const data=new FormData(manual);data.set('action','addendum_manual_clause');return save(data);};
 render();
}());
