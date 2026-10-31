/* Run with node tests/test_pm_municipal_accounts_lookup.cjs; no browser/network. */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const source=fs.readFileSync('app/static/js/pm_municipal_accounts_lookup.js','utf8');
class Element {
  constructor(value='') { this.value=value;this.dataset={};this.listeners={};this.children=[];this.textContent=''; }
  addEventListener(name,fn) { this.listeners[name]=fn; }
  replaceChildren(...children) { this.children=children; }
  appendChild(child) { this.children.push(child); }
}
function harness(fetcher) {
  const ids={};for (const name of ['municipal-account-form','account_number','account-lookup-status','account-lookup-details','account-lookup-retry','owner_name','water_number','electricity_number','water_opening_reading','water_opening_date','electricity_opening_reading','electricity_opening_date','water_latest_recorded','electricity_latest_recorded','account-association','association-confirmed','associate-account','association-error']) ids[name]=new Element();
  const form=ids['municipal-account-form'];form.dataset={lookupUrl:'/billing/utilities/26/accounts/lookup',existingAccount:'no',associateUrl:'/billing/utilities/26/accounts/associate'};
  const account=new Element(),version=new Element(),csrf=new Element('csrf-value'),buttons=[new Element(),new Element()];
  const groups=['water','electricity'].map(kind=>({dataset:{meterBaseline:kind,established:'no'}}));
  form.querySelectorAll=selector=>selector.includes('save_action')?buttons:groups;
  form.querySelector=selector=>selector.includes('account_id')?account:selector.includes('csrf_token')?csrf:version;
  vm.runInNewContext(source,{URL,URLSearchParams,AbortController,fetch:fetcher,window:{location:{origin:'https://ait.example'}},setTimeout:()=>1,clearTimeout:()=>{},document:{getElementById:key=>ids[key],createElement:()=>new Element()}});
  return {ids,account,version,buttons,async search(number) { ids.account_number.value=number;ids.account_number.listeners.input();await ids.account_number.listeners.change(); }};
}
function response(body,type='application/json') { return {ok:true,status:200,headers:{get:()=>type},json:async()=>body}; }
const own={found:true,association:'selected_property',editable:true,details_available:true,account_id:1,version:'saved-version',message:'Selected property',values:{account_number:'MET-26',owner_name:'Recorded Owner',water_number:'W-26',electricity_number:'',water_opening_reading:'10',water_opening_date:'2020-01-01',electricity_opening_reading:'',electricity_opening_date:''},meters:{water:[{number:'W-26',opening:{value:'10',date:'2020-01-01'},latest:{value:'45',date:'2026-10-01'}}],electricity:[]},owner_email:'recorded@example.com',owner_address:'Saved address'};
(async()=>{
  let requested;
  const selected=harness(async url=>{requested=String(url);return response(own);});await selected.search('met-26');
  assert.match(requested,/utilities\/26\/accounts\/lookup/);assert.equal(selected.ids.owner_name.value,'Recorded Owner');
  assert.equal(selected.ids.account_number.value,'MET-26');assert.equal(selected.account.value,1);assert.equal(selected.version.value,'saved-version');
  assert.equal(selected.ids['account-lookup-status'].hidden,true);assert.equal(selected.buttons[0].disabled,false);
  for (const association of ['unassigned','other_property']) {
    const review=harness(async()=>response({...own,association,editable:false,message:'Read only'}));await review.search('MET-26');
    assert.equal(review.ids.owner_name.value,'Recorded Owner');assert.equal(review.account.value,1);assert.equal(review.ids.owner_name.readOnly,true);assert.equal(review.buttons[0].disabled,true);
  }
  const denied=harness(async()=>response({found:true,association:'other_property',editable:false,details_available:false,message:'Restricted'}));denied.ids.owner_name.value='Previously typed';await denied.search('FOREIGN');
  assert.equal(denied.ids.owner_name.value,'');assert.equal(denied.buttons[0].disabled,true);
  const fresh=harness(async()=>response({found:false,association:'not_found',editable:true,message:'Create new account'}));await fresh.search('NEW');assert.equal(fresh.buttons[0].disabled,false);assert.equal(fresh.account.value,'');
  const html=harness(async()=>response(null,'text/html'));await html.search('MET-26');assert.equal(html.buttons[0].disabled,true);assert.match(html.ids['account-lookup-status'].textContent,/sign-in page/);assert.equal(html.ids['account-lookup-retry'].hidden,false);
  const pending=[];const racing=harness(()=>new Promise(resolve=>pending.push(resolve)));
  racing.ids.account_number.value='OLD';racing.ids.account_number.listeners.input();const first=racing.ids.account_number.listeners.change();
  racing.ids.account_number.value='NEW';racing.ids.account_number.listeners.input();const second=racing.ids.account_number.listeners.change();
  pending[1](response({found:false,editable:true,message:'New account'}));await second;pending[0](response(own));await first;
  assert.equal(racing.ids.account_number.value,'NEW');assert.equal(racing.account.value,'');assert.equal(racing.buttons[0].disabled,false);
  const unassigned=harness(async()=>response({...own,association:'unassigned',editable:true,version:'review-token'}));
  await unassigned.search('MET-26');assert.equal(unassigned.account.value,1);assert.equal(unassigned.version.value,'review-token');assert.equal(unassigned.buttons[0].disabled,false);
  console.log('8 lookup UI scenarios passed: populate, unassigned, other property, restricted details, new account, HTML failure and stale response.');
})().catch(error=>{ console.error(error);process.exitCode=1; });
