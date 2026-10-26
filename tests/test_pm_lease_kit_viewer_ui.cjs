const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
module.exports = async function run(rootPath, prefix="lease-kit", pageCount=9, companion=false, lazy=false) {
  const elements = new Map(), added = [], requests = [], timers = new Map(), events = {};
  const bytes = new TextEncoder().encode('%PDF-actual protected bytes').buffer;
  let printed = 0, revoked = 0, downloaded = 0, rendered = [], nextTimer = 1, blob;
  function element(id) {
    return { id, disabled: true, hidden: true, textContent: '', value: '1', style: {}, dataset: {}, clientWidth: 800, clientHeight: 900,
      addEventListener(name,fn){(this.events??={})[name]=fn;}, setAttribute() {}, getContext() { return {}; }, focus() {}, reportValidity() { return true; }, remove() {}, click() { downloaded++; } };
  }
  ['document','status','viewport','canvas','page','page-count','email-form','email-status','email-cancel','email-send',
    'play','pause','zoom-out','zoom-in','previous','next','print','download','email'].forEach(name => elements.set(prefix+'-'+name, element(prefix+'-'+name)));
  const get = name => elements.get(prefix+'-'+name);
  get('document').dataset = { viewerPrefix:prefix, pdfUrl: '/protected/'+prefix+'.pdf', emailUrl: '/protected/kit/email', filename: 'AIT-Core-REVIEW.pdf' };
  if(companion){
    for(const name of ['document','status','viewport','canvas','page','page-count','zoom-out','zoom-in','previous','next'])elements.set('lease-sc-'+name,element('lease-sc-'+name));
    elements.get('lease-sc-document').dataset={viewerPrefix:'lease-sc',pdfUrl:'/protected/lease-sc.pdf'};
  }
  if(lazy)get('document').dataset.pdfUrl='';
  get('email-form').elements = { recipient: { focus() {} } }; get('email-send').disabled = false;
  const document = { querySelectorAll:()=>Array.from(elements.values()).filter(item=>item.id.endsWith('-document')), getElementById: id => elements.get(id), createElement: tag => {
    const item = element(tag);
    if (tag === 'iframe') item.contentWindow = { focus() {}, print() { printed++; } };
    return item;
  }, body: { appendChild(item) { added.push(item); if (item.onload) item.onload(); } } };
  const pdf = { numPages: pageCount, async getPage(number) { return {
    getViewport({ scale }) { return { width: 595*scale, height: 842*scale }; },
    render() { rendered.push(number); return { promise: Promise.resolve() }; }
  }; } };
  const sandbox = { document, Blob, Uint8Array, console, Error, Number, Object,
    URL: { createObjectURL(value) { blob = value; return 'blob:private-pdf'; }, revokeObjectURL() { revoked++; } },
    FormData: class { set(key,value) { this[key]=value; } },
    setInterval(fn) { const id=nextTimer++;timers.set(id,fn);return id; }, clearInterval(id) { timers.delete(id); },
    fetch: async (url, options) => { requests.push({url,options});return { ok:true,status:200,
      headers:{get(name){ return name==='Content-Type'?'application/pdf':'signed-selection'; }},
      arrayBuffer:async()=>bytes,json:async()=>({message:'PDF submitted'}) }; }
  };
  sandbox.window = { devicePixelRatio:1, pdfjsLib:{GlobalWorkerOptions:{},getDocument(options){
    assert.deepEqual(Array.from(options.data),Array.from(new Uint8Array(bytes)));
    return { promise:Promise.resolve(pdf) }; }}, addEventListener(name,fn){events[name]=fn;} };
  vm.runInNewContext(fs.readFileSync(rootPath+'/app/static/js/lease_kit.js','utf8'),sandbox);
  const settle=async()=>{for(let i=0;i<15;i++)await Promise.resolve();};
  if(lazy){assert.equal(requests.length,0);get('document').events['lease-pdf-load']({detail:{url:'/protected/'+prefix+'.pdf'}});}
  await settle();
  assert.equal(get('page-count').textContent,'of '+pageCount);assert.equal(get('previous').disabled,true);
  get('next').onclick();await settle();assert.equal(get('page').value,2);
  get('previous').onclick();await settle();assert.equal(get('page').value,1);
  const initialWidth=get('canvas').width;get('zoom-in').onclick();await settle();assert.ok(get('canvas').width>initialWidth);
  get('zoom-out').onclick();await settle();assert.equal(get('canvas').width,initialWidth);
  get('page').value=String(pageCount);get('page').onchange();await settle();assert.equal(get('next').disabled,true);
  assert.equal(requests.length,companion?2:1);if(companion){assert.equal(elements.get('lease-sc-page').value,1);assert.equal(elements.get('lease-sc-previous').disabled,true);assert.equal(elements.get('lease-sc-page-count').textContent,'of '+pageCount);}assert.equal(requests[0].url,'/protected/'+prefix+'.pdf');
  assert.equal(requests[0].options.credentials,'same-origin');
  assert.equal(added.length,0,'Inspection must not create external/print/download frames');
  for (const name of ['play','pause','print','download','email']) assert.equal(get(name).onclick,undefined);
  console.log('Inspection viewer passed: nine-page canvas, zoom, navigation, no document actions or external frames.');
};
