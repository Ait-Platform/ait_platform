const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
module.exports = async function run(rootPath) {
  const elements = new Map(), added = [], requests = [], timers = new Map(), events = {};
  const bytes = new TextEncoder().encode('%PDF-actual protected bytes').buffer;
  let printed = 0, revoked = 0, downloaded = 0, rendered = [], nextTimer = 1, blob;
  function element(id) {
    return { id, disabled: true, hidden: true, textContent: '', value: '1', style: {}, dataset: {}, clientWidth: 800, clientHeight: 900,
      setAttribute() {}, getContext() { return {}; }, focus() {}, reportValidity() { return true; }, remove() {}, click() { downloaded++; } };
  }
  ['document','status','viewport','canvas','page','page-count','email-form','email-status','email-cancel','email-send',
    'play','pause','zoom-out','zoom-in','previous','next','print','download','email'].forEach(name => elements.set('lease-kit-'+name, element('lease-kit-'+name)));
  const get = name => elements.get('lease-kit-'+name);
  get('document').dataset = { pdfUrl: '/protected/kit.pdf', emailUrl: '/protected/kit/email', filename: 'AIT-Core-REVIEW.pdf' };
  get('email-form').elements = { recipient: { focus() {} } }; get('email-send').disabled = false;
  const document = { getElementById: id => elements.get(id), createElement: tag => {
    const item = element(tag);
    if (tag === 'iframe') item.contentWindow = { focus() {}, print() { printed++; } };
    return item;
  }, body: { appendChild(item) { added.push(item); if (item.onload) item.onload(); } } };
  const pdf = { numPages: 9, async getPage(number) { return {
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
  await settle();
  assert.equal(get('page-count').textContent,'of 9');assert.equal(get('previous').disabled,true);
  get('next').onclick();await settle();assert.equal(get('page').value,2);
  get('previous').onclick();await settle();assert.equal(get('page').value,1);
  const initialWidth=get('canvas').width;get('zoom-in').onclick();await settle();assert.ok(get('canvas').width>initialWidth);
  get('zoom-out').onclick();await settle();assert.equal(get('canvas').width,initialWidth);
  get('play').onclick();assert.equal(timers.size,1);[...timers.values()][0]();await settle();assert.equal(get('page').value,2);
  get('pause').onclick();assert.equal(timers.size,0);
  get('page').value='9';get('page').onchange();await settle();assert.equal(get('next').disabled,true);
  get('download').onclick();assert.equal(downloaded,1);assert.equal(added.at(-1).href,'blob:private-pdf');assert.equal(added.at(-1).download,'AIT-Core-REVIEW.pdf');
  assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),new Uint8Array(bytes));
  get('print').onclick();assert.equal(printed,1);assert.equal(added.at(-1).src,'blob:private-pdf');
  get('email').onclick();assert.equal(get('email-form').hidden,false);
  await get('email-form').onsubmit({preventDefault(){}});assert.equal(requests.at(-1).url,'/protected/kit/email');
  assert.equal(requests.at(-1).options.body.document_token,'signed-selection');assert.equal(get('email-status').textContent,'PDF submitted');
  assert.equal(requests[0].url,'/protected/kit.pdf');assert.equal(requests[0].options.credentials,'same-origin');
  events.pagehide();assert.equal(revoked,1);assert.equal(timers.size,0);
  console.log('Viewer controls passed: nine-page canvas, Play/Pause, zoom, navigation, exact PDF print/download, explicit email, cleanup.');
};
