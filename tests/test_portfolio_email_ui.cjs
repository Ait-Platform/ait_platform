const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'../templates/program_billing/property_portfolio.html'),'utf8');
const script=source.split('<script>')[1].split('</script>')[0];
class Element {
 constructor(){this.listeners={};this.dataset={open:'false'};this.open=false;this.focused=false;}
 addEventListener(name,callback){this.listeners[name]=callback;}
 click(){this.listeners.click();}
 showModal(){this.open=true;}
 close(){this.open=false;}
 focus(){this.focused=true;}
}
const ids={};
for(const key of ['portfolio-email-dialog','portfolio-email-open','portfolio-email-cancel','portfolio-recipient'])ids[key]=new Element();
let requests=0;
const context={document:{getElementById:id=>ids[id]},fetch:()=>{requests++;},XMLHttpRequest:()=>{requests++;}};
vm.runInNewContext(script,context);
assert.equal(ids['portfolio-email-dialog'].open,false);
ids['portfolio-email-open'].click();
assert.equal(ids['portfolio-email-dialog'].open,true);
assert.equal(ids['portfolio-recipient'].focused,true);
assert.equal(requests,0);
ids['portfolio-email-cancel'].click();
assert.equal(ids['portfolio-email-dialog'].open,false);
assert.equal(requests,0);
assert.match(source,/id="portfolio-email-open"[^>]*>/);
assert.match(source,/<button type="button" id="portfolio-email-open"/);
assert.match(source,/id="portfolio-email-cancel" type="button"/);
assert.match(source,/type="submit" name="intent" value="send"/);
assert.match(source,/name="recipient_email" type="email" required/);
assert.match(source,/name="csrf_token"/);
ids['portfolio-email-dialog'].dataset.open='true';
vm.runInNewContext(script,context);
assert.equal(ids['portfolio-email-dialog'].open,true);
assert.equal(requests,0);
console.log('Portfolio email modal: open/cancel never send, explicit submit, editable recipient and failure reopen passed.');
