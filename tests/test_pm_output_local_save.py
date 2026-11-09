"""Real browser IndexedDB/filesystem handles; controlled PDF and permission inputs."""
import unittest
from playwright.sync_api import sync_playwright
from test_pm_directory_register import open_browser


class OutputLocalSaveTests(unittest.TestCase):
    def test_real_local_saves_all_reports_collision_sequence_and_scope(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                result=page.evaluate("""async()=>{
                    const a=AITDirectories,s={user:'7',property:'26'},root=await navigator.storage.getDirectory();
                    const folder=await root.getDirectoryHandle('Exports',{create:true});
                    await a.set(s,folder);
                    const accounts=await (await folder.getDirectoryHandle('Municipal Accounts')).getDirectoryHandle('Account Register');
                    const existing=await accounts.getFileHandle('091126_Account_Register.pdf',{create:true});
                    const writer=await existing.createWritable();await writer.write('%PDF-existing account report');await writer.close();
                    const codes=['history','municipal_submission','metro_reconciliation','utilities_reconciliation','metsoa_statement','account_register','arrangement_register'];
                    const metsoa=await (await folder.getDirectoryHandle('METSOA')).getDirectoryHandle('METSOA Statement');
                    window.fetch=async()=>new Response('%PDF-actual existing renderer',{status:200,headers:{'Content-Type':'application/pdf'}});
                    const date=new Date(2026,10,9),names=[];
                    for(const code of codes)names.push(await a.save(s,code,'/existing-pdf',date));
                    names.push(await a.save(s,'metsoa_statement','/existing-pdf',date));
                    names.push(await a.save(s,'metsoa_statement','/existing-pdf',date));
                    const parallel=await Promise.all([a.save(s,'metsoa_statement','/existing-pdf',date),a.save(s,'metsoa_statement','/existing-pdf',date)]);
                    const contents=await (await (await metsoa.getFileHandle('091126_METSOA.pdf')).getFile()).text();
                    const errors=[];
                    for(const scope of [{user:'8',property:'26'},{user:'7',property:'28'}]){
                        try{await a.save(scope,'metsoa_statement','/existing-pdf',date);}catch(e){errors.push(e.message);}
                    }
                    return {names,parallel,contents,errors,existing:await (await existing.getFile()).text()};
                }""")
                self.assertEqual(len(result['names']),9)
                self.assertIn('091126_Account_Register(2).pdf',result['names'])
                self.assertEqual(result['existing'],'%PDF-existing account report')
                self.assertIn('091126_Metro_Reconciliation.pdf',result['names'])
                self.assertEqual(result['names'][-2:],['091126_METSOA(2).pdf','091126_METSOA(3).pdf'])
                self.assertEqual(set(result['parallel']),{'091126_METSOA(4).pdf','091126_METSOA(5).pdf'})
                self.assertEqual(result['contents'],'%PDF-actual existing renderer')
                self.assertEqual(len(result['errors']),2)
                self.assertTrue(all('No main folder' in e for e in result['errors']))
            finally:browser.close()

    def test_changed_main_folder_missing_subfolder_and_failed_change(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                result=page.evaluate("""async()=>{
                    const a=AITDirectories,s={user:'7',property:'26'},root=await navigator.storage.getDirectory();
                    const old=await root.getDirectoryHandle('Old',{create:true}),updated=await root.getDirectoryHandle('New',{create:true});
                    window.fetch=async()=>new Response('%PDF-preserved');const day=new Date(2026,10,9);
                    await a.set(s,old);await a.save(s,'history','/pdf',day);
                    let denied=false;
                    try{await a.set(s,{kind:'directory',queryPermission:async()=>'denied',requestPermission:async()=>'denied'});}catch(_){denied=true;}
                    const retained=(await a.get(s)).handle.name;
                    await a.set(s,updated);
                    const readings=await updated.getDirectoryHandle('Meter Readings');
                    await readings.removeEntry('Municipal Reading Submission');
                    const name=await a.save(s,'municipal_submission','/pdf',day);
                    const target=await readings.getDirectoryHandle('Municipal Reading Submission');
                    const newText=await (await (await target.getFileHandle(name)).getFile()).text();
                    const oldTarget=await (await old.getDirectoryHandle('Meter Readings')).getDirectoryHandle('Reading History');
                    const oldText=await (await (await oldTarget.getFileHandle('091126_Reading_History.pdf')).getFile()).text();
                    return {denied,retained,current:(await a.get(s)).handle.name,newText,oldText};
                }""")
                self.assertEqual(result,dict(denied=True,retained='Old',current='New',newText='%PDF-preserved',oldText='%PDF-preserved'))
            finally:browser.close()

    def test_output_page_catalog_drives_real_save_button(self):
        import test_pm_utilities_output as fixture
        owner=fixture.UtilitiesOutputTests(methodName='test_email_pdf_failures_validation_and_stale_preview')
        owner.setUp();self.addCleanup(owner.doCleanups);self.addCleanup(owner.tearDown)
        response=owner.preview();self.assertEqual(response.status_code,200,response.text)
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,markup=response.text)
            try:
                page.evaluate("""async()=>{
                    const scope=JSON.parse(document.getElementById('output-directory-context').textContent);
                    const root=await navigator.storage.getDirectory();await AITDirectories.set(scope,root);
                    window.fetch=async()=>new Response('%PDF-existing output');
                }""")
                page.locator('#output-save').click()
                page.wait_for_function("document.getElementById('output-save-message').textContent.includes('Saved ')")
                result=page.evaluate("""async()=>{
                    const root=await navigator.storage.getDirectory(),folder=await (await root.getDirectoryHandle('Meter Readings')).getDirectoryHandle('Reading History');
                    const names=[];for await(const [name] of folder.entries())names.push(name);return names;
                }""")
                self.assertEqual(len(result),1);self.assertTrue(result[0].endswith('_Reading_History.pdf'))
            finally:browser.close()

    def test_permission_denial_cancel_and_invalid_pdf_never_write(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                result=page.evaluate("""async()=>{
                    const a=AITDirectories,errors=[];let fetches=0,requests=0,writes=0;
                    window.fetch=async()=>{fetches++;return new Response('<html>login</html>');};
                    const handle={queryPermission:async()=>'prompt',requestPermission:async()=>{requests++;return 'denied';},getFileHandle:async()=>{writes++;}};
                    a.get=async()=>({handle});
                    const run=async()=>{try{await a.save({user:'7',property:'26'},'history','/pdf');}catch(e){errors.push(e.name+': '+e.message);}};
                    await run();
                    handle.requestPermission=async()=>{throw new DOMException('Cancelled','AbortError');};await run();
                    handle.queryPermission=async()=>'granted';await run();
                    window.fetch=async()=>{fetches++;return new Response('Failed',{status:503});};await run();
                    return {errors,fetches,requests,writes};
                }""")
                self.assertEqual(result['writes'],0)
                self.assertEqual(result['requests'],1)
                self.assertEqual(result['fetches'],2)
                self.assertIn('permission was not granted',result['errors'][0])
                self.assertIn('AbortError',result['errors'][1])
                self.assertIn('did not return a PDF',result['errors'][2])
                self.assertIn('could not be generated',result['errors'][3])
            finally:browser.close()

    def test_prompt_permission_can_be_granted_before_fetch_and_write(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                result=page.evaluate("""async()=>{
                    const a=AITDirectories,root=await navigator.storage.getDirectory(),events=[];
                    const folder=await root.getDirectoryHandle('Granted',{create:true});
                    a.get=async()=>({handle:{
                        queryPermission:async()=>{events.push('query');return 'prompt';},
                        requestPermission:async()=>{events.push('permission');return 'granted';},
                        getDirectoryHandle:(...args)=>folder.getDirectoryHandle(...args)
                    }});
                    window.fetch=async()=>{events.push('pdf');return new Response('%PDF-granted');};
                    const name=await a.save({user:'7',property:'26'},'history','/pdf',new Date(2026,10,9));
                    const target=await (await folder.getDirectoryHandle('Meter Readings')).getDirectoryHandle('Reading History');
                    return {events,name,text:await (await (await target.getFileHandle(name)).getFile()).text()};
                }""")
                self.assertEqual(result['events'],['query','permission','pdf'])
                self.assertEqual(result['name'],'091126_Reading_History.pdf')
                self.assertEqual(result['text'],'%PDF-granted')
            finally:browser.close()

    def test_save_button_busy_and_normal_download_fallback(self):
        markup="""<html><body><button id="output-save">Save Report</button><a id="output-download" href="/existing-pdf?download=1">Download PDF</a><span id="output-save-message"></span><script id="output-directory-context" type="application/json">{"user":"7","property":"26","report":"history"}</script><script src="/static/js/pm_local_directories.js" defer></script><script src="/static/js/pm_output_save.js" defer></script></body></html>"""
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,markup=markup)
            try:
                page.evaluate("""()=>{window.calls=0;AITDirectories.save=()=>{calls++;return new Promise(resolve=>{window.complete=resolve;});};}""")
                page.locator('#output-save').click()
                self.assertTrue(page.locator('#output-save').is_disabled())
                page.evaluate("document.getElementById('output-save').click()")
                self.assertEqual(page.evaluate('calls'),1)
                page.evaluate("complete('091126_Reading_History.pdf')")
                page.wait_for_function("document.getElementById('output-save-message').textContent.includes('Saved 091126')")
                self.assertTrue(page.locator('#output-save').is_enabled())
                page.evaluate("""()=>{AITDirectories.supported=()=>false;window.downloads=0;document.getElementById('output-download').addEventListener('click',e=>{e.preventDefault();downloads++;});}""")
                page.locator('#output-save').click()
                self.assertEqual(page.evaluate('downloads'),1)
                self.assertIn('Downloading',page.locator('#output-save-message').inner_text())
            finally:browser.close()


if __name__=='__main__':unittest.main()
