"""Use actual preview links; distinguish expiry, access and PDF renderer errors."""
import time
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit,parse_qs
from lxml import html
import test_pm_central_utilities_output as fixture
from test_pm_directory_register import open_browser


class OutputPDFFailureTests(unittest.TestCase):
    def setUp(self):
        self.owner=fixture.CentralOutputTests(methodName='test_all_seven_reports_use_one_work_area_and_preserve_records');self.owner.setUp();self.addCleanup(self.owner.doCleanups)
        self.fx=self.owner.fx;self.output=self.owner.output
        self.fx.app.context_processor(lambda:dict(can_manage_municipal_arrangements=self.owner.arrangements.can_manage))
        if 'billing_bp.municipal_payment_arrangements' not in self.fx.app.view_functions:
            self.fx.app.add_url_rule('/billing/utilities/<int:property_id>/payment-arrangements',endpoint='billing_bp.municipal_payment_arrangements',view_func=lambda **kw:'Existing arrangements route')

    def preview(self):
        page=self.fx.client.get(self.owner.url,query_string=dict(source='metsoa',report='metsoa_statement',recipient_id='11',preview='1',by_month='2026-09',billing_month='2026-09'),headers=self.fx.headers)
        self.assertEqual(page.status_code,200,page.text)
        return page,html.fromstring(page.data).get_element_by_id('output-download').get('href')

    def request(self,url):return self.fx.client.get(url,headers=dict(self.fx.headers,Accept='application/json'))

    def test_september_metsoa_all_accounts_actual_preview_link_is_valid(self):
        page,url=self.preview();self.assertIn('Statement For All Metro Meters',page.text)
        response=self.request(url);self.assertEqual(response.status_code,200,response.text[:100] if response.status_code!=200 else '')
        self.assertEqual(response.content_type,'application/pdf');self.assertTrue(response.data.startswith(b'%PDF-'))

    def test_expired_visible_preview_rejected_with_precise_error_without_rendering(self):
        with patch('itsdangerous.timed.time.time',return_value=time.time()-1900):page,url=self.preview()
        self.assertIn('output-preview',page.text)
        with patch.object(self.output,'render_pdf') as renderer:
            response=self.request(url)
            self.assertEqual(response.status_code,400)
            self.assertIn('expired after 30 minutes',response.json['error']);renderer.assert_not_called()

    def test_renderer_unavailability_is_not_misreported_as_stale_preview(self):
        page,url=self.preview()
        with patch.object(self.output,'render_pdf',side_effect=RuntimeError("BrowserType.launch: Executable doesn't exist at /private/browser/path")):
            with self.assertLogs(self.fx.app.logger,level='ERROR'):response=self.request(url)
        self.assertEqual(response.status_code,503)
        self.assertIn('PDF browser is unavailable',response.json['error'])
        self.assertNotIn('/private/',response.json['error'])
        with patch.object(self.output,'render_pdf',side_effect=RuntimeError('Synthetic render error')):
            with self.assertLogs(self.fx.app.logger,level='ERROR'):response=self.request(url)
        self.assertEqual(response.status_code,503);self.assertIn('PDF renderer failed',response.json['error'])
        self.assertNotIn('Synthetic render error',response.json['error'])

    def test_save_report_displays_server_error_and_does_not_write_a_pdf(self):
        from playwright.sync_api import sync_playwright
        page_response,url=self.preview()
        with patch.object(self.output,'render_pdf',side_effect=RuntimeError('Synthetic renderer failure')):
            with self.assertLogs(self.fx.app.logger,level='ERROR'):failure=self.request(url)
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,markup=page_response.text)
            try:
                page.evaluate("""async failure=>{
                    const root=await navigator.storage.getDirectory(),scope=JSON.parse(document.getElementById('output-directory-context').textContent);
                    await AITDirectories.set(scope,root);
                    window.fetch=async()=>new Response(JSON.stringify(failure),{status:503,headers:{'Content-Type':'application/json'}});
                }""",failure.json)
                page.locator('#output-save').click();page.wait_for_function("document.getElementById('output-save-message').textContent.includes('HTTP 503')")
                self.assertIn('PDF renderer failed',page.locator('#output-save-message').inner_text())
                self.assertTrue(page.locator('#output-preview').is_visible());self.assertTrue(page.locator('#output-download').is_visible())
                files=page.evaluate("""async()=>{const root=await navigator.storage.getDirectory(),folder=await (await root.getDirectoryHandle('METSOA')).getDirectoryHandle('METSOA Statement');const names=[];for await(const [name] of folder.entries())names.push(name);return names;}""")
                self.assertEqual(files,[])
            finally:browser.close()

    def test_client_distinguishes_validation_access_redirect_and_network_failures(self):
        from playwright.sync_api import sync_playwright
        response,_=self.preview()
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,markup=response.text)
            try:
                result=page.evaluate("""async()=>{
                    const scope=JSON.parse(document.getElementById('output-directory-context').textContent),root=await navigator.storage.getDirectory();await AITDirectories.set(scope,root);
                    const cases=[
                        {status:400,body:JSON.stringify({error:'This preview expired after 30 minutes.'}),type:'application/json'},
                        {status:409,body:JSON.stringify({error:'Report records changed after preview.'}),type:'application/json'},
                        {status:403,body:'<p role="alert">You cannot access this property.</p>',type:'text/html'},
                        {status:502,body:'Bad Gateway',type:'text/plain'},
                        {status:200,body:'<html>Sign in</html>',type:'text/html',redirect:true},
                        {network:true}
                    ],messages=[],accept=[];
                    for(const item of cases){
                        window.fetch=async(url,options)=>{
                            accept.push(options.headers.Accept);if(item.network)throw new TypeError('Failed to fetch');
                            const result=new Response(item.body,{status:item.status,headers:{'Content-Type':item.type}});
                            if(item.redirect)Object.defineProperty(result,'redirected',{value:true});return result;
                        };
                        try{await AITDirectories.save(scope,scope.report,'/existing-pdf');}catch(error){messages.push(error.message);}
                    }
                    return {messages,accept};
                }""")
                messages=result['messages'];self.assertEqual(len(messages),6)
                for message,expected in zip(messages,['expired after 30 minutes','records changed','cannot access','HTTP 502','session may have expired','could not reach AIT']):self.assertIn(expected,message)
                self.assertEqual(result['accept'],['application/json']*6)
            finally:browser.close()

    def test_utilities_grid_preserves_eight_routes_and_output_menu(self):
        from playwright.sync_api import sync_playwright
        response=self.fx.client.get('/soa-test/utilities_hub?property_id=26&billing_month=2026-09',headers=self.fx.headers)
        self.assertEqual(response.status_code,200,response.text)
        tree=html.fromstring(response.data);links=tree.xpath('//nav[@aria-label="Municipal Utilities"]/a')
        self.assertEqual(len(links),8)
        self.assertEqual([a.text_content() for a in links],['Accounts','Readings','Reconciliation','Exceptions','Generate METSOA','Link Meters','Payment Arrangements','Output'])
        for link in links:
            target=urlsplit(link.get('href'))
            self.assertTrue('26' in target.path.split('/') or parse_qs(target.query).get('property_id')==['26'])
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,markup=response.text)
            try:
                page.set_viewport_size(dict(width=1024,height=900))
                boxes=page.locator('nav[aria-label="Municipal Utilities"] a').evaluate_all('(links)=>links.map(a=>{const r=a.getBoundingClientRect();return {top:r.top,width:r.width,height:r.height};})')
                self.assertEqual(len({b['top'] for b in boxes}),2)
                self.assertEqual(len({b['top'] for b in boxes[:4]}),1);self.assertEqual(len({b['top'] for b in boxes[4:]}),1)
                self.assertLess(max(b['width'] for b in boxes)-min(b['width'] for b in boxes),1)
                self.assertEqual({b['height'] for b in boxes},{64})
            finally:browser.close()
        output,_=self.preview();menu=html.fromstring(output.data).get_element_by_id('output-actions')
        self.assertEqual(menu.xpath('./button/text()|./a/text()'),['Print','Email','Save Report','Folders'])


if __name__=='__main__':unittest.main()
