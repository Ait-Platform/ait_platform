"""Linked-meter snapshots and registered first-save destinations on isolated data."""
import json
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit
from io import BytesIO
from lxml import html
from pypdf import PdfReader
from sqlalchemy import text,event
from playwright.sync_api import sync_playwright
import test_pm_central_utilities_output as fixture
from test_pm_directory_register import open_browser,browser_html,ROOT,REPORTS


class LinkedMetersTests(unittest.TestCase):
    def setUp(self):
        self.owner=fixture.CentralOutputTests(methodName='test_all_seven_reports_use_one_work_area_and_preserve_records')
        self.owner.setUp();self.addCleanup(self.owner.doCleanups);self.fx=self.owner.fx
        s=self.fx.session
        s.execute(text('ALTER TABLE bil_meter ADD COLUMN IF NOT EXISTS pointing_to text'))
        s.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,parent_meter_id,pointing_to) VALUES(90,26,'BULK-W','Water',NULL,'Main water supply'),(91,26,'SUB-W','Water',90,'Shop water'),(92,26,'BULK-E','Electrical',NULL,'Main electricity'),(93,26,'SUB-E','Electricity',92,'Shop electricity'),(94,28,'FOREIGN-PRIVATE','Water',NULL,'Private'),(95,26,'UNLINKED-W','Water',NULL,'Garden'),(96,26,'LEGACY-POINTER','Water',90,'Unconfirmed')"))
        for parent,children in [(90,[91]),(92,[93])]:
            s.execute(text("INSERT INTO bil_utilities_capture_audit(property_id,kind,record_id,actor_id,after_data) VALUES(26,'bulk_link',:parent,7,CAST(:details AS json))"),dict(parent=parent,details=json.dumps(dict(source='bulk_meter_linking',children=children))))
        s.commit();self.fx.conn.commit()
    def preview(self,**filters):return self.owner.get('linked_meters','linked_meters',**filters)
    def pdf(self,preview):
        values=self.owner.fields(preview)
        response=self.fx.client.get(self.owner.url,query_string=dict(values,format='pdf'),headers=self.fx.headers)
        self.assertEqual(response.status_code,200,response.text[:80] if response.mimetype!='application/pdf' else '')
        return response.data

    def test_correct_grouping_identifiers_descriptions_scope_and_read_only_snapshot(self):
        before=self.fx.snapshot();queries=[]
        def observe(conn,cursor,sql,params,ctx,many):queries.append(sql.lstrip().lower())
        event.listen(self.fx.conn,'before_cursor_execute',observe)
        try:response=self.preview()
        finally:event.remove(self.fx.conn,'before_cursor_execute',observe)
        self.assertEqual(response.status_code,200,response.text)
        tree=html.fromstring(response.data)
        self.assertEqual(tree.xpath('//select[@id="output-report"]/option[@selected]/text()'),['Linked Meters'])
        rows=tree.xpath('//*[@id="utilities-report"]//tbody/tr')
        grouped=[row.text_content() for row in rows]
        self.assertTrue(any('Bulk meter' in row and 'BULK-W' in row and 'Main water supply' in row for row in grouped))
        self.assertTrue(any('Sub-meter of BULK-W' in row and 'SUB-W' in row for row in grouped))
        self.assertTrue(any('Sub-meter of BULK-E' in row and 'SUB-E' in row for row in grouped))
        self.assertTrue(any('Unlinked' in row and 'LEGACY-POINTER' in row for row in grouped))
        self.assertTrue(any('Unlinked' in row and 'UNLINKED-W' in row for row in grouped))
        self.assertNotIn('FOREIGN-PRIVATE',response.text)
        self.assertIn('Property ID 26',tree.text_content());self.assertIn('Generated',tree.text_content())
        self.assertFalse(any(q.startswith(('insert ','update ','delete ')) for q in queries))
        self.assertEqual(before,self.fx.snapshot())
        self.assertEqual(self.preview(item_id='94').status_code,403)
        foreign=self.fx.client.get('/billing/utilities/28/output?source=linked_meters&report=linked_meters&preview=1',headers=self.fx.headers)
        self.assertEqual(foreign.status_code,403)
        only=self.preview(item_id='91');self.assertIn('SUB-W',only.text);self.assertIn('BULK-W',only.text);self.assertNotIn('BULK-E</td>',only.text)

    def test_pdf_email_and_preview_rejects_relationship_changes(self):
        preview=self.preview();pdf=self.pdf(preview)
        extracted=' '.join(page.extract_text() for page in PdfReader(BytesIO(pdf)).pages)
        for label in ['Linked Meters','BULK-W','SUB-W','Unlinked Meters','Garden']:self.assertIn(label,extracted)
        fields=self.owner.fields(preview)
        with patch('app.utils.mailer.send_pdf_email',return_value=True) as send:
            response=self.fx.client.post(self.owner.url,data=dict(fields,action='email',email_to='recipient@example.com'),headers=self.fx.headers)
        self.assertEqual(response.status_code,302,response.text)
        self.assertTrue(send.called)
        emailed=send.call_args.args[3]
        self.assertIn('SUB-W',' '.join(p.extract_text() for p in PdfReader(BytesIO(emailed)).pages))
        # A saved PDF remains historical; a stale generation token cannot silently
        # produce changed relationship contents.
        self.fx.session.execute(text('UPDATE bil_meter SET parent_meter_id=NULL WHERE id=91'));self.fx.session.commit();self.fx.conn.commit()
        stale=self.fx.client.get(self.owner.url,query_string=dict(fields,format='pdf'),headers=self.fx.headers)
        self.assertEqual(stale.status_code,409)
        self.assertIn('SUB-W',extracted)

    def test_first_save_reuses_destination_refreshes_register_and_preserves_history(self):
        pdf=self.pdf(self.preview());markup=self.preview().text
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,markup=markup,init="if(window!==window.top)window.print=()=>{window.parent.printedHistorical=true;};")
            try:
                page.route('**/billing/utilities/26/output?**',lambda route:route.fulfill(body=pdf,content_type='application/pdf'))
                page.evaluate("""async()=>{const root=await navigator.storage.getDirectory(),almond=await root.getDirectoryHandle('1Property Management',{create:true});
                    await AITDirectories.set({user:'7',property:'26'},almond);await AITDirectories.set({user:'7',property:'27'},await root.getDirectoryHandle('1PM Dale',{create:true}));}""")
                scope=page.evaluate("JSON.parse(document.getElementById('output-directory-context').textContent)")
                self.assertEqual(page.evaluate("async()=>Array.fromAsync((await AITDirectories.get({user:'7',property:'26'})).handle.keys())"),[])
                for number in range(2):
                    page.locator('#output-save').click();page.wait_for_function("document.getElementById('output-save-message').textContent.startsWith('Saved ')")
                    page.wait_for_function("!document.getElementById('output-save').disabled")
                stored=page.evaluate("""async()=>{const root=(await AITDirectories.get({user:'7',property:'26'})).handle,folder=await root.getDirectoryHandle('Link Meters');return {folders:await Array.fromAsync(root.keys()),files:await Array.fromAsync(folder.keys())};}""")
                self.assertEqual(stored['folders'],['Link Meters']);self.assertEqual(len(stored['files']),2)
                self.assertTrue(any('(1)' in name for name in stored['files']))
                page.route('**/folders',lambda route:route.fulfill(body=browser_html(26),content_type='text/html'))
                page.goto('https://ait.test/folders');page.locator('#folder-register-open').click()
                page.wait_for_function("document.getElementById('folder-register-rows').lastChild.children[2].textContent==='2'")
                self.assertEqual(page.locator('#folder-register-rows tr').count(),8)
                page.route('**/snapshot.pdf',lambda route:route.fulfill(body=pdf,content_type='application/pdf'))
                page.evaluate("async scope=>AITDirectories.save(scope,'linked_meters','/snapshot.pdf')",scope)
                page.wait_for_function("document.getElementById('folder-register-rows').lastChild.children[2].textContent==='3'")
                page.get_by_role('button',name='Link Meters',exact=True).click()
                page.wait_for_function("document.getElementById('folder-register-files').children.length===3")
                self.assertEqual(page.locator('#folder-register-files tr').count(),3)
                # No endpoint is used to retrieve the historical file, even after
                # live relationship records have changed.
                self.fx.session.execute(text('UPDATE bil_meter SET parent_meter_id=NULL WHERE id=91'));self.fx.session.commit();self.fx.conn.commit()
                page.get_by_role('button',name=stored['files'][0],exact=True).click()
                page.wait_for_function("document.getElementById('folder-register-message').textContent.includes('page(s)')")
                url=page.locator('#folder-view-pages').get_attribute('data-object-url')
                self.assertEqual(bytes(page.evaluate('async url=>Array.from(new Uint8Array(await(await fetch(url)).arrayBuffer()))',url)),pdf)
                page.locator('#folder-view-print').click();page.wait_for_function('window.printedHistorical===true')
                page.evaluate("document.getElementById('folder-view-pdf').contentWindow.dispatchEvent(new Event('afterprint'))")
                sent=[]
                def deliver(route):sent.append(route.request.post_data_buffer);route.fulfill(json=dict(message='Original accepted'))
                page.route('**/billing_bp.directory_email',deliver)
                page.locator('#folder-view-email').click();page.locator('#folder-email-to').fill('recipient@example.com');page.locator('#folder-view-email-form button').click()
                page.wait_for_function("document.getElementById('folder-register-message').textContent==='Original accepted'")
                self.assertIn(pdf,sent[0])
                self.assertEqual(page.evaluate("async()=>(await AITDirectories.get({user:'7',property:'27'})).handle.name"),'1PM Dale')
            finally:browser.close()

    def test_new_destination_permission_denial_creates_nothing(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.evaluate("""async()=>{const root=await navigator.storage.getDirectory(),folder=await root.getDirectoryHandle('No write',{create:true});await AITDirectories.set({user:'7',property:'26'},folder);
                    FileSystemDirectoryHandle.prototype.queryPermission=async()=> 'denied';FileSystemDirectoryHandle.prototype.requestPermission=async()=> 'denied';}""")
                result=page.evaluate("""async()=>{try{await AITDirectories.save({user:'7',property:'26',period_end:'2026-11-20'},'linked_meters','/pdf');return 'incorrect success';}catch(error){return error.message;}}""")
                self.assertNotEqual(result,'incorrect success')
                self.assertEqual(page.evaluate("async()=>Array.fromAsync((await AITDirectories.get({user:'7',property:'26'})).handle.keys())"),[])
            finally:browser.close()

    def test_future_report_registration_feeds_the_same_destination_catalogue(self):
        from app.program_billing import utilities_output_reports as reports
        with patch.dict(reports.CATALOG,{'future_source':[('future_report','Future Report')]}),patch.dict(reports.SOURCES,{'future_source':'Future Source'}),patch.dict(reports.REPORT_PATHS,{'future_report':('Future Category','Future Report')}):
            destination=next(r for r in reports.report_destinations() if r['code']=='future_report')
            self.assertEqual(destination['directories'],['Future Category','Future Report'])
            self.assertTrue(destination['create_on_save'])
        self.assertEqual(len(reports.report_destinations()),8)
