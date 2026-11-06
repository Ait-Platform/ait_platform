"""Ordinary in-place meter identifier edits on an isolated Almond/account fixture."""
import unittest
from unittest.mock import patch
from sqlalchemy import text
from test_pm_account_corrections import AccountCorrectionsTests

class IdentifierCorrectionTests(AccountCorrectionsTests):
    def setUp(self):
        super().setUp()
        self.session.execute(text("UPDATE bil_property SET name='Almond' WHERE id=26"))
        self.session.execute(text("UPDATE bil_muni_account SET account_number='83192896040',muni_water_meter_no='CEL888_CWA074',muni_elec_meter_no='2188802S' WHERE id=1"))
        self.session.execute(text("UPDATE bil_meter SET meter_number=CASE id WHEN 1 THEN 'CEL888_CWA074' ELSE '2188802S' END,municipal_bill_number='83192896040',municipal_reference=CASE id WHEN 1 THEN 'W4049314' ELSE NULL END"))
        self.session.commit();self.conn.commit()
    def correction(self,**changes):
        return self.edit(**changes)
    def audit_rows(self):
        return self.session.execute(text("SELECT * FROM bil_utilities_capture_audit WHERE kind='meter_correction' ORDER BY id")).mappings().all()
    def test_supplied_account_both_identifiers_and_metadata_correct_in_place_with_audit(self):
        history=self.immutable_history();count=self.session.execute(text('SELECT count(*) FROM bil_meter')).scalar()
        response=self.correction(water_number='CEL888',electricity_number='02188802S',owner_name='Correct Owner',water_municipal_reference='W4049314-CORRECTED',electricity_municipal_reference='E-CORRECTED')
        self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter')).scalar(),count)
        self.assertEqual(self.session.execute(text('SELECT water_meter_id,elec_meter_id FROM bil_muni_account WHERE id=1')).one(),(1,2))
        self.assertEqual(history,self.immutable_history())
        events=self.audit_rows();self.assertEqual(len(events),2)
        for event,old,new in zip(events,['CEL888_CWA074','2188802S'],['CEL888','02188802S']):
            self.assertEqual(event['property_id'],26);self.assertEqual(event['actor_id'],7)
            self.assertEqual(event['before_data']['meter_number'],old);self.assertEqual(event['after_data']['meter_number'],new)
            self.assertEqual(event['before_data']['id'],event['after_data']['id']);self.assertEqual(event['record_id'],event['before_data']['id'])
            self.assertTrue(event['after_data']['identifier_updated_in_place'])
    def test_explicit_case_correction_is_saved_and_audited(self):
        history=self.immutable_history()
        response=self.correction(water_number='cel888_cwa074')
        self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(self.session.execute(text('SELECT meter_number FROM bil_meter WHERE id=1')).scalar(),'cel888_cwa074')
        event=self.audit_rows()[0];self.assertEqual(event['before_data']['meter_number'],'CEL888_CWA074');self.assertEqual(event['after_data']['meter_number'],'cel888_cwa074')
        self.assertEqual(history,self.immutable_history())
    def test_metadata_and_account_number_changes_need_no_identifier_confirmation(self):
        history=self.immutable_history()
        response=self.edit(account_number='00083192896040',owner_name='Correct Owner',water_number='',water_municipal_reference='W4049314-CORRECTED')
        self.assertEqual(response.status_code,302,response.text);self.assertEqual(history,self.immutable_history());self.assertFalse(self.audit_rows())
        self.assertEqual(list(self.session.execute(text('SELECT meter_number FROM bil_meter ORDER BY id'))),[('CEL888_CWA074',),('2188802S',)])
    def test_ordinary_save_updates_in_place_without_classification(self):
        history=self.immutable_history()
        ids=list(self.session.execute(text('SELECT id,property_id FROM bil_meter ORDER BY id')))
        response=self.edit(water_number='UPDATED-WATER')
        self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(ids,list(self.session.execute(text('SELECT id,property_id FROM bil_meter ORDER BY id'))))
        self.assertEqual(history,self.immutable_history());self.assertEqual(len(self.audit_rows()),1)
        self.assertNotIn('same_physical_meter_confirmed',self.audit_rows()[0]['after_data'])
    def test_duplicate_owned_identifier_identifies_conflict_without_merging(self):
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_bill_number) VALUES(90,27,'DUPLICATE-W','Water','DUPLICATE-ACCOUNT')"));self.session.commit();self.conn.commit()
        before=self.snapshot();history=self.immutable_history()
        response=self.correction(water_number='duplicate-w')
        self.assertEqual(response.status_code,409,response.text)
        for value in ('record #90','Water','(#27)','DUPLICATE-ACCOUNT'):self.assertIn(value,response.text)
        self.assertEqual(before,self.snapshot());self.assertEqual(history,self.immutable_history());self.assertFalse(self.audit_rows())
    def test_foreign_identifiers_are_independent_and_foreign_record_ids_stay_blocked(self):
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_bill_number) VALUES(90,28,'CEL888','Water','83192896040')"))
        self.session.execute(text("INSERT INTO bil_muni_account(id,property_id,account_number,water_meter_id) VALUES(90,28,'83192896040',90)"));self.session.commit();self.conn.commit()
        foreign=self.session.execute(text('SELECT * FROM bil_meter WHERE id=90')).one();history=self.immutable_history()
        response=self.correction(water_number='CEL888');self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(foreign,self.session.execute(text('SELECT * FROM bil_meter WHERE id=90')).one());self.assertEqual(history,self.immutable_history())
        self.assertEqual(self.client.get('/billing/utilities/28/accounts?edit=90',headers=self.headers).status_code,403)
        self.assertEqual(self.client.get(self.accounts_url+'?edit=90',headers=self.headers).status_code,404)
        data=self.account_form(1);data.pop('csrf_token');data.update(account_id='90',water_number='ATTACK')
        before=self.snapshot();self.assertEqual(self.capture_post(self.accounts_url,**data).status_code,404);self.assertEqual(before,self.snapshot())
    def test_saved_declaration_keeps_original_id_without_filling_missing_links(self):
        self.session.execute(text('UPDATE bil_muni_account SET water_meter_id=NULL WHERE id=1'))
        self.session.execute(text('UPDATE bil_meter SET municipal_bill_number=NULL WHERE id=1'));self.session.commit();self.conn.commit();history=self.immutable_history()
        response=self.correction(water_number='CEL888');self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(history,self.immutable_history());self.assertIsNone(self.session.execute(text('SELECT water_meter_id FROM bil_muni_account WHERE id=1')).scalar())
        self.assertEqual(self.session.execute(text('SELECT muni_water_meter_no FROM bil_muni_account WHERE id=1')).scalar(),'CEL888')
        self.assertIsNone(self.session.execute(text('SELECT municipal_bill_number FROM bil_meter WHERE id=1')).scalar());self.assertEqual(self.audit_rows()[0]['record_id'],1)
    def test_audit_failure_rolls_back_identifier_owner_and_reference_changes(self):
        self.session.execute(text("ALTER TABLE bil_utilities_capture_audit ADD CONSTRAINT fail_meter_audit CHECK(kind<>'meter_correction')"));self.session.commit();self.conn.commit();before=self.snapshot();history=self.immutable_history()
        with self.assertLogs(self.app.logger,level='ERROR'):response=self.correction(water_number='CEL888',owner_name='Correct Owner',water_municipal_reference='NEW-REFERENCE')
        self.assertEqual(response.status_code,503,response.text);self.assertEqual(before,self.snapshot());self.assertEqual(history,self.immutable_history());self.assertFalse(self.audit_rows())
    def test_missing_audit_and_stale_forms_cannot_save_a_correction(self):
        from app.program_billing import utilities_capture
        before=self.snapshot()
        with patch.object(utilities_capture,'available',return_value=False):response=self.correction(water_number='CEL888')
        self.assertEqual(response.status_code,503);self.assertEqual(before,self.snapshot())
        data=self.account_form(1);data.pop('csrf_token');data.update(water_number='CEL888')
        self.assertEqual(self.capture_post(self.accounts_url,**data).status_code,302)
        before=self.snapshot();self.assertEqual(self.capture_post(self.accounts_url,**data).status_code,409);self.assertEqual(before,self.snapshot());self.assertEqual(len(self.audit_rows()),1)

    def test_browser_saves_meter_number_without_additional_controls(self):
        from playwright.sync_api import sync_playwright
        from http.server import HTTPServer,BaseHTTPRequestHandler
        from threading import Thread
        case=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_GET(self):self.respond(case.client.get(self.path,headers=case.headers))
            def do_POST(self):self.respond(case.client.post(self.path,data=self.rfile.read(int(self.headers['Content-Length'])),content_type=self.headers.get('Content-Type'),headers=case.headers))
            def respond(self,response):
                self.send_response(response.status_code);self.send_header('Content-Type',response.content_type)
                if response.location:self.send_header('Location',response.location)
                self.end_headers();self.wfile.write(response.data);response.close()
        server=HTTPServer(('127.0.0.1',0),Handler);thread=Thread(target=server.serve_forever,daemon=True);thread.start()
        self.addCleanup(lambda:(server.shutdown(),server.server_close(),thread.join(timeout=2)))
        before=self.snapshot();history=self.immutable_history()
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.goto('http://127.0.0.1:'+str(server.server_port)+self.accounts_url+'?edit=1')
                self.assertEqual(page.locator('#water_number').input_value(),'CEL888_CWA074')
                self.assertEqual(page.locator('#water_municipal_reference').input_value(),'W4049314')
                self.assertEqual(page.locator('#electricity_number').input_value(),'2188802S')
                self.assertEqual(page.locator('#electricity_municipal_reference').input_value(),'')
                page.locator('#water_number').fill('CEL888')
                self.assertEqual(page.locator('#meter_change_type').count(),0)
                self.assertNotIn('Identifier correction (same physical meter)',page.content())
                self.assertNotIn('Record a physical replacement in Exceptions',page.content())
                page.get_by_role('button',name='Save',exact=True).click()
                self.assertTrue(page.locator('#account-register').is_visible())
                self.assertIn('Municipal account saved.',page.content())
                self.assertEqual(self.session.execute(text('SELECT meter_number FROM bil_meter WHERE id=1')).scalar(),'CEL888')
                self.assertEqual(len(self.audit_rows()),1)
            finally:browser.close()
        self.assertEqual(history,self.immutable_history())

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(IdentifierCorrectionTests(name) for name in IdentifierCorrectionTests.__dict__ if name.startswith('test_'))
