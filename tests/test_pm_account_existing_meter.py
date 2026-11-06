"""Existing Accounts meters resolve from saved data; blank inputs never replace them."""
import unittest
from sqlalchemy import text
from lxml import html
from test_pm_account_corrections import AccountCorrectionsTests

class ExistingAccountMeterTests(AccountCorrectionsTests):
    def associations(self):
        return (list(self.session.execute(text('SELECT id,property_id,water_meter_id,elec_meter_id,muni_water_meter_no,muni_elec_meter_no FROM bil_muni_account ORDER BY id'))),list(self.session.execute(text('SELECT id,property_id,meter_number,municipal_bill_number,parent_meter_id FROM bil_meter ORDER BY id'))))
    def missing_pointers(self,declared=True,missing_bill=False):
        self.session.execute(text('UPDATE bil_muni_account SET water_meter_id=NULL,elec_meter_id=NULL WHERE id=1'))
        if not declared:self.session.execute(text('UPDATE bil_muni_account SET muni_water_meter_no=NULL,muni_elec_meter_no=NULL WHERE id=1'))
        if missing_bill:self.session.execute(text('UPDATE bil_meter SET municipal_bill_number=NULL WHERE id IN (1,2)'))
        self.session.commit();self.conn.commit()
    def test_blank_fields_preserve_known_meter_ids_and_allow_owner_reference_edits(self):
        before=self.associations();history=self.immutable_history()
        response=self.edit(water_number='',electricity_number='',owner_name='Correct Owner',water_municipal_reference='W-CORRECTED',electricity_municipal_reference='E-CORRECTED')
        self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(before,self.associations());self.assertEqual(history,self.immutable_history())
        self.assertEqual(list(self.session.execute(text('SELECT id,municipal_reference FROM bil_meter ORDER BY id'))),[(1,'W-CORRECTED'),(2,'E-CORRECTED')])
    def test_bill_fallback_resolves_missing_pointers_without_repairing_associations(self):
        self.missing_pointers(declared=False)
        before=self.associations();history=self.immutable_history()
        response=self.edit(water_number='',electricity_number='',owner_name='Correct Owner',water_municipal_reference='W-BILL',electricity_municipal_reference='E-BILL')
        self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(before,self.associations());self.assertEqual(history,self.immutable_history())
    def test_saved_declaration_resolves_original_meter_before_history_comparison(self):
        self.missing_pointers(missing_bill=True)
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_bill_number) VALUES(90,26,'OTHER-HISTORICAL-W','Water','MET-26')"))
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(90,'2025-09-30',999)"));self.session.commit();self.conn.commit()
        before=self.associations();history=self.immutable_history()
        page=self.client.get(self.accounts_url+'?edit=1',headers=self.headers)
        tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//input[@name="water_number"]/@value'),[' 000wR '])
        response=self.edit(water_number='000wr',electricity_number='',owner_name='Correct Owner',water_municipal_reference='W-ORIGINAL')
        self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(before,self.associations());self.assertEqual(history,self.immutable_history())
        self.assertEqual(self.session.execute(text('SELECT municipal_reference FROM bil_meter WHERE id=1')).scalar(),'W-ORIGINAL')
        self.assertIsNone(self.session.execute(text('SELECT municipal_reference FROM bil_meter WHERE id=90')).scalar())
    def test_unconfirmed_identifier_changes_are_blocked_after_declared_meter_resolution(self):
        self.missing_pointers(missing_bill=True);before=self.snapshot();history=self.immutable_history()
        for fields in (dict(water_number='DIFFERENT-W'),dict(electricity_number='DIFFERENT-E')):
            response=self.edit(**fields);self.assertEqual(response.status_code,409,response.text)
            self.assertIn('Select Identifier correction (same physical meter)',response.text)
            self.assertEqual(before,self.snapshot());self.assertEqual(history,self.immutable_history())
    def test_unresolved_declaration_stays_unresolved_without_creating_a_replacement(self):
        self.session.execute(text("UPDATE bil_muni_account SET water_meter_id=NULL,muni_water_meter_no='UNRESOLVED-W' WHERE id=1"));self.session.commit();self.conn.commit()
        before=self.associations();history=self.immutable_history()
        response=self.edit(water_number='',owner_name='Correct Owner')
        self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(before,self.associations());self.assertEqual(history,self.immutable_history())
        for fields in (dict(water_number='REPLACEMENT-W'),dict(water_number='',water_municipal_reference='UNKNOWN-TARGET')):
            before=self.snapshot();response=self.edit(**fields)
            self.assertEqual(response.status_code,409,response.text);self.assertIn('could not be verified',response.text)
            self.assertEqual(before,self.snapshot())
    def test_ambiguous_meter_identity_fails_closed_without_any_writes(self):
        self.missing_pointers(missing_bill=True)
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type) VALUES(90,26,'000wr','Water')"));self.session.commit();self.conn.commit()
        before=self.snapshot();response=self.edit(water_number='',owner_name='Correct Owner')
        self.assertEqual(response.status_code,409,response.text);self.assertIn('Multiple water meters',response.text)
        self.assertEqual(before,self.snapshot())
    def test_foreign_meter_is_not_resolved_by_same_declared_number(self):
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_bill_number) VALUES(90,28,'FOREIGN-W','Water','MET-26')"))
        self.session.execute(text("INSERT INTO bil_muni_account(id,property_id,account_number,water_meter_id) VALUES(90,28,'MET-26',90)"))
        self.session.execute(text("UPDATE bil_muni_account SET water_meter_id=NULL,muni_water_meter_no='FOREIGN-W' WHERE id=1"));self.session.commit();self.conn.commit()
        before=self.associations();history=self.immutable_history()
        response=self.edit(water_number='',owner_name='Correct Owner')
        self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(before,self.associations());self.assertEqual(history,self.immutable_history())
        self.assertEqual(self.client.get(self.accounts_url+'?edit=90',headers=self.headers).status_code,404)
        self.assertEqual(self.client.get('/billing/utilities/28/accounts?edit=90',headers=self.headers).status_code,403)
        data=self.account_form(1);data.pop('csrf_token');data['account_id']='90'
        before=self.snapshot();self.assertEqual(self.capture_post(self.accounts_url,**data).status_code,404)
        self.assertEqual(before,self.snapshot())
    def test_consumption_only_history_remains_protected(self):
        self.session.execute(text('DELETE FROM bil_meter_reading WHERE meter_id=1'));self.session.commit();self.conn.commit()
        before=self.snapshot();response=self.edit(water_number='NEW-PHYSICAL-W')
        self.assertEqual(response.status_code,409,response.text);self.assertEqual(before,self.snapshot())
        self.assertEqual(self.edit(water_number='',owner_name='Correct Owner').status_code,302)

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ExistingAccountMeterTests(name) for name in ExistingAccountMeterTests.__dict__ if name.startswith('test_'))
