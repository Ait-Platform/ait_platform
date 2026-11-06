"""Ordinary account details remain editable without changing unresolved meters."""
import unittest
from sqlalchemy import text
import test_pm_municipal_accounts as fixture

class AccountRestrictionTests(fixture.MunicipalAccountsTests):
    def test_rates_only_account_does_not_create_meters(self):
        before=self.snapshot()
        result=self.post(dict(account_number='RATES-26',owner_name='Rates Owner'))
        self.assertEqual(result.status_code,302,result.text)
        self.assertEqual(before['bil_meter'],self.snapshot()['bil_meter'])
        self.assertEqual(before['bil_meter_reading'],self.snapshot()['bil_meter_reading'])
        row=self.session.execute(text("SELECT * FROM bil_muni_account WHERE account_number='RATES-26'")).mappings().one()
        self.assertIsNone(row['water_meter_id']);self.assertIsNone(row['elec_meter_id'])

    def test_ambiguous_meter_associations_allow_owner_edit_but_not_meter_edit(self):
        self.session.execute(text("UPDATE bil_muni_account SET water_meter_id=NULL WHERE id=1"))
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_bill_number) VALUES(99,26,'SECOND-W','Water','MET-26')"))
        self.session.commit();self.conn.commit()
        before=self.snapshot();fields=self.account_form(1);fields.pop('csrf_token');fields.pop('billing_month');fields['owner_name']='Corrected Owner'
        self.assertEqual(self.post(fields).status_code,302)
        for table in ('bil_meter','bil_meter_reading','bil_consumption'):
            self.assertEqual(before[table],self.snapshot()[table])
        fields=self.account_form(1);fields.pop('csrf_token');fields.pop('billing_month');fields['water_number']='NEW-IDENTITY'
        self.assertEqual(self.post(fields).status_code,409)
        self.assertEqual(before['bil_meter'],self.snapshot()['bil_meter'])


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(AccountRestrictionTests(n) for n in AccountRestrictionTests.__dict__ if n.startswith('test_'))
