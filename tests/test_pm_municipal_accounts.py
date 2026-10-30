"""Independent municipal accounts on isolated PostgreSQL; no live writes."""
from unittest.mock import patch
from pathlib import Path
from jinja2 import ChoiceLoader,DictLoader
from sqlalchemy import text
from lxml import html
import test_pm_utilities_configuration as fixture

class MunicipalAccountsTests(fixture.UtilitiesConfigurationTests):
    accounts_url='/billing/utilities/26/accounts'
    def setUp(self):
        super().setUp()
        self.app.jinja_loader=ChoiceLoader([DictLoader({'partials/flash_messages.html':Path('templates/partials/flash_messages.html').read_text(encoding='utf-8')}),self.app.jinja_loader])
        from app.program_billing import municipal_accounts
        scoped=patch.object(municipal_accounts,'db',self.db);scoped.start();self.addCleanup(scoped.stop)
    def account_form(self,account_id=None):
        response=self.client.get(self.accounts_url+('?edit='+str(account_id) if account_id else ''),headers=self.headers)
        self.assertEqual(response.status_code,200,response.text)
        tree=html.fromstring(response.data)
        return {e.get('name'):e.get('value','') for e in tree.xpath('//form//input[@name]')}
    def post(self,data,url=None):
        return self.client.post(url or self.accounts_url,data=dict(csrf_token=self.csrf,billing_month='2026-11',**data),headers=self.headers)
    def test_create_individual_accounts_with_optional_meters_and_property_anchor(self):
        before=self.snapshot()
        a=self.post(dict(account_number='83347721969',owner_name='S. Nanhoo',water_number='CXN998',electricity_number='7150696S'))
        self.assertEqual(a.status_code,302,a.text)
        b=self.post(dict(account_number='SECOND-ACCOUNT',owner_name='Second Owner',electricity_number='ELEC-SECOND'))
        self.assertEqual(b.status_code,302,b.text)
        new=self.session.execute(text("SELECT * FROM bil_muni_account WHERE account_number='83347721969'")).mappings().one()
        self.assertEqual(new['property_id'],26);self.assertIsNotNone(new['water_meter_id']);self.assertIsNotNone(new['elec_meter_id'])
        self.assertFalse(new['is_bulk_account'])
        for table in ('bil_meter_reading','bil_consumption','bil_architecture_draft'):self.assertEqual(before[table],self.snapshot()[table])
        self.assertEqual(self.snapshot()['bil_meter'][0],before['bil_meter'][0])
        page=self.client.get(a.location,headers=self.headers);self.assertEqual(page.status_code,200)
        self.assertIn('Municipal account saved',page.text);self.assertIn('SECOND-ACCOUNT',page.text)
        self.assertIn('CXN998',page.text);self.assertIn('billing_month=2026-11',page.text)
    def test_independent_edit_uses_stable_ids_preserves_history_and_rejects_stale_form(self):
        before=self.snapshot();data=self.account_form(1);data.pop('csrf_token');data['billing_month']='2026-11';data['owner_name']='Correct Owner'
        response=self.post({k:v for k,v in data.items() if k!='billing_month'})
        self.assertEqual(response.status_code,302,response.text)
        after=self.snapshot()
        for table in ('bil_meter','bil_meter_reading','bil_consumption','bil_architecture_draft'):self.assertEqual(before[table],after[table])
        self.assertEqual(after['bil_muni_account'][0]['id'],1)
        data['owner_name']='Stale typed owner';data.pop('billing_month')
        rejected=self.post(data);self.assertEqual(rejected.status_code,409,rejected.text);self.assertIn('Stale typed owner',rejected.text)
        self.assertIn('changed after',rejected.text)
    def test_new_account_requires_meter_and_duplicate_and_identifier_validation(self):
        base=dict(account_number='NEW-ACCOUNT',owner_name='Owner')
        self.assertEqual(self.post(base).status_code,400)
        self.assertEqual(self.post(dict(base,water_number='contains spaces')).status_code,400)
        self.assertEqual(self.post(dict(base,water_number='<script>')).status_code,400)
        self.assertEqual(self.post(dict(base,water_number='SAME',electricity_number='same')).status_code,400)
        self.assertEqual(self.post(dict(account_number=' met-26 ',owner_name='Owner',water_number='NEW-W')).status_code,409)
        self.assertEqual(self.post(dict(base,water_number='WATER-26')).status_code,409)
        self.session.execute(text("INSERT INTO bil_muni_account(account_number) VALUES('UNASSIGNED')"));self.session.commit();self.conn.commit()
        self.assertEqual(self.post(dict(account_number='UNASSIGNED',owner_name='Owner',water_number='NEW-W')).status_code,409)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter')).scalar(),1)
    def test_historical_identifiers_are_protected_but_second_meter_can_be_added(self):
        before=self.snapshot();data=self.account_form(1);data.pop('csrf_token');data.pop('billing_month')
        data.update(owner_name='Owner',water_number='RENUMBERED')
        self.assertEqual(self.post(data).status_code,409);self.assertEqual(before,self.snapshot())
        data['water_number']='WATER-26';data['account_number']='RENUMBERED-ACCOUNT'
        self.assertEqual(self.post(data).status_code,409);self.assertEqual(before,self.snapshot())
        data['account_number']='MET-26';data['electricity_number']='ADDED-E'
        self.assertEqual(self.post(data).status_code,302)
        for table in ('bil_meter_reading','bil_consumption'):self.assertEqual(before[table],self.snapshot()[table])
    def test_permissions_csrf_and_other_property_accounts_are_not_exposed(self):
        self.assertEqual(self.client.get('/billing/utilities/28/accounts',headers=self.headers).status_code,403)
        self.assertEqual(self.client.get(self.accounts_url).status_code,403)
        self.assertEqual(self.client.post(self.accounts_url,data={ },headers=self.headers).status_code,400)
        self.session.execute(text("INSERT INTO bil_muni_account(id,property_id,account_number) VALUES(99,28,'PRIVATE-ACCOUNT')"));self.session.commit();self.conn.commit()
        self.assertNotIn('PRIVATE-ACCOUNT',self.client.get(self.accounts_url,headers=self.headers).text)
        self.assertEqual(self.client.get(self.accounts_url+'?edit=99',headers=self.headers).status_code,404)
        self.assertEqual(self.post(dict(account_id='99',account_number='PRIVATE-ACCOUNT',owner_name='Owner',water_number='NEW')).status_code,404)
    def test_legacy_mandatory_unit_constraint_reports_failure_without_partial_save(self):
        self.session.execute(text('ALTER TABLE bil_meter ADD COLUMN sectional_unit_id integer'))
        self.session.execute(text('UPDATE bil_meter SET sectional_unit_id=26'))
        self.session.execute(text('ALTER TABLE bil_meter ALTER COLUMN sectional_unit_id SET NOT NULL'));self.session.commit();self.conn.commit()
        before=self.snapshot()
        response=self.post(dict(account_number='NEW',owner_name='Owner',water_number='NEW-W'))
        self.assertEqual(response.status_code,503,response.text);self.assertIn('sectional_unit_id is mandatory',response.text)
        self.assertEqual(before,self.snapshot())

    def test_utilities_action_opens_accounts_with_property_and_month(self):
        response=self.client.post('/soa-test/utilities_hub',data=dict(csrf_token=self.csrf,property_id=26,billing_month='2026-11',action='accounts'),headers=self.headers)
        self.assertEqual(response.status_code,302)
        self.assertEqual(response.location,self.accounts_url+'?billing_month=2026-11')
        page=self.client.get(response.location,headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        self.assertEqual(html.fromstring(page.data).xpath('//nav[@aria-label="Utilities"]/a[@aria-current="page"]/text()'),['Municipal Accounts'])
    def test_unposted_account_and_meter_can_be_corrected_without_replacing_ids(self):
        response=self.post(dict(account_number='NEW-ORIGINAL',owner_name='Owner',water_number='NEW-WATER'))
        self.assertEqual(response.status_code,302)
        row=self.session.execute(text("SELECT id,water_meter_id FROM bil_muni_account WHERE account_number='NEW-ORIGINAL'")).mappings().one()
        data=self.account_form(row['id']);data.pop('csrf_token');data.pop('billing_month')
        data.update(account_number='NEW-CORRECTED',owner_name='Corrected Owner',water_number='CORRECTED-WATER')
        self.assertEqual(self.post(data).status_code,302)
        updated=self.session.execute(text('SELECT id,water_meter_id FROM bil_muni_account WHERE id=:id'),dict(id=row['id'])).mappings().one()
        self.assertEqual(row,updated)
        meter=self.session.execute(text('SELECT meter_number,municipal_bill_number,parent_meter_id FROM bil_meter WHERE id=:id'),dict(id=row['water_meter_id'])).mappings().one()
        self.assertEqual(meter['meter_number'],'CORRECTED-WATER');self.assertEqual(meter['municipal_bill_number'],'NEW-CORRECTED');self.assertIsNone(meter['parent_meter_id'])

    def test_conflicting_existing_meter_pointer_cannot_reassign_history(self):
        self.session.execute(text("INSERT INTO bil_muni_account(id,property_id,account_number,water_meter_id) VALUES(98,26,'DIFFERENT-ACCOUNT',1)"));self.session.commit();self.conn.commit()
        before=self.snapshot()
        page=self.client.get(self.accounts_url+'?edit=98',headers=self.headers)
        self.assertIn('pointer and municipal account number disagree',page.text)
        data=self.account_form(98);data.pop('csrf_token');data.pop('billing_month');data['owner_name']='Owner'
        self.assertEqual(self.post(data).status_code,409);self.assertEqual(before,self.snapshot())
