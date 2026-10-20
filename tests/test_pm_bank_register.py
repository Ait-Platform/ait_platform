"""One focused local PostgreSQL account-register and property-link check."""
import copy, unittest
import importlib.util
from pathlib import Path
from alembic.operations import Operations
from alembic.migration import MigrationContext
from lxml import html
import test_pm_lease_property_payments as fixture


class BankRegisterCheck(unittest.TestCase):
    load_lease_workspace=True
    setUp=fixture.PropertyPaymentsTests.setUp
    sign_in=fixture.PropertyPaymentsTests.sign_in
    sql=fixture.PropertyPaymentsTests.sql
    payload=fixture.PropertyPaymentsTests.payload
    assign=fixture.PropertyPaymentsTests.assign
    create=fixture.PropertyPaymentsTests.create
    version=fixture.PropertyPaymentsTests.version
    signed=fixture.PropertyPaymentsTests.signed
    url_history=fixture.PropertyPaymentsTests.url_history
    ledger=fixture.PropertyPaymentsTests.ledger
    docx=fixture.PropertyPaymentsTests.docx
    url=fixture.PropertyPaymentsTests.url
    migrate=fixture.PropertyPaymentsTests.migrate
    account=fixture.PropertyPaymentsTests.account

    def test_create_select_link_and_preserve_history(self):
        self.migrate()
        before=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar())
        self.assertEqual(self.account(for_rental='',for_utilities='').status_code,503)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_bank_detail').scalar(),0)
        spec=importlib.util.spec_from_file_location('bank_register_migration',Path('migrations/versions/pm_bank_register_002.py'))
        migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        migration.op=Operations(MigrationContext.configure(self.session.connection()))
        migration.upgrade()
        self.sql("UPDATE alembic_version SET version_num='pm_bank_register_002' WHERE version_num='pm_lease_payments_001'")
        self.sql("INSERT INTO bil_property(id,name,address,manager_id,is_archived) VALUES(128,'Second property','Synthetic address',7,false)")
        self.session.commit()
        # Account creation writes only to the register, with no inferred allocation.
        self.assertEqual(self.account(for_rental='',for_utilities='').status_code,302)
        self.assertEqual(self.account(for_rental='',for_utilities='').status_code,302)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_bank_detail').scalar(),1)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_payment_account').scalar(),0)
        bank=self.sql('SELECT id FROM bil_bank_detail').scalar()
        data={'action':'allocate_existing','bank_detail_id':str(bank)}
        self.assertEqual(self.client.post(self.url(),data=data).status_code,302)
        self.assertEqual(self.client.post('/billing/leases/property/128/banking',data=data).status_code,302)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_payment_account').scalar(),2)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_payment_account WHERE for_rental OR for_utilities').scalar(),0)
        # Reselecting an existing allocated link never rewrites its historical flags.
        self.sql('UPDATE bil_property_payment_account SET for_rental=true WHERE property_id=26');self.session.commit()
        self.assertEqual(self.client.post(self.url(),data=data).status_code,302)
        self.assertTrue(self.sql('SELECT for_rental FROM bil_property_payment_account WHERE property_id=26').scalar())
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),before)
        self.assertEqual(self.signed().status_code,302)
        signed=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar())
        self.assertEqual(self.client.post(self.url(),data=data).status_code,302)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),signed)
        response=self.client.get(self.url());self.assertEqual(response.status_code,200)
        page=response.get_data(as_text=True);tree=html.fromstring(page)
        self.assertIn('Select Bank Account',page);self.assertIn('Add Bank Account',page)
        self.assertIn('Second property',page)
        self.assertFalse(tree.xpath('//input[@name="for_rental" or @name="for_utilities"]'))
        self.assertNotIn('Lease payment instructions',page)
        self.assertNotIn('part5_',page)
        self.sign_in(8)
        self.assertEqual(self.client.post(self.url(),data=data).status_code,404)
