"""One focused PostgreSQL edit/save, portfolio and snapshot preservation check."""
import copy
import importlib.util
import unittest
from pathlib import Path
from alembic.operations import Operations
from alembic.migration import MigrationContext
from lxml import html
import test_pm_bank_register as fixture


class BankEditCheck(unittest.TestCase):
    load_lease_workspace=True
    setUp=fixture.BankRegisterCheck.setUp
    sign_in=fixture.BankRegisterCheck.sign_in
    sql=fixture.BankRegisterCheck.sql
    payload=fixture.BankRegisterCheck.payload
    assign=fixture.BankRegisterCheck.assign
    create=fixture.BankRegisterCheck.create
    version=fixture.BankRegisterCheck.version
    signed=fixture.BankRegisterCheck.signed
    url_history=fixture.BankRegisterCheck.url_history
    ledger=fixture.BankRegisterCheck.ledger
    docx=fixture.BankRegisterCheck.docx
    url=fixture.BankRegisterCheck.url
    migrate=fixture.BankRegisterCheck.migrate
    account=fixture.BankRegisterCheck.account

    def test_edit_existing_account_preserves_links_and_signed_snapshot(self):
        self.migrate()
        spec=importlib.util.spec_from_file_location('register_migration',Path('migrations/versions/pm_bank_register_002.py'))
        migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        migration.op=Operations(MigrationContext.configure(self.session.connection()));migration.upgrade()
        self.sql("UPDATE alembic_version SET version_num='pm_bank_register_002' WHERE version_num='pm_lease_payments_001'")
        self.session.commit()
        self.assertEqual(self.account(for_rental='',for_utilities='').status_code,302)
        bank=self.sql('SELECT id FROM bil_bank_detail').scalar()
        data={'action':'allocate_existing','bank_detail_id':str(bank)}
        self.assertEqual(self.client.post(self.url(),data=data).status_code,302)
        self.assertEqual(self.signed().status_code,302)
        snapshots=copy.deepcopy([dict(row) for row in self.sql('SELECT * FROM bil_lease_draft').mappings()])
        documents=copy.deepcopy([dict(row) for row in self.sql('SELECT * FROM bil_lease_document').mappings()])
        links=[dict(row) for row in self.sql('SELECT * FROM bil_property_payment_account').mappings()]
        original=dict(self.sql('SELECT * FROM bil_bank_detail').mappings().one())
        page=self.client.get(self.url());self.assertEqual(page.status_code,200)
        tree=html.fromstring(page.data)
        self.assertFalse(tree.xpath('//table//text()[contains(.,"'+original['account_number']+'")]'))
        self.assertIn('******4567',page.get_data(as_text=True))
        edit_url=self.url()+'&edit_bank_detail_id='+str(bank)
        page=self.client.get(edit_url);self.assertEqual(page.status_code,200)
        tree=html.fromstring(page.data)
        for key in ('bank_name','branch_name','account_holder','account_number','branch_code'):
            self.assertEqual(tree.xpath('//section[@id="add-bank"]//input[@name="'+key+'"]/@value'),[original[key] or ''])
        self.assertEqual(tree.xpath('//section[@id="add-bank"]//select[@name="account_type"]/option[@selected]/@value'),[original['account_type']])
        updated=dict(action='edit_account',bank_detail_id=str(bank),bank_name='Corrected Synthetic Bank',
            branch_name='Corrected Branch',account_holder='Corrected Holder',account_number='0009876543',
            account_type='savings',branch_code='654321')
        response=self.client.post(self.url(),data=updated)
        self.assertEqual(response.status_code,302)
        with self.client.session_transaction() as session:
            self.assertTrue(any('Bank account updated successfully' in message for category,message in session.get('_flashes',[])))
        saved=dict(self.sql('SELECT * FROM bil_bank_detail').mappings().one())
        self.assertEqual(saved['id'],bank)
        for key in ('bank_name','branch_name','account_holder','account_number','account_type','branch_code'):
            self.assertEqual(saved[key],updated[key])
        self.assertEqual([dict(row) for row in self.sql('SELECT * FROM bil_property_payment_account').mappings()],links)
        self.assertEqual([dict(row) for row in self.sql('SELECT * FROM bil_lease_draft').mappings()],snapshots)
        self.assertEqual([dict(row) for row in self.sql('SELECT * FROM bil_lease_document').mappings()],documents)
        # Reject duplicate identities without changing either record.
        self.assertEqual(self.account(account_number='0001111111',for_rental='',for_utilities='').status_code,302)
        duplicate=dict(updated,bank_name='Synthetic Bank',branch_code='123456',account_number='0001111111')
        self.assertEqual(self.client.post(self.url(),data=duplicate).status_code,400)
        self.assertEqual(dict(self.sql('SELECT * FROM bil_bank_detail WHERE id='+str(bank)).mappings().one()),saved)
        # The manager of another authorized property cannot edit this portfolio's account.
        self.sign_in(8)
        other='/billing/leases/property/27/banking'
        self.assertEqual(self.client.get(other+'?edit_bank_detail_id='+str(bank)).status_code,404)
        self.assertEqual(self.client.post(other,data=updated).status_code,404)
        self.assertEqual(self.client.post(self.url(),data=updated).status_code,404)
        # Linking from a second portfolio makes shared register editing unavailable.
        self.assertEqual(self.client.post(other,data=data).status_code,302)
        self.sign_in(7)
        self.assertEqual(self.client.get(edit_url).status_code,404)
        self.assertEqual(self.client.post(self.url(),data=updated).status_code,404)
        self.assertEqual(dict(self.sql('SELECT * FROM bil_bank_detail WHERE id='+str(bank)).mappings().one()),saved)
