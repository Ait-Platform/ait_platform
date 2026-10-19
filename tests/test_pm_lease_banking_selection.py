"""Single essential Branch Name and existing-account allocation save/reopen check."""
import copy,json,unittest
from sqlalchemy import text
from lxml import html
import test_pm_lease_property_payments as fixture

class BankingSelectionSmokeTest(unittest.TestCase):
    load_lease_workspace=True
    setUp=fixture.PropertyPaymentsTests.setUp
    sign_in=fixture.PropertyPaymentsTests.sign_in
    sql=fixture.PropertyPaymentsTests.sql
    payload=fixture.PropertyPaymentsTests.payload
    assign=fixture.PropertyPaymentsTests.assign
    create=fixture.PropertyPaymentsTests.create
    migrate=fixture.PropertyPaymentsTests.migrate
    account=fixture.PropertyPaymentsTests.account
    url=fixture.PropertyPaymentsTests.url

    def test_branch_and_existing_property_accounts_save_without_duplicate_banks_or_snapshot_changes(self):
        self.migrate()
        snapshot={'_banking_utilities':{'bank_name':'Signed snapshot bank','branch_name':'Original branch','account_number':'0011111111'},'_lease_history':{'documents':[{'kind':'signed_original','state':'available'}]},'sc-01':{'value':'Keep original condition'}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(snapshot)});self.session.commit()
        self.assertEqual(self.account(branch_name='Synthetic Central Branch').status_code,302)
        self.assertEqual(self.sql('SELECT branch_name FROM bil_bank_detail').scalar(),'Synthetic Central Branch')
        self.assertEqual(self.account(branch_name='Synthetic Central Branch').status_code,302)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_bank_detail').scalar(),1)
        self.sql('ALTER TABLE bil_property ADD COLUMN bank_detail_id integer')
        self.sql("INSERT INTO bil_bank_detail(bank_name,branch_name,branch_code,account_number,account_holder,account_type) VALUES ('Legacy Bank','Existing Branch','654321','0008888888','Existing Owner','savings')")
        legacy_id=self.sql('SELECT max(id) FROM bil_bank_detail').scalar()
        self.session.execute(text('UPDATE bil_property SET bank_detail_id=:b WHERE id=26'),{'b':legacy_id});self.session.commit()
        page=self.client.get(self.url());self.assertEqual(page.status_code,200)
        tree=html.fromstring(page.data)
        self.assertTrue(tree.xpath('//input[@name="branch_name"]'))
        options=tree.xpath('//*[@id="existing-bank-account"]/option/@value');self.assertIn(str(legacy_id),options)
        banks_before=[dict(r) for r in self.sql('SELECT * FROM bil_bank_detail ORDER BY id').mappings()]
        for purpose,rental,utilities in [('rental',True,False),('utilities',False,True),('both',True,True)]:
            data={'action':'allocate_existing','bank_detail_id':str(legacy_id),'allocation':purpose}
            for _ in range(2):self.assertEqual(self.client.post(self.url(),data=data).status_code,302)
            row=self.session.execute(text('SELECT * FROM bil_property_payment_account WHERE property_id=26 AND bank_detail_id=:b'),{'b':legacy_id}).mappings().one()
            self.assertEqual((row['for_rental'],row['for_utilities']),(rental,utilities))
            self.assertEqual(self.client.get(self.url()).status_code,200)
            self.assertEqual([dict(r) for r in self.sql('SELECT * FROM bil_bank_detail ORDER BY id').mappings()],banks_before)
            self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),snapshot)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_payment_account').scalar(),2)
        self.assertEqual(self.client.post(self.url(),data={'action':'allocate_existing','bank_detail_id':'99999','allocation':'both'}).status_code,404)
        self.sign_in(8);self.assertEqual(self.client.post(self.url(),data={'action':'allocate_existing','bank_detail_id':str(legacy_id),'allocation':'both'}).status_code,404)
