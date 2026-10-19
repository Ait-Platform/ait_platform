"""Essential local property banking/utilities, immutable snapshots and permissions."""
import copy,importlib.util,json,unittest
from pathlib import Path
from sqlalchemy import text
from alembic.operations import Operations
from alembic.migration import MigrationContext
from lxml import html
import test_pm_lease_history as fixture

class PropertyPaymentsTests(unittest.TestCase):
    load_lease_workspace=True
    setUp=fixture.LeaseHistoryTests.setUp
    sign_in=fixture.LeaseHistoryTests.sign_in
    sql=fixture.LeaseHistoryTests.sql
    payload=fixture.LeaseHistoryTests.payload
    assign=fixture.LeaseHistoryTests.assign
    create=fixture.LeaseHistoryTests.create
    version=fixture.LeaseHistoryTests.version
    signed=fixture.LeaseHistoryTests.signed
    url_history=fixture.LeaseHistoryTests.url_history
    ledger=fixture.LeaseHistoryTests.ledger
    docx=fixture.LeaseHistoryTests.docx

    def url(self,p=26):return '/billing/leases/property/'+str(p)+'/banking?draft_id='+str(self.draft)
    def migrate(self):
        self.sql('CREATE TEMP TABLE bil_bank_detail(id serial PRIMARY KEY,bank_name varchar(100) NOT NULL,branch_name varchar(100),branch_code varchar(50),account_number varchar(100) NOT NULL,account_holder varchar(150),account_type varchar(50))');self.session.commit()
        spec=importlib.util.spec_from_file_location('test_payments_migration',Path('migrations/versions/pm_lease_payments_001.py'))
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        self.migration=module
        before=set(self.sql('SELECT version_num FROM alembic_version').scalars())
        module.op=Operations(MigrationContext.configure(self.session.connection()));module.upgrade()
        self.sql("INSERT INTO alembic_version VALUES('pm_lease_payments_001')");self.session.commit()
        self.assertEqual(set(self.sql('SELECT version_num FROM alembic_version').scalars()),before|{'pm_lease_payments_001'})

    def account(self,**extra):
        data=dict(action='add_account',bank_name='Synthetic Bank',account_holder='Test Owner',account_number='0001234567',account_type='current',branch_code='123456',for_rental='on',for_utilities='on');data.update(extra)
        return self.client.post(self.url(),data=data)

    def test_property_accounts_utilities_reuse_snapshot_and_signed_changes_require_addendum(self):
        self.migrate()
        original={'_banking_utilities':{'utility_allocation_method':'Existing Almond allocation - keep exactly','utility_reference_mode':'provided','utility_references':'METER-A\nMETER-B','utility_no_references_reason':'','bank_name':'Legacy Bank','account_holder':'Existing holder','account_number':'0007654321','account_type':'savings','branch_code':'654321','utilities_confirmed':False,'banking_confirmed':False},'sc-01':{'value':'Existing condition'},'_parties':{'saved':'unchanged'},'_premises':{'saved':'unchanged'}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(original)});self.session.commit()
        self.assertEqual(self.account().status_code,302);self.assertEqual(self.account().status_code,302)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_bank_detail').scalar(),1)
        self.assertEqual(self.account(account_number='0009999999',for_rental='',for_utilities='on').status_code,302)
        self.assertEqual(self.client.post(self.url(),data={'action':'reuse_utilities'}).status_code,302)
        existing=self.sql('SELECT * FROM bil_property_utility_arrangement').mappings().one()
        self.assertEqual(existing['details']['utility_references'],'METER-A\nMETER-B')
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),original)
        ids=[str(r[0]) for r in self.sql('SELECT id FROM bil_property_payment_account ORDER BY created_at,id')]
        data={'action':'apply','rental_account_id':ids[0],'utilities_account_id':ids[1],'utility_arrangement_id':str(existing['id']),'version':self.version()}
        self.assertEqual(self.client.post(self.url(),data=data).status_code,302)
        saved=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar())
        from app.program_billing import lease_workflow as workflow,lease_property_payments as payments
        mapped=workflow.banking_utilities_placeholder_values(saved)
        self.assertIn('Rental:',mapped[workflow.BANKING_TOKEN]);self.assertIn('Utilities:',mapped[workflow.BANKING_TOKEN]);self.assertIn('0009999999',mapped[workflow.BANKING_TOKEN])
        self.assertEqual(mapped['[UTILITY METER OR ACCOUNT REFERENCES]'],'METER-A\nMETER-B')
        self.assertEqual(payments.snapshot_errors(saved),[])
        self.assertFalse(saved['_banking_utilities']['banking_confirmed'])
        for key in ('sc-01','_parties','_premises'):self.assertEqual(saved[key],original[key])
        self.assertEqual(self.client.post(self.url(),data={'action':'save_utilities','utility_mode':'no_charge'}).status_code,302)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),saved)
        self.assertEqual(self.session.execute(text('SELECT details FROM bil_property_utility_arrangement WHERE id=:id'),{'id':existing['id']}).scalar(),existing['details'])
        self.assertEqual(self.signed().status_code,302)
        signed_snapshot=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar())
        self.assertEqual(self.client.post(self.url(),data=dict(data,version=self.version())).status_code,409)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),signed_snapshot)
        self.assertEqual(self.client.post('/billing/leases/property/26/particulars/amounts?draft_id='+str(self.draft),data={'workspace_section':'banking_utilities','version':self.version()}).status_code,409)
        addendum=self.client.get('/billing/leases/property/26/addendum?draft_id='+str(self.draft)+'&utility_arrangement_id='+str(existing['id']))
        self.assertEqual(addendum.status_code,200);self.assertIn('Existing Almond allocation - keep exactly',addendum.get_data(as_text=True))
        self.assertEqual(self.client.get('/billing/leases/property/26/history?draft_id='+str(self.draft)).status_code,200)
        page=self.client.get(self.url());self.assertEqual(page.status_code,200)
        tree=html.fromstring(page.data)
        self.assertFalse(tree.xpath('//input[contains(@name,"confirmed")]'))
        self.assertEqual(self.client.get('/billing/leases/property/26/particulars/conditions?draft_id='+str(self.draft)).status_code,302)
        self.sign_in(8);self.assertEqual(self.client.get(self.url()).status_code,404)
        self.assertEqual(self.client.post(self.url(),data={'action':'add_account'}).status_code,404)

    def test_missing_schema_safe_gate_and_no_implicit_dale_or_almond_changes(self):
        before=self.sql('SELECT items FROM bil_lease_draft').scalar()
        response=self.client.get(self.url());self.assertEqual(response.status_code,200)
        self.assertIn('pm_lease_payments_001',response.get_data(as_text=True))
        self.assertEqual(self.account().status_code,503)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),before)
        self.migrate()
        before_properties=[dict(r) for r in self.sql('SELECT * FROM bil_property ORDER BY id').mappings()]
        self.assertEqual(self.client.post(self.url(),data={'action':'save_utilities','utility_mode':'no_charge'}).status_code,302)
        self.assertEqual(self.sql('SELECT details FROM bil_property_utility_arrangement').scalar()['utility_allocation_method'],'No utilities are charged to the tenant.')
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),before)
        self.assertEqual([dict(r) for r in self.sql('SELECT * FROM bil_property ORDER BY id').mappings()],before_properties)
        with self.assertRaises(RuntimeError):self.migration.downgrade()
