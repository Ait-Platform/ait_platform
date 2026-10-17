"""Essential register/save/reopen/authorization and one Word merge smoke check."""
import importlib.util, io, copy, re, unittest
from pathlib import Path
from zipfile import ZipFile
from alembic.operations import Operations
from alembic.migration import MigrationContext
import test_pm_lease_owner_reuse as reuse

class TenantRegisterTests(unittest.TestCase):
    load_lease_workspace=True
    setUp=reuse.RegisteredOwnerPartiesTests.setUp
    sign_in=reuse.RegisteredOwnerPartiesTests.sign_in
    sql=reuse.RegisteredOwnerPartiesTests.sql
    payload=reuse.RegisteredOwnerPartiesTests.payload
    assign=reuse.RegisteredOwnerPartiesTests.assign
    create=reuse.RegisteredOwnerPartiesTests.create
    stored=reuse.RegisteredOwnerPartiesTests.stored
    route=reuse.RegisteredOwnerPartiesTests.route
    post=reuse.RegisteredOwnerPartiesTests.post
    generate=reuse.RegisteredOwnerPartiesTests.generate
    owners=reuse.RegisteredOwnerPartiesTests.owners

    def schema(self):
        spec=importlib.util.spec_from_file_location('tenant_migration',Path('docs/proposed_migrations/pm_tenant_001.py'))
        migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        migration.op=Operations(MigrationContext.configure(self.session.connection()));migration.upgrade();self.session.commit()
        return migration

    def form(self,**changes):
        values=dict(party_type='natural_person',legal_name='Synthetic Test Tenant',identity_number='TEST-TENANT-1',
            address='1 Synthetic Street',email='test@example.com',phone='0100000000')
        values.update(changes);return values

    def register(self,**changes):
        return self.client.post(self.route('tenants'),data=self.form(**changes))

    def save_party(self,owner,tenant,**changes):
        data=dict(parties_form_version='4',representative_owner_id=str(owner['owner_id']),tenant_id=str(tenant),lessee_legal_name='FORGED')
        data.update(changes);return self.post('parties',data)

    def test_register_reuse_select_reopen_and_word(self):
        self.schema();owners=self.owners()
        self.assertEqual(self.register().status_code,302)
        self.assertEqual(self.register(legal_name='Must not overwrite').status_code,302)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_tenant_identity').scalar(),1)
        tenant=self.sql('SELECT id FROM bil_tenant_identity').scalar()
        self.assertEqual(self.save_party(owners[1],tenant).status_code,302)
        saved=self.stored();self.assertEqual(len(saved['_contracting_lessors']['identities']),3)
        self.assertEqual(saved['_parties']['lessee']['legal_name'],'Synthetic Test Tenant')
        self.assertEqual(saved['_parties']['lessee']['tenant_identity_id'],tenant)
        values=self.workflow_module.party_placeholder_values(saved)
        self.assertIn(owners[1]['name'],values['[LANDLORD SIGNATORY NAME AND CAPACITY]'])
        self.assertIn(owners[1]['residential_address'],values['[S1 LANDLORD SERVICE ADDRESS AND CONTACT PARTICULARS]'])
        html=self.client.get(self.route('particulars/parties')).get_data(as_text=True)
        self.assertIn('Synthetic Test Tenant',html);self.assertIn('value="'+str(tenant)+'" selected',html)
        for removed in ('Selected representative','owner-capacity-confirmation','lessee-dialog'):self.assertNotIn(removed,html)
        import test_pm_lease_premises_offline as premises
        import test_pm_lease_period_offline as period
        import test_pm_lease_amounts_offline as financial
        import test_pm_lease_part5_offline as utilities
        for group,data in [('premises',premises.PremisesOfflineTests.form(self)),('period',period.PeriodOfflineTests.form(self)),('amounts',financial.AmountsOfflineTests.form(self)),('amounts',utilities.Part5OfflineTests.form(self))]:
            self.assertEqual(self.post(group,data).status_code,302)
        all_parts=copy.deepcopy(self.stored())
        self.assertEqual(self.save_party(owners[0],tenant).status_code,302)
        for key in ('_premises','_period','_amounts','_banking_utilities'):
            if key in all_parts:self.assertEqual(self.stored()[key],all_parts[key])
        generated=self.generate();self.assertEqual(generated.status_code,200)
        with ZipFile(io.BytesIO(generated.data)) as archive:
            from lxml import etree
            root=etree.fromstring(archive.read('word/document.xml'));visible=''.join(root.itertext())
            self.assertNotRegex(visible,r'\[[^\[\]]+\]');self.assertIn('Synthetic Test Tenant',visible)
            for owner in owners:self.assertIn(owner['name'],visible);self.assertIn(owner['identity_number'],visible)
            self.assertEqual(len(self.workflow_module.stage5_placeholder_values(self.stored())),37)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease').scalar(),2)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership').scalar(),3)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),3)

    def test_juristic_and_authorization_validation_and_flag(self):
        migration=self.schema();owners=self.owners()
        self.assertEqual(self.register(party_type='juristic_person').status_code,400)
        self.assertEqual(self.register(party_type='juristic_person',signatory_name='Test Director',signatory_identity_number='TEST-DIRECTOR',signatory_capacity='Director').status_code,302)
        tenant=self.sql('SELECT id FROM bil_tenant_identity').scalar()
        self.assertEqual(self.save_party(owners[0],tenant).status_code,302)
        self.assertIn('Test Director',self.workflow_module.party_placeholder_values(self.stored())['[TENANT SIGNATORY NAME AND CAPACITY]'])
        before=copy.deepcopy(self.stored())
        self.assertEqual(self.save_party(owners[0],9999).status_code,404)
        self.assertEqual(self.save_party(owners[0],tenant,representative_owner_id=[str(o['owner_id']) for o in owners]).status_code,400)
        self.assertEqual(self.stored(),before)
        self.sign_in(8)
        self.assertEqual(self.client.get(self.route('tenants')).status_code,404)
        import uuid
        other=self.client.post('/billing/leases/property/27/drafts',data={'registration_token':str(uuid.uuid4())})
        self.assertEqual(other.status_code,302)
        other_id=self.sql('SELECT id FROM bil_lease_draft WHERE manager_id=8').scalar()
        self.assertEqual(self.client.post('/billing/leases/property/27/particulars/parties',data=dict(parties_form_version='4',tenant_id=tenant,draft_id=str(other_id),version=1)).status_code,404)
        self.sign_in(7);self.app.config['PM_LEASE_PARTICULARS_WRITES_APPROVED']=False
        self.assertEqual(self.register().status_code,503)
        self.assertEqual(self.save_party(owners[0],tenant).status_code,503)
        self.assertIn('PM_LEASE_PARTICULARS_WRITES_APPROVED',self.client.get(self.route('particulars/parties')).get_data(as_text=True))
        with self.assertRaises(RuntimeError):migration.downgrade()
        self.session.rollback()

    def test_missing_schema_fails_safely_and_empty_rollback(self):
        owners=self.owners();html=self.client.get(self.route('particulars/parties')).get_data(as_text=True)
        self.assertIn('pm_tenant_001',html)
        self.assertEqual(self.register().status_code,503)
        self.assertEqual(self.save_party(owners[0],1).status_code,503)
        self.assertEqual(self.stored(),{})
        migration=self.schema();migration.downgrade();self.session.commit()
        self.assertIsNone(self.sql("SELECT to_regclass('pg_temp.bil_tenant_identity')").scalar())
