"""Enabled owner operations on local temporary fixtures using prepared revisions."""
import importlib.util
import unittest
from unittest.mock import patch
import sqlalchemy as sa
from alembic.operations import Operations
from alembic.migration import MigrationContext
import test_pm_property_management as property_tests

class OwnerOperationsTests(unittest.TestCase):
    sign_in=property_tests.PropertyManagementTests.sign_in
    def setUp(self):
        property_tests.PropertyManagementTests.setUp(self)
        migration_root=property_tests.ROOT.parent/'pm-owner-stage2'/'migrations'/'versions'
        if not (migration_root/'pm_owner_002.py').exists():self.skipTest('Prepared migration worktree required for enabled integration tests')
        self.session.rollback()
        with self.conn.begin():
            self.conn.exec_driver_sql('CREATE TEMP TABLE "user"(id integer PRIMARY KEY)')
            self.conn.exec_driver_sql('INSERT INTO "user" VALUES (7),(8)')
            for revision in ('pm_owner_001','pm_owner_002'):
                spec=importlib.util.spec_from_file_location(revision,migration_root/(revision+'.py'));mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
                mod.op=Operations(MigrationContext.configure(self.conn));mod.upgrade()
            self.conn.exec_driver_sql("CREATE TEMP TABLE alembic_version(version_num varchar(32)); INSERT INTO alembic_version VALUES ('pm_owner_002')")
            self.conn.exec_driver_sql("INSERT INTO bil_property(id,name,address,manager_id,is_archived) VALUES (28,'Property Two','Other address',7,false)")
        self.app.config['PM_OWNER_WRITES_APPROVED']=True

    def sql(self,query):return self.session.execute(sa.text(query))
    def payload(self,**overrides):
        data=dict(type='individual',name='Owner One',identity_number='ID001',email='one@example.invalid',phone='123',residential_address='Owner home',postal_address='PO Box',percentage='50')
        data.update(overrides);return data
    def assign(self,property_id=26,**overrides):return self.client.post(f'/billing/property-owner/{property_id}/owners',json=self.payload(**overrides))

    def test_new_owner_atomic_save_sync_and_almond_preserved(self):
        response=self.assign(postal_same=True)
        self.assertEqual(response.status_code,200)
        owners=response.get_json()['owners'];self.assertEqual(len(owners),1);self.assertEqual(owners[0]['postal_address'],'Owner home')
        self.assertEqual(self.sql('SELECT name,address FROM bil_property WHERE id=26').one(),('Almond','Original address'))
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),1)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership').scalar(),1)

    def test_reuse_identity_across_properties_and_retry_without_duplicates(self):
        response=self.assign();owner_id=response.get_json()['owners'][0]['owner_id']
        self.assertEqual(self.assign().status_code,200)
        self.assertEqual(self.client.post('/billing/property-owner/28/owners',json={'owner_id':owner_id,'percentage':'100'}).status_code,200)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),1)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership').scalar(),2)
        self.assertEqual(len(self.client.get('/billing/property-owner/owners?q=Owner').get_json()['owners']),1)

    def test_multiple_owner_types_assigned_to_almond(self):
        for kind,number in [('individual','ID1'),('company','REG2'),('trust','TR3')]:self.assertEqual(self.assign(type=kind,identity_number=number).status_code,200)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),3)
        html=self.client.get('/billing/property-owner/26/edit').get_data(as_text=True)
        for value in ('ID1','REG2','TR3','Owner home','PO Box'):self.assertIn(value,html)

    def test_shared_owner_edit_preserves_assignment_snapshots(self):
        owner=self.assign().get_json()['owners'][0];owner_id=owner['owner_id']
        self.client.post('/billing/property-owner/28/owners',json={'owner_id':owner_id})
        original=self.sql('SELECT owner_identity_snapshot FROM bil_property_ownership ORDER BY id').scalars().all()
        response=self.client.patch(f'/billing/property-owner/owners/{owner_id}',json=self.payload(name='Corrected Owner',postal_address='New postal'))
        self.assertEqual(response.status_code,200)
        self.assertEqual(self.sql('SELECT display_name,postal_address FROM bil_owner_identity').one(),('Corrected Owner','New postal'))
        self.assertEqual(self.sql('SELECT owner_identity_snapshot FROM bil_property_ownership ORDER BY id').scalars().all(),original)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership').scalar(),2)

    def test_percentage_edit_and_removal_only_affect_selected_property(self):
        owner=self.assign().get_json()['owners'][0]
        self.client.post('/billing/property-owner/28/owners',json={'owner_id':owner['owner_id'],'percentage':'100'})
        url=f"/billing/property-owner/26/assignments/{owner['assignment_id']}"
        changed=self.client.patch(url,json={'percentage':'25'});self.assertEqual(changed.status_code,200)
        current=changed.get_json()['owners'][0]
        response=self.client.delete(f"/billing/property-owner/26/assignments/{current['assignment_id']}")
        self.assertEqual(response.status_code,200);self.assertEqual(response.get_json()['owners'],[])
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),1)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership WHERE property_id=28').scalar(),1)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership WHERE property_id=26').scalar(),3)

    def test_cross_manager_operations_are_denied(self):
        owner=self.assign().get_json()['owners'][0]
        self.sign_in(8)
        self.assertEqual(self.client.post('/billing/property-owner/26/owners',json=self.payload()).status_code,404)
        self.assertEqual(self.client.post('/billing/property-owner/27/owners',json={'owner_id':owner['owner_id']}).status_code,404)
        self.assertEqual(self.client.patch(f"/billing/property-owner/owners/{owner['owner_id']}",json=self.payload()).status_code,404)
        self.assertEqual(self.client.delete(f"/billing/property-owner/26/assignments/{owner['assignment_id']}").status_code,404)
        self.assertEqual(self.client.get('/billing/property-owner/owners?q=Owner').get_json()['owners'],[])

    def test_assignment_failure_rolls_back_new_identity(self):
        original=self.session.execute
        def failed(statement,*args,**kwargs):
            if 'INSERT INTO bil_property_ownership' in str(statement):raise sa.exc.SQLAlchemyError('synthetic assignment failure')
            return original(statement,*args,**kwargs)
        with patch.object(self.session,'execute',side_effect=failed):response=self.assign()
        self.assertEqual(response.status_code,503)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),0)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership').scalar(),0)

    def test_commit_failure_rolls_back_both_records(self):
        with patch.object(self.session,'commit',side_effect=sa.exc.SQLAlchemyError('synthetic commit failure')):response=self.assign()
        self.assertEqual(response.status_code,503);self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),0)

    def test_disabled_flag_denies_all_owner_operations(self):
        self.app.config['PM_OWNER_WRITES_APPROVED']=False
        with patch.object(self.session,'execute',side_effect=AssertionError('Disabled owner routes must not query database')):
            self.assertEqual(self.client.post('/billing/property-owner/26/owners',json=self.payload()).status_code,503)
            self.assertEqual(self.client.get('/billing/property-owner/owners').status_code,503)

    def test_unapproved_schema_or_invalid_percentage_fails_closed(self):
        self.assertEqual(self.assign(percentage='101').status_code,400)
        self.assertEqual(self.assign(percentage='NaN').status_code,400)
        self.app.config['PM_OWNER_WRITES_APPROVED']='true'
        self.assertEqual(self.assign().status_code,503)

if __name__=='__main__':unittest.main()
