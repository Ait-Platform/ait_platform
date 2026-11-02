"""Municipal authority evidence and permissions on migrated temporary PostgreSQL."""
import unittest
from sqlalchemy import text
from lxml import html
from test_pm_tariff_management import ManagementTests

class AuthorityTests(ManagementTests):
    def base(self):
        return dict(utility_type='electricity',rate='3.5747',effective_date='2026-12-01',unit='kWh',
                    municipality='Synthetic Municipality',municipal_category='Domestic electricity',source_reference='Official synthetic bill 123')
    def test_blank_add_and_edit_store_null_and_preserve_original(self):
        before=self.snapshot()['bil_tariff']
        blanks=dict(municipality='',municipal_category='   ',source_reference='')
        response=self.post('/admin/billing/tariffs/new',dict(self.base(),**blanks))
        self.assertEqual(response.status_code,302,response.text)
        added=self.snapshot()['bil_tariff'][-1]
        response=self.post('/admin/billing/tariffs/'+str(added['id'])+'/edit',dict(base_id=str(added['id']),rate='4',effective_date='2027-01-01',**blanks))
        self.assertEqual(response.status_code,302,response.text)
        evidence=list(self.conn.execute(text('SELECT * FROM bil_tariff_verification ORDER BY tariff_id')).mappings())
        self.assertEqual(len(evidence),2)
        for row in evidence:
            for key in blanks:self.assertIsNone(row[key])
            self.assertEqual(row['verified_by'],7);self.assertIsNotNone(row['recorded_at'])
        self.assertEqual(self.snapshot()['bil_tariff'][:1],before)
    def test_supplied_edit_details_are_preserved_and_lengths_validated(self):
        form=dict(self.base(),base_id='1')
        response=self.post('/admin/billing/tariffs/1/edit',form)
        self.assertEqual(response.status_code,302,response.text)
        evidence=self.conn.execute(text('SELECT * FROM bil_tariff_verification')).mappings().one()
        for key in ('municipality','municipal_category','source_reference'):self.assertEqual(evidence[key],form[key])
        before=self.snapshot()
        for key,size in (('municipality',200),('municipal_category',200),('source_reference',1000)):
            invalid=dict(self.base());invalid[key]='x'*(size+1)
            self.assertEqual(self.post('/admin/billing/tariffs/new',invalid).status_code,400)
        self.assertEqual(self.snapshot(),before)
    def test_omitted_inputs_store_null_and_optional_migration_is_idempotent(self):
        import importlib.util
        from unittest.mock import patch
        from alembic.operations import Operations
        from alembic.migration import MigrationContext
        from test_pm_admin_landing import ROOT
        spec=importlib.util.spec_from_file_location('optional_migration',ROOT/'migrations/versions/pm_tariff_authority_002.py')
        migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        before=self.snapshot()
        # These columns are already nullable, matching confirmed production.
        with patch.object(migration,'op',Operations(MigrationContext.configure(self.conn))):migration.upgrade()
        self.conn.commit();self.assertEqual(self.snapshot(),before)
        token=html.fromstring(self.client.get('/admin/billing/tariffs/new').data).xpath('//input[@name="csrf_token"]/@value')[0]
        response=self.client.post('/admin/billing/tariffs/new',data=dict(utility_type='electricity',rate='4',effective_date='2026-12-01',unit='kWh',csrf_token=token))
        self.conn.commit();self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(tuple(self.conn.execute(text('SELECT municipality,municipal_category,source_reference FROM bil_tariff_verification')).one()),(None,None,None))
    def test_manually_nullable_columns_accept_blanks_without_revision_stamp(self):
        # Reproduce the supplied production state via manual column changes only.
        for column in ('municipality','municipal_category','source_reference'):
            self.conn.execute(text('ALTER TABLE bil_tariff_verification ALTER COLUMN '+column+' SET NOT NULL'))
            self.conn.execute(text('ALTER TABLE bil_tariff_verification ALTER COLUMN '+column+' DROP NOT NULL'))
        self.conn.commit()
        before=self.snapshot()['bil_tariff']
        response=self.post('/admin/billing/tariffs/new',dict(self.base(),municipality='',municipal_category='',source_reference=''))
        self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(self.snapshot()['bil_tariff'][:1],before)
        evidence=[dict(r) for r in self.conn.execute(text('SELECT * FROM bil_tariff_verification')).mappings()]
        self.assertEqual(len(evidence),1)
        for key in ('municipality','municipal_category','source_reference'):self.assertIsNone(evidence[0][key])
        import importlib.util
        from unittest.mock import patch
        from alembic.operations import Operations
        from alembic.migration import MigrationContext
        from test_pm_admin_landing import ROOT
        spec=importlib.util.spec_from_file_location('optional_schema_compatibility',ROOT/'migrations/versions/pm_tariff_authority_002.py')
        migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        with patch.object(migration,'op',Operations(MigrationContext.configure(self.conn))):migration.upgrade()
        self.conn.commit()
        self.assertEqual([dict(r) for r in self.conn.execute(text('SELECT * FROM bil_tariff_verification')).mappings()],evidence)
    def test_server_records_verifier_timestamps_and_preserves_legacy(self):
        before=self.snapshot()['bil_tariff']
        response=self.post('/admin/billing/tariffs/new',dict(self.base(),verified_by='999',verified_at='2001-01-01',recorded_at='2001-01-01'))
        self.assertEqual(response.status_code,302,response.text)
        verification=self.conn.execute(text('SELECT * FROM bil_tariff_verification')).mappings().one()
        self.assertEqual(verification['verified_by'],7)
        self.assertEqual(verification['municipality'],'Synthetic Municipality')
        self.assertEqual(str(verification['effective_date']),'2026-12-01')
        self.assertIsNotNone(verification['verified_at'].tzinfo);self.assertIsNotNone(verification['recorded_at'].tzinfo)
        self.assertGreater(verification['recorded_at'].year,2001)
        self.assertEqual(before,self.snapshot()['bil_tariff'][:1])
        self.assertEqual(self.conn.execute(text('SELECT count(*) FROM bil_tariff_verification WHERE tariff_id=1')).scalar(),0)
        self.assertIn('Legacy',self.client.get('/admin/billing/tariffs').text)
        self.assertIn('Verified municipal source',self.client.get('/admin/billing/tariffs').text)
        self.assertIn('Official synthetic bill 123',self.client.get('/admin/billing/tariffs/'+str(verification['tariff_id'])+'/history').text)
    def test_owner_tenant_and_ordinary_users_cannot_read_or_post_tariffs(self):
        before=self.snapshot()
        token=html.fromstring(self.client.get('/admin/billing/tariffs/new').data).xpath('//input[@name="csrf_token"]/@value')[0]
        for role in ('owner','tenant','manager','learner'):
            self.conn.execute(text('UPDATE pm_admin_test_identity SET role=:role'),dict(role=role));self.conn.commit()
            for suffix in ('','/new','/1/edit','/1/history'):
                self.assertEqual(self.client.get('/admin/billing/tariffs'+suffix).status_code,403)
            for suffix in ('/new','/1/edit'):
                response=self.client.post('/admin/billing/tariffs'+suffix,data=dict(self.base(),csrf_token=token,base_id='1'))
                self.assertEqual(response.status_code,403)
        self.assertEqual(before,self.snapshot())
    def test_missing_migration_blocks_saving_but_legacy_list_remains_available(self):
        self.conn.execute(text('DROP TABLE bil_tariff_verification'));self.conn.commit()
        before=self.snapshot()
        response=self.client.get('/admin/billing/tariffs/new');self.assertEqual(response.status_code,200)
        doc=html.fromstring(response.data);self.assertTrue(doc.xpath('//button[@type="submit"]/@disabled'))
        response=self.post('/admin/billing/tariffs/new',self.base());self.assertEqual(response.status_code,400)
        self.assertIn('Database setup',response.text)
        self.assertEqual(self.snapshot(),before)
        self.assertEqual(self.client.get('/admin/billing/tariffs').status_code,200)
    def test_failed_evidence_insert_rolls_back_tariff_insert_atomically(self):
        # The authenticated fixture identity remains; only its temporary FK target is absent.
        self.conn.execute(text('DELETE FROM "user" WHERE id=7'));self.conn.commit()
        before=self.snapshot()
        response=self.post('/admin/billing/tariffs/new',self.base())
        self.assertEqual(response.status_code,503,response.text)
        self.assertEqual(before,self.snapshot())
        self.assertEqual(self.conn.execute(text('SELECT count(*) FROM bil_tariff_verification')).scalar(),0)

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(AuthorityTests(name) for name in AuthorityTests.__dict__ if name.startswith('test_'))
