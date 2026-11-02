"""Municipal authority evidence and permissions on migrated temporary PostgreSQL."""
import unittest
from sqlalchemy import text
from lxml import html
from test_pm_tariff_management import ManagementTests

class AuthorityTests(ManagementTests):
    def base(self):
        return dict(utility_type='electricity',rate='3.5747',effective_date='2026-12-01',unit='kWh',
                    municipality='Synthetic Municipality',municipal_category='Domestic electricity',source_reference='Official synthetic bill 123')
    def test_required_evidence_rejects_missing_and_blank_fields(self):
        before=self.snapshot()
        for key in ('municipality','municipal_category','source_reference'):
            for value in ('','   '):
                form=self.base();form[key]=value
                response=self.post('/admin/billing/tariffs/new',form)
                self.assertEqual(response.status_code,400,response.text);self.assertIn('required',response.text)
        self.assertEqual(self.snapshot(),before)
        self.assertEqual(self.conn.execute(text('SELECT count(*) FROM bil_tariff_verification')).scalar(),0)
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
