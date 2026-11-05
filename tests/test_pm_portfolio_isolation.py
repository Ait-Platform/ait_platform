"""Two independent PM users may reuse names and identifiers; isolated PostgreSQL."""
import unittest,sys,uuid,json
from sqlalchemy import text
from lxml import html
from test_pm_utilities_capture import UtilitiesCaptureTests

class PortfolioIdentityTests(UtilitiesCaptureTests):
    def setUp(self):
        super().setUp()
        self.session.execute(text('ALTER TABLE bil_meter ADD COLUMN municipal_reference varchar(100)'))
        self.session.execute(text("UPDATE bil_property SET name='Almond' WHERE id IN (26,28)"))
        self.session.execute(text("INSERT INTO bil_owner_identity(id,display_name) VALUES(901,'Owner X'),(902,'Owner Y')"))
        self.session.execute(text('INSERT INTO bil_property_ownership(property_id,owner_id) VALUES(26,901),(28,902)'))
        self.session.commit();self.conn.commit()
    def create(self,p,owner,account='SHARED-001',meter='000SAME-W'):
        return self.capture_post('/billing/utilities/'+str(p)+'/accounts',account_number=account,owner_name=owner,water_number=meter)
    def test_same_account_and_meter_identifiers_are_independent_and_edit_has_no_false_duplicate(self):
        for user,p,owner in [(7,26,'Owner X'),(8,28,'Owner Y')]:
            self.user.id=user
            response=self.create(p,owner);self.assertEqual(response.status_code,302,response.text)
            account=self.session.execute(text("SELECT id,water_meter_id FROM bil_muni_account WHERE property_id=:p AND account_number='SHARED-001'"),dict(p=p)).one()
            data=self.account_form_for(p,account.id);data.pop('csrf_token');data['water_municipal_reference']='W-000REF'
            response=self.capture_post('/billing/utilities/'+str(p)+'/accounts',**data);self.assertEqual(response.status_code,302,response.text)
            reading=self.capture_post('/billing/utilities/'+str(p)+'/meter-readings',meter_id=str(account.water_meter_id),reading_date='2025-09-30',reading_value=str(user*100))
            self.assertEqual(reading.status_code,302,reading.text)
            page=self.client.get('/billing/utilities/'+str(p)+'/accounts/lookup?account_number=SHARED-001',headers=self.headers)
            self.assertEqual(page.json['account_id'],account.id)
            from app.program_billing.metsoa_setup_safety import owned_account_id
            with self.app.test_request_context():self.assertEqual(owned_account_id('SHARED-001'),account.id)
        meters=self.session.execute(text("SELECT id,property_id FROM bil_meter WHERE meter_number='000SAME-W' ORDER BY property_id")).all()
        self.assertEqual([r.property_id for r in meters],[26,28]);self.assertNotEqual(meters[0].id,meters[1].id)
        self.assertEqual(self.session.execute(text("SELECT count(*) FROM bil_meter_reading WHERE meter_id=ANY(:ids)"),dict(ids=[r.id for r in meters])).scalar(),2)
        self.user.id=7
        first=self.session.execute(text("SELECT id FROM bil_muni_account WHERE property_id=26 AND account_number='SHARED-001'")).scalar()
        data=self.account_form_for(26,first);data.pop('csrf_token')
        self.assertEqual(self.capture_post('/billing/utilities/26/accounts',**data).status_code,302)

    def account_form_for(self,p,a):
        self.accounts_url='/billing/utilities/'+str(p)+'/accounts'
        return self.account_form(a)
    def test_foreign_and_unowned_accounts_are_invisible_and_not_claimed(self):
        self.user.id=8;self.assertEqual(self.create(28,'Owner Y',account='PRIVATE',meter='PRIVATE-W').status_code,302)
        private=self.session.execute(text("SELECT id FROM bil_muni_account WHERE account_number='PRIVATE'")).scalar()
        self.user.id=7;before=self.snapshot()
        response=self.client.get('/billing/utilities/26/accounts/lookup?account_number=PRIVATE',headers=self.headers)
        self.assertFalse(response.json['found']);self.assertNotIn('Owner Y',response.text)
        response=self.capture_post('/billing/utilities/26/accounts',account_id=str(private),account_number='PRIVATE',owner_name='Owner X',water_number='PRIVATE-W')
        self.assertEqual(response.status_code,404);self.assertEqual(before,self.snapshot())
        self.session.execute(text("INSERT INTO bil_muni_account(id,account_number) VALUES(900,'UNPROVEN')"));self.session.commit();self.conn.commit()
        self.user.has_role=lambda role:True
        self.assertFalse(self.client.get('/billing/utilities/26/accounts/lookup?account_number=UNPROVEN',headers=self.headers).json['found'])
        from app.program_billing import municipal_association
        with self.app.test_request_context():
            from werkzeug.exceptions import NotFound
            with self.assertRaises(NotFound):municipal_association.state(26,900)
    def test_both_users_can_create_identical_configuration_without_claiming_foreign_records(self):
        for user,p in [(7,27),(8,28)]:
            self.user.id=user
            data=self.payload(p)
            data.update(accounts=[dict(id='a',number='SAME-CONFIG')],subWater=[dict(id='w',number='SAME-CONFIG-W')],subElec=[],mapping=[dict(account_id='a',water='w')],owners=[])
            response=self.save(data,p);self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(self.session.execute(text("SELECT count(*) FROM bil_meter WHERE meter_number='SAME-CONFIG-W'")).scalar(),2)
    def test_lease_property_and_draft_ids_are_private_to_each_user(self):
        foreign=uuid.uuid4()
        self.session.execute(text("INSERT INTO bil_lease_draft(id,property_id,manager_id,status,source_version,items,version,updated_at) VALUES(:d,28,8,'draft',:source,CAST(:items AS jsonb),1,now())"),dict(d=foreign,source=self.catalog['version'],items=json.dumps(self.initial)))
        self.session.commit();self.conn.commit()
        snapshot=sys.modules['app.program_billing.lease_snapshot']
        from werkzeug.exceptions import NotFound
        before=list(self.session.execute(text('SELECT * FROM bil_lease_draft ORDER BY id')))
        with self.app.test_request_context():
            with self.assertRaises(NotFound):snapshot.authorised_property(28)
            with self.assertRaises(NotFound):snapshot.draft_row(26,foreign)
        self.assertEqual(self.client.get('/certificate/28',headers=self.headers).status_code,404)
        self.user.id=8
        with self.app.test_request_context():
            self.assertEqual(snapshot.authorised_property(28)['id'],28)
            self.assertEqual(snapshot.draft_row(28,foreign)['id'],foreign)
            with self.assertRaises(NotFound):snapshot.draft_row(28,self.identifier)
        self.assertEqual(self.client.get('/certificate/26',headers=self.headers).status_code,404)
        self.assertEqual(before,list(self.session.execute(text('SELECT * FROM bil_lease_draft ORDER BY id'))))
    def test_cross_property_reading_and_account_ids_are_rejected_without_writes(self):
        self.user.id=8;self.assertEqual(self.create(28,'Owner Y').status_code,302)
        a,m=self.session.execute(text("SELECT id,water_meter_id FROM bil_muni_account WHERE property_id=28")).one()
        response=self.capture_post('/billing/utilities/28/meter-readings',meter_id=str(m),reading_date='2025-09-30',reading_value='100');self.assertEqual(response.status_code,302)
        r=self.session.execute(text('SELECT id FROM bil_meter_reading WHERE meter_id=:m'),dict(m=m)).scalar()
        self.user.id=7;before=self.snapshot()
        for path in ['/billing/utilities/28/accounts','/billing/utilities/28/meter-readings','/billing/utilities/28/metro-reconciliation','/billing/utilities/28/generate-metsoa','/billing/utilities/28/bulk-meter-linking','/meter_exceptions/28']:
            self.assertEqual(self.client.get(path,headers=self.headers).status_code,403,path)
        self.assertEqual(self.capture_post('/billing/utilities/26/meter-readings',meter_id=str(m),reading_date='2025-10-01',reading_value='110').status_code,403)
        self.assertEqual(self.client.get('/billing/utilities/26/meter-readings?edit='+str(r),headers=self.headers).status_code,404)
        self.assertEqual(self.client.get('/billing/utilities/26/accounts?edit='+str(a),headers=self.headers).status_code,404)
        self.assertEqual(before,self.snapshot())

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(PortfolioIdentityTests(name) for name in PortfolioIdentityTests.__dict__ if name.startswith('test_'))
