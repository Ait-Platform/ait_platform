"""Read saved METSOA totals and post PM-only transfers on isolated PostgreSQL."""
import json
import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import patch
from lxml import html
from sqlalchemy import text
import test_pm_soa_utilities as fixture


class MetsoaTransferTests(unittest.TestCase):
    path=fixture.PMUtilityDefinitionTests.path
    fixed_form=fixture.PMUtilityDefinitionTests.fixed_form
    save=fixture.PMUtilityDefinitionTests.save
    utility_form=fixture.PMUtilityDefinitionTests.utility_form
    def setUp(self):
        fixture.PMUtilityDefinitionTests.setUp(self)
        from app.program_billing import lease_soa_metsoa
        clock=patch.object(lease_soa_metsoa,'today',return_value=date(2026,10,28));clock.start();self.addCleanup(clock.stop)
        self.transfer=lease_soa_metsoa
        self.session.execute(text('CREATE TEMP TABLE bil_meter(id integer PRIMARY KEY,property_id integer,meter_number varchar(50),utility_type varchar(50))'))
        # Column names/types verified against the actual local public METSOA schema.
        self.session.execute(text('CREATE TEMP TABLE bil_metsoa_meter_month(id integer PRIMARY KEY,tenant_id integer,meter_id integer,month varchar(7),utility_type varchar(50),total_due double precision,curr_date varchar(50),updated_at timestamp,UNIQUE(tenant_id,meter_id,month))'))
        self.session.execute(text("INSERT INTO bil_meter VALUES(1,26,'W1','water'),(2,26,'E1','electricity'),(3,27,'PRIVATE','water')"))
        self.session.execute(text("""INSERT INTO bil_metsoa_meter_month VALUES
            (1,11,1,'2026-10','water',345.67,'2026/10/27',now()),
            (2,11,2,'2026-10','electricity',456.78,'2026/10/27',now()),
            (3,11,3,'2026-10','water',999,'2026/10/27',now()),
            (4,22,1,'2026-10','water',888,'2026/10/27',now()),
            (5,11,1,'2026-12','water',111,'2026/12/27',now())"""));self.session.commit()
    def page(self):return self.client.get(self.path()+'/fixed/utilities',headers=self.headers)
    def transfer_form(self,source_id=1):
        page=self.page();self.assertEqual(page.status_code,200,page.text)
        form=html.fromstring(page.data).xpath('//form[input[@name="source_id"][@value="'+str(source_id)+'"]]')[0]
        return {e.get('name'):e.get('value') for e in form.xpath('./input')}
    def post_source(self,data,prop=26,tenant=11,headers=None):
        return self.client.post(self.path(prop,tenant)+'/fixed/utilities/post',data=data,headers=self.headers if headers is None else headers)
    def test_source_matching_exact_amount_audit_reference_and_idempotency(self):
        before=self.session.execute(text('SELECT * FROM bil_metsoa_meter_month ORDER BY id')).all()
        page=self.page();tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//*[@id="metsoa-charges"]/thead/tr/th/text()'),['Billing Period','Utility','Amount','Status','Action'])
        self.assertEqual(len(tree.xpath('//*[@id="metsoa-charges"]/tbody/tr')),3)
        self.assertNotIn('999.00',page.text);self.assertNotIn('888.00',page.text)
        self.assertEqual(tree.xpath('//input[@name="source_id"]/@value'),['1','2'])
        data=self.transfer_form();self.assertEqual(self.post_source(data).status_code,302)
        self.assertEqual(self.post_source(data).status_code,302)
        row=self.session.execute(text('SELECT * FROM bil_tenant_ledger')).mappings().one()
        self.assertEqual((row['amount'],row['ref'],row['month']),(Decimal('345.67'),'D0001','2026-10'))
        audit=json.loads(row['posting_notes'][len('PMMETSOA:'):])
        self.assertEqual(audit['metsoa_reference'],'bil_metsoa_meter_month:1')
        self.assertEqual((audit['property_id'],audit['tenant_id'],audit['meter_id']),(26,11,1))
        self.assertEqual(self.session.execute(text('SELECT * FROM bil_metsoa_meter_month ORDER BY id')).all(),before)
        self.assertIn('Posted — D0001',self.page().text)
        self.assertEqual(self.post_source(self.transfer_form(2)).status_code,302)
        self.assertEqual(self.session.execute(text('SELECT sum(amount) FROM bil_tenant_ledger')).scalar(),Decimal('802.45'))
    def test_changed_source_future_period_csrf_auth_and_tenant_scope(self):
        data=self.transfer_form()
        self.session.execute(text('UPDATE bil_metsoa_meter_month SET total_due=999 WHERE id=1'));self.session.commit()
        self.assertEqual(self.post_source(data).status_code,409)
        self.assertEqual(self.post_source(dict(data,source_id='5')).status_code,409)
        self.assertEqual(self.post_source(dict(data,source_id='3')).status_code,404)
        self.assertEqual(self.post_source(dict(data,source_id='4')).status_code,404)
        self.assertEqual(self.post_source(data,28,33).status_code,404)
        self.assertEqual(self.post_source(data,headers={}).status_code,403)
        self.assertEqual(self.post_source(dict(data,csrf_token='bad')).status_code,400)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),0)
    def test_legacy_posting_and_changed_posted_source_are_not_duplicated(self):
        self.session.execute(text("INSERT INTO bil_tenant_ledger(tenant_id,month,description,kind,amount,ref) VALUES(11,'2026-10','Utilities (METSOA)','charge',802.45,'METSOA-AUTO')"));self.session.commit()
        page=self.page();self.assertIn('Existing utility posting requires reconciliation',page.text)
        self.assertFalse(html.fromstring(page.data).xpath('//form'))
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),1)
    def test_posted_source_updates_do_not_overwrite_history(self):
        self.assertEqual(self.post_source(self.transfer_form()).status_code,302)
        before=self.session.execute(text('SELECT * FROM bil_tenant_ledger')).all()
        self.session.execute(text('UPDATE bil_metsoa_meter_month SET total_due=400 WHERE id=1'));self.session.commit()
        self.assertIn('source subsequently changed',self.page().text)
        self.assertEqual(self.session.execute(text('SELECT * FROM bil_tenant_ledger')).all(),before)
    def test_missing_saved_sources_give_clear_notice(self):
        self.session.execute(text('DROP TABLE bil_metsoa_meter_month'));self.session.commit()
        page=self.page();self.assertEqual(page.status_code,200)
        self.assertIn('Saved METSOA meter/month totals',page.text)
        self.assertFalse(html.fromstring(page.data).xpath('//form'))
    def test_transferred_water_prevents_scheduled_water_and_sewerage_double_billing(self):
        self.assertEqual(self.post_source(self.transfer_form()).status_code,302)
        # Existing recurring definitions remain untouched; only an overlapping due charge is skipped.
        with self.app.app_context():
            self.fixed.save(26,11,dict(charge_type='water',fixed_agreed='1',amount='345.67',start_date='2026-10-01',end_date='2026-10-31',day_of_month='1',status='active',csrf_token=self.csrf,posting_key=self.fixed.form_token(26,11,None,'utility')))
            self.assertEqual(self.fixed.run_due(),dict(posted=0,failed=0))
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),1)

    def test_database_source_key_unique_under_concurrent_transactions(self):
        import concurrent.futures, threading, uuid
        from sqlalchemy import create_engine
        from sqlalchemy.exc import IntegrityError
        from sqlalchemy.orm import scoped_session, sessionmaker
        from app.program_billing import lease_soa_posting
        with self.app.app_context():charge=self.transfer.list_charges(26,11)[0]
        name='pm_metsoa_test_'+uuid.uuid4().hex
        self.assertRegex(name,r'^pm_metsoa_test_[a-f0-9]{32}$')
        table='public.'+name;sequence=table+'_seq'
        self.session.commit()
        self.conn.execute(text('CREATE TABLE '+table+' (LIKE bil_tenant_ledger INCLUDING ALL)'))
        self.conn.execute(text('CREATE SEQUENCE '+sequence+' MINVALUE 1 MAXVALUE 9999 NO CYCLE'));self.conn.commit()
        engine=create_engine(self.engine.url);sessions=scoped_session(sessionmaker(bind=engine))
        worker_db=type('WorkerDB',(),{'session':sessions})()
        barrier=threading.Barrier(2)
        def run(_):
            try:
                barrier.wait(timeout=10)
                ref=lease_soa_posting.next_reference('debit')
                sessions.execute(text('INSERT INTO '+table+"""(tenant_id,txn_date,month,description,kind,amount,ref,posting_key,posting_hash)
                    VALUES(11,'2026-10-28','2026-10','Water / Sewerage','charge',345.67,:ref,:key,:digest)"""),dict(ref=ref,key=charge['key'],digest=charge['digest']))
                sessions.commit();return True
            except IntegrityError:
                sessions.rollback();return False
            finally:sessions.remove()
        try:
            with patch.object(lease_soa_posting,'db',worker_db),patch.dict(lease_soa_posting.REFERENCE_SEQUENCES,{'D':sequence}):
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                    self.assertEqual(sorted(pool.map(run,[1,2])),[False,True])
            with engine.connect() as connection:
                self.assertEqual(connection.execute(text('SELECT count(*) FROM '+table)).scalar(),1)
        finally:
            sessions.remove();engine.dispose()
            self.conn.execute(text('DROP TABLE '+table));self.conn.execute(text('DROP SEQUENCE '+sequence));self.conn.commit()

    def test_valid_future_source_token_is_still_rejected(self):
        with self.app.app_context():
            charge=next(c for c in self.transfer.list_charges(26,11) if c['id']==5)
        response=self.post_source(dict(csrf_token=self.csrf,source_id='5',posting_key=charge['token']))
        self.assertEqual(response.status_code,409)
        self.assertIn('Future',response.text)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),0)

    def test_failed_transfer_rolls_back_and_retry_posts_once(self):
        from sqlalchemy.exc import SQLAlchemyError
        data=self.transfer_form()
        original=self.transfer.posting.append_entry
        def fail_after_insert(*args,**kwargs):
            original(*args,**kwargs)
            raise SQLAlchemyError('Synthetic failed transfer')
        with patch.object(self.transfer.posting,'append_entry',side_effect=fail_after_insert):
            response=self.post_source(data)
        self.assertEqual(response.status_code,503)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),0)
        self.assertEqual(self.post_source(data).status_code,302)
        self.assertEqual(self.post_source(data).status_code,302)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),1)
