"""Controlled PM corrections against isolated PostgreSQL, never production."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
from decimal import Decimal
from sqlalchemy import text, event
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from alembic.operations import Operations
from alembic.migration import MigrationContext
from lxml import html
import test_pm_soa_ledger as fixture


def apply_corrections(connection):
    path=Path(__file__).resolve().parents[1]/'migrations/versions/pm_soa_002_corrections.py'
    spec=importlib.util.spec_from_file_location('pm_corrections_migration',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    with patch.object(module,'op',Operations(MigrationContext.configure(connection))):
        module.upgrade()


class PMCorrectionTests(unittest.TestCase):
    path=fixture.PMTenantLedgerTests.path
    posting_form=fixture.PMTenantLedgerTests.posting_form
    submit=fixture.PMTenantLedgerTests.submit
    def setUp(self):
        fixture.PMTenantLedgerTests.setUp(self)
        apply_corrections(self.conn);self.conn.commit()
        from app.program_billing import lease_soa_corrections
        scoped=patch.object(lease_soa_corrections,'db',self.db);scoped.start();self.addCleanup(scoped.stop)
    def seed(self,kind='charge',**values):
        data=self.posting_form(kind,**values)
        self.assertEqual(self.submit(data).status_code,302)
        return self.session.execute(text('SELECT id FROM bil_tenant_ledger ORDER BY id DESC LIMIT 1')).scalar()
    def edit_form(self,row_id,property_id=26,tenant_id=11,**changes):
        page=self.client.get(self.path(property_id,tenant_id)+'?edit='+str(row_id),headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data)
        data={el.get('name'):el.get('value','') for el in tree.xpath('//form//input')}
        data['direction']=tree.xpath('//select[@name="direction"]/option[@selected]/@value')[0]
        data['reason']='Correct amount entered in error'
        data.update(changes)
        return data
    def correct(self,row_id,data,property_id=26,tenant_id=11,headers=None):
        return self.client.post(self.path(property_id,tenant_id)+'/'+str(row_id)+'/correct',
            data=data,headers=self.headers if headers is None else headers)
    def test_summary_four_columns_and_preserved_original_linked_pair(self):
        row_id=self.seed()
        before=dict(self.session.execute(text('SELECT * FROM bil_tenant_ledger WHERE id=:id'),dict(id=row_id)).mappings().one())
        data=self.edit_form(row_id,amount='125.50',description='Rent Due corrected',txn_date='2026-10-28')
        statements=[]
        def record(conn,cursor,statement,parameters,context,executemany):statements.append(statement.lower())
        event.listen(self.conn,'before_cursor_execute',record)
        try:result=self.correct(row_id,data)
        finally:event.remove(self.conn,'before_cursor_execute',record)
        self.assertEqual(result.status_code,302,result.text)
        self.assertEqual(before,dict(self.session.execute(text('SELECT * FROM bil_tenant_ledger WHERE id=:id'),dict(id=row_id)).mappings().one()))
        rows=self.session.execute(text('SELECT * FROM bil_tenant_ledger WHERE correction_of=:id ORDER BY id'),dict(id=row_id)).mappings().all()
        self.assertEqual([r['amount'] for r in rows],[Decimal('-100.10'),Decimal('125.50')])
        self.assertEqual([r['correction_role'] for r in rows],['reversal','replacement'])
        self.assertTrue(all(r['txn_date'].strftime('%Y-%m-%d')=='2026-10-28' for r in rows))
        self.assertEqual([r['ref'] for r in rows],['C0001','D0002'])
        self.assertTrue(all(r['correction_actor']==7 and r['correction_reason']==data['reason'] and r['correction_recorded_at'] for r in rows))
        self.assertEqual(rows[0]['correction_group'],rows[1]['correction_group'])
        page=self.client.get(self.path(),headers=self.headers);tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//*[@id="ledger-summary"]/thead/tr/th/text()'),
            ['Property','Registered tenant','Tenant account','Outstanding Balance'])
        self.assertEqual(tree.xpath('string(//*[@id="ledger-summary"]/tbody/tr/td[4])'),'R 125.50')
        self.assertNotIn('Outstanding balance: R',page.text)
        self.assertEqual(len(tree.xpath('//*[@id="ledger-transactions"]//tbody/tr')),3)
        self.assertEqual(len(tree.xpath('//*[@id="ledger-transactions"]//tbody/tr/td[last()]/*[text()="Edit"]')),3)
        self.assertIn('Reversal of D0001',page.text);self.assertIn('Replacement of D0001',page.text)
        self.assertIn(data['reason'],page.text)
        self.assertFalse(any('debtor' in s or 'job_card' in s or 'metsoa' in s for s in statements))
    def test_payment_correction_and_duplicate_payload_protection(self):
        row_id=self.seed('payment',amount='80.05')
        data=self.edit_form(row_id,amount='40.10')
        self.assertEqual(self.correct(row_id,data).status_code,302)
        self.assertEqual(self.correct(row_id,data).status_code,302)
        self.assertEqual(self.correct(row_id,dict(data,amount='41.10')).status_code,409)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),3)
        self.assertEqual(self.session.execute(text('SELECT sum(amount) FROM bil_tenant_ledger')).scalar(),Decimal('-40.10'))
    def test_opening_correction_keeps_one_marker_and_supports_replacement_chain(self):
        row_id=self.seed('opening_balance',amount='100',direction='debit')
        data=self.edit_form(row_id,amount='25',direction='credit')
        self.assertEqual(self.correct(row_id,data).status_code,302)
        replacement=self.session.execute(text("SELECT id FROM bil_tenant_ledger WHERE correction_role='replacement'")).scalar()
        self.assertEqual(self.correct(replacement,self.edit_form(replacement,amount='0',direction='zero')).status_code,302)
        self.assertEqual(self.session.execute(text("SELECT count(*) FROM bil_tenant_ledger WHERE kind='opening_balance' OR lower(btrim(description))='opening balance'")).scalar(),1)
        self.assertEqual(self.session.execute(text('SELECT sum(amount) FROM bil_tenant_ledger')).scalar(),Decimal('0.00'))
        self.assertEqual(self.submit(self.posting_form('opening_balance')).status_code,409)
        page=self.client.get(self.path(),headers=self.headers)
        self.assertTrue(html.fromstring(page.data).xpath('//button[@disabled][text()="Opening Balance"]'))
    def test_already_corrected_source_and_audit_reversal_cannot_be_corrected_again(self):
        row_id=self.seed()
        first=self.edit_form(row_id)
        second=self.edit_form(row_id,amount='200')
        self.assertEqual(self.correct(row_id,first).status_code,302)
        self.assertEqual(self.correct(row_id,second).status_code,409)
        reversal=self.session.execute(text("SELECT id FROM bil_tenant_ledger WHERE correction_role='reversal'")).scalar()
        self.assertEqual(self.correct(reversal,self.edit_form(reversal)).status_code,409)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),3)
    def test_atomic_failure_rolls_back_both_rows_and_retry_succeeds(self):
        from app.program_billing import lease_soa_posting as posting
        row_id=self.seed()
        data=self.edit_form(row_id,amount='75.25')
        allocator=posting.next_reference
        calls=[]
        def fail_second(direction):
            calls.append(direction)
            if len(calls)==2:raise SQLAlchemyError('Synthetic second-row failure')
            return allocator(direction)
        with patch.object(posting,'next_reference',side_effect=fail_second):
            result=self.correct(row_id,data)
        self.assertEqual(result.status_code,503,result.text)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),1)
        self.assertEqual(self.correct(row_id,data).status_code,302)
        self.assertEqual(self.session.execute(text('SELECT sum(amount) FROM bil_tenant_ledger')).scalar(),Decimal('75.25'))
    def test_validation_and_stale_source_preserve_form(self):
        row_id=self.seed()
        data=self.edit_form(row_id)
        for change in ({'reason':''},{'amount':'-1'},{'amount':'0'},{'amount':'NaN'},
                {'txn_date':'2026-02-30'},{'description':'Opening Balance'},{'amount':'100000000'}):
            result=self.correct(row_id,dict(data,**change))
            self.assertEqual(result.status_code,400,result.text)
            self.assertEqual(html.fromstring(result.data).xpath('//input[@name="posting_key"]/@value'),[data['posting_key']])
        # Simulate a legacy editor changing a source after this form was issued.
        self.session.execute(text("UPDATE bil_tenant_ledger SET description='Legacy concurrent change' WHERE id=:id"),dict(id=row_id));self.session.commit()
        self.assertEqual(self.correct(row_id,data).status_code,409)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),1)
    def test_auth_csrf_scope_and_other_tenant_access(self):
        row_id=self.seed()
        data=self.edit_form(row_id)
        self.assertEqual(self.client.get(self.path()+'/'+str(row_id)+'/correct',headers=self.headers).status_code,405)
        self.assertEqual(self.correct(row_id,data,headers={}).status_code,403)
        self.assertEqual(self.correct(row_id,dict(data,csrf_token='')).status_code,400)
        self.assertEqual(self.correct(row_id,dict(data,posting_key='invalid')).status_code,400)
        self.assertEqual(self.correct(row_id,data,28,33).status_code,404)
        self.assertEqual(self.correct(row_id,data,27,22).status_code,404)
        self.assertEqual(self.client.get(self.path(27,22)+'?edit='+str(row_id),headers=self.headers).status_code,404)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),1)
    def test_database_foreign_key_and_unique_role_guard(self):
        row_id=self.seed()
        self.assertEqual(self.correct(row_id,self.edit_form(row_id)).status_code,302)
        self.session.commit()
        with self.assertRaises(IntegrityError):
            self.session.execute(text("""INSERT INTO bil_tenant_ledger(tenant_id,kind,amount,correction_of,
                correction_role,correction_group,correction_actor,correction_recorded_at,correction_reason)
                VALUES(22,'correction_reversal',-10,:id,'reversal',:group,7,now(),'Wrong tenant')"""),
                dict(id=row_id,group='a'*32))
        self.session.rollback()
        with self.assertRaises(IntegrityError):
            self.session.execute(text("""INSERT INTO bil_tenant_ledger(tenant_id,kind,amount,correction_of,
                correction_role,correction_group,correction_actor,correction_recorded_at,correction_reason)
                VALUES(11,'correction_reversal',-10,:id,'reversal',:group,7,now(),'Duplicate reversal')"""),
                dict(id=row_id,group='b'*32))
        self.session.rollback()
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),3)
    def test_missing_correction_migration_blocks_without_writing(self):
        row_id=self.seed()
        data=self.edit_form(row_id)
        self.session.execute(text('DROP INDEX uq_pm_ledger_correction_role'));self.session.commit()
        result=self.correct(row_id,data)
        self.assertEqual(result.status_code,503);self.assertIn('pm_soa_002',result.text)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),1)

    def test_correction_retry_is_serialised_by_persistent_database_lock(self):
        import hashlib
        from app.program_billing import lease_soa_posting as posting
        row_id=self.seed()
        data=self.edit_form(row_id)
        with self.app.test_request_context():nonce=posting.signer().loads(data['posting_key'])[4]
        key=int.from_bytes(hashlib.sha256(('PMCORRECT:'+nonce).encode()).digest()[:8],'big',signed=True)
        original_model=posting.ledger_model
        def check_lock():
            with self.engine.connect() as second:
                self.assertFalse(second.execute(text('SELECT pg_try_advisory_xact_lock(:key)'),dict(key=key)).scalar())
                second.rollback()
            return original_model()
        with patch.object(posting,'ledger_model',side_effect=check_lock):
            self.assertEqual(self.correct(row_id,data).status_code,302)
        self.assertEqual(self.correct(row_id,data).status_code,302)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),3)
