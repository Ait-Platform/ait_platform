"""Continuing charges are incident annotations, never financial postings."""
import unittest,uuid,importlib.util
from pathlib import Path
from unittest.mock import patch
from io import BytesIO
from decimal import Decimal
from sqlalchemy import text
from alembic.operations import Operations
from alembic.migration import MigrationContext
import test_pm_meter_exception_records as fixture
class ExceptionChargeTests(fixture.ExceptionRecordTests):
    def setUp(self):
        super().setUp()
        self.session.execute(text('SET search_path TO pg_temp,public'))
        spec=importlib.util.spec_from_file_location('charge_migration',Path('migrations/versions/pm_meter_exceptions_002.py'));migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        with patch.object(migration,'op',Operations(MigrationContext.configure(self.conn))):migration.upgrade()
        self.session.commit();self.conn.commit()
        for account in (1,2,3):
            self.session.execute(text("INSERT INTO bil_municipal_statement(account_id,period,statement_date,arrears,balance,due,due_date) VALUES(:a,'2026-10','2026-10-20',10.01,100.02,90.01,'2026-10-30')"),dict(a=account))
        self.session.commit();self.conn.commit()
        self.assertEqual(self.post_exception(self.fields()).status_code,302);self.incident=self.last_id()
        self.bill_id=self.session.execute(text('SELECT id FROM bil_municipal_statement WHERE account_id=1')).scalar()
    def charge_fields(self,**changes):
        fields=dict(action='charge',account_id='1',exception_id=str(self.incident),statement_id=str(self.bill_id),amount='19.30',charge_reference='BILL-LINE-1',charge_description='Insurance on stolen meter',charge_remarks='Pending municipal investigation',submission_key=str(uuid.uuid4()))
        fields.update(changes);return fields
    def statements_snapshot(self):return [dict(r) for r in self.session.execute(text('SELECT * FROM bil_municipal_statement ORDER BY id')).mappings()]
    def test_charge_persistence_replay_and_financial_preservation(self):
        before=self.snapshot();bills=self.statements_snapshot();fields=self.charge_fields()
        result=self.post_exception(fields);self.assertEqual(result.status_code,302,result.text)
        self.assertEqual(self.post_exception(fields).status_code,302)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_exception_charge')).scalar(),1)
        charge=self.session.execute(text('SELECT * FROM bil_meter_exception_charge')).mappings().one()
        self.assertEqual(charge['amount'],Decimal('19.30'));self.assertEqual(charge['bill_snapshot']['balance'],'100.02')
        page=self.client.get(result.location,headers=self.headers);self.assertIn('Pending investigation',page.text);self.assertIn('R19.30',page.text)
        self.assertEqual(bills,self.statements_snapshot());self.assertEqual(before,self.snapshot())
        self.assertEqual(self.post_exception(dict(fields,amount='20.00')).status_code,409)
        self.assertEqual(self.post_exception(self.charge_fields(amount='30.00')).status_code,302)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_exception_charge')).scalar(),2)
    def test_account_bill_and_amount_validation(self):
        other=self.session.execute(text('SELECT id FROM bil_municipal_statement WHERE account_id=2')).scalar()
        for changes in [dict(statement_id=str(other)),dict(amount='0'),dict(amount='-1'),dict(amount='NaN'),dict(amount='1.001'),dict(amount='999999999999999999')]:
            result=self.post_exception(self.charge_fields(**changes));self.assertEqual(result.status_code,400,result.text)
        self.assertEqual(self.post_exception(self.charge_fields(account_id='2')).status_code,403)
        with patch.object(self.exceptions,'charges_ready',return_value=False):self.assertEqual(self.post_exception(self.charge_fields()).status_code,503)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_exception_charge')).scalar(),0)
    def test_charge_evidence_and_snapshot_preserve_original_bill(self):
        from app.program_billing import meter_exception_evidence as storage
        objects={}
        def put(key,data,**kw):objects[key]=data;return key
        with patch.object(storage.transport,'upload_bytes_to_r2',side_effect=put),patch.object(storage.transport,'read_file_from_r2',side_effect=lambda key:objects[key]):
            fields=self.charge_fields();fields['evidence']=(BytesIO(self.png()),'bill-evidence.png')
            result=self.post_exception(fields);self.assertEqual(result.status_code,302,result.text)
            linked=self.session.execute(text('SELECT charge_id,evidence_id FROM bil_meter_exception_charge_evidence')).one()
            evidence=self.client.get('/meter_exceptions/26/evidence/'+str(linked.evidence_id),headers=self.headers)
            self.assertEqual(evidence.status_code,200);self.assertEqual(evidence.data,self.png())
        # Simulate a later authorised bill edit only in the temporary fixture.
        self.session.execute(text('UPDATE bil_municipal_statement SET balance=200.00,version=version+1 WHERE id=:id'),dict(id=self.bill_id));self.session.commit();self.conn.commit()
        saved=self.session.execute(text('SELECT bill_snapshot FROM bil_meter_exception_charge')).scalar()
        self.assertEqual(saved['balance'],'100.02');self.assertEqual(saved['version'],1)
    def test_wrong_incident_evidence_link_and_upload_failure_are_atomic(self):
        from app.program_billing import meter_exception_evidence as storage
        before=self.statements_snapshot()
        with patch.object(storage.transport,'upload_bytes_to_r2',side_effect=RuntimeError('test storage failure')):
            with self.assertLogs(self.app.logger,level='ERROR'):result=self.post_exception(dict(self.charge_fields(),evidence=(BytesIO(self.png()),'bill.png')))
        self.assertEqual(result.status_code,503);self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_exception_charge')).scalar(),0)
        self.assertEqual(before,self.statements_snapshot())
        self.assertEqual(self.post_exception(self.charge_fields()).status_code,302)
        charge=self.session.execute(text('SELECT id FROM bil_meter_exception_charge')).scalar()
        self.assertEqual(self.post_exception(self.fields()).status_code,302);other=self.last_id()
        result=self.post_exception(dict(action='evidence',account_id='1',exception_id=str(other),charge_id=str(charge),evidence=(BytesIO(self.png()),'wrong.png')))
        self.assertEqual(result.status_code,403)

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ExceptionChargeTests(name) for name in ExceptionChargeTests.__dict__ if name.startswith('test_'))
