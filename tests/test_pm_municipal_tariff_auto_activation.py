"""Atomic activation against isolated PostgreSQL records, never production."""
from contextlib import contextmanager
from copy import deepcopy
from datetime import date
from decimal import Decimal
from unittest.mock import patch
import unittest
from sqlalchemy import text,event
from test_pm_municipal_tariff_verification import VerificationTests


class AutomaticActivationTests(VerificationTests):
    def setUp(self):
        super().setUp()
        self.auto=self.load('municipal_tariff_auto_activation',{})
        self.conn.execute(text('DELETE FROM bil_tariff_verification'));self.conn.execute(text('DELETE FROM bil_tariff'));self.conn.commit()
        self.run_download()
        self.app.config['PM_TARIFF_AUTO_ACTIVATION_ENABLED']=True
        with self.app.app_context():self.receipt=self.documents.listing(self.documents.backend(),'2026/27')[0]['receipt_id']
        self.verified=self.extraction(True)
        # Atomic-writer contract fixture: narrow scope to two source-backed bands.
        # Missing real schedule components are tested separately as HOLD.
        self.verified['extracted']={key:self.verified['extracted'][key] for key in ('W01','S01')}
        self.verified['issues']=[]

    @contextmanager
    def supported(self):
        with self.app.app_context(),patch.object(self.verification,'REQUIRED',('W01','S01')),patch.object(self.verification,'extract',side_effect=lambda *args:deepcopy(self.verified)):
            yield

    def activate(self):
        return self.auto.activate(self.tariff_service,self.documents,self.verification,self.documents.backend(),'2026/27',self.receipt,7)

    def test_whole_batch_preserves_history_and_is_idempotent(self):
        self.seed(80,'W01','39.39','2025-07-01','water','kL');self.seed(81,'S01','5.45','2025-07-01','sewerage','kL')
        self.conn.execute(text('UPDATE bil_tariff SET block_start=0,block_end=200,reduction_factor=0.2 WHERE id IN (80,81)'));self.conn.commit()
        before=self.snapshot()['bil_tariff']
        with self.supported():
            result=self.activate();self.conn.commit()
            self.assertEqual(result['created'],2)
            again=self.activate();self.conn.commit();self.assertEqual(again['created'],0);self.assertEqual(again['skipped'],2)
        after=self.snapshot()['bil_tariff'];self.assertEqual(len(after),4)
        for old in before:self.assertIn(old,after)
        for code,rate in (('W01','44.1000'),('S01','6.5300')):
            family=[r for r in after if r['code']==code]
            self.assertEqual(self.review.resolve(family,date(2026,6,30))[0][0]['effective_date'],'2025-07-01')
            self.assertEqual(Decimal(str(self.review.resolve(family,date(2026,7,1))[0][0]['rate'])),Decimal(rate))
        audit=list(self.conn.execute(text('SELECT * FROM bil_tariff_verification')).mappings())
        self.assertEqual(len(audit),2);self.assertTrue(all('AIT automatic annual 2026/27' in r['source_reference'] for r in audit))

    def test_failure_on_second_audit_rolls_back_entire_schedule(self):
        before=self.snapshot()
        count=0
        def fail(conn,cursor,sql,parameters,context,many):
            nonlocal count
            if sql.startswith('INSERT INTO bil_tariff_verification'):
                count+=1
                if count==2:raise RuntimeError('Synthetic second audit failure')
        with self.supported():
            event.listen(self.conn,'before_cursor_execute',fail)
            try:
                with self.assertRaises(RuntimeError):self.activate()
            finally:event.remove(self.conn,'before_cursor_execute',fail)
        self.conn.rollback();self.assertEqual(self.snapshot(),before)
        self.assertEqual(self.conn.execute(text('SELECT count(*) FROM bil_tariff_verification')).scalar(),0)

    def test_hold_missing_source_duplicate_date_and_changed_units_block_all(self):
        with self.supported():
            self.verified['adoption']=None
            with self.assertRaises(ValueError):self.activate()
            self.verified['adoption']={'date':'2026-05-29','statement':'SYNTHETIC TEST'}
            self.seed(82,'W01','99','2026-07-01','water','kL')
            self.conn.execute(text('UPDATE bil_tariff SET block_start=0,block_end=200,reduction_factor=0.2 WHERE id=82'));self.conn.commit()
            before=self.snapshot()
            with self.assertRaises(ValueError):self.activate()
            self.assertEqual(self.snapshot(),before)
        self.assertEqual(self.conn.execute(text('SELECT count(*) FROM bil_tariff_verification')).scalar(),0)

    def test_default_disabled_does_not_touch_database(self):
        before=self.snapshot()
        self.app.config.pop('PM_TARIFF_AUTO_ACTIVATION_ENABLED')
        with self.app.app_context(),patch.object(self.tariff_service,'lock_identity',side_effect=AssertionError('disabled')):
            result=self.activate();self.assertEqual(result['status'],'DISABLED')
        self.assertEqual(before,self.snapshot())

    def test_audit_actor_does_not_inherit_logged_in_administrator_authority(self):
        import sys,types
        fake=types.ModuleType('app.models.auth');fake.User=object
        self.app.config['PM_TARIFF_AUTOMATION_ACTOR_ID']=7
        actor=types.SimpleNamespace(id=7,role='manager',is_admin=False,is_active=True)
        with self.app.test_request_context(),patch.dict(sys.modules,{'app.models.auth':fake}),patch.object(self.tariff_service.db.session,'get',return_value=actor):
            with self.assertRaises(ValueError):self.auto.audit_actor(self.tariff_service)
            actor.role='admin';self.assertEqual(self.auto.audit_actor(self.tariff_service),7)
            actor.is_active=False
            with self.assertRaises(ValueError):self.auto.audit_actor(self.tariff_service)

    def test_future_versions_are_selected_only_from_effective_date(self):
        self.tariff_service.today=lambda:date(2026,6,1)
        with self.supported():
            self.assertEqual(self.activate()['status'],'SCHEDULED');self.conn.commit()
        rows=self.snapshot()['bil_tariff']
        self.assertFalse(self.review.resolve(rows,date(2026,6,30))[0])
        self.assertEqual(len(self.review.resolve(rows,date(2026,7,1))[0]),2)


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(AutomaticActivationTests(name) for name in AutomaticActivationTests.__dict__ if name.startswith('test_'))
