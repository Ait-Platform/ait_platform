"""Annual monitoring, persistent deduplication and opt-in worker tests."""
from contextlib import contextmanager
from datetime import date
import json
from pathlib import Path
import unittest
from unittest.mock import patch,Mock
from test_pm_municipal_tariff_verification import VerificationTests


class MonitorTests(VerificationTests):
    def setUp(self):
        super().setUp()
        self.auto=self.load('municipal_tariff_auto_activation',{})
        self.monitor=self.load('municipal_tariff_monitor',{})
        self.monitor.services=lambda:(self.tariff_service,self.source,self.documents,self.verification,self.workflow,self.auto)
        # Existing approved source; adoption and individual rates stay unverified.
        self.run_download()
        with self.app.app_context():
            source=self.documents.listing(self.documents.backend(),'2026/27')[0]
            signed=self.policy.issue_token(7,'2026/27',source['receipt_id'],'approve_source',self.policy.digest(source))
            self.policy.decide(self.policy.backend(),7,'2026/27',source['receipt_id'],'approve_source',signed,dict(source=source))

        @contextmanager
        def single(service):yield True
        self.single=single

    @contextmanager
    def deterministic(self):
        discovery=self.source.discovery;retrieve=self.source.retrieve
        with self.app.app_context(),patch.object(self.monitor,'singleton',self.single),patch.object(self.source,'discovery',side_effect=lambda year,**kw:discovery(year,fetch=self.fetch,**kw)),patch.object(self.source,'retrieve',side_effect=lambda candidate,**kw:retrieve(candidate,fetch=self.fetch,**kw)):
            yield

    def test_may_june_july_and_late_targets(self):
        self.assertEqual(self.monitor.targets(date(2026,4,30),{}),['2025/26'])
        self.assertEqual(self.monitor.targets(date(2026,5,1),{}),['2025/26','2026/27'])
        self.assertEqual(self.monitor.targets(date(2026,6,1),{}),['2025/26','2026/27'])
        self.assertEqual(self.monitor.targets(date(2026,7,1),{}),['2026/27'])
        self.assertEqual(self.monitor.targets(date(2027,8,1),{'2026/27':{'verified':False}}),['2026/27','2027/28'])

    def test_real_download_verification_path_never_activates_unverified_rates(self):
        before=self.snapshot()
        with self.deterministic():
            result=self.monitor.run(date(2026,7,1))
            self.assertEqual(len(result['checks']),1)
            check=result['checks'][0];self.assertFalse(check['verified'])
            self.assertIn('Publication adoption is not confirmed',str(check['issues']))
            self.assertIn('late',str(check['issues']))
            self.assertEqual(check['activation']['status'],'DISABLED')
            self.assertEqual(len(self.documents.listing(self.documents.backend(),'2026/27')),6)
        self.assertEqual(self.snapshot(),before)

    def test_daily_retry_and_alert_deduplication(self):
        with self.deterministic():
            first=self.monitor.run(date(2026,6,1))
            store=self.documents.backend();alerts=store.keys(self.monitor.ROOT+'alerts/')
            self.assertTrue(alerts)
            self.assertEqual(self.monitor.run(date(2026,6,1))['checks'],[])
            second=self.monitor.run(date(2026,6,2))
            self.assertEqual(alerts,store.keys(self.monitor.ROOT+'alerts/'))
            self.assertTrue(second['checks'])
            history=self.monitor.history(store)
            self.assertEqual(history['2026/27']['last_successful_check'],'2026-06-02')

    def test_retrieval_failure_is_persistent_and_existing_tariffs_unchanged(self):
        before=self.snapshot()
        with self.deterministic(),patch.object(self.source,'discovery',return_value=([],['Synthetic municipal outage'])):
            result=self.monitor.run(date(2026,7,1))
            self.assertIn('No official annual tariff publication',str(result))
            self.assertFalse(result['checks'][0]['successful_retrieval'])
            self.assertIn('Synthetic municipal outage',result['checks'][0]['retrieval_details'])
        self.assertEqual(before,self.snapshot())

    def test_administrator_register_displays_persistent_notifications(self):
        with self.deterministic():self.monitor.run(date(2026,7,1))
        response=self.client.get(self.url,query_string=dict(workspace='register',financial_year='2026/27'))
        self.assertEqual(response.status_code,200,response.text)
        self.assertIn('2026/27 exceptions',response.text)
        self.assertIn('Publication adoption is not confirmed',response.text)
        self.assertIn('Exceptions',response.text)

    def test_native_worker_and_activation_disabled_by_default(self):
        from flask import Flask
        app=Flask('monitor-fixture')
        with patch.dict('os.environ',{},clear=True),patch.object(self.monitor.threading,'Thread') as thread:
            self.monitor.install(app)
            self.assertFalse(app.config['PM_TARIFF_AUTO_ACTIVATION_ENABLED'])
            self.assertFalse(app.config['PM_TARIFF_MONITOR_ENABLED'])
            thread.assert_not_called()
            self.assertIn('pm-check-municipal-tariffs',app.cli.commands)

    def test_native_worker_starts_only_once_when_explicitly_enabled(self):
        from flask import Flask
        app=Flask('enabled-monitor-fixture');app.config['PM_TARIFF_MONITOR_ENABLED']=True
        with patch.dict('os.environ',{},clear=True),patch.object(self.monitor.threading,'Thread') as thread:
            self.monitor.install(app);self.monitor.install(app)
            self.assertEqual(thread.call_count,1)
            thread.return_value.start.assert_called_once()
            self.assertFalse(app.config['PM_TARIFF_AUTO_ACTIVATION_ENABLED'])

    def test_postgresql_session_lock_prevents_concurrent_workers(self):
        self.tariff_service.db.engine=self.conn.engine
        with self.monitor.singleton(self.tariff_service) as first:
            self.assertTrue(first)
            with self.monitor.singleton(self.tariff_service) as second:self.assertFalse(second)
        with self.monitor.singleton(self.tariff_service) as again:self.assertTrue(again)


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(MonitorTests(name) for name in MonitorTests.__dict__ if name.startswith('test_'))
