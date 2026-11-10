"""Persistent tariff audit and evidence-specific HOLD tests; no production storage."""
from datetime import date
from hashlib import sha256
import json
from pathlib import Path
import types
import unittest
from unittest.mock import Mock,patch
from sqlalchemy import text
from test_pm_admin_landing import ROOT
from test_pm_municipal_tariff_policy import PolicyTests


class FinalisationTests(PolicyTests):
    def setUp(self):
        super().setUp()
        self.storage=self.modules['app.admin.programs.billing.municipal_tariff_audit_storage']

    def test_disk_approval_survives_new_backend_and_signing_key_rotation(self):
        with self.app.app_context():
            self.approve_source();old_key=self.app.secret_key
            keys=self.policy.backend().keys(self.policy.PREFIX+'sources/')
            before={key:self.policy.backend().get(key) for key in keys}
            self.assertTrue(self.policy.trusted(self.policy.backend(),self.source_record))
            private_root=self.policy.backend().root
        from flask import Flask
        restarted=Flask('restarted-tariff-review',root_path=str(ROOT/'app'))
        restarted.secret_key=old_key;restarted.config['PM_TARIFF_REVIEW_ROOT']=str(private_root)
        with restarted.app_context():
            self.assertTrue(self.policy.trusted(self.policy.backend(),self.source_record))
            restarted.secret_key='separately-configured-new-test-key'
            restarted.config['SECRET_KEY_FALLBACKS']=[old_key]
            self.assertTrue(self.policy.trusted(self.policy.backend(),self.source_record))
            self.assertEqual(before,{key:self.policy.backend().get(key) for key in keys})
            event=self.policy.events(self.policy.backend(),'sources')[0]
            self.assertEqual(event['recorded_by'],7);self.assertEqual(event['action'],'approve_source')
            self.assertTrue(event['recorded_at']);self.assertEqual(event['source']['sha256'],self.source_record['sha256'])

    def test_approval_and_rejection_history_survives_new_application(self):
        self.seed_verified()
        with self.app.app_context():
            self.approve_source();store=self.policy.backend()
            def reviewed():return self.policy.decorate(self.report_data('111'),self.tariff_service.records(),self.tariff_service.verification_records())
            data=reviewed();row=self.row(data);scope='rates/'+row['review']['key']
            for action in ('approve','reject'):
                signed=self.policy.issue_token(7,'2026/27',self.source_record['receipt_id'],action,row['review']['key'])
                self.policy.decide(store,7,'2026/27',self.source_record['receipt_id'],action,signed,reviewed())
            root=store.root;secret=self.app.secret_key
        from flask import Flask
        restarted=Flask('restarted-rate-review',root_path=str(ROOT/'app'));restarted.secret_key=secret
        restarted.config['PM_TARIFF_REVIEW_ROOT']=str(root)
        with restarted.app_context():
            history=self.policy.events(self.policy.backend(),scope)
            self.assertEqual([row['action'] for row in history],['approve','reject'])
            self.assertTrue(all(row['recorded_by']==7 and row['recorded_at'] and row['code']=='W01' for row in history))
            self.assertTrue(all(row['candidate']['sha256'] and row['previous'] for row in history))

    def test_render_requires_durable_private_storage_and_rejects_ephemeral_path(self):
        with self.app.app_context(),patch.dict('os.environ',{'RENDER':'true'},clear=True):
            self.app.config.pop('PM_TARIFF_DOCUMENT_ROOT');self.app.config['PM_LEASE_PRIVATE_STORAGE_APPROVED']=False
            with self.assertRaisesRegex(ValueError,'Persistent private'):self.policy.backend()
            self.app.config['PM_TARIFF_REVIEW_ROOT']=self.folder.name
            with patch.object(self.storage,'persistent_mount',return_value=False),self.assertRaisesRegex(ValueError,'ephemeral'):self.policy.backend()
            with patch.object(self.storage,'persistent_mount',return_value=True):self.assertIsInstance(self.policy.backend(),self.documents.Disk)

    def test_mount_validation_rejects_root_and_tmpfs(self):
        mounts='1 0 0:1 / / rw - overlay overlay rw\n2 1 0:2 / /var/data rw - ext4 /dev/disk rw\n3 1 0:3 / /tmp rw - tmpfs tmpfs rw\n'
        with patch.object(self.storage.Path,'read_text',return_value=mounts):
            self.assertTrue(self.storage.persistent_mount('/var/data/tariff-audits'))
            self.assertFalse(self.storage.persistent_mount('/tmp/tariff-audits'))
            self.assertFalse(self.storage.persistent_mount('/opt/render/project/instance/audits'))

    def test_private_r2_reuses_existing_transport_and_survives_new_adapter(self):
        objects={};calls=[]
        def put(key,data,**kwargs):
            if key in objects and objects[key]!=data:raise ValueError('Immutable collision')
            calls.append(key);objects[key]=data
        client=types.SimpleNamespace(list_objects_v2=lambda **kw:dict(Contents=[dict(Key=key) for key in sorted(objects) if key.startswith(kw['Prefix'])]))
        api=types.SimpleNamespace(settings=Mock(),upload_bytes_to_r2=put,read_file_from_r2=lambda key:objects[key],r2_client=lambda:(client,'existing-approved-private-pm-bucket'))
        with self.app.app_context(),patch.dict('os.environ',{'RENDER':'true'},clear=True),patch.object(self.storage,'import_module',return_value=api):
            self.app.config.pop('PM_TARIFF_DOCUMENT_ROOT');self.app.config['PM_LEASE_PRIVATE_STORAGE_APPROVED']=True
            self.approve_source()
            self.assertTrue(self.policy.trusted(self.policy.backend(),self.source_record))
            keys=self.policy.backend().keys(self.policy.PREFIX+'sources/')
            original=self.policy.backend().get(keys[0])
            with self.assertRaises(ValueError):self.policy.backend().put(keys[0],b'changed','application/json')
            self.assertEqual(self.policy.backend().get(keys[0]),original)
            self.assertTrue(api.settings.called)
        self.assertTrue(all(key.startswith('private/pm-tariff-audit/') for key in calls))

    def test_existing_local_ledger_is_not_hidden_by_r2_configuration(self):
        with self.app.app_context():
            self.approve_source();key=self.policy.backend().keys(self.policy.PREFIX+'sources/')[0]
            content=self.policy.backend().get(key)
        from flask import Flask
        restarted=Flask('existing-local-ledger',root_path=str(ROOT/'app'))
        restarted.instance_path=str(Path(self.folder.name)/'existing-instance');restarted.secret_key=self.app.secret_key
        restarted.config['PM_LEASE_PRIVATE_STORAGE_APPROVED']=True
        with restarted.app_context(),patch.dict('os.environ',{},clear=True):
            local=self.documents.Disk(Path(restarted.instance_path)/'pm_tariff_review')
            local.put(key,content,'application/json')
            with patch.object(self.storage,'PrivateR2',side_effect=AssertionError('Must preserve existing local ledger')):
                self.assertTrue(self.policy.trusted(self.policy.backend(),self.source_record))

    def test_failed_audit_readback_never_confirms_approval(self):
        with self.app.app_context():
            store=self.policy.backend();source=self.source_record
            signed=self.policy.issue_token(7,'2026/27',source['receipt_id'],'approve_source',self.policy.digest(source))
            with patch.object(store,'get',return_value=b'not-the-written-audit'),self.assertRaisesRegex(ValueError,'no approval was confirmed'):
                self.policy.decide(store,7,'2026/27',source['receipt_id'],'approve_source',signed,dict(source=source))
            # A retry verifies the already-durable signed audit; it does not duplicate it.
            self.assertFalse(self.policy.decide(store,7,'2026/27',source['receipt_id'],'approve_source',signed,dict(source=source)))
            self.assertEqual(len(self.policy.events(store,'sources')),1)

    def test_missing_storage_returns_clear_error_without_success_or_tariff_writes(self):
        from lxml import html
        path=self.publication_url+'/2026-27/'+self.source_record['receipt_id']+'/verification'
        page=html.fromstring(self.client.get(path).data)
        values={node.get('name'):node.get('value') for node in page.xpath('//form[.//input[@value="approve_source"]]//input')}
        before=self.snapshot()
        with patch.object(self.policy,'backend',side_effect=ValueError('Persistent approval storage unavailable')):
            response=self.client.post(path,data=values)
        self.assertEqual(response.status_code,409,response.text)
        self.assertIn('Persistent approval storage unavailable',response.text)
        self.assertNotIn('Official download source approved',response.text)
        self.assertEqual(before,self.snapshot())

    def electricity(self,replacement=None):
        fixture=json.loads((ROOT/'tests/fixtures/pm_tariff_sources/electricity_2026.json').read_text(encoding='utf8'))
        if replacement:fixture['pages']={key:value.replace(*replacement) for key,value in fixture['pages'].items()}
        document=types.SimpleNamespace(is_encrypted=False,pages=[types.SimpleNamespace(extract_text=lambda text=value:text) for value in fixture['pages'].values()])
        source=dict(financial_year='2026/27',source_url=fixture['source'],sha256=sha256(self.pdf).hexdigest(),publication_status='UNCLASSIFIED')
        with patch.object(self.verification,'PdfReader',return_value=document):return self.verification.extract(self.pdf,source)

    def test_electricity_rate_units_date_extract_but_actual_scale_stays_hold(self):
        result=self.electricity();candidate=result['extracted']['ELEC01'][0]
        self.assertEqual(candidate['rate'],'3.5747');self.assertEqual(candidate['unit'],'kWh')
        self.assertEqual(candidate['effective_date'],'2026-07-01');self.assertIsNone(result['adoption'])
        self.seed(90,'ELEC01','3.2795','2026-06-30','electricity','kWh')
        result['adoption']=dict(date='2026-05-29',statement='SYNTHETIC TEST ADOPTION')
        def row(evidence):return next(r for r in self.verification.compare(result,self.tariff_service.records(),evidence)['rows'] if r['code']=='ELEC01')
        self.assertEqual(row({})['status'],'HOLD')
        evidence={90:dict(municipal_category='Residential Scale 12',source_reference='Synthetic scale evidence',verified_by=7,verified_at='test')}
        self.assertEqual(row(evidence)['status'],'HOLD')
        evidence[90]['municipal_category']='Residential Scale 4'
        self.assertEqual(row(evidence)['status'],'VERIFIED',row(evidence)) # Isolated mapping contract only.

    def test_electricity_wrong_units_date_and_negative_rate_are_not_mapped(self):
        for replacement in (('(c/kWh)','(R/month)'),('01 JULY 2026','01 JULY 2025'),('357.47','-357.47')):
            with self.subTest(replacement=replacement):
                self.assertNotIn('ELEC01',self.electricity(replacement)['extracted'])

    def test_hold_review_keeps_incomplete_adoption_and_classifications_blocked(self):
        with self.app.app_context():findings=self.policy.hold_findings('2026/27')
        self.assertEqual(len(findings),5);self.assertTrue(all(row['status']=='HOLD' for row in findings))
        self.assertIn('R1.50',findings[0]['documented']);self.assertIn('R1.48',findings[0]['documented'])
        self.assertIn('per-submeter',findings[2]['documented'])
        self.assertIn('proposed',findings[3]['documented'])
        self.assertIn('R400000',findings[4]['documented'])
        self.assertTrue(all(source['sha256'] and '#page=' in source['url'] for row in findings for source in row['sources']))
        path=self.publication_url+'/2026-27/'+self.source_record['receipt_id']+'/verification'
        before=self.snapshot();page=self.client.get(path)
        self.assertEqual(page.status_code,200,page.text);self.assertIn('HOLD evidence review',page.text)
        self.assertIn('CCMGMT',page.text);self.assertEqual(before,self.snapshot())


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(FinalisationTests(name) for name in FinalisationTests.__dict__ if name.startswith('test_'))
