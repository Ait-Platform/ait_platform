"""Tariff-only source trust/variance controls, isolated PostgreSQL and private files."""
from copy import deepcopy
from datetime import datetime,timezone
from decimal import Decimal
import unittest
from unittest.mock import patch
from lxml import html
from sqlalchemy import text
from test_pm_municipal_tariff_verification import VerificationTests


class PolicyTests(VerificationTests):
    def setUp(self):
        super().setUp()
        self.run_download()
        with self.app.app_context():self.source_record=next(row for row in self.documents.listing(self.documents.backend(),'2026/27') if row['publication_status']=='FINAL_LABELLED')

    def approve_source(self):
        source=self.source_record
        token=self.policy.issue_token(7,'2026/27',source['receipt_id'],'approve_source',self.policy.digest(source))
        return self.policy.decide(self.policy.backend(),7,'2026/27',source['receipt_id'],'approve_source',token,dict(source=source))

    def report_data(self,rate='110',adopt=True):
        raw=self.extraction(adopt);raw['source']=self.source_record
        raw['extracted']={'W01':[dict(raw['extracted']['W01'][0],rate=rate)]}
        return self.verification.compare(raw,self.tariff_service.records(),self.tariff_service.verification_records())

    def seed_verified(self,rate='100'):
        self.seed(80,'W01',rate,'2025-07-01','water','kL')
        self.conn.execute(text('UPDATE bil_tariff SET block_start=0,block_end=200,reduction_factor=0.2 WHERE id=80'))
        self.conn.execute(text("INSERT INTO bil_tariff_verification(tariff_id,municipality,municipal_category,source_reference,effective_date,verified_by) VALUES(80,'eThekwini Municipality','Domestic','Synthetic verified fixture source','2025-07-01',7)"));self.conn.commit()

    def row(self,report):return next(r for r in report['rows'] if r['code']=='W01')

    def test_percent_boundaries_decrease_new_and_zero_base(self):
        for old,new,label,percent in [('100','100','PASS','0'),('100','110','PASS','10'),('100','110.0001','ABOVE 10%','10.0001'),('100','99','DECREASE','-1'),(None,'12','NEW',None),('0','0','PASS','0'),('0','1','ZERO BASE',None)]:
            result=self.policy.variance(old,new);self.assertEqual(result['result'],label)
            self.assertEqual(result['percent'],Decimal(percent) if percent is not None else None)

    def test_first_source_approval_does_not_approve_adoption(self):
        before=self.snapshot()
        with self.app.app_context():
            self.assertFalse(self.policy.trusted(self.policy.backend(),self.source_record))
            self.assertTrue(self.approve_source())
            self.assertTrue(self.policy.trusted(self.policy.backend(),self.source_record))
            changed=dict(self.source_record,financial_year='2027/28',source_url=self.source_record['source_url'].replace('2026-2027','2027-2028'))
            self.assertTrue(self.policy.trusted(self.policy.backend(),changed))
            result=self.policy.decorate(self.report_data(adopt=False),self.tariff_service.records(),self.tariff_service.verification_records())
            self.assertEqual(self.row(result)['status'],'HOLD')
            self.assertFalse(self.row(result)['can_clear'])
        self.assertEqual(before,self.snapshot())

    def test_last_verified_base_ignores_newer_unverified_rate(self):
        self.seed_verified()
        self.seed(81,'W01','109','2026-06-01','water','kL')
        with self.app.app_context():
            self.approve_source()
            result=self.policy.decorate(self.report_data(),self.tariff_service.records(),self.tariff_service.verification_records())
            row=self.row(result);self.assertEqual(row['review']['previous']['id'],80)
            self.assertEqual(row['review']['check']['percent'],10)
            self.assertEqual(row['status'],'VERIFIED')
        self.assertIn(81,[r['id'] for r in self.snapshot()['bil_tariff']])

    def test_approval_rejection_stale_evidence_and_no_tariff_writes(self):
        self.seed_verified();before=self.snapshot()
        with self.app.app_context():
            self.approve_source()
            def reviewed():return self.policy.decorate(self.report_data('111'),self.tariff_service.records(),self.tariff_service.verification_records())
            report=reviewed();row=self.row(report);self.assertTrue(row['can_clear']);self.assertEqual(row['status'],'HOLD')
            token=self.policy.issue_token(7,'2026/27',self.source_record['receipt_id'],'approve',row['review']['key'])
            self.assertTrue(self.policy.decide(self.policy.backend(),7,'2026/27',self.source_record['receipt_id'],'approve',token,report))
            self.assertFalse(self.policy.decide(self.policy.backend(),7,'2026/27',self.source_record['receipt_id'],'approve',token,report))
            self.assertEqual(self.row(reviewed())['status'],'VERIFIED')
            reject=self.policy.issue_token(7,'2026/27',self.source_record['receipt_id'],'reject',row['review']['key'])
            self.policy.decide(self.policy.backend(),7,'2026/27',self.source_record['receipt_id'],'reject',reject,reviewed())
            self.assertEqual(self.row(reviewed())['status'],'HOLD')
            with self.assertRaises(ValueError):self.policy.decide(self.policy.backend(),99,'2026/27',self.source_record['receipt_id'],'approve',token,report)
            self.conn.execute(text("UPDATE bil_tariff SET rate=101 WHERE id=80"));self.conn.commit()
            with self.assertRaises(ValueError):self.policy.decide(self.policy.backend(),7,'2026/27',self.source_record['receipt_id'],'approve',self.policy.issue_token(7,'2026/27',self.source_record['receipt_id'],'approve',row['review']['key']),reviewed())
        self.conn.execute(text('UPDATE bil_tariff SET rate=100 WHERE id=80'));self.conn.commit()
        self.assertEqual(before,self.snapshot())

    def test_clearance_audit_serializes_postgresql_decimals_and_dates(self):
        from datetime import date
        self.seed_verified()
        native=[dict(row,rate=Decimal(str(row['rate'])),effective_date=date.fromisoformat(str(row['effective_date']))) for row in self.tariff_service.records()]
        with self.app.app_context(),patch.object(self.tariff_service,'records',return_value=native):
            self.approve_source()
            data=self.policy.decorate(self.report_data('111'),native,self.tariff_service.verification_records())
            row=self.row(data);signed=self.policy.issue_token(7,'2026/27',self.source_record['receipt_id'],'approve',row['review']['key'])
            self.policy.decide(self.policy.backend(),7,'2026/27',self.source_record['receipt_id'],'approve',signed,data)
            history=self.policy.events(self.policy.backend(),'rates/'+row['review']['key'])
            self.assertEqual(history[0]['previous']['effective_date'],'2025-07-01')
            self.assertEqual(Decimal(history[0]['previous']['rate']),100)

    def test_identical_verified_annual_rate_passes_without_new_clearance(self):
        self.seed_verified('110')
        self.conn.execute(text("UPDATE bil_tariff SET effective_date='2026-07-01' WHERE id=80"))
        self.conn.execute(text("UPDATE bil_tariff_verification SET effective_date='2026-07-01' WHERE tariff_id=80"));self.conn.commit()
        with self.app.app_context():
            self.approve_source()
            result=self.policy.decorate(self.report_data('110'),self.tariff_service.records(),self.tariff_service.verification_records())
            row=self.row(result);self.assertEqual(row['review']['check']['percent'],0)
            self.assertEqual(row['status'],'VERIFIED');self.assertFalse(row['can_clear'])
            conflict=self.policy.decorate(self.report_data('111'),self.tariff_service.records(),self.tariff_service.verification_records())
            self.assertEqual(self.row(conflict)['status'],'HOLD')
            self.assertFalse(self.row(conflict)['can_clear'])

    def test_signed_clearance_cannot_be_copied_to_a_different_rate(self):
        self.seed_verified()
        with self.app.app_context():
            self.approve_source();store=self.policy.backend()
            first=self.policy.decorate(self.report_data('111'),self.tariff_service.records(),self.tariff_service.verification_records())
            row=self.row(first);token=self.policy.issue_token(7,'2026/27',self.source_record['receipt_id'],'approve',row['review']['key'])
            self.policy.decide(store,7,'2026/27',self.source_record['receipt_id'],'approve',token,first)
            old_key=store.keys(self.policy.PREFIX+'rates/'+row['review']['key']+'/')[0]
            changed=self.policy.decorate(self.report_data('112'),self.tariff_service.records(),self.tariff_service.verification_records())
            new_key=self.policy.PREFIX+'rates/'+self.row(changed)['review']['key']+'/'+old_key.rsplit('/',1)[-1]
            store.put(new_key,store.get(old_key),'application/json')
            with self.assertRaises(ValueError):self.policy.decorate(self.report_data('112'),self.tariff_service.records(),self.tariff_service.verification_records())

    def test_real_flagged_rate_approval_post_preserves_tariffs(self):
        self.seed_verified();before=self.snapshot()
        with self.app.app_context():self.approve_source()
        raw=self.extraction(True);raw['source']=self.source_record
        raw['extracted']['W01'][0]['rate']='111'
        path=self.publication_url+'/2026-27/'+self.source_record['receipt_id']+'/verification'
        with patch.object(self.verification,'extract',side_effect=lambda *args:deepcopy(raw)):
            doc=html.fromstring(self.client.get(path).data)
            row=doc.xpath('//tr[@data-component="W01"]')[0]
            self.assertIn('11.0000%',row.text_content())
            form=row.xpath('.//form[.//input[@value="approve"]]')[0]
            values={n.get('name'):n.get('value') for n in form.xpath('.//input')}
            response=self.client.post(path,data=values,follow_redirects=True)
            self.assertEqual(response.status_code,200,response.text)
            row=html.fromstring(response.data).xpath('//tr[@data-component="W01"]')[0]
            self.assertIn('VERIFIED',row.text_content())
            self.assertIn('Correct existing tariff',row.text_content())
        self.assertEqual(before,self.snapshot())

    def test_automatic_download_requires_first_approval_then_reuses_trust(self):
        import sys
        from datetime import date
        monitor=self.load('municipal_tariff_monitor',{})
        auto=sys.modules['app.admin.programs.billing.municipal_tariff_auto_activation']
        monitor.services=lambda:(self.tariff_service,self.source,self.documents,self.verification,self.workflow,auto)
        discovery=self.source.discovery;retrieve=self.source.retrieve
        with self.app.app_context(),patch.object(self.source,'discovery',side_effect=lambda year,**kw:discovery(year,fetch=self.fetch,**kw)),patch.object(self.source,'retrieve',side_effect=lambda candidate,**kw:retrieve(candidate,fetch=self.fetch,**kw)) as download:
            store=self.documents.backend();first=monitor.check_year(store,'2026/27',date(2026,7,1))
            self.assertIn('First-time administrator approval',str(first['issues']))
            download.assert_not_called()
            self.approve_source()
            second=monitor.check_year(store,'2026/27',date(2026,7,2))
            self.assertTrue(second['successful_retrieval']);self.assertEqual(download.call_count,3)
            self.assertFalse(second['verified']) # Website trust never implies adoption.

    def test_real_source_approval_post_auth_csrf_and_existing_correction_link(self):
        path=self.publication_url+'/2026-27/'+self.source_record['receipt_id']+'/verification'
        page=self.client.get(path);self.assertEqual(page.status_code,200)
        doc=html.fromstring(page.data);form=doc.xpath('//form[.//input[@value="approve_source"]]')[0]
        values={n.get('name'):n.get('value') for n in form.xpath('.//input')}
        self.assertEqual(self.client.post(path,data={k:v for k,v in values.items() if k!='csrf_token'}).status_code,400)
        response=self.client.post(path,data=values,follow_redirects=True);self.assertEqual(response.status_code,200,response.text)
        self.assertIn('Official download source approved',response.text)
        self.assertIn('/admin/billing/tariffs/',response.text)
        self.conn.execute(text("UPDATE pm_admin_test_identity SET role='manager'"));self.conn.commit()
        self.assertEqual(self.client.post(path,data=values).status_code,403)


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(PolicyTests(name) for name in PolicyTests.__dict__ if name.startswith('test_'))
