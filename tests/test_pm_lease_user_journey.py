"""Ordinary-manager local journey with PostgreSQL and private/mail test transports."""
import copy,io,json,unittest
from pathlib import Path
from hashlib import sha256
from lxml import html
from sqlalchemy import event
import test_pm_lease_history as history
import test_pm_lease_standard as standard_fixture
import test_pm_lease_tenant_page_selection as tenant

class LeaseUserJourneyTests(unittest.TestCase):
    load_lease_workspace=True
    setUp=history.LeaseHistoryTests.setUp
    for_name=()
    def test_ordinary_manager_saved_certificate_view_email_addendum_pdf_history_navigation(self):
        directory=Path('tmp/lease-user-journey');directory.mkdir(exist_ok=True)
        owners,ids=self.prepare()
        self.assertEqual(self.select([ids[0],ids[2]]).status_code,302)
        self.assertEqual(self.save_parties(owners[0]).status_code,302)
        self.complete_other_parts()
        reference=Path('../../output/pm_stage5/Lease_Review_Draft_R01.docx').read_bytes()
        self.assertEqual(self.upload(reference,'Lease_Review_Draft_R01.docx').status_code,302)
        self.assertEqual(self.acknowledge().status_code,302)
        from app.program_billing import lease_standard as standard
        items=copy.deepcopy(self.stored())
        rows=standard.certificate_rows(items)
        self.assertEqual(len(rows),42)
        certificate=self.client.get(self.standard_url());self.assertEqual(certificate.status_code,200)
        tree=html.fromstring(certificate.data)
        displayed=tree.xpath('//table[thead/tr/th[text()="Certificate label"]]/tbody/tr')
        self.assertEqual(len(displayed),42)
        for row,expected in zip(displayed,rows):
            self.assertIn(' '.join(expected['label'].split()),' '.join(row.text_content().split()))
            self.assertIn(' '.join(expected['value'].split()),' '.join(row.text_content().split()))
        statements=[]
        def capture(conn,cursor,statement,*args):statements.append(statement)
        event.listen(self.conn,'before_cursor_execute',capture)
        try:view=self.client.get('/billing/leases/property/26/view?draft_id='+str(self.draft))
        finally:event.remove(self.conn,'before_cursor_execute',capture)
        self.assertEqual(view.status_code,200)
        self.assertFalse(any(s.lstrip().upper().startswith(('INSERT','UPDATE','DELETE')) for s in statements))
        page=html.fromstring(view.data);visible=page.xpath('//article')[0].text_content()
        self.assertLess(visible.index('AIT Standard Lease'),visible.index('Lease Certificate'))
        self.assertLess(visible.index('Lease Certificate'),visible.index('AIT Core Terms and Conditions'))
        for value in [o['name'] for o in owners]+['First Natural Tenant','Contracting Company','Company Representative']:
            self.assertIn(value,visible)
        self.assertEqual(self.stored(),items)
        header=page.xpath('//*[@id="lease-header-actions"]')[0]
        self.assertEqual(header.xpath('./a/text()'),['Back'])
        self.assertEqual(header.xpath('./div/a/text()'),['Email'])
        self.assertEqual(header.xpath('./div/button/text()'),['Print'])
        markup=view.get_data(as_text=True)
        css='<style>body{font:16px Arial;margin:30px}.flex{display:flex}.flex-col{flex-direction:column}.items-end{align-items:flex-end}.justify-between{justify-content:space-between}.justify-end{justify-content:flex-end}.gap-2{gap:8px}a,button{padding:8px 16px}.whitespace-pre-wrap{white-space:pre-wrap}</style>'
        (directory/'lease.html').write_text(css+markup,encoding='utf-8')
        with self.app.app_context():source,payload=standard.source_master(26,self.standard_state()['source'])
        expected_document=standard.build_document(items,self.standard_state(),source,payload)
        (directory/'lease.docx').write_bytes(expected_document)
        mail,guard=self.sender()
        link=self.mail_link(view)
        with guard:
            self.assertEqual(self.client.get(link).status_code,200);mail.send_standard_report_email.assert_not_called()
            sent=self.client.post(link,data={'recipient':'attorney@example.com','message':'Synthetic local test only'})
            self.assertEqual(sent.status_code,302)
            args,kwargs=mail.send_standard_report_email.call_args
            self.assertIn(expected_document,list(args)+list(kwargs.values()))
            self.assertEqual(self.ledger()['emails'][-1]['status'],'submitted')
        import fitz
        def pdf(label):
            doc=fitz.open();p=doc.new_page();p.insert_text((40,50),label+' - SYNTHETIC NOT ACTUALLY SIGNED');data=doc.tobytes();doc.close();return data
        original_pdf=pdf('Original lease')
        self.assertEqual(self.signed(signed_document=(io.BytesIO(original_pdf),'Original.pdf')).status_code,302)
        original=copy.deepcopy(self.ledger()['documents'][-1])
        wording='The deposit is amended to ZAR 2500.00. All other agreed provisions remain unchanged.'
        result=self.create_addendum(wording=wording);self.assertEqual(result.status_code,302)
        generated=self.ledger()['documents'][-1]
        content=html.fromstring(self.client.get(result.location).data).xpath('//article')[0].text_content()
        self.assertLess(content.index('Addendum No.'),content.index(wording));self.assertLess(content.index(wording),content.index('Signatures'))
        self.assertEqual(content.count('Signature: ____________________'),5)
        for value in [o['name'] for o in owners]+['First Natural Tenant','Contracting Company','Company Representative']:self.assertIn(value,content)
        signed_addendum=pdf('Signed addendum')
        self.assertEqual(self.signed('signed_addendum',original_id=original['id'],signed_document=(io.BytesIO(signed_addendum),'Addendum.pdf')).status_code,302)
        final=self.ledger()['documents'][-1]
        for entry,data in ((original,original_pdf),(final,signed_addendum)):
            for _ in range(2):self.assertEqual(self.client.get(self.history_doc_url(entry['id'],'/download')).data,data)
        self.assertEqual(self.ledger()['documents'][0],original)
        for url in ['/billing/dashboard','/billing/leases?property_id=26&draft_id='+str(self.draft),self.standard_url(),self.route('particulars/parties'),self.url_history(),'/billing/leases/property/26/addendum?draft_id='+str(self.draft)]:
            self.assertEqual(self.client.get(url,follow_redirects=True).status_code,200,url)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),1)
        self.sign_in(8)
        self.assertEqual(self.client.get(self.standard_url('/view')).status_code,404)
        self.assertEqual(self.client.get(self.history_doc_url(original['id'],'/download')).status_code,404)
        report={'certificate_rows':42,'populated':sum(not r['value'].startswith('[') for r in rows),'unresolved':[r['label'] for r in rows if r['value'].startswith('[')],'ordinary_manager_id':7,'signed_pdf_integrity':'PASS','email_exact_bytes':'PASS','view_no_writes':'PASS','navigation':'PASS','draft_count':1}
        (directory/'result.json').write_text(json.dumps(report,indent=2),encoding='utf-8')

for cls in (history.LeaseHistoryTests,standard_fixture.StandardLeaseTests,tenant.TenantPageSelectionTests):
    for name in ('sign_in','sql','payload','assign','create','stored','route','post','generate','owners','schema','form','register','prepare','complete_other_parts','select_form','select','save_parties','upload','docx','cover_master','url','url_history','ledger','version','signed','history_doc_url','create_addendum','mail_link','sender','standard_url','standard_state','acknowledge'):
        if hasattr(cls,name):setattr(LeaseUserJourneyTests,name,getattr(cls,name))
