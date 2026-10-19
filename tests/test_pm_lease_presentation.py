"""Presentation-only PDF and Certificate regression tests on disposable Postgres."""
import copy,io,json,re,unittest
from hashlib import sha256
from pathlib import Path
from lxml import html
from sqlalchemy import event
from pypdf import PdfReader
from unittest.mock import patch
import test_pm_lease_user_journey as fixture

class LeasePresentationTests(fixture.LeaseUserJourneyTests):
    def prepared(self):
        owners,ids=self.prepare()
        self.assertEqual(self.select([ids[0],ids[2]]).status_code,302)
        self.assertEqual(self.save_parties(owners[0]).status_code,302)
        self.complete_other_parts()
        source=Path('../../output/pm_stage5/Lease_Review_Draft_R01.docx').read_bytes()
        self.assertEqual(self.upload(source,'Lease_Review_Draft_R01.docx').status_code,302)
        self.assertEqual(self.acknowledge().status_code,302)
        return source

    def pdf_url(self):return self.standard_url('/terms.pdf')

    def test_pdf_all_source_text_headers_footers_names_page_totals_and_no_writes(self):
        source=self.prepared();before=copy.deepcopy(self.stored())
        from app.program_billing import lease_terms_pdf
        lease_terms_pdf.build_pdf(source,self.stored(),{'sha256':sha256(source).hexdigest(),'version':1})
        statements=[]
        def capture(conn,cursor,statement,*args):statements.append(statement)
        event.listen(self.conn,'before_cursor_execute',capture)
        try:response=self.client.get(self.pdf_url())
        finally:event.remove(self.conn,'before_cursor_execute',capture)
        self.assertEqual(response.status_code,200,response.get_data(as_text=True) if response.status_code!=200 else '')
        self.assertEqual(response.mimetype,'application/pdf');self.assertIn('no-store',response.headers['Cache-Control'])
        self.assertFalse(any(s.lstrip().upper().startswith(('INSERT','UPDATE','DELETE')) for s in statements))
        reader=PdfReader(io.BytesIO(response.data));self.assertGreater(len(reader.pages),2)
        for i,page in enumerate(reader.pages,1):
            text=page.extract_text()
            for expected in ('AGREEMENT OF LEASE','Alexandra Jane Example','Benjamin Mark Example','Charlotte Ruth Example','First Natural Tenant','Contracting Company','Landlord 1:','Landlord 2:','Landlord 3:','Tenant 1:','Tenant 2:','Page '+str(i)+' of '+str(len(reader.pages)),'NOT FOR SIGNATURE OR ISSUANCE'):
                self.assertIn(expected,text)
        from app.program_billing import lease_terms_pdf,lease_standard,lease_preview
        from app.program_billing.lease_merge import merge_draft_docx,TOKEN
        values=lease_standard.certificate_values(self.stored())
        supplied={k:v for k,v in values.items() if v.strip().strip('_') and not TOKEN.search(v)}
        merged=lease_preview.merged_preview(lease_standard.saved_particulars(self.stored()),source,sha256(source).hexdigest())
        body_text=''
        for page in reader.pages:
            text=page.extract_text()
            text=re.sub(r'AGREEMENT OF LEASE\s+Landlord\(s\):.*?NOT FOR SIGNATURE OR ISSUANCE','',text,flags=re.S)
            text=re.sub(r'Landlord 1:.*?Page \d+ of \d+','',text,flags=re.S)
            body_text+=text
        printed=re.sub(r'\s+','',body_text)
        def paragraphs(blocks):
            for b in blocks:
                if b['kind']=='table':
                    for row in b['rows']:
                        for cell in row:yield from paragraphs(cell)
                elif b.get('text','').strip():yield b['text']
        for value in paragraphs(lease_preview.extract_blocks(merged)):
            self.assertTrue(re.sub(r'\s+','',value) in printed,'Missing source paragraph: '+value[:90])
        self.assertEqual(self.stored(),before)
        directory=Path('output/pdf');directory.mkdir(parents=True,exist_ok=True)
        (directory/'AIT-Core-Terms-LOCAL-REVIEW.pdf').write_bytes(response.data)
        import fitz
        pdf=fitz.open(stream=response.data,filetype='pdf')
        for index in (0,len(pdf)//2,len(pdf)-1):pdf[index].get_pixmap(matrix=fitz.Matrix(1.2,1.2)).save(directory/('terms-page-'+str(index+1)+'.png'))
        (directory/'verification.json').write_text(json.dumps({'pages':len(reader.pages),'source_sha256':sha256(source).hexdigest(),'source_paragraphs_preserved':True,'saved_particulars_unchanged':True},indent=2))

    def test_eight_menu_items_and_administration_keep_context(self):
        self.prepared()
        page=self.client.get('/billing/leases?property_id=26&draft_id='+str(self.draft));self.assertEqual(page.status_code,200)
        tree=html.fromstring(page.data)
        links=tree.xpath('//nav[@aria-label="Lease submenu"]/a')
        self.assertEqual([a.text_content() for a in links],['1. Parties','2. Premises','3. Period','4. Amounts','5. Special Conditions','6. View Lease','7. Addendum','8. Lease History'])
        self.assertFalse(tree.xpath('//nav//a[contains(text(),"Generate PDF") or contains(text(),"Upload Master") or contains(text(),"Certificate")]'))
        admin=tree.xpath('//*[@id="lease-document-administration"]')[0]
        for link in admin.xpath('.//a'):
            self.assertIn('26',link.get('href'));self.assertIn(str(self.draft),link.get('href'))
            self.assertEqual(self.client.get(link.get('href')).status_code,200)

    def test_existing_forms_save_reopen_preserve_all_certificate_values_and_other_data(self):
        self.prepared()
        from app.program_billing import lease_standard
        before=copy.deepcopy(self.stored())
        expected=lease_standard.certificate_rows(before)
        for group in ('premises','period','amounts','conditions'):
            url=self.route('particulars/'+group)
            first=self.client.get(url);self.assertEqual(first.status_code,200)
            tree=html.fromstring(first.data)
            self.assertNotIn('R01 placeholders:',first.get_data(as_text=True))
            forms=tree.xpath('//form[@method="post"]')
            for index in range(len(forms)):
                # Reload the optimistic version after every successful save.
                tree=html.fromstring(self.client.get(url).data)
                form=tree.xpath('//form[@method="post"]')[index]
                data={}
                for node in form.xpath('.//input[@name] | .//textarea[@name] | .//select[@name]'):
                    name=node.get('name')
                    if node.tag=='textarea':value=node.text or ''
                    elif node.tag=='select':
                        options=node.xpath('./option[@selected]') or node.xpath('./option')[:1]
                        value=options[0].get('value','') if options else ''
                    elif node.get('type')=='checkbox':
                        if node.get('checked') is None:continue
                        value=node.get('value','on')
                    else:value=node.get('value','')
                    self.assertNotIn(name,data,'Duplicate field '+name)
                    data[name]=value
                response=self.client.post(url,data=data)
                self.assertEqual(response.status_code,302,response.get_data(as_text=True))
                self.assertEqual(self.client.get(url).status_code,200)
            self.assertEqual(lease_standard.certificate_rows(self.stored()),expected)
        for key in ('_parties','_contracting_lessors','_ait_standard'):
            self.assertEqual(self.stored()[key],before[key])

    def test_pdf_rejects_wrong_property_stale_revision_failure_and_keeps_legal_gate(self):
        self.prepared()
        self.assertEqual(self.client.get(self.pdf_url()+'&revision=0').status_code,409)
        self.assertEqual(self.client.get(self.pdf_url().replace('/26/','/27/')).status_code,404)
        from app.program_billing import lease_terms_pdf,lease_standard
        with patch.object(lease_terms_pdf,'build_pdf',side_effect=ValueError('synthetic conversion failure')):
            self.assertEqual(self.client.get(self.pdf_url()).status_code,503)
        self.assertFalse(lease_standard.RELEASE['terms_approved'])
        self.assertIn('Ready for Signature',self.client.get(self.standard_url()).get_data(as_text=True))
        self.sign_in(8);self.assertEqual(self.client.get(self.pdf_url()).status_code,404)
