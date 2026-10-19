"""Synthetic final-verification artifacts; not the unavailable historic addenda."""
import io,json,unittest
from hashlib import sha256
from pathlib import Path
from lxml import html
from sqlalchemy import text
import test_pm_lease_history as fixture


class FinalLeaseVerificationTests(fixture.LeaseHistoryTests):
    def test_multi_amendment_word_and_browser_artifacts_preserve_exact_signed_original(self):
        directory=fixture.fixture.ROOT/'tmp'/'lease-final-verification';directory.mkdir(parents=True,exist_ok=True)
        reference=fixture.fixture.ROOT.parent.parent/'output'/'pm_stage5'/'Lease_Review_Draft_R01.docx'
        source=reference.read_bytes();before=sha256(source).hexdigest()
        self.assertEqual(self.upload(source,'Lease_Review_Draft_R01.docx').status_code,302)
        items={'_contracting_lessors':{'identities':[{'owner_id':1,'name':'Synthetic Landlord One','identity_number':'TEST-L1'},
            {'owner_id':2,'name':'Synthetic Landlord Two','identity_number':'TEST-L2'},
            {'owner_id':3,'name':'Synthetic Landlord Three','identity_number':'TEST-L3'}]},
            '_parties':{'schema_version':2,'lessor':{'signatory_name':'Synthetic Landlord Representative',
                'signatory_identity_number':'TEST-LREP','signatory_capacity':'Authorised representative'},
                'lessees':[{'legal_name':'Synthetic Tenant Company','party_type':'juristic_person','identity_number':'TEST-COMPANY',
                    'address':'1 Test Street','email':'synthetic@example.com','phone':'0000000000',
                    'signatory_name':'Synthetic Tenant Representative','signatory_identity_number':'TEST-TREP','signatory_capacity':'Director'}]},'_premises':{'property_name':'Synthetic Almond','property_address':'1 Test Street'}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb)'),{'items':json.dumps(items)});self.session.commit()
        self.assertEqual(self.signed().status_code,302);original=self.ledger()['documents'][0]
        original_bytes=self.client.get(self.history_doc_url(original['id'],'/download')).data
        artifacts=[]
        for label,effective,rent,deposit,renewal in [('synthetic-december-2023','2024-01-01','1000.00','2000.00','2024-12-31'),
            ('synthetic-july-2024','2024-07-01','1250.00','2500.00','2025-12-31')]:
            wording=('1. Synthetic rental amendment: monthly rental becomes ZAR '+rent+' from '+effective+'.\n'
                '2. Synthetic deposit amendment: deposit becomes ZAR '+deposit+'.\n'
                '3. Synthetic renewal amendment: the agreed renewal expiry becomes '+renewal+'.')
            response=self.create_addendum(wording=wording)
            self.assertEqual(response.status_code,302)
            entry=self.ledger()['documents'][-1]
            payload=self.client.get(self.history_doc_url(entry['id'],'/download')).data
            from app.program_billing import lease_preview as preview
            text_value='\n'.join(preview.block_text(b) for b in preview.extract_blocks(payload))
            for expected in (effective,rent,deposit,renewal,'1. Synthetic','2. Synthetic','3. Synthetic','Signatures',
                'Synthetic Landlord One','Synthetic Landlord Two','Synthetic Landlord Three','Synthetic Tenant Company',original['id']):
                self.assertIn(expected,text_value)
            self.assertEqual(entry['original_id'],original['id']);self.assertEqual(entry['original_sha256'],sha256(original_bytes).hexdigest())
            view=self.client.get(response.location);self.assertEqual(view.status_code,200)
            page=html.fromstring(view.get_data(as_text=True))
            header=page.xpath('//*[@id="lease-header-actions"]')[0]
            self.assertEqual(header.xpath('./a/text()'),['Back'])
            self.assertEqual(header.xpath('./div/a/text()'),['Email']);self.assertEqual(header.xpath('./div/button/text()'),['Print'])
            markup=view.get_data(as_text=True).replace('</body>','')
            # Test fixture uses a minimal layout; supply Tailwind-equivalent CSS
            # for the exact header classes rather than loading a public CDN.
            css='<style>body{font:16px Arial;margin:30px}.flex{display:flex}.flex-col{flex-direction:column}.items-end{align-items:flex-end}.justify-between{justify-content:space-between}.justify-end{justify-content:flex-end}.gap-2{gap:8px}.items-center{align-items:center}a,button{padding:8px 16px}#lease-merged-text{border:1px solid #bbb;padding:20px}.font-bold{font-weight:bold}.whitespace-pre-wrap{white-space:pre-wrap}</style>'
            (directory/(label+'.docx')).write_bytes(payload)
            (directory/(label+'.html')).write_text('<!doctype html><html><head>'+css+'</head><body>'+markup+'</body></html>',encoding='utf-8')
            artifacts.append({'label':label,'original_id':original['id'],'original_sha256':sha256(original_bytes).hexdigest(),
                'generated_sha256':sha256(payload).hexdigest(),'expected':{'rent':rent,'deposit':deposit,'renewal':renewal,'effective':effective}})
        self.assertEqual(self.client.get(self.history_doc_url(original['id'],'/download')).data,original_bytes)
        self.assertEqual(reference.read_bytes(),source)
        (directory/'artifacts.json').write_text(json.dumps({'source_sha256':before,'source_is_production_verified':False,
            'historic_examples_available':False,'artifacts':artifacts},indent=2),encoding='utf-8')

    def test_wrong_draft_original_document_and_signed_copy_bytes_are_rejected(self):
        self.assertEqual(self.upload().status_code,302);self.assertEqual(self.signed().status_code,302)
        original=self.ledger()['documents'][0]
        self.assertEqual(self.create_addendum(original_id=str(self.draft)).status_code,404)
        self.sql("UPDATE bil_lease_document SET sha256=repeat('0',64)");self.session.commit()
        self.assertEqual(self.client.get(self.history_doc_url(original['id'],'/download')).status_code,409)
        self.assertEqual(self.create_addendum().status_code,409)
