"""Final simplified design over the existing local Lease implementation."""
import copy,io,json,unittest
from zipfile import ZipFile
from lxml import html
from sqlalchemy import text
import test_pm_lease_history as fixture

class SimplifiedAddendumTests(fixture.LeaseHistoryTests):
    def test_three_parts_one_content_numbered_under_lease_and_individual_signature_sections(self):
        self.assertEqual(self.upload(self.cover_master('Full Terms not reproduced')).status_code,302)
        data={'_contracting_lessors':{'identities':[{'name':'Alice Owner','identity_number':'TEST-L1'},{'name':'Bob Owner','identity_number':'TEST-L2'}]},
            '_parties':{'schema_version':2,'lessor':{'signatory_name':'Recorded Agent','signatory_identity_number':'TEST-A','signatory_capacity':'Agent'},
                'lessees':[{'party_type':'natural_person','legal_name':'Natural Tenant','identity_number':'TEST-T1'},
                {'party_type':'juristic_person','legal_name':'Company Tenant','identity_number':'TEST-T2','signatory_name':'Company Representative','signatory_identity_number':'TEST-R','signatory_capacity':'Director'}]}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb)'),{'items':json.dumps(data)});self.session.commit()
        self.assertEqual(self.signed().status_code,302)
        original=copy.deepcopy(self.ledger()['documents'][0])
        wording='  1. User wording is unchanged.\n2. Keep [MONTHLY RENT INITIAL] literally; no merge or interpretation.  '
        for number in (1,2):
            response=self.create_addendum(wording=wording,addendum_number='999')
            self.assertEqual(response.status_code,302)
            entry=self.ledger()['documents'][-1];self.assertEqual(entry['addendum_number'],number)
            payload=self.client.get(self.history_doc_url(entry['id'],'/download')).data
            from app.program_billing import lease_preview as preview
            blocks=preview.extract_blocks(payload);content='\n'.join(preview.block_text(b) for b in blocks)
            self.assertTrue(content.startswith('Addendum No. '+str(number)))
            self.assertIn(wording,content);self.assertNotIn('Full Terms not reproduced',content)
            self.assertEqual([b['text'] for b in blocks if b['kind']=='heading'][1:],['Amendment Content','Signatures'])
            self.assertEqual(content.count('Signature: ____________________'),4)
            for name in ('Alice Owner','Bob Owner','Natural Tenant','Company Tenant','Recorded Agent','Company Representative'):
                self.assertIn(name,content)
            self.assertNotIn('effective_date',entry);self.assertNotIn('continuing_effect',entry)
        self.assertEqual(self.ledger()['documents'][0],original)
        page=self.client.get('/billing/leases/property/26/addendum?draft_id='+str(self.draft))
        tree=html.fromstring(page.get_data(as_text=True))
        self.assertEqual(tree.xpath('//textarea/@name'),['wording'])
        self.assertFalse(tree.xpath('//input[@name="effective_date"] | //textarea[@name="continuing"]'))


import test_pm_lease_standard as standard_fixture
class SimplifiedCertificateTests(standard_fixture.StandardLeaseTests):
    def test_cover_is_abbreviated_certificate_uses_full_saved_values_and_view_lease_reuses_builder(self):
        self.start()
        parties={'_contracting_lessors':{'identities':[{'name':'Alice Anne Owner','identity_number':'TEST-A','type':'individual'}]},
            '_parties':{'schema_version':2,'lessees':[{'legal_name':'Tenant Trading Company','party_type':'juristic_person','identity_number':'TEST-C'}]}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb)'),{'items':json.dumps(parties)});self.session.commit()
        self.assertEqual(self.acknowledge().status_code,302)
        workspace=self.client.get(self.standard_url());self.assertEqual(workspace.status_code,200)
        tree=html.fromstring(workspace.get_data(as_text=True))
        rows=tree.xpath('//table[thead/tr/th[text()="Certificate label"]]/tbody/tr')
        self.assertEqual(len(rows),44)
        from app.program_billing import lease_standard as standard
        for field in standard.DICTIONARY['fields']:
            self.assertTrue(field['source']);self.assertTrue(field['label']);self.assertIn(field['workspace'],('parties','premises','period','amounts','conditions'))
        direct=self.client.get('/billing/leases/property/26/view?draft_id='+str(self.draft));self.assertEqual(direct.status_code,200)
        visible=html.fromstring(direct.get_data(as_text=True)).xpath('//article')[0].text_content()
        self.assertIn('A. Owner',visible);self.assertIn('Tenant Trading Company',visible)
        self.assertIn('Alice Anne Owner',visible);self.assertIn('Tenant Trading Company',visible)
        self.assertIn('AIT Core Terms and Conditions',visible)
        self.assertIn('not been separately approved',visible)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar()['_contracting_lessors'],parties['_contracting_lessors'])
