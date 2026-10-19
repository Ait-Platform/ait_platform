"""Quick Amounts save/reopen and automatic ZAR wording check."""
import copy,json,unittest
from lxml import html
from sqlalchemy import text
import test_pm_lease_premises_simple as fixture
import test_pm_lease_amounts_offline as financial_fixture

class SimplifiedAmountsSmokeTest(unittest.TestCase):
    load_lease_workspace=True
    setUp=fixture.SimplifiedPremisesSmokeTest.setUp
    sign_in=fixture.SimplifiedPremisesSmokeTest.sign_in
    sql=fixture.SimplifiedPremisesSmokeTest.sql
    payload=fixture.SimplifiedPremisesSmokeTest.payload
    assign=fixture.SimplifiedPremisesSmokeTest.assign
    create=fixture.SimplifiedPremisesSmokeTest.create
    route=fixture.SimplifiedPremisesSmokeTest.route
    post=fixture.SimplifiedPremisesSmokeTest.post
    stored=fixture.SimplifiedPremisesSmokeTest.stored

    def test_save_reopen_auto_words_parking_and_preserve_hidden_financial_values(self):
        from app.program_billing import lease_workflow as workflow,lease_standard as standard
        expected={'0':'zero rand and zero cents','1.01':'one rand and one cent','21.50':'twenty-one rand and fifty cents','10000.00':'ten thousand rand and zero cents','1001.02':'one thousand and one rand and two cents'}
        for amount,words in expected.items():self.assertEqual(workflow.zar_words(amount),words)
        self.assertIn('billion',workflow.zar_words('999999999999.99'))
        original={'s1-17':{'value':'24 months'},'s1-18':{'value':'2027-01-01'},'s1-19':{'value':'2027-01-15'},'s1-20':{'value':'2028-12-31'},'_premises':{'saved':'premises'},'_parties':{'saved':'parties'},'sc-01':{'value':'Keep'},'s1-31':{'value':'Saved fee'},'_amounts':{'parking_bay_count':'2','parking_bay_identifiers':'OLD-P1\nOLD-P2','parking_rent_per_bay':'250.00','custom_fee':'preserve'}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(original)});self.session.commit()
        data=financial_fixture.AmountsOfflineTests.form(self)
        for key in ('monthly_rent_words','parking_bay_count','parking_bay_identifiers','parking_rent_per_bay'):data.pop('amounts_'+key,None)
        data.update(amounts_form_version='2',amounts_parking_applicability='other',amounts_parking_details='Use of visitor parking by arrangement')
        self.assertEqual(self.post('amounts',data).status_code,302)
        saved=self.stored();self.assertEqual(saved['_amounts']['monthly_rent_words'],expected['10000.00'])
        for key,value in original['_amounts'].items():self.assertEqual(saved['_amounts'][key],value)
        for key in original:
            if key!='_amounts':self.assertEqual(saved[key],original[key])
        tree=html.fromstring(self.client.get(self.route('particulars/amounts')).data)
        self.assertFalse(tree.xpath('//*[@name="amounts_monthly_rent_words" or @name="amounts_parking_bay_count" or @name="amounts_parking_bay_identifiers" or @name="amounts_parking_rent_per_bay"]'))
        self.assertEqual(tree.xpath('//*[@id="amounts-monthly-rent-words"]/text()'),['(ten thousand rand and zero cents)'])
        self.assertIn('Rental may be adjusted every six months by an addendum agreed and signed by both parties.',tree.text_content())
        self.assertNotIn('Existing R01 financial particulars retained for review',tree.text_content())
        self.assertEqual(tree.xpath('//*[@name="amounts_parking_details"]/@value'),['Use of visitor parking by arrangement'])
        rows={r['key']:r for r in standard.certificate_rows(saved)}
        self.assertEqual(rows['monthly_rent_words']['value'],'(ten thousand rand and zero cents)')
        self.assertEqual(rows['parking_bay_count']['value'],'Other')
        self.assertEqual(rows['parking_bay_identifiers']['value'],'Use of visitor parking by arrangement')
        self.assertEqual(workflow.amounts_placeholder_values(saved)['[PARKING BAY COUNT]'],'2')
        import test_pm_property_lease_master as masters
        from hashlib import sha256
        source=masters.PropertyLeaseMasterTests.docx(self,'Unchanged source wording')
        record={'sha256':sha256(source).hexdigest(),'version':1}
        state={'terms_version':'test','applicability':{'terms_sha256':record['sha256'],'excluded':[], 'clause_inventory_sha256':standard.digest(standard.inventory(source))}}
        merged=standard.build_document(saved,state,record,source)
        from app.program_billing import lease_preview
        content='\n'.join(lease_preview.block_text(b) for b in lease_preview.extract_blocks(merged))
        self.assertIn('(ten thousand rand and zero cents)',content);self.assertIn('Other',content)
        data['amounts_parking_applicability']='none'
        self.assertEqual(self.post('amounts',data).status_code,302)
        self.assertEqual(self.stored()['_amounts']['parking_bay_identifiers'],'OLD-P1\nOLD-P2')
        self.assertEqual(standard.certificate_values(self.stored())['[PARKING BAY IDENTIFIERS]'],'Not applicable (no allocated bays)')
