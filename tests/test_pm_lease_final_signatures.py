"""Focused representative signatures, source contamination guard and immutable records."""
import copy,importlib.util,unittest
from pathlib import Path
from lxml import html
import fitz
from unittest.mock import patch
import test_pm_lease_html_architecture as fixture
ROOT=Path(__file__).resolve().parents[1]

class FinalSignatureTests(unittest.TestCase):
    setUp=fixture.HtmlLeaseKitTests.setUp
    context=fixture.HtmlLeaseKitTests.context
    def particulars(self):
        self.items['_contracting_lessors']['identities'].append(dict(type='individual',name='Second Owner',identity_number='OWNER-2'))
        self.items['_parties']['lessor']=dict(signatory_name='Mandated Agent',signatory_identity_number='AGENT-1',signatory_capacity='Authorised Agent')
        for party in self.signing.rows(self.items):
            self.items.setdefault(self.signing.KEY,{})[party['key']]={'place':'Warner Beach, Durban','date':'2026-10-24'}
    def test_clean_place_actual_dates_and_source_save_preserve_other_information(self):
        self.particulars();before=copy.deepcopy(self.items)
        statement='SIGNED at WARNER BEACH, DURBAN on this the 24th day of OCTOBER 2026'
        for row in self.signing.rows(self.items):self.assertEqual(row['statement'],statement)
        row=self.signing.rows(self.items)[0]
        saved=self.signing.update(self.items,{'signing_place_'+row['key']:self.items[self.signing.KEY][row['key']]['place']})
        self.assertEqual(saved[self.signing.KEY][row['key']]['place'],'Warner Beach, Durban')
        self.assertEqual(saved[self.signing.KEY][row['key']]['date'],'2026-10-24');self.assertEqual(self.items,before)
        self.assertEqual(self.signing.signing_statement('Cape Town','2028-02-21'),'SIGNED at CAPE TOWN on this the 21st day of FEBRUARY 2028')
        with self.assertRaises(ValueError):self.signing.update(self.items,{'signing_place_'+row['key']:'Change Financial Particulars'})
    def test_shared_html_and_pdf_one_lessor_mandate_and_independent_witnesses(self):
        self.particulars();before=copy.deepcopy(self.items)
        page=self.client.get('/view/26?draft_id=draft-one&component=signature',headers=self.headers)
        self.assertEqual(page.status_code,200);tree=html.fromstring(page.data);sections=tree.xpath('//*[contains(@class,"signature-party")]')
        self.assertEqual(len(sections),2) # one lessor mandate and one lessee section, retaining two genuine signers
        self.assertEqual(len(sections[0].xpath('./h3[text()="LESSOR"]')),1)
        self.assertIn('FOR AND ON BEHALF OF THE LESSOR',sections[0].text_content());self.assertIn('Mandated Agent',sections[0].text_content())
        self.assertIn('Authorised Agent',sections[0].text_content())
        for section in sections:
            self.assertEqual(len(section.xpath('.//*[contains(@class,"signing-statement")]')),1)
            self.assertEqual(section.text_content().count('Witness 1 - name'),1);self.assertEqual(section.text_content().count('Witness 2 - name'),1)
            self.assertNotIn('CHANGE FINANCIAL',section.text_content())
        with self.context('signature'):
            payload=self.pdf.build_pdf('signature',self.kit.preview_context(26))
            pdf=fitz.open(stream=payload,filetype='pdf');text=''.join(page.get_text() for page in pdf)
            self.assertEqual(text.count('FOR AND ON BEHALF OF THE LESSOR'),1)
            self.assertEqual(text.count('SIGNED at WARNER BEACH, DURBAN'),2)
            self.assertEqual(text.count('Witness 1 - name'),2);self.assertEqual(text.count('Witness 2 - name'),2)
        self.assertEqual(before,self.items)
    def test_heading_instruction_rejected_and_user_can_save_actual_place(self):
        self.particulars();row=self.signing.rows(self.items)[0];before=copy.deepcopy(self.items)
        with self.assertRaises(ValueError):
            self.signing.update(self.items,{'signing_place_'+row['key']:'Warner Beach, Durban CHANGE FINANCIAL PARTICULARS TO FINANCIAL'})
        updated=self.signing.update(self.items,{'signing_place_'+row['key']:'Warner Beach, Durban','signing_date_'+row['key']:'2026-10-24'})
        self.assertEqual(self.signing.rows(updated)[0]['statement'],'SIGNED at WARNER BEACH, DURBAN on this the 24th day of OCTOBER 2026')
        self.assertEqual(self.items,before)

if __name__=='__main__':unittest.main()
