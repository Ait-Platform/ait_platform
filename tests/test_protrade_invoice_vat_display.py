"""Invoice VAT suppression is presentation-only, including frozen historic payloads."""
from copy import deepcopy
from pathlib import Path
import unittest
from flask import Flask,render_template
from lxml import html


class InvoiceVatDisplayTests(unittest.TestCase):
    def render(self,vat='0.00',title='Invoice',treatment='nonvendor'):
        app=Flask('vat-display',template_folder=str(Path(__file__).resolve().parents[1]/'templates'))
        document=dict(title=title,reference='INV-HISTORIC-001',date='2026-11-15',recipient=None,note='Recorded legal basis',
            columns=['Description','Amount (R)'],rows=[['Work','100.00'],['Tax',vat]],
            totals=[('Subtotal (R)','100.00'),('VAT (0%) (R)',vat),('Total (R)','100.00')],
            tax=dict(treatment=treatment,rate='0' if vat=='0.00' else '15',legal_basis='Qualifying zero-rated export'))
        before=deepcopy(document)
        with app.test_request_context():
            output=render_template('program_mechanic/output_document.html',document=document,
                issuer=dict(name='Supplier',address='Supplier address',phone='',email='',tax_number='4123456789',vat_registered=treatment!='nonvendor'),generated='2026-11-15T10:00:00')
        self.assertEqual(document,before)
        return html.fromstring(output)

    def test_zero_vat_amount_rows_suppressed(self):
        tree=self.render()
        self.assertFalse(tree.xpath('//tbody/tr[td[1]="Tax"]'))
        self.assertFalse(tree.xpath('//dt[starts-with(text(),"VAT")]'))
        self.assertIn('Total (R)',tree.text_content())

    def test_positive_vat_breakdown_remains(self):
        tree=self.render(vat='15.00',title='Tax Invoice',treatment='standard')
        self.assertTrue(tree.xpath('//tbody/tr[td[1]="Tax"]'))
        self.assertTrue(tree.xpath('//dt[starts-with(text(),"VAT")]'))

    def test_zero_rated_invoice_keeps_legal_information(self):
        tree=self.render(title='Tax Invoice',treatment='zero')
        self.assertIn('VAT rate: 0%',tree.text_content())
        self.assertIn('Qualifying zero-rated export',tree.text_content())
        self.assertIn('4123456789',tree.text_content())
        self.assertFalse(tree.xpath('//dt[starts-with(text(),"VAT")]'))

    def test_quotation_rendering_unchanged(self):
        self.assertTrue(self.render(title='Quotation').xpath('//dt[starts-with(text(),"VAT")]'))
