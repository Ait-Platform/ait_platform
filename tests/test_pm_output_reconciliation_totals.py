"""Filtered display totals shared by preview, print and PDF/email output."""
import unittest
from unittest.mock import patch
from decimal import Decimal
from io import BytesIO
from lxml import html
from pypdf import PdfReader
import test_pm_central_utilities_output as fixture


class OutputReconciliationTotalsTests(unittest.TestCase):
    def setUp(self):
        self.owner=fixture.CentralOutputTests(methodName='test_all_seven_reports_use_one_work_area_and_preserve_records')
        self.owner.setUp();self.addCleanup(self.owner.doCleanups)
        self.fx=self.owner.fx;self.output=self.owner.output

    def test_preview_is_last_selection_control_and_upper_menu_retained(self):
        response=self.owner.get('readings','history')
        tree=html.fromstring(response.data)
        form=tree.get_element_by_id('output-filters')
        self.assertEqual(form[-1].text,'Preview')
        self.assertEqual(form[-1].get('name'),'preview')
        self.assertIn('ml-auto',form[-1].get('class'))
        self.assertLess(list(form).index(tree.get_element_by_id('output-to').getparent()),len(form)-1)
        self.assertEqual(tree.get_element_by_id('output-actions').xpath('./button/text()|./a/text()'),['Print','Email','Download PDF'])

    def test_both_filtered_reports_totals_and_pdf_email_share_markup(self):
        for key,columns in [('metro_reconciliation',['arrears','balance','due','paid']),('utilities_reconciliation',['calculated','billed','difference'])]:
            with self.subTest(report=key):
                args=dict(source='reconciliation',report=key,preview='1',billing_month='2026-10',recipient_id='11',item_id='1')
                response=self.fx.client.get(self.owner.url,query_string=args,headers=self.fx.headers)
                self.assertEqual(response.status_code,200,response.text)
                tree=html.fromstring(response.data)
                with self.fx.app.test_request_context():
                    data=self.output.report(26,args)
                    pdf=self.output.render_pdf(data)
                rows=data['rows'] if key=='metro_reconciliation' else data['report_payload']['pm_comparisons']
                expected=[]
                for column in columns:
                    expected.append('Unavailable' if any(r[column] is None for r in rows) else 'R{:,.2f}'.format(sum((Decimal(str(r[column])).quantize(Decimal('.01')) for r in rows),Decimal(0))))
                self.assertEqual(tree.xpath('//*[@id="utilities-report"]//tfoot/tr/td/strong/text()'),expected)
                self.assertEqual(tree.xpath('//*[@id="utilities-report"]//tfoot/tr/th/strong/text()'),['TOTAL'])
                text=' '.join(page.extract_text() or '' for page in PdfReader(BytesIO(pdf)).pages)
                self.assertIn('TOTAL',text)
                for value in expected:self.assertIn(value,text)
                if key=='utilities_reconciliation':
                    self.assertEqual(tree.xpath('//*[@id="metsoa-comparison"]/thead/tr/th/text()'),['Account','Utilities charges','Due','Difference'])
                    self.assertIn('Utilities charges',text)
                    self.assertNotIn('Captured municipal Amount Due',text)
                fields=self.owner.fields(response);fields.update(action='email',email_to='manager@example.com')
                with patch.object(self.output,'email_pdf',return_value=True) as sender:
                    sent=self.fx.client.post(self.owner.url,data=fields,headers=self.fx.headers)
                    self.assertEqual(sent.status_code,302,sent.text)
                    attachment=sender.call_args.args[2]
                    self.assertIn('TOTAL',' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(attachment)).pages))

    def test_total_uses_displayed_precision_and_does_not_hide_missing_values(self):
        with self.fx.app.test_request_context():
            template=self.fx.app.jinja_env.from_string("{% from 'program_billing/output_reconciliation_totals.html' import totals %}{{ totals(rows,['amount']) }}")
            result=template.render(rows=[dict(amount=Decimal('1.005')),dict(amount=Decimal('1.005'))])
            self.assertIn('R2.00',result)
            self.assertIn('Unavailable',template.render(rows=[dict(amount=Decimal(1)),dict(amount=None)]))
            self.assertIn('R0.00',template.render(rows=[]))


if __name__=='__main__':unittest.main()
