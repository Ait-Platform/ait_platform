"""Administrator category/date selection and read-only municipal differences."""
import unittest
from decimal import Decimal
from sqlalchemy import text,event
from lxml import html
import test_pm_municipal_metsoa as fixture

class AdminSelectionTests(fixture.MunicipalMetsoaTests):
    february='/billing/utilities/26/generate-metsoa?billing_month=2026-02&recipient_id=11&generate=1'
    def setUp(self):
        super().setUp()
        self.session.execute(text('CREATE TEMP TABLE bil_tariff_verification(tariff_id integer PRIMARY KEY,municipal_category text)'))
        self.session.execute(text("UPDATE bil_tariff SET effective_date='2025-07-01'"))
        self.session.execute(text("UPDATE bil_tariff SET code='WF01' WHERE code='W_LOSS'"))
        self.session.execute(text("INSERT INTO bil_tariff_verification SELECT id,'Water Loss Levy (insurance)' FROM bil_tariff WHERE code='WF01'"))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date=reading_date-interval '8 months'"))
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(5,'2026-01-25',0)"))
        self.session.execute(text("UPDATE bil_municipal_statement SET period='2026-02',statement_date='2026-02-28',due_date='2026-03-07'"))
        self.session.commit();self.conn.commit()
    def data(self):
        with self.app.test_request_context():return self.metsoa.build(26,'2026-02',recipient_id=11)

    def test_admin_generated_code_and_explicit_category_enable_february_calculation(self):
        before=self.snapshot();tariffs=list(self.session.execute(text('SELECT * FROM bil_tariff ORDER BY id')))
        data=self.data();self.assertTrue(data['pm_complete'],data['pm_issues'])
        for meter in data['water']['meters']:
            loss=next(r for r in meter['ws_breakdown'] if r['desc']=='Water Loss Levy (insurance)')
            self.assertEqual(loss['due'],Decimal('19.30'));self.assertEqual(loss['cons'],1)
        self.assertEqual(before,self.snapshot())
        self.assertEqual(tariffs,list(self.session.execute(text('SELECT * FROM bil_tariff ORDER BY id'))))

    def test_actual_later_dates_remain_missing_without_backdating(self):
        self.session.execute(text("UPDATE bil_tariff SET effective_date='2026-06-05' WHERE utility_type<>'electricity'"))
        self.session.commit();self.conn.commit();data=self.data()
        self.assertEqual(data['water']['meters'],[])
        self.assertIn('No applicable water consumption bands',str(data['pm_issues']))
        self.assertIn('No applicable sewerage consumption bands',str(data['pm_issues']))
        self.assertIn('W_LOSS',str(data['pm_issues']))
        self.assertTrue(data['electricity']['rows'])

    def test_unidentified_fixed_charge_is_not_silently_substituted_for_insurance(self):
        self.session.execute(text("UPDATE bil_tariff_verification SET municipal_category='Management fee'"))
        self.session.commit();self.conn.commit();data=self.data()
        self.assertFalse(data['pm_complete']);self.assertIn('W_LOSS',str(data['pm_issues']))

    def test_legacy_sanitation_alias_versions_select_once(self):
        self.session.execute(text("UPDATE bil_tariff SET utility_type='sanitation' WHERE code='S01'"))
        self.session.execute(text("INSERT INTO bil_tariff(utility_type,code,description,rate,block_start,block_end,reduction_factor,unit,effective_date) SELECT 'sewerage',code,description,rate,block_start,block_end,reduction_factor,unit,'2025-08-01' FROM bil_tariff WHERE code='S01'"))
        self.session.commit();self.conn.commit();data=self.data()
        self.assertTrue(data['pm_complete'],data['pm_issues'])

    def test_debit_credit_comparisons_are_account_scoped_and_never_posted(self):
        initial=self.data();comparison=next(r for r in initial['pm_comparisons'] if r['account']=='83192896040')
        calculated=comparison['calculated'];self.assertIsNotNone(calculated)
        for amount,label in [(calculated-10,'Debit difference'),(calculated+10,'Credit difference')]:
            self.session.execute(text('UPDATE bil_municipal_statement SET due=:v WHERE account_id=1'),dict(v=amount))
            self.session.commit();self.conn.commit()
            queries=[]
            def capture(conn,cursor,sql,params,context,many):queries.append(sql.lower().lstrip())
            event.listen(self.conn,'before_cursor_execute',capture)
            try:data=self.data()
            finally:event.remove(self.conn,'before_cursor_execute',capture)
            result=next(r for r in data['pm_comparisons'] if r['account']=='83192896040')
            self.assertEqual(result['label'],label);self.assertEqual(abs(result['difference']),Decimal('10.00'))
            self.assertFalse(any(q.startswith(('insert ','update ','delete ')) for q in queries))
        page=self.client.get(self.february,headers=self.headers)
        self.assertFalse(html.fromstring(page.data).xpath('//*[@id="metsoa-comparison"]'))
        before=self.snapshot()
        from io import BytesIO
        from pypdf import PdfReader
        pdf=self.client.get(self.february+'&format=pdf',headers=self.headers)
        self.assertEqual(pdf.status_code,200)
        content=' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages)
        for text_to_remove in ('Municipal billing comparison','Captured municipal Amount Due','Credit difference','Debit difference','Comparison unavailable'):
            self.assertNotIn(text_to_remove,page.text);self.assertNotIn(text_to_remove,content)
        for expected in ('Electricity Sub-Total','Water meter readings','Water and sewerage calculations','%.2f'%data['grand_total']):
            self.assertIn(expected,page.text);self.assertIn(expected,content)
        self.assertEqual(before,self.snapshot())
        from pathlib import Path
        import fitz
        output=Path('tmp/metsoa-tenant-facing-review');output.mkdir(parents=True,exist_ok=True)
        (output/'statement.pdf').write_bytes(pdf.data)
        document=fitz.open(stream=pdf.data,filetype='pdf')
        for i,p in enumerate(document):p.get_pixmap(matrix=fitz.Matrix(1.2,1.2)).save(str(output/('page-'+str(i+1)+'.png')))

        self.assertEqual(self.client.get(self.february.replace('/26/','/28/'),headers=self.headers).status_code,403)
        self.session.execute(text("DELETE FROM bil_tariff WHERE code='SDSurcharge'"))
        self.session.commit();self.conn.commit();data=self.data()
        result=next(r for r in data['pm_comparisons'] if r['account']=='83192896040')
        self.assertIsNotNone(result['difference']);self.assertIsNotNone(result['calculated'])
        self.assertFalse(data['pm_sewerage_surcharge_configured'])


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(AdminSelectionTests(n) for n in AdminSelectionTests.__dict__ if n.startswith('test_'))
