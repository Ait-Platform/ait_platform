"""Bulk diagnostics are internal to MAR; tenant reports and financial values stay intact."""
import unittest
from io import BytesIO
from pathlib import Path
from lxml import html
from pypdf import PdfReader
from sqlalchemy import text,event
import test_pm_saved_tariff_substitutes as fixture

class InternalBulkReviewTests(fixture.SavedSubstituteTests):
    def test_five_link_warning_is_only_in_mar_and_read_only(self):
        self.fill();before=self.snapshot()
        rates=self.rates();audit=list(self.session.execute(text('SELECT * FROM bil_utilities_capture_audit ORDER BY id')))
        original=self.build_february()
        queries=[]
        def record(conn,cursor,sql,params,context,many):queries.append(sql.lower().lstrip())
        event.listen(self.conn,'before_cursor_execute',record)
        try:
            mar=self.client.get('/billing/utilities/26/metro-reconciliation?billing_month=2026-02&workspace=register',headers=self.headers)
            self.assertEqual(mar.status_code,200,mar.text)
            tree=html.fromstring(mar.data);review=tree.get_element_by_id('mar-bulk-review')
            self.assertIn('ANG489',review.text_content())
            self.assertIn('bulk consumption 82.0 minus linked individual consumption 85.0 = -3.0 kL',review.text_content())
            self.assertIn('no automatic refund',review.text_content())
            self.assertEqual(review.getparent().get('id'),'mar-register')
            self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter WHERE parent_meter_id=5')).scalar(),5)
            for month in ('2026-01','2026-03'):
                other=self.client.get('/billing/utilities/26/metro-reconciliation?billing_month='+month+'&workspace=register',headers=self.headers)
                self.assertNotIn('= -3.0 kL',other.text)
            page=self.client.get(self.february,headers=self.headers)
            pdf=self.client.get(self.february+'&format=pdf',headers=self.headers)
            self.assertEqual(pdf.status_code,200)
            content=' '.join(' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages).split())
            for forbidden in ('minus linked individual consumption','Bulk difference','= -3.0 kL','no automatic refund'):
                self.assertNotIn(forbidden,page.text);self.assertNotIn(forbidden,content)
            self.assertIn('%.2f'%original['grand_total'],content)
            self.assertIn('Water and sewerage calculations',content)
            after=self.build_february()
        finally:event.remove(self.conn,'before_cursor_execute',record)
        self.assertEqual(after['grand_total'],original['grand_total'])
        self.assertEqual(after['water'],original['water']);self.assertEqual(after['pm_consumption_rows'],original['pm_consumption_rows'])
        self.assertEqual(before,self.snapshot());self.assertEqual(rates,self.rates())
        self.assertEqual(audit,list(self.session.execute(text('SELECT * FROM bil_utilities_capture_audit ORDER BY id'))))
        self.assertFalse(any(q.startswith(('insert ','update ','delete ')) for q in queries))
        self.assertEqual(self.client.get('/billing/utilities/28/metro-reconciliation?billing_month=2026-02&workspace=register',headers=self.headers).status_code,403)
        output=Path('tmp/mar-internal-bulk-review');output.mkdir(parents=True,exist_ok=True);(output/'tenant.pdf').write_bytes(pdf.data)
        import fitz
        document=fitz.open(stream=pdf.data,filetype='pdf')
        for i,p in enumerate(document):p.get_pixmap(matrix=fitz.Matrix(1.1,1.1)).save(str(output/('page-'+str(i+1)+'.png')))


def load_tests(loader,tests,pattern):return unittest.TestSuite(InternalBulkReviewTests(n) for n in InternalBulkReviewTests.__dict__ if n.startswith('test_'))
