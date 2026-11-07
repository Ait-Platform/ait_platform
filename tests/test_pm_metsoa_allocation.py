"""Property fees allocated once; existing meter tariffs and report safeguards retained."""
import unittest
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from lxml import html
from pypdf import PdfReader
from sqlalchemy import text
import test_pm_metsoa_bulk as fixture

class AllocationTests(fixture.BulkMetsoaTests):
    def test_bulk_only_management_and_refuse_reconcile_without_writes(self):
        self.setup_bulk();before=self.snapshot();data=self.data()
        water=data['water']['meters']
        management=[(w['meter_number'],r) for w in water for r in w['ws_breakdown'] if r['desc']=='Management Fee']
        refuse=[(w['meter_number'],r) for w in water for r in w['sd_breakdown'] if r['desc']=='Refuse']
        self.assertEqual([(n,r['due']) for n,r in management],[('ANG489',Decimal('144.54'))])
        self.assertEqual([(n,r['due']) for n,r in refuse],[('ANG489',Decimal('206.93'))])
        self.assertEqual(len([r for w in water for r in w['ws_breakdown'] if r['desc']=='Water Loss Levy (insurance)']),3)
        for w in water:
            self.assertEqual(w['ws_cost'],sum(r['due'] for r in w['ws_breakdown']))
            self.assertEqual(w['sd_cost'],sum(r['due'] for r in w['sd_breakdown']))
            self.assertEqual(w['total'],w['ws_cost']+w['sd_cost'])
        self.assertEqual(data['grand_total'],data['electricity']['subtotal']+sum(w['total'] for w in water))
        self.assertEqual(before,self.snapshot())

    def test_supplied_february_estimate_allocation_only_total(self):
        self.setup_bulk()
        for meter,utility in [(6,'Electrical'),(7,'Electrical'),(8,'Electrical'),(9,'Electrical'),(10,'Water'),(11,'Water'),(12,'Water'),(13,'Water')]:
            self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_bill_number) VALUES(:m,26,:n,:u,'83192896040')"),dict(m=meter,n='TEST-'+str(meter),u=utility))
        # The supplied February report has no configured sewerage surcharge.
        self.session.execute(text("DELETE FROM bil_tariff WHERE code='SDSurcharge'"))
        self.session.execute(text('DELETE FROM bil_meter_reading'))
        for meter,consumption in [(1,24),(2,36),(5,82),(10,0),(11,25),(12,0),(13,0),(3,93),(4,534),(6,414),(7,244),(8,86),(9,439)]:
            for day,value in [('2026-01-30',0),('2026-02-28',consumption)]:
                self.session.execute(text('INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(:m,:d,:v)'),dict(m=meter,d=day,v=value))
        self.session.commit();self.conn.commit();self.confirm_bulk(5,[1,2,10,11,12,13])
        before=self.snapshot();rates=list(self.session.execute(text('SELECT * FROM bil_tariff ORDER BY id')))
        with self.app.test_request_context():data=self.metsoa.build(26,'2026-02',recipient_id=11,use_current_tariffs=True)
        self.assertEqual(data['pm_status'],'ESTIMATED');self.assertFalse(data['pm_complete'])
        self.assertEqual(data['electricity']['subtotal'],Decimal('5935.89'))
        self.assertEqual(data['grand_total'],Decimal('11714.42'))
        self.assertEqual(Decimal('13823.24')-data['grand_total'],6*(Decimal('144.54')+Decimal('206.93')))
        self.assertIn('= -3.0 kL',str(data['pm_bulk_notes']))
        page=self.client.get(self.generation_url.replace('2026-10','2026-02')+'&use_current_tariffs=1',headers=self.headers)
        self.assertIn('11714.42',page.text);self.assertIn('historical tariff verification pending.',page.text)
        self.assertEqual(page.text.count('>Management Fee<'),1);self.assertEqual(page.text.count('>Refuse<'),1)
        self.assertEqual(before,self.snapshot());self.assertEqual(rates,list(self.session.execute(text('SELECT * FROM bil_tariff ORDER BY id'))))
        self.assertEqual(self.client.get(self.generation_url.replace('/26/','/28/'),headers=self.headers).status_code,403)

    def test_no_or_ambiguous_bulk_never_guesses_a_fee_target(self):
        self.session.execute(text('UPDATE bil_muni_account SET is_bulk_account=false'))
        self.session.commit();self.conn.commit()
        for ambiguous in (False,True):
            if ambiguous:
                self.session.execute(text('UPDATE bil_muni_account SET is_bulk_account=true'));self.session.commit();self.conn.commit()
            data=self.data();self.assertEqual(data['pm_status'],'INCOMPLETE')
            self.assertIn('unique bulk water meter',str(data['pm_issues']))
            self.assertFalse(any(r['desc'] in ('Management Fee','Refuse') for w in data['water']['meters'] for r in w['ws_breakdown']+w['sd_breakdown']))
            self.assertTrue(all(r['calculated'] is None for r in data['pm_comparisons']))

    def test_property_fee_date_change_does_not_reject_individual_meters(self):
        self.setup_bulk()
        self.session.execute(text("UPDATE bil_tariff SET effective_date='2026-10-01' WHERE code='W_MGT'"));self.session.commit();self.conn.commit()
        data=self.data();names=[w['meter_number'] for w in data['water']['meters']]
        self.assertIn('CEL888',names);self.assertIn('CXN998',names);self.assertNotIn('ANG489',names)
        self.assertEqual(data['pm_status'],'INCOMPLETE')

    def test_thirteen_meter_pdf_is_compact_with_natural_pagination(self):
        self.setup_bulk()
        for meter,utility,consumption in [(6,'Electrical',113),(7,'Electrical',300),(8,'Electrical',389),(9,'Electrical',625),(10,'Water',12),(11,'Water',23),(12,'Water',10),(13,'Water',78)]:
            self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_bill_number) VALUES(:m,26,:n,:u,'83192896040')"),dict(m=meter,n='TEST-'+str(meter),u=utility))
            for day,value in [('2026-09-30',0),('2026-10-30',consumption)]:
                self.session.execute(text('INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(:m,:d,:v)'),dict(m=meter,d=day,v=value))
        self.session.commit();self.conn.commit();self.confirm_bulk(5,[1,2,10,11,12,13])
        before=self.snapshot();data=self.data();self.assertEqual(data['pm_status'],'FINAL')
        page=self.client.get(self.generation_url,headers=self.headers)
        tree=html.fromstring(page.data)
        self.assertEqual(len(tree.xpath('//*[@id="metsoa-consumption"]')),1)
        self.assertEqual(len(tree.xpath('//section[contains(@class,"details")]')),7)
        self.assertNotIn('page-break-before',page.text)
        pdf=self.client.get(self.generation_url+'&format=pdf',headers=self.headers)
        self.assertEqual(pdf.status_code,200)
        reader=PdfReader(BytesIO(pdf.data));self.assertLess(len(reader.pages),6)
        content=' '.join(' '.join(p.extract_text() or '' for p in reader.pages).split())
        self.assertEqual(content.count('Management Fee'),1);self.assertEqual(content.count('Refuse'),1)
        self.assertIn('Report status: FINAL',content);self.assertIn('no automatic refund',content)
        self.assertEqual(content.count('Water Loss Levy (insurance)'),7)
        self.assertIn('%.2f'%data['grand_total'],content)
        output=Path('tmp/metsoa-allocation-review');output.mkdir(parents=True,exist_ok=True)
        (output/'statement.pdf').write_bytes(pdf.data)
        import fitz
        doc=fitz.open(stream=pdf.data,filetype='pdf')
        for index,p in enumerate(doc):p.get_pixmap(matrix=fitz.Matrix(1.3,1.3)).save(str(output/('page-'+str(index+1)+'.png')))
        self.assertEqual(before,self.snapshot())


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(AllocationTests(n) for n in AllocationTests.__dict__ if n.startswith('test_'))
