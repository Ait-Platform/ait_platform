"""Recovered review data is not an active tariff; stored dated B rates take precedence."""
import unittest
from decimal import Decimal
from datetime import date
from io import BytesIO
from pathlib import Path
from pypdf import PdfReader
from lxml import html
from sqlalchemy import text
import test_pm_metsoa_bulk as fixture
from app.program_billing import recovered_tariffs

class ScheduleBTests(fixture.BulkMetsoaTests):
    february='/billing/utilities/26/generate-metsoa?billing_month=2026-02&recipient_id=11&generate=1'
    def portfolio(self):
        self.setup_bulk()
        self.session.execute(text("UPDATE bil_tariff SET effective_date='2026-06-05'"))
        for meter,utility in [(6,'Electrical'),(7,'Electrical'),(8,'Electrical'),(9,'Electrical'),(10,'Water'),(11,'Water'),(12,'Water'),(13,'Water')]:
            self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_bill_number) VALUES(:m,26,:n,:u,'83192896040')"),dict(m=meter,n='TEST-'+str(meter),u=utility))
        self.session.execute(text('DELETE FROM bil_meter_reading'))
        for meter,consumption in [(1,24),(2,36),(5,82),(10,0),(11,25),(12,0),(13,0),(3,93),(4,534),(6,414),(7,244),(8,86),(9,439)]:
            for day,value in [('2026-01-30',0),('2026-02-28',consumption)]:
                self.session.execute(text('INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(:m,:d,:v)'),dict(m=meter,d=day,v=value))
        self.session.commit();self.conn.commit();self.confirm_bulk(5,[1,2,10,11,12,13])

    def save_supported_fixture_versions(self):
        records=[dict(r) for r in self.session.execute(text('SELECT * FROM bil_tariff ORDER BY id')).mappings()]
        schedule=next(s for s in recovered_tariffs.review(records) if s['id']=='B')
        self.assertFalse(next(r for r in schedule['rows'] if r['variable']=='MthMgtFee14')['ready'])
        for row in schedule['rows']:
            if not row['ready']:continue
            # Isolated fixtures only: emulate the dated versions after explicit Admin saves.
            self.session.execute(text('INSERT INTO bil_tariff(utility_type,code,description,rate,block_start,block_end,reduction_factor,unit,effective_date) SELECT utility_type,code,description,:rate,block_start,block_end,reduction_factor,unit,:start FROM bil_tariff WHERE id=:id'),dict(rate=row['literal'],start=schedule['stated_start'],id=row['target']['id']))
        self.session.commit();self.conn.commit()
        return records

    def build_february(self,override=False):
        with self.app.test_request_context():return self.metsoa.build(26,'2026-02',recipient_id=11,use_current_tariffs=override)

    def test_inactive_catalogue_is_not_an_activation_or_silent_fallback(self):
        self.portfolio();before=self.snapshot()
        data=self.build_february()
        self.assertFalse(data['pm_estimated']);self.assertEqual(data['pm_status'],'INCOMPLETE')
        self.assertEqual(data['electricity']['rows'],[]);self.assertEqual(data['water']['meters'],[])
        from app.program_billing import metsoa_tariffs
        with self.app.test_request_context():rates,issues=metsoa_tariffs.select(date(2026,2,28))
        self.assertIsNone(rates['electricity']);self.assertIn('W_MGT',str(rates['unavailable_components']))
        self.assertEqual(recovered_tariffs.catalogue()['activation'],'Inactive review data; never loaded by METSOA tariff selection')
        self.assertEqual(before,self.snapshot())

    def test_saved_b_wins_per_component_and_unsupported_rates_stay_unavailable(self):
        self.portfolio();current=self.save_supported_fixture_versions();before=self.snapshot()
        data=self.build_february()
        self.assertFalse(data['pm_estimated']);self.assertEqual(data['pm_status'],'INCOMPLETE')
        chosen={r.code:r for r in data['pm_tariff_records']}
        expected={'ELEC01':'3.5747','W01':'40.1','W02':'47.5','W03':'63.3','W04':'101.84','S01':'5.45','S02':'9.2','S03':'17.54','S04':'27.38','WSSurcharge':'1.48','SDSurcharge':'1.48','W_LOSS':'17.12','S_REFUSE':'206.93'}
        self.assertEqual(set(chosen),set(expected))
        for code,rate in expected.items():
            self.assertEqual(chosen[code].rate,Decimal(rate));self.assertEqual(chosen[code].effective_date,date(2024,11,1))
            target=next(r for r in current if r['code']==code)
            for key in ('block_start','block_end','reduction_factor','unit'):self.assertEqual(getattr(chosen[code],key),target[key])
        missing={r['component'] for r in data['pm_unpriced_components']}
        self.assertIn('W05',missing);self.assertIn('W_MGT',missing)
        lines=[r for w in data['water']['meters'] for r in w['ws_breakdown']+w['sd_breakdown']]
        self.assertFalse(any(r['desc']=='Management Fee' for r in lines))
        self.assertEqual(len([r for r in lines if r['desc']=='Refuse']),1)
        self.assertEqual([r['due'] for r in lines if r['desc']=='Water Loss Levy (insurance)'],[Decimal('17.12')]*7)
        self.assertIn('= -3.0 kL',str(data['pm_bulk_notes']))
        # Historical amounts are calculated, not copied from the previous June-rate estimate.
        self.assertEqual(data['electricity']['subtotal'],Decimal('6470.21'))
        self.assertEqual(data['grand_total'],sum(w['total'] for w in data['water']['meters'])+data['electricity']['subtotal'])
        self.assertEqual(data['grand_total'],Decimal('12318.97'))
        self.assertNotEqual(data['grand_total'],Decimal('11714.42'))
        page=self.client.get(self.february,headers=self.headers)
        self.assertNotIn('historical tariff verification pending.',page.text);self.assertIn('W_MGT',page.text)
        pdf=self.client.get(self.february+'&format=pdf',headers=self.headers)
        content=' '.join(' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages).split())
        self.assertIn('12318.97',content);self.assertIn('INCOMPLETE',content)
        self.assertNotIn('historical tariff verification pending.',content);self.assertIn('W_MGT',content)
        output=Path('tmp/metsoa-schedule-b-review');output.mkdir(parents=True,exist_ok=True)
        (output/'supported-b-incomplete-fixture.pdf').write_bytes(pdf.data)

        self.assertEqual(before,self.snapshot())
        with self.app.test_request_context():latest,_=__import__('app.program_billing.metsoa_tariffs',fromlist=['select']).select(date(2026,10,31))
        self.assertEqual(latest['electricity'],Decimal('3.2795'))

    def test_partial_override_never_replaces_stored_b_and_disclosure_is_once(self):
        self.portfolio();self.save_supported_fixture_versions();before=self.snapshot()
        data=self.build_february(True)
        self.assertEqual(data['pm_status'],'ESTIMATED');self.assertFalse(data['pm_complete'])
        self.assertEqual({r['code'] for r in data['pm_estimated_components']},{'W05','W_MGT'})
        self.assertTrue(all(r.rate==Decimal('3.5747') for r in data['pm_tariff_records'] if r.code=='ELEC01'))
        page=self.client.get(self.february+'&use_current_tariffs=1',headers=self.headers)
        tree=html.fromstring(page.data);self.assertEqual(len(tree.xpath('//*[@id="historical-estimate"]')),1)
        self.assertNotIn('The user must verify the calculations.',page.text)
        self.assertEqual(page.text.count('historical tariff verification pending.'),1)
        self.assertEqual(page.text.count('>Management Fee<'),1);self.assertEqual(page.text.count('>Refuse<'),1)
        self.assertEqual(before,self.snapshot())

    def test_actual_local_missing_surcharge_families_are_not_invented(self):
        self.portfolio()
        self.session.execute(text("DELETE FROM bil_tariff WHERE code IN ('WSSurcharge','SDSurcharge')"));self.session.commit();self.conn.commit()
        self.save_supported_fixture_versions()
        data=self.build_february()
        self.assertEqual(data['pm_status'],'INCOMPLETE');self.assertFalse(data['pm_estimated'])
        self.assertIn('Water surcharge',str(data['pm_unpriced_components']))
        self.assertFalse(data['pm_sewerage_surcharge_configured'])
        self.assertEqual(data['grand_total'],Decimal('12067.37'))
        self.assertTrue(all('surcharge' not in r['desc'].lower() for w in data['water']['meters'] for r in w['ws_breakdown']+w['sd_breakdown']))

    def test_actual_local_january_electricity_date_supersedes_november(self):
        self.portfolio();self.save_supported_fixture_versions()
        self.session.execute(text("INSERT INTO bil_tariff(utility_type,code,description,rate,unit,effective_date) VALUES('electricity','ELEC01','Local dated version',3.2795,'R/kWh','2026-01-01')"));self.session.commit();self.conn.commit()
        data=self.build_february()
        self.assertEqual(data['electricity']['subtotal'],Decimal('5935.89'))
        chosen=next(r for r in data['pm_tariff_records'] if r.code=='ELEC01')
        self.assertEqual(chosen.effective_date,date(2026,1,1));self.assertEqual(chosen.rate,Decimal('3.2795'))
        self.assertFalse(data['pm_estimated'])

    def test_complete_stored_history_is_final_even_with_override_checkbox(self):
        self.portfolio();self.save_supported_fixture_versions()
        # Separate synthetic Admin-maintained historical records establish full coverage.
        # These are NOT asserted to be verified recovered Schedule B mappings.
        for code in ('W_MGT','W05'):
            self.session.execute(text("INSERT INTO bil_tariff(utility_type,code,description,rate,block_start,block_end,reduction_factor,unit,effective_date) SELECT utility_type,code,description,rate,block_start,block_end,reduction_factor,unit,'2025-07-01' FROM bil_tariff WHERE code=:code AND effective_date='2026-06-05'"),dict(code=code))
        self.session.commit();self.conn.commit();before=self.snapshot()
        data=self.build_february(True)
        self.assertEqual(data['pm_status'],'FINAL');self.assertEqual(data['pm_estimated_components'],[])
        page=self.client.get(self.february+'&use_current_tariffs=1',headers=self.headers)
        self.assertNotIn('historical tariff verification pending.',page.text);self.assertNotIn('CURRENT TARIFFS APPLIED',page.text)
        self.assertNotIn('The user must verify the calculations.',page.text)
        pdf=self.client.get(self.february+'&use_current_tariffs=1&format=pdf',headers=self.headers)
        content=' '.join(' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages).split())
        self.assertIn('Report status: FINAL',content);self.assertNotIn('historical tariff verification pending.',content)
        self.assertIn('= -3.0 kL',content)
        self.assertEqual(page.text.count('>Management Fee<'),1);self.assertEqual(page.text.count('>Refuse<'),1)
        output=Path('tmp/metsoa-schedule-b-review');output.mkdir(parents=True,exist_ok=True);(output/'complete-fixture.pdf').write_bytes(pdf.data)
        import fitz
        document=fitz.open(stream=pdf.data,filetype='pdf')
        for i,p in enumerate(document):p.get_pixmap(matrix=fitz.Matrix(1.2,1.2)).save(str(output/('page-'+str(i+1)+'.png')))
        self.assertLess(len(document),6)
        self.assertEqual(before,self.snapshot())
        self.assertEqual(self.client.get(self.february.replace('/26/','/28/'),headers=self.headers).status_code,403)


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ScheduleBTests(n) for n in ScheduleBTests.__dict__ if n.startswith('test_'))
