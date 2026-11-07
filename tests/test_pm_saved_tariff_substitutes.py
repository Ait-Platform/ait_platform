"""Audited missing-only historical copies; five confirmed bulk children; no live writes."""
import ast,types,unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from io import BytesIO
from pypdf import PdfReader
from sqlalchemy import text
from unittest.mock import patch
import test_pm_metsoa_schedule_b as fixture

class SavedSubstituteTests(fixture.ScheduleBTests):
    def setUp(self):
        super().setUp();self.portfolio();self.confirm_bulk(5,[1,2,11,12,13])
        self.session.execute(text("UPDATE bil_tariff SET effective_date='2026-01-01' WHERE utility_type='electricity'"))
        self.session.execute(text('CREATE TEMP TABLE bil_tariff_verification(tariff_id integer PRIMARY KEY,municipality text,municipal_category text,source_reference text,effective_date text,verified_by integer,verified_at timestamp DEFAULT now(),recorded_at timestamp DEFAULT now())'))
        self.session.commit();self.conn.commit()
        from app.utils.tariff_versions import identity,resolve
        self.service=types.ModuleType('substitute_admin_fixture')
        source=ast.parse(Path('app/admin/programs/billing/tariff_management.py').read_text(encoding='utf-8'))
        source.body=[n for n in source.body if not (isinstance(n,ast.ImportFrom) and (n.module or '').startswith('app.'))]
        self.service.__dict__.update(db=self.db,is_admin=lambda:True,identity=identity,resolve=resolve)
        exec(compile(source,'tariff_management.py','exec'),self.service.__dict__)
        self.service.current_user=types.SimpleNamespace(id=7,is_authenticated=True)
        self.service.today=lambda:date(2026,11,7)
    def fill(self):
        with self.app.test_request_context():return self.service.fill_missing_historical_substitutes('2026-02')
    def rates(self):return [dict(r) for r in self.session.execute(text('SELECT * FROM bil_tariff ORDER BY id')).mappings()]

    def test_saved_copies_are_estimated_without_checkbox_and_five_links_preserved(self):
        before=self.snapshot();old=self.rates();copied=self.fill()
        self.assertEqual(len(copied),14)
        self.assertTrue(all(r['effective_date']==date(2025,7,1) for r in copied))
        for r in old:self.assertIn(r,self.rates())
        self.assertEqual(self.snapshot(),before)
        self.assertEqual(self.fill(),[])
        data=self.build_february()
        self.assertEqual(data['pm_status'],'ESTIMATED');self.assertTrue(data['pm_pricing_complete']);self.assertFalse(data['pm_complete'])
        self.assertEqual(data['electricity']['subtotal'],Decimal('5935.89'))
        self.assertEqual(data['grand_total'],Decimal('11840.22'))
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter WHERE parent_meter_id=5')).scalar(),5)
        self.assertEqual(data['pm_substitute_dates'],['2026-11-07'])
        self.assertTrue(all(r['effective_date']=='2026-06-05' for r in data['pm_estimated_components']))
        rows=[r for w in data['water']['meters'] for r in w['ws_breakdown']+w['sd_breakdown']]
        self.assertEqual(len([r for r in rows if r['desc']=='Management Fee']),1)
        self.assertEqual(len([r for r in rows if r['desc']=='Refuse']),1)
        self.assertEqual(len([r for r in rows if r['desc']=='Water Loss Levy (insurance)']),7)
        audit=list(self.session.execute(text('SELECT source_reference,verified_by FROM bil_tariff_verification')))
        self.assertEqual(len(audit),14);self.assertTrue(all(r[0].startswith('AIT_CURRENT_RATE_SUBSTITUTE_V1:') and r[1]==7 for r in audit))
        url=self.february
        page=self.client.get(url,headers=self.headers);self.assertEqual(page.status_code,200)
        self.assertIn('Current-rate substitutes saved on 2026-11-07',page.text);self.assertNotIn('INCOMPLETE',page.text)
        pdf=self.client.get(url+'&format=pdf',headers=self.headers)
        content=' '.join(' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages).split())
        self.assertIn('11840.22',content);self.assertIn('Report status: ESTIMATED',content);self.assertNotIn('INCOMPLETE',content)
        self.assertIn('= -3.0 kL',content)
        output=Path('tmp/saved-tariff-substitutes');output.mkdir(parents=True,exist_ok=True);(output/'february-fixture.pdf').write_bytes(pdf.data)
        import fitz
        doc=fitz.open(stream=pdf.data,filetype='pdf')
        for i,p in enumerate(doc):p.get_pixmap(matrix=fitz.Matrix(1.1,1.1)).save(str(output/('page-'+str(i+1)+'.png')))
        self.assertEqual(self.client.get(url.replace('/26/','/28/'),headers=self.headers).status_code,403)

    def test_unconfigured_sewerage_surcharge_is_not_added(self):
        self.session.execute(text("DELETE FROM bil_tariff WHERE code='SDSurcharge'"));self.session.commit();self.conn.commit()
        copied=self.fill();self.assertEqual(len(copied),13)
        data=self.build_february()
        self.assertEqual(data['pm_status'],'ESTIMATED');self.assertEqual(data['grand_total'],Decimal('11714.42'))
        self.assertFalse(data['pm_sewerage_surcharge_configured'])
        self.assertFalse(any('sewerage' in r['utility_type'] and 'surcharge' in r['code'].lower() for r in self.rates()))

    def test_existing_historical_rate_and_verified_electricity_are_never_copied(self):
        self.session.execute(text("INSERT INTO bil_tariff(utility_type,code,description,rate,block_start,block_end,reduction_factor,unit,effective_date) SELECT utility_type,code,description,41,block_start,block_end,reduction_factor,unit,'2025-07-01' FROM bil_tariff WHERE code='W02'"));self.session.commit();self.conn.commit()
        old=self.rates();copied=self.fill()
        self.assertNotIn('W02',[r['code'] for r in copied]);self.assertNotIn('ELEC01',[r['code'] for r in copied])
        data=self.build_february();self.assertEqual(next(r.rate for r in data['pm_tariff_records'] if r.code=='W02'),Decimal(41))
        for r in old:self.assertIn(r,self.rates())

    def test_unavailable_surcharge_is_not_invented_and_missing_required_rate_remains_incomplete(self):
        self.session.execute(text("DELETE FROM bil_tariff WHERE code IN ('WSSurcharge','SDSurcharge')"));self.session.commit();self.conn.commit()
        self.fill();data=self.build_february()
        self.assertEqual(data['pm_status'],'ESTIMATED (INCOMPLETE)')
        self.assertIn('Water surcharge',str(data['pm_unpriced_components']))
        self.assertFalse(data['pm_sewerage_surcharge_configured'])

    def test_ambiguous_history_and_unauthorised_actor_cannot_copy(self):
        self.session.execute(text("INSERT INTO bil_tariff(utility_type,code,rate,effective_date) VALUES('water','W01',9,'2025-07-01'),('water','W01',10,'2025-07-01')"));self.session.commit();self.conn.commit()
        before=self.rates()
        with self.assertRaises(ValueError):self.fill()
        self.assertEqual(before,self.rates())
        with patch.object(self.service,'is_admin',return_value=False):
            from werkzeug.exceptions import Forbidden
            with self.assertRaises(Forbidden):self.fill()
        self.assertEqual(before,self.rates())


def load_tests(loader,tests,pattern):return unittest.TestSuite(SavedSubstituteTests(n) for n in SavedSubstituteTests.__dict__ if n.startswith('test_'))
