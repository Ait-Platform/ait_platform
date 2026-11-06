"""Independently versioned infrastructure surcharges; no invented sewerage rate."""
import unittest
from decimal import Decimal
from datetime import date
from io import BytesIO
from pypdf import PdfReader
from sqlalchemy import text
import test_pm_municipal_metsoa as fixture

class InfrastructureSurchargeTests(fixture.MunicipalMetsoaTests):
    def setUp(self):
        super().setUp()
        self.session.execute(text("DELETE FROM bil_tariff WHERE code='SDSurcharge'"))
        self.session.execute(text("INSERT INTO bil_tariff(utility_type,code,description,rate,unit,effective_date) VALUES('water_surcharge','WIS01','Water Infrastructure Surcharge',1.48,'R/kL','2026-07-01'),('water_fixed','W_LOSS','Water Loss Charge',21.60,'R/month','2026-07-01')"))
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(5,'2026-09-25',0)"))
        self.session.commit();self.conn.commit()
    def data(self,month='2026-10',override=False):
        with self.app.test_request_context():return self.metsoa.build(26,month,recipient_id=11,use_current_tariffs=override)

    def test_october_twenty_kl_is_29_60_and_water_loss_is_separate(self):
        before=self.snapshot();tariffs=list(self.session.execute(text('SELECT * FROM bil_tariff ORDER BY id')))
        data=self.data();self.assertTrue(data['pm_complete'],data['pm_issues'])
        meter=next(w for w in data['water']['meters'] if w['meter_number']=='ANG489')
        surcharge=next(r for r in meter['ws_breakdown'] if r['desc']=='Water surcharge')
        loss=next(r for r in meter['ws_breakdown'] if r['desc']=='Water Loss Levy (insurance)')
        self.assertEqual(meter['summary']['consumption'],Decimal(20))
        self.assertEqual(surcharge['rate'],Decimal('1.48'));self.assertEqual(surcharge['due'],Decimal('29.60'))
        self.assertEqual(loss['due'],Decimal('21.60'));self.assertEqual(loss['cons'],1)
        self.assertFalse(any(r['desc']=='Sewerage surcharge' for r in meter['sd_breakdown']))
        self.assertFalse(any('sewerage surcharge tariff' in i for i in data['pm_issues']))
        pdf=self.client.get(self.generation_url+'&format=pdf',headers=self.headers)
        self.assertEqual(pdf.status_code,200)
        content=' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages)
        self.assertIn('Water Infrastructure Surcharge',content);self.assertIn('29.60',content);self.assertIn('21.60',content)
        self.assertIn('Sewerage infrastructure surcharge not applied',content)
        self.assertEqual(before,self.snapshot());self.assertEqual(tariffs,list(self.session.execute(text('SELECT * FROM bil_tariff ORDER BY id'))))

    def test_historical_selection_and_authorised_current_override(self):
        from app.program_billing import metsoa_tariffs
        self.session.execute(text("UPDATE bil_tariff SET rate=0.8 WHERE code='WSSurcharge'"))
        self.session.commit();self.conn.commit()
        with self.app.test_request_context():
            earlier,_=metsoa_tariffs.select(date(2026,6,30));current,_=metsoa_tariffs.select(date(2026,10,31))
        self.assertEqual(earlier['water_surcharge'],Decimal('0.8'))
        self.assertEqual(current['water_surcharge'],Decimal('1.48'))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date=reading_date-interval '8 months'"))
        self.session.commit();self.conn.commit()
        self.assertFalse(self.data('2026-02')['water']['meters'])
        data=self.data('2026-02',True)
        self.assertTrue(data['pm_estimated']);self.assertTrue(any(r['code']=='WIS01' for r in data['pm_estimated_components']))
        self.assertEqual(next(r for r in data['water']['meters'] if r['meter_number']=='ANG489')['summary']['consumption'],20)

    def test_missing_water_rate_is_not_replaced_by_a_constant(self):
        self.session.execute(text("DELETE FROM bil_tariff WHERE utility_type='water_surcharge' OR code='WSSurcharge'"))
        self.session.commit();self.conn.commit();data=self.data()
        self.assertFalse(data['pm_complete'])
        self.assertIn('No applicable water surcharge tariff',str(data['pm_issues']))
        for meter in data['water']['meters']:
            self.assertFalse(any(r['desc']=='Water surcharge' for r in meter['ws_breakdown']))

    def test_configured_sewerage_rate_is_independent_and_ownership_is_retained(self):
        self.session.execute(text("INSERT INTO bil_tariff(utility_type,code,description,rate,unit,effective_date) VALUES('sewerage_surcharge','SIS01','Documented sewerage surcharge',0.6,'R/kL','2026-07-01')"))
        self.session.commit();self.conn.commit();data=self.data()
        meter=next(w for w in data['water']['meters'] if w['meter_number']=='ANG489')
        surcharge=next(r for r in meter['sd_breakdown'] if r['desc']=='Sewerage surcharge')
        self.assertEqual(surcharge['due'],Decimal('12.00'));self.assertEqual(surcharge['rate'],Decimal('0.6'))
        self.assertEqual(self.client.get(self.generation_url.replace('/26/','/28/'),headers=self.headers).status_code,403)
        self.assertEqual(self.client.get(self.generation_url).status_code,403)


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(InfrastructureSurchargeTests(n) for n in InfrastructureSurchargeTests.__dict__ if n.startswith('test_'))
