"""Production-format S_SURCHARGE regression on temporary PostgreSQL tables."""
import unittest
from decimal import Decimal as D
from sqlalchemy import text
import test_pm_sewerage_discharge as fixture

class SewerageSurchargeKLTests(unittest.TestCase):
    def setUp(self):
        self.owner=fixture.SewerageStatementGuardTests(methodName='test_seven_meters_real_route_partial_prices_and_no_writes')
        self.fx=self.owner.seven_meter_fixture()
        self.addCleanup(self.owner.doCleanups)
        s=self.fx.session
        s.execute(text("DELETE FROM bil_tariff WHERE code='SDSurcharge'"))
        s.execute(text("INSERT INTO bil_tariff(id,utility_type,code,description,rate,reduction_factor,unit,effective_date) VALUES(31,'sewerage','S_SURCHARGE','Sewerage infrastructure surcharge',1.48,1,'kL','2026-07-01')"))
        s.execute(text("UPDATE bil_meter_reading SET reading_date=CASE WHEN reading_date='2026-09-29' THEN DATE '2026-09-30' ELSE DATE '2026-10-30' END WHERE meter_id IN(1,2,5,6,7,8,9)"))
        s.execute(text("UPDATE bil_meter_reading SET reading_value=CASE WHEN reading_date='2026-09-30' THEN 3208 ELSE 3261 END WHERE meter_id=1"))
        s.execute(text("UPDATE bil_meter_reading SET reading_value=200 WHERE meter_id=5 AND reading_date='2026-10-30'"))
        s.commit();self.fx.conn.commit()

    def report(self):
        before=self.fx.snapshot();tariffs=list(self.fx.session.execute(text('SELECT * FROM bil_tariff ORDER BY id')))
        with self.fx.app.test_request_context():data=self.fx.metsoa.build(26,'2026-10',recipient_id=11)
        self.assertEqual(before,self.fx.snapshot())
        self.assertEqual(tariffs,list(self.fx.session.execute(text('SELECT * FROM bil_tariff ORDER BY id'))))
        return data

    def test_id31_kl_format_prices_blz_and_other_six_meters_once(self):
        data=self.report()
        water=data['water']['meters'];self.assertEqual(len(water),7)
        self.assertFalse(any('surcharge' in issue.lower() for issue in data['pm_issues']),data['pm_issues'])
        for w in water:
            charges=[r for r in w['sd_breakdown'] if r['desc']=='Sewerage surcharge']
            self.assertEqual(len(charges),1,w['meter_number'])
            self.assertEqual(charges[0]['rate'],D('1.48'))
        blz=next(w for w in water if w['meter_number']=='BLZ080_DBC933')
        surcharge=next(r for r in blz['sd_breakdown'] if r['desc']=='Sewerage surcharge')
        self.assertEqual(blz['summary']['consumption'],53)
        self.assertEqual(surcharge['cons'],D('38.25'))
        self.assertEqual(surcharge['due'],D('56.61'))
        selected=[r for r in data['pm_tariff_records'] if r.code=='S_SURCHARGE']
        self.assertTrue(selected)
        self.assertTrue(all(r.id==31 and r.effective_date.isoformat()=='2026-07-01' for r in selected))
        page=self.fx.client.get(self.fx.generation_url,headers=self.fx.headers)
        self.assertEqual(page.status_code,200,page.text)
        self.assertNotIn('needs verified per-kL units',page.text)
        self.assertNotIn('No applicable sewerage surcharge tariff',page.text)
        self.assertIn('56.61',page.text)
        self.assertIn('S05',str(data['pm_issues']))  # genuine missing rate remains visible
        self.assertFalse(data['pm_complete'])

    def test_future_versions_do_not_replace_october_and_june_is_not_backdated(self):
        from app.program_billing import metsoa_tariffs
        self.fx.session.execute(text("INSERT INTO bil_tariff(utility_type,code,description,rate,reduction_factor,unit,effective_date) VALUES('sewerage','S_SURCHARGE','Future version',99,1,'kL','2027-07-01')"))
        self.fx.session.commit();self.fx.conn.commit()
        rates,issues=metsoa_tariffs.select('2026-10')
        self.assertEqual(rates['sewerage_surcharge'],D('1.48'))
        earlier,_=metsoa_tariffs.select('2026-06')
        self.assertIsNone(earlier['sewerage_surcharge'])

    def test_ambiguous_base_units_remain_rejected(self):
        from app.program_billing import metsoa_tariffs
        for unit,factor in [('kWh',1),('R/month',1),('',1),(None,1),('kL',2),('kL',0),('kL',None)]:
            with self.subTest(unit=unit,factor=factor):
                self.fx.session.execute(text('UPDATE bil_tariff SET unit=:unit,reduction_factor=:factor WHERE id=31'),dict(unit=unit,factor=factor))
                self.fx.session.commit();self.fx.conn.commit()
                rates,issues=metsoa_tariffs.select('2026-10')
                self.assertIsNone(rates['sewerage_surcharge'])
                self.assertIn('needs verified per-kL units',str(issues))

    def test_kl_alias_does_not_authorize_unknown_surcharge_codes(self):
        from app.program_billing import metsoa_tariffs
        self.fx.session.execute(text("UPDATE bil_tariff SET code='S_OTHER_SURCHARGE' WHERE id=31"))
        self.fx.session.commit();self.fx.conn.commit()
        rates,issues=metsoa_tariffs.select('2026-10')
        self.assertIsNone(rates['sewerage_surcharge'])
        self.assertIn('needs verified per-kL units',str(issues))

    def test_conflicting_versions_remain_blocked(self):
        from app.program_billing import metsoa_tariffs
        self.fx.session.execute(text("INSERT INTO bil_tariff(utility_type,code,rate,reduction_factor,unit,effective_date) VALUES('sewerage','S_SURCHARGE',2,1,'kL','2026-07-01')"))
        self.fx.session.commit();self.fx.conn.commit()
        rates,issues=metsoa_tariffs.select('2026-10')
        self.assertIsNone(rates['sewerage_surcharge'])
        self.assertIn('conflicting records',str(issues))

    def test_existing_explicit_per_kl_units_are_unchanged(self):
        from app.program_billing import metsoa_tariffs
        for unit in ('R/kL','ZAR/kL','per kL'):
            self.fx.session.execute(text('UPDATE bil_tariff SET unit=:unit WHERE id=31'),dict(unit=unit))
            self.fx.session.commit();self.fx.conn.commit()
            rates,issues=metsoa_tariffs.select('2026-10')
            self.assertEqual(rates['sewerage_surcharge'],D('1.48'))
            self.assertNotIn('needs verified per-kL units',str(issues))

if __name__=='__main__':unittest.main()
