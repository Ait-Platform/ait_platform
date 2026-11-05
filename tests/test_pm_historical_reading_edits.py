"""Historical reading corrections and dependent consumption; isolated PostgreSQL only."""
import unittest
from decimal import Decimal
from sqlalchemy import text
from lxml import html
import test_pm_utilities_capture as fixture
import test_pm_municipal_metsoa as metsoa_fixture

class HistoricalReadingEditTests(fixture.UtilitiesCaptureTests):
    def setUp(self):
        super().setUp()
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type) VALUES(98,26,'HIST-E','Electrical'),(99,26,'HIST-W','Water'),(97,28,'FOREIGN','Water')"))
        for meter in (98,99):
            self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(:m,'2025-08-31',100),(:m,'2025-09-30',200),(:m,'2025-10-31',300)"),dict(m=meter))
            self.session.execute(text("INSERT INTO bil_consumption(meter_id,meter_number,month,last_date,new_date,last_read,new_read,days,consumption) VALUES(:m,:number,'2025-09','2025-08-31','2025-09-30',100,200,30,100),(:m,:number,'2025-10','2025-09-30','2025-10-31',200,300,31,100),(:m,:number,'2025-11','2025-11-30','2025-11-30',0,0,30,777)"),dict(m=meter,number='HIST-E' if meter==98 else 'HIST-W'))
        self.session.execute(text("INSERT INTO bil_municipal_statement(account_id,period,statement_date,arrears,balance,due,due_date) VALUES(1,'2025-09','2025-09-30',1,2,3,'2025-10-10')"))
        self.session.commit();self.conn.commit()
    def edit(self,meter=98,day='2025-09-30',**changes):
        record=self.session.execute(text('SELECT id FROM bil_meter_reading WHERE meter_id=:m AND reading_date=CAST(:d AS date)'),dict(m=meter,d=day)).scalar()
        data=self.capture_form(self.readings_url+'?edit='+str(record));data.pop('csrf_token');data.update(changes)
        return record,data
    def state(self):
        return self.snapshot(),list(self.session.execute(text('SELECT * FROM bil_municipal_statement ORDER BY id'))),list(self.session.execute(text('SELECT * FROM bil_utilities_capture_audit ORDER BY id')))

    def test_consumed_and_superseded_readings_edit_in_place_and_refresh_both_intervals(self):
        for meter in (98,99):
            before=self.state();record,data=self.edit(meter,reading_value='220')
            self.assertEqual(self.capture_post(self.readings_url,**data).status_code,302)
            self.assertEqual(self.session.execute(text('SELECT reading_value FROM bil_meter_reading WHERE id=:r'),dict(r=record)).scalar(),220)
            self.assertEqual(len(before[0]['bil_meter_reading']),len(self.snapshot()['bil_meter_reading']))
            actual=list(self.session.execute(text("SELECT month,consumption,last_read,new_read FROM bil_consumption WHERE meter_id=:m ORDER BY month"),dict(m=meter)))
            self.assertEqual(actual,[('2025-09',120,100,220),('2025-10',80,220,300),('2025-11',777,0,0)])
            event=self.session.execute(text("SELECT before_data,after_data FROM bil_utilities_capture_audit WHERE kind='reading' AND record_id=:r"),dict(r=record)).one()
            self.assertEqual(event[0]['reading_value'],200);self.assertEqual(event[1]['reading_value'],'220')
            for table in ('bil_muni_account','bil_meter','bil_architecture_draft'):self.assertEqual(before[0][table],self.snapshot()[table])
            self.assertEqual(before[1],self.state()[1])
            for row in before[0]['bil_consumption']:
                if row['meter_id']!=meter:self.assertIn(row,self.snapshot()['bil_consumption'])
            snapshot=self.state();self.assertEqual(self.capture_post(self.readings_url,**data).status_code,409);self.assertEqual(snapshot,self.state())

    def test_date_correction_rebuilds_days_and_invalidates_only_obsolete_derived_cache(self):
        record,data=self.edit(reading_date='2025-09-20')
        self.assertEqual(self.capture_post(self.readings_url,**data).status_code,302)
        self.assertEqual(list(self.session.execute(text("SELECT month,days FROM bil_consumption WHERE meter_id=98 AND month IN ('2025-09','2025-10') ORDER BY month"))),[('2025-09',20),('2025-10',41)])
        before=self.state();record,data=self.edit(day='2025-09-20',reading_date='2025-10-15',reading_value='220')
        self.assertEqual(self.capture_post(self.readings_url,**data).status_code,302)
        self.assertEqual(self.session.execute(text("SELECT count(*) FROM bil_consumption WHERE meter_id=98 AND month='2025-09'")).scalar(),0)
        self.assertEqual(self.session.execute(text("SELECT consumption FROM bil_consumption WHERE meter_id=98 AND month='2025-10'")).scalar(),80)
        audit=self.session.execute(text("SELECT before_data,after_data FROM bil_utilities_capture_audit WHERE kind='reading_consumption' AND after_data->>'invalidated'='true'")).one()
        self.assertEqual(audit[0]['month'],'2025-09');self.assertTrue(audit[1]['invalidated'])
        self.assertEqual(before[1],self.state()[1])

    def test_stale_derived_cache_uses_applicable_saved_pair_and_opening_can_be_corrected(self):
        # A previously captured intervening reading can make an older derived interval stale.
        self.session.execute(text("UPDATE bil_consumption SET last_date='2025-08-31',last_read=100,days=61,consumption=200 WHERE meter_id=98 AND month='2025-10'"));self.session.commit();self.conn.commit()
        _,data=self.edit(reading_value='220');self.assertEqual(self.capture_post(self.readings_url,**data).status_code,302)
        self.assertEqual(self.session.execute(text("SELECT consumption FROM bil_consumption WHERE meter_id=98 AND month='2025-10'")).scalar(),80)
        _,data=self.edit(day='2025-08-31',reading_value='110');self.assertEqual(self.capture_post(self.readings_url,**data).status_code,302)
        self.assertEqual(self.session.execute(text("SELECT consumption FROM bil_consumption WHERE meter_id=98 AND month='2025-09'")).scalar(),110)

    def test_existing_same_date_readings_can_be_corrected_without_creating_another(self):
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(98,'2025-09-30',210)"));self.session.commit();self.conn.commit()
        record=self.session.execute(text("SELECT id FROM bil_meter_reading WHERE meter_id=98 AND reading_date='2025-09-30' AND reading_value=200")).scalar()
        data=self.capture_form(self.readings_url+'?edit='+str(record));data.pop('csrf_token');data['reading_value']='210'
        count=self.session.execute(text('SELECT count(*) FROM bil_meter_reading')).scalar()
        self.assertEqual(self.capture_post(self.readings_url,**data).status_code,302)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_reading')).scalar(),count)
        self.assertEqual(list(self.session.execute(text("SELECT month,consumption FROM bil_consumption WHERE meter_id=98 AND month IN ('2025-09','2025-10') ORDER BY month"))),[('2025-09',110),('2025-10',90)])
        # An unresolved conflict must not leave the former corrected endpoint cached.
        data=self.capture_form(self.readings_url+'?edit='+str(record));data.pop('csrf_token');data['reading_value']='230'
        self.assertEqual(self.capture_post(self.readings_url,**data).status_code,302)
        self.assertEqual(self.session.execute(text("SELECT count(*) FROM bil_consumption WHERE meter_id=98 AND month IN ('2025-09','2025-10')")).scalar(),0)
        data=self.capture_form(self.readings_url+'?edit='+str(record));data.pop('csrf_token');data['reading_value']='210'
        self.assertEqual(self.capture_post(self.readings_url,**data).status_code,302)
        self.assertEqual(list(self.session.execute(text("SELECT month,consumption FROM bil_consumption WHERE meter_id=98 AND month IN ('2025-09','2025-10') ORDER BY month"))),[('2025-09',110),('2025-10',90)])

    def test_capture_historical_month_is_not_blocked_by_a_saved_consumption_snapshot(self):
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type) VALUES(96,26,'IMPORTED-W','Water')"))
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(96,'2025-01-31',100)"))
        self.session.execute(text("INSERT INTO bil_consumption(meter_id,meter_number,month,last_date,new_date,last_read,new_read,days,consumption) VALUES(96,'IMPORTED-W','2025-02','2025-01-31','2025-02-28',100,200,28,100)"))
        self.session.commit();self.conn.commit();before=self.state()
        response=self.capture_post(self.readings_url,meter_id='96',reading_date='2025-02-28',reading_value='200')
        self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(before[0]['bil_consumption'],self.snapshot()['bil_consumption'])
        self.assertEqual(self.session.execute(text("SELECT count(*) FROM bil_meter_reading WHERE meter_id=96 AND reading_date='2025-02-28'")).scalar(),1)

    def test_validation_permissions_audit_atomicity_and_month_duplicate_warning_remain(self):
        for changes,code in [(dict(reading_value='-1'),400),(dict(reading_value='350'),400),(dict(meter_id='97'),403)]:
            _,data=self.edit(**changes);before=self.state();self.assertEqual(self.capture_post(self.readings_url,**data).status_code,code);self.assertEqual(before,self.state())
        fields=dict(meter_id='98',reading_date='2025-09-20',reading_value='150')
        warning=self.capture_post(self.readings_url,**fields);self.assertEqual(warning.status_code,200);self.assertIn('reading-duplicate-warning',warning.text)
        self.session.execute(text("ALTER TABLE bil_utilities_capture_audit ADD CONSTRAINT fail_reading_audit CHECK(kind<>'reading')"));self.session.commit();self.conn.commit()
        _,data=self.edit(reading_value='220');before=self.state()
        with self.assertLogs(self.app.logger,level='ERROR'):self.assertEqual(self.capture_post(self.readings_url,**data).status_code,503)
        self.assertEqual(before,self.state())


class CorrectedMetsoaTests(metsoa_fixture.MunicipalMetsoaTests):
    def test_corrected_readings_drive_existing_water_electricity_calculations_without_financial_rewrites(self):
        before_financial={table:list(self.session.execute(text('SELECT * FROM '+table+' ORDER BY id'))) for table in ['bil_municipal_statement','bil_muni_payment','bil_statement_payment']}
        with self.app.test_request_context():before=self.metsoa.build(26,'2026-10',recipient_id=11)
        for meter,new in [(3,'49580'),(1,'1')]:
            record=self.session.execute(text('SELECT id FROM bil_meter_reading WHERE meter_id=:m ORDER BY reading_date LIMIT 1'),dict(m=meter)).scalar()
            data=self.capture_form(self.readings_url+'?edit='+str(record));data.pop('csrf_token');data['reading_value']=new
            self.assertEqual(self.capture_post(self.readings_url,**data).status_code,302)
        with self.app.test_request_context():after=self.metsoa.build(26,'2026-10',recipient_id=11)
        self.assertEqual({r['meter_number']:r for r in before['electricity']['rows']}['7150696S']['consumption'],15)
        self.assertEqual({r['meter_number']:r for r in after['electricity']['rows']}['7150696S']['consumption'],12)
        self.assertEqual({r['meter_number']:r for r in after['water']['meters']}['CEL888']['summary']['consumption'],30)
        for table,rows in before_financial.items():self.assertEqual(rows,list(self.session.execute(text('SELECT * FROM '+table+' ORDER BY id'))))


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(cls(name) for cls in [HistoricalReadingEditTests,CorrectedMetsoaTests] for name in cls.__dict__ if name.startswith('test_'))
