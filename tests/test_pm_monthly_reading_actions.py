"""Monthly maximum consumption and confirmed in-place replacement; isolated database."""
import unittest
from datetime import date
from decimal import Decimal
from lxml import html
from sqlalchemy import text
import test_pm_utilities_capture as fixture
from app.program_billing.municipal_metsoa import reading_pair_from_rows

class MonthlyReadingActionsTests(fixture.UtilitiesCaptureTests):
    def setUp(self):
        super().setUp()
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type) VALUES(98,26,'MONTH-E','Electricity'),(99,26,'MONTH-W','Water')"))
        for meter in (98,99):
            self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(:m,'2025-09-30',30000),(:m,'2025-10-05',30550),(:m,'2025-10-20',30572),(:m,'2025-10-25',30600),(:m,'2025-11-30',31000)"),dict(m=meter))
            self.session.execute(text("INSERT INTO bil_consumption(meter_id,meter_number,month,last_date,new_date,last_read,new_read,days,consumption) VALUES(:m,:n,'2025-10','2025-09-30','2025-10-25',30000,30600,25,600),(:m,:n,'2025-11','2025-10-25','2025-11-30',30600,31000,36,400)"),dict(m=meter,n='MONTH-E' if meter==98 else 'MONTH-W'))
        self.session.commit();self.conn.commit()
    def rows(self,meter):
        return self.session.execute(text('SELECT id,reading_date,reading_value FROM bil_meter_reading WHERE meter_id=:m ORDER BY reading_date,id'),dict(m=meter)).mappings().all()
    def pair(self,meter,month=10):
        return reading_pair_from_rows({'meter_number':str(meter)},date(2025,month,1),date(2025,month,30 if month==11 else 31),self.rows(meter))
    def review(self,fields,mode,target=None):
        data=dict(fields,reading_duplicate_action='review_'+mode)
        if target is not None:data['reading_replace_id']=str(target)
        response=self.capture_post(self.readings_url,**data)
        self.assertEqual(response.status_code,200,response.text)
        tree=html.fromstring(response.data)
        data['reading_duplicate_token']=tree.xpath('//input[@name="reading_duplicate_token"]/@value')[0]
        data['reading_duplicate_action']='replace' if mode=='replace' else 'confirm'
        return data
    def test_replace_earlier_later_same_date_both_types_preserves_identity_and_audit(self):
        for meter in (98,99):
            target=next(r['id'] for r in self.rows(meter) if r['reading_value']==30572)
            count=len(self.rows(meter))
            for day,value in [('2025-10-01','30700'),('2025-10-31','30710'),('2025-10-31','30720')]:
                fields=dict(meter_id=str(meter),reading_date=day,reading_value=value)
                warning=self.capture_post(self.readings_url,**fields)
                self.assertEqual(warning.status_code,200)
                for row in self.rows(meter):
                    if row['reading_date'].month==10:self.assertIn('Reading #'+str(row['id']),warning.text)
                data=self.review(fields,'replace',target)
                self.assertEqual(len(self.rows(meter)),count)
                response=self.capture_post(self.readings_url,**data)
                self.assertEqual(response.status_code,302,response.text)
                self.assertEqual(len(self.rows(meter)),count)
                saved=next(r for r in self.rows(meter) if r['id']==target)
                self.assertEqual(str(saved['reading_date']),day);self.assertEqual(saved['reading_value'],Decimal(value))
                actual=list(self.session.execute(text("SELECT month,consumption FROM bil_consumption WHERE meter_id=:m ORDER BY month"),dict(m=meter)))
                self.assertEqual(actual,[('2025-10',Decimal(value)-30000),('2025-11',31000-Decimal(value))])
                event=self.session.execute(text("SELECT before_data,after_data FROM bil_utilities_capture_audit WHERE kind='reading' AND record_id=:r ORDER BY id DESC LIMIT 1"),dict(r=target)).one()
                self.assertTrue(event[1]['replacement_confirmed']);self.assertIsNotNone(event[0])
                tree=html.fromstring(self.client.get(response.location,headers=self.headers).data)
                self.assertEqual(tree.xpath('//input[@name="reading_date"]/@value'),[day])
                self.assertEqual(tree.xpath('//input[@name="reading_value"]/@value'),[''])
                self.assertFalse(tree.xpath('//select[@name="meter_id"]/option[@selected and @value!=""]'))
    def test_additional_uses_maximum_not_latest_date_and_preserves_originals(self):
        for meter in (98,99):
            original=list(self.rows(meter))
            self.assertEqual(self.pair(meter)['difference'],600)
            for day,value,expected in [('2025-10-30','30590',600),('2025-10-02','30800',800)]:
                fields=dict(meter_id=str(meter),reading_date=day,reading_value=value)
                data=self.review(fields,'additional')
                self.assertEqual(self.capture_post(self.readings_url,**data).status_code,302)
                self.assertEqual(self.pair(meter)['difference'],expected)
                self.assertEqual(self.pair(meter,11)['difference'],1000-expected)
                for row in original:self.assertIn(row,self.rows(meter))
    def test_replacement_confirmation_binds_target_and_exact_payload(self):
        fields=dict(meter_id='98',reading_date='2025-10-12',reading_value='30650')
        targets=[r['id'] for r in self.rows(98) if r['reading_date'].month==10]
        warning=self.capture_post(self.readings_url,**fields)
        initial=html.fromstring(warning.data).xpath('//input[@name="reading_duplicate_token"]/@value')[0]
        before=self.snapshot()
        unconfirmed=dict(fields,reading_duplicate_action='replace',reading_replace_id='999999',reading_duplicate_token=initial)
        self.assertEqual(self.capture_post(self.readings_url,**unconfirmed).status_code,200)
        self.assertEqual(before,self.snapshot())
        data=self.review(fields,'replace',targets[0]);before=self.snapshot()
        for changes in [dict(reading_replace_id=str(targets[1])),dict(reading_date='2025-10-13'),dict(reading_value='30660'),dict(meter_id='99')]:
            changed=dict(data);changed.update(changes)
            response=self.capture_post(self.readings_url,**changed)
            self.assertEqual(response.status_code,200,response.text);self.assertEqual(before,self.snapshot())
        cancel=dict(data,reading_duplicate_action='cancel')
        response=self.capture_post(self.readings_url,**cancel)
        self.assertEqual(response.status_code,200);self.assertEqual(before,self.snapshot())
        tree=html.fromstring(response.data)
        for key in ('reading_date','reading_value'):self.assertEqual(tree.xpath('//input[@name="'+key+'"]/@value'),[fields[key]])
    def test_previous_month_maximum_and_year_boundary(self):
        rows=[dict(id=1,reading_date=date(2024,12,10),reading_value=30000),dict(id=2,reading_date=date(2024,12,31),reading_value=29990),dict(id=3,reading_date=date(2025,1,10),reading_value=30600),dict(id=4,reading_date=date(2025,1,31),reading_value=30500),dict(id=5,reading_date=date(2025,1,10),reading_value=30610)]
        result=reading_pair_from_rows({'meter_number':'test'},date(2025,1,1),date(2025,1,31),rows)
        self.assertIsNone(result['error']);self.assertEqual(result['difference'],610)
    def test_missing_previous_month_and_decrease_require_review(self):
        rows=[dict(id=1,reading_date=date(2025,8,31),reading_value=100),dict(id=2,reading_date=date(2025,10,31),reading_value=200)]
        result=reading_pair_from_rows({'meter_number':'test'},date(2025,10,1),date(2025,10,31),rows)
        self.assertIn('previous month',result['error']);self.assertIsNone(result['difference'])
        before=self.snapshot()
        fields=dict(meter_id='98',reading_date='2025-10-20',reading_value='32000')
        target=next(r['id'] for r in self.rows(98) if r['reading_value']==30572)
        data=self.review(fields,'replace',target)
        response=self.capture_post(self.readings_url,**data)
        self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(len(before['bil_meter_reading']),len(self.snapshot()['bil_meter_reading']))
        self.assertIn('Monthly consumption is unavailable',self.client.get(response.location,headers=self.headers).text)
        self.assertIsNone(self.pair(98,11)['difference'])

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(MonthlyReadingActionsTests(name) for name in MonthlyReadingActionsTests.__dict__ if name.startswith('test_'))
