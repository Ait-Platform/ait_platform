"""Existing reading UI and property-only posting; temporary PostgreSQL fixtures."""
import sys,types
from unittest.mock import patch
from flask import request,abort,render_template,redirect,url_for,flash,current_app
from sqlalchemy import text,Table,MetaData
from sqlalchemy.orm import registry
from flask_sqlalchemy.query import Query
from lxml import html
import test_pm_utilities_capture as fixture
import test_pm_lease_lessee_persistence as loader

class UtilitiesReadingsTests(fixture.UtilitiesCaptureTests):
    def setUp(self):
        super().setUp()
        models=sys.modules['app.models.billing'];mapper=registry()
        class Consumption:pass
        record=Table('bil_consumption',MetaData(),autoload_with=self.conn)
        mapper.map_imperatively(Consumption,record,primary_key=[record.c.id]);Consumption.query=Query(Consumption,session=self.session)
        models.BilConsumption=Consumption;sys.modules['app.models'].BilConsumption=Consumption
        from app.program_billing import utilities_readings
        db_patch=patch.object(utilities_readings,'db',self.db);db_patch.start();self.addCleanup(db_patch.stop)
        def auth(f):
            def wrapped(*args,**kwargs):
                if request.headers.get('X-Test-Owner')!='yes':abort(403)
                return f(*args,**kwargs)
            return wrapped
        env=dict(__package__='app.program_billing',billing_bp=types.SimpleNamespace(route=lambda *a,**kw:lambda f:f),login_required=auth,request=request,abort=abort,render_template=render_template,redirect=redirect,url_for=url_for,flash=flash,current_app=current_app,db=self.db)
        loader.functions('app/program_billing/routes.py',{'property_hub','save_reading'},env)
        self.app._got_first_request=False
        self.app.view_functions['billing_bp.property_hub']=env['property_hub']
        self.app.add_url_rule('/readings/<int:property_id>',endpoint='billing_bp.property_hub',view_func=env['property_hub'],methods=['POST'])
        self.app.add_url_rule('/api/save_reading',endpoint='billing_bp.save_reading',view_func=env['save_reading'],methods=['POST'])
    def reading(self,**kwargs):
        data=dict(meter_id=1,property_id=26,reading_month='2026-09',date='2026-09-30',reading='150',prev_reading='123',prev_date='2026-08-31');data.update(kwargs)
        return self.client.post('/api/save_reading',json=data,headers=dict(self.headers,**{'X-CSRFToken':self.csrf}))
    def test_readings_page_no_tenants_property_month_and_type_groups(self):
        page=self.client.get('/readings/26?billing_month=2026-11',headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data)
        self.assertTrue(tree.xpath('//h3[text()="Water"]'));self.assertTrue(tree.xpath('//h3[text()="Electricity"]'))
        self.assertIn('WATER-26',page.text);self.assertNotIn('No tenants',page.text)
        self.assertEqual(tree.xpath('//input[@type="month" and not(@id="metsoa-billing-month")]/@value'),['2026-11'])
        self.assertEqual(self.client.get('/readings/28',headers=self.headers).status_code,403)
    def test_legacy_capture_and_edit_reuse_audited_service(self):
        from app.program_billing.utilities_capture import fingerprint
        before=self.snapshot();response=self.reading();self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(self.reading().status_code,200)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_reading')).scalar(),2)
        saved=self.session.execute(text('SELECT * FROM bil_meter_reading WHERE id>1')).mappings().one()
        response=self.reading(record_id=saved['id'],version=fingerprint(saved),reading='100')
        self.assertEqual(response.status_code,200,response.text)
        self.assertTrue(response.json['warnings'])
        self.assertEqual(self.session.execute(text('SELECT reading_value FROM bil_meter_reading WHERE id=:r'),dict(r=saved['id'])).scalar(),100)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_reading')).scalar(),2)
        self.assertEqual(self.session.execute(text("SELECT count(*) FROM bil_utilities_capture_audit WHERE kind='reading'")).scalar(),2)
        self.assertEqual(before['bil_consumption'],self.snapshot()['bil_consumption'])

    def test_legacy_duplicate_warning_and_confirm_additional(self):
        self.assertEqual(self.reading().status_code,200)
        self.conn.commit()
        response=self.reading(reading='151');self.assertEqual(response.status_code,409)
        self.assertTrue(response.json['confirmation_required'])
        response=self.reading(reading='151',reading_duplicate_action='confirm',reading_duplicate_token=response.json['reading_duplicate_token'])
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_reading')).scalar(),3)

    def test_reading_security_and_invalid_values(self):
        before=self.snapshot()
        self.assertEqual(self.reading(property_id=27).status_code,403)
        for value in ('NaN','-1','','not-a-number'):
            self.assertEqual(self.reading(reading=value).status_code,400)
        self.assertEqual(self.reading(date='2999-01-01').status_code,400)
        self.assertEqual(before,self.snapshot())


def load_tests(loader,tests,pattern):
    import unittest
    return unittest.TestSuite(UtilitiesReadingsTests(n) for n in UtilitiesReadingsTests.__dict__ if n.startswith('test_'))
