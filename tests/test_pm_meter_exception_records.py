"""Incident and private-evidence persistence on isolated PostgreSQL."""
import unittest,uuid,importlib.util
from pathlib import Path
from io import BytesIO
from unittest.mock import patch
from sqlalchemy import text,inspect
from alembic.operations import Operations
from alembic.migration import MigrationContext
import test_pm_meter_exceptions as fixture
class ExceptionRecordTests(fixture.MeterExceptionTests):
    def setUp(self):
        super().setUp()
        mock=patch.object(self.exceptions,'current_user',self.user);mock.start();self.addCleanup(mock.stop)
        for table in ('bil_property','bil_meter'):
            if not inspect(self.conn).get_pk_constraint(table)['constrained_columns']:self.session.execute(text('ALTER TABLE '+table+' ADD PRIMARY KEY(id)'))
        self.session.commit();self.conn.commit()
        before=self.snapshot();self.session.execute(text('SET search_path TO pg_temp,public'))
        spec=importlib.util.spec_from_file_location('meter_exception_migration',Path('migrations/versions/pm_meter_exceptions_001.py'));migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        with patch.object(migration,'op',Operations(MigrationContext.configure(self.conn))):migration.upgrade()
        self.session.commit();self.conn.commit();self.assertEqual(before,self.snapshot())
        self.assertEqual(self.session.execute(text("SELECT relpersistence FROM pg_class WHERE oid='bil_meter_exception'::regclass")).scalar(),'t')
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_bill_number,status) VALUES(92,26,'REPLACEMENT-W','Water','MET-26','active'),(93,26,'ACCOUNT-E','Electrical','MET-26','active'),(94,26,'ANOTHER-W','Water','MET-26','active')"))
        self.session.commit();self.conn.commit()
    def fields(self,**changes):
        data=dict(action='create',account_id='1',meter_id='1',exception_type='Stolen',incident_date='2026-09-01',notification_date='2026-09-02',municipal_reference='MUNI-123',description='Meter incident',remarks='Original meter preserved',replacement_meter_id='',replacement_date='',submission_key=str(uuid.uuid4()))
        data.update(changes);return data
    def post_exception(self,data):return self.capture_post(self.exceptions_url,**data)
    def last_id(self):return self.session.execute(text('SELECT max(id) FROM bil_meter_exception')).scalar()
    def png(self):
        from PIL import Image
        file=BytesIO();Image.new('RGB',(5,5),'blue').save(file,format='PNG');return file.getvalue()
    def test_stolen_without_replacement_and_replaced_requires_both(self):
        from lxml import html
        page=self.client.get(self.exceptions_url+'?account_id=1&view=register',headers=self.headers)
        form=html.fromstring(page.data).xpath('//form[.//input[@value="create"]]')[0]
        self.assertFalse(form.xpath('.//*[@name="replacement_meter_id" or @name="replacement_date"]/@required'))
        fields=self.fields(replacement_meter_id='',replacement_date='')
        result=self.post_exception(fields);self.assertEqual(result.status_code,302,result.text)
        row=self.session.execute(text('SELECT * FROM bil_meter_exception WHERE id=:e'),dict(e=self.last_id())).mappings().one()
        self.assertIsNone(row['replacement_meter_id']);self.assertIsNone(row['replacement_date'])
        for changes in ({},{'replacement_meter_id':'92'},{'replacement_date':'2026-09-03'}):
            result=self.post_exception(self.fields(exception_type='Replaced',**changes));self.assertEqual(result.status_code,400,result.text)
        self.assertEqual(self.post_exception(self.fields(exception_type='Replaced',replacement_meter_id='92',replacement_date='2026-09-03')).status_code,302)

    def test_multiple_types_idempotency_and_original_records_unchanged(self):
        before=self.snapshot()
        for kind in self.exceptions.TYPES:
            fields=self.fields(exception_type=kind,**(dict(replacement_meter_id='92',replacement_date='2026-09-03') if kind=='Replaced' else {}))
            self.assertEqual(self.post_exception(fields).status_code,302)
            self.assertEqual(self.post_exception(fields).status_code,302)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_exception')).scalar(),6)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_exception_event')).scalar(),6)
        self.assertEqual(before,self.snapshot())
        self.assertEqual(self.post_exception(dict(fields,description='Changed replay')).status_code,409)
    def test_invalid_account_meter_and_replacement_relationships(self):
        for changes in [dict(meter_id='90'),dict(meter_id='91'),dict(replacement_meter_id='1',replacement_date='2026-09-03'),dict(replacement_meter_id='90',replacement_date='2026-09-03'),dict(replacement_meter_id='93',replacement_date='2026-09-03'),dict(exception_type='Replaced'),dict(notification_date='2026-08-31')]:
            result=self.post_exception(self.fields(**changes));self.assertEqual(result.status_code,400,result.text)
        self.assertEqual(self.post_exception(self.fields(account_id='3',meter_id='91')).status_code,403)
        self.assertEqual(self.post_exception(self.fields(replacement_meter_id='92',replacement_date='2026-09-03')).status_code,302)
        cycle=self.post_exception(self.fields(meter_id='92',replacement_meter_id='1',replacement_date='2026-09-03'))
        self.assertEqual(cycle.status_code,400);self.assertIn('circular',cycle.text)
        other=self.post_exception(self.fields(meter_id='94',replacement_meter_id='92',replacement_date='2026-09-03'))
        self.assertEqual(other.status_code,409)
    def test_private_evidence_upload_authorised_retrieval_integrity_and_dedup(self):
        from app.program_billing import meter_exception_evidence as storage
        data=self.png();objects={}
        def upload(key,content,**kw):objects[key]=content;return key
        with patch.object(storage.transport,'upload_bytes_to_r2',side_effect=upload),patch.object(storage.transport,'read_file_from_r2',side_effect=lambda key:objects[key]):
            fields=self.fields();result=self.post_exception(dict(fields,evidence=(BytesIO(data),'photo.png')))
            self.assertEqual(result.status_code,302,result.text);eid=self.last_id()
            row=self.session.execute(text('SELECT * FROM bil_meter_exception_evidence')).mappings().one()
            self.assertTrue(row['storage_key'].startswith('private/pm-utilities/property-26/account-1/exception-'))
            detail=self.client.get(result.location,headers=self.headers);self.assertNotIn(row['storage_key'],detail.text)
            url='/meter_exceptions/26/evidence/'+str(row['id'])
            response=self.client.get(url,headers=self.headers);self.assertEqual(response.status_code,200);self.assertEqual(response.data,data);self.assertIn('no-store',response.headers['Cache-Control'])
            self.assertEqual(self.client.get(url).status_code,403);self.assertEqual(self.client.get(url.replace('/26/','/28/'),headers=self.headers).status_code,403)
            repeat=self.post_exception(dict(action='evidence',account_id='1',exception_id=str(eid),evidence=(BytesIO(data),'same.png')))
            self.assertEqual(repeat.status_code,302);self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_exception_evidence')).scalar(),1)
            objects[row['storage_key']]=b'corrupt'
            with self.assertLogs(self.app.logger,level='ERROR'):bad=self.client.get(url,headers=self.headers)
            self.assertEqual(bad.status_code,503)
    def test_invalid_files_and_storage_failure_roll_back(self):
        from app.program_billing import meter_exception_evidence as storage
        before=self.snapshot()
        for name,content in [('bad.exe',b'data'),('fake.png',b'not an image'),('large.png',b'x'*(storage.MAX_BYTES+1))]:
            result=self.post_exception(dict(self.fields(),evidence=(BytesIO(content),name)))
            self.assertEqual(result.status_code,400,result.text)
        with patch.object(storage.transport,'upload_bytes_to_r2',side_effect=RuntimeError('test storage failure')):
            with self.assertLogs(self.app.logger,level='ERROR'):failed=self.post_exception(dict(self.fields(),evidence=(BytesIO(self.png()),'photo.png')))
        self.assertEqual(failed.status_code,503)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_exception')).scalar(),0)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_exception_event')).scalar(),0)
        self.assertEqual(before,self.snapshot())
    def test_schema_guards_foreign_keys_and_csrf(self):
        with patch.object(self.exceptions,'ready',return_value=False):
            self.assertEqual(self.post_exception(self.fields()).status_code,503)
        self.assertEqual(self.client.post(self.exceptions_url,data=self.fields(),headers=self.headers).status_code,400)
        from sqlalchemy.exc import IntegrityError
        with self.assertRaises(IntegrityError):
            with self.session.begin_nested():self.session.execute(text("INSERT INTO bil_meter_exception(property_id,account_id,meter_id,exception_type,incident_date,description,submission_key,created_by) VALUES(26,9999,1,'Stolen','2026-09-01','Invalid account',:key,7)"),dict(key=uuid.uuid4()))

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ExceptionRecordTests(name) for name in ExceptionRecordTests.__dict__ if name.startswith('test_'))
