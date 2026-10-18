"""Disposable PostgreSQL draft fixtures and authenticated R2 API doubles."""
import importlib.util,sys,types,uuid,json,io,os,unittest
from unittest.mock import patch
from alembic.operations import Operations
from alembic.migration import MigrationContext
from reportlab.pdfgen import canvas
from botocore.exceptions import ClientError
import test_pm_lease_foundation as foundation
import test_pm_property_management as properties

class LeaseSnapshotTests(unittest.TestCase):
    load_lease_workspace=True
    sign_in=foundation.LeaseFoundationTests.sign_in
    sql=foundation.LeaseFoundationTests.sql
    payload=foundation.LeaseFoundationTests.payload
    assign=foundation.LeaseFoundationTests.assign
    def setUp(self):
        foundation.LeaseFoundationTests.setUp(self)
        spec=importlib.util.spec_from_file_location('proposed_pm_lease_001',properties.ROOT/'docs/proposed_migrations/pm_lease_001.py')
        migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        with self.conn.begin():
            migration.op=Operations(MigrationContext.configure(self.conn));migration.upgrade()
            self.conn.exec_driver_sql("INSERT INTO alembic_version VALUES('pm_lease_001')")
        self.app.config.update(PM_LEASE_DRAFT_WRITES_APPROVED=True,PM_LEASE_PARTICULARS_WRITES_APPROVED=True,PM_LEASE_PRIVATE_STORAGE_APPROVED=True)
        utils=types.ModuleType('app.utils');utils.__path__=[]
        sys.modules['app.utils']=utils
        for name,path in [('app.utils.cloudflare_r2','app/utils/cloudflare_r2.py'),('app.program_billing.lease_r2','app/program_billing/lease_r2.py'),('app.program_billing.lease_storage','app/program_billing/lease_storage.py')]:
            spec=importlib.util.spec_from_file_location(name,properties.ROOT/path)
            module=importlib.util.module_from_spec(spec);sys.modules[name]=module;spec.loader.exec_module(module)
            self.addCleanup(sys.modules.pop,name,None)
            if name.endswith('cloudflare_r2'):utils.cloudflare_r2=module
            elif name.endswith('lease_r2'):self.r2=module
            else:self.storage=module
        environment=patch.dict(os.environ,{'R2_BUCKET_NAME':'synthetic-general','SACE_HOME_R2_BUCKET':'synthetic-home','PM_LEASE_R2_BUCKET':'ait-lease-controlled-private','PM_LEASE_R2_ENDPOINT_URL':'https://synthetic.r2.cloudflarestorage.com','PM_LEASE_R2_ACCESS_KEY':'synthetic-lease-access','PM_LEASE_R2_SECRET_KEY':'synthetic-lease-secret'})
        environment.start();self.addCleanup(environment.stop)
        self.objects={};self.calls=[];self.deny=False
        test=self
        class S3:
            def put_object(self,**values):
                test.calls.append(('put',values['Bucket'],values['Key']))
                if test.deny:raise ClientError({'Error':{'Code':'AccessDenied'}},'PutObject')
                if values['Key'] in test.objects:raise ClientError({'Error':{'Code':'PreconditionFailed'}},'PutObject')
                test.assertEqual(values['IfNoneMatch'],'*');test.objects[values['Key']]=values['Body'];return {}
            def get_object(self,**values):
                test.calls.append(('get',values['Bucket'],values['Key']))
                if test.deny:raise ClientError({'Error':{'Code':'AccessDenied'}},'GetObject')
                return {'Body':io.BytesIO(test.objects[values['Key']])}
        client=patch.object(self.r2,'r2_client',side_effect=lambda bucket=None:(S3(),bucket))
        client.start();self.addCleanup(client.stop)
        self.draft=self.create()

    def create(self):
        token=str(uuid.uuid4())
        response=self.client.post('/billing/leases/property/26/drafts',data={'registration_token':token})
        self.assertEqual(response.status_code,302)
        self.client.post('/billing/leases/property/26/drafts',data={'registration_token':token})
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),1)
        return self.sql('SELECT id FROM bil_lease_draft').scalar()

    def item(self,label):
        return next(i for i in self.snapshot_module.CATALOG['items'] if i['label']==label)

    def save(self,item,value='Captured value',**extra):
        version=self.sql('SELECT version FROM bil_lease_draft WHERE id=\''+str(self.draft)+'\'').scalar()
        return self.client.post(f"/billing/leases/property/26/snapshot/{item['key']}",
            data=dict(draft_id=str(self.draft),version=version,value=value,**extra))

    def pdf(self):
        stream=io.BytesIO();c=canvas.Canvas(stream,invariant=1);c.drawString(40,700,'Synthetic private lease');c.save();return stream.getvalue()

    def upload(self,data=None,name='lease.pdf'):
        return self.client.post('/billing/leases/property/26/documents',
            data={'draft_id':str(self.draft),'item_key':'lease-document','document':(io.BytesIO(self.pdf() if data is None else data),name)})

    def test_snapshot_navigation_persistence_and_no_legal_approval(self):
        item=self.item('Landlord legal name(s)')
        response=self.save(item,'Private landlord particulars')
        self.assertEqual(response.status_code,302)
        html=self.client.get(response.location).get_data(as_text=True)
        self.assertIn('2. Lease Particulars',html);self.assertNotIn('Private landlord particulars',html)
        self.assertIn('Data completeness is separate from legal approval',html)
        page=self.client.get(f"/billing/leases/property/26/snapshot/{item['key']}?draft_id={self.draft}").get_data(as_text=True)
        self.assertNotIn('aria-label="Property Management"',page)
        self.assertIn('Private landlord particulars',page)
        self.assertIn('[LEGAL LANDLORD(S) TO VERIFY]',page)
        with self.client.session_transaction() as session:self.assertNotIn('Private landlord particulars',str(dict(session)))
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property WHERE id=26').scalar(),1)
        self.assertEqual(self.sql('SELECT status FROM bil_lease_draft').scalar(),'draft')

    def test_only_conditional_na_and_optimistic_conflict(self):
        essential=self.item('Tenant legal identity')
        self.assertEqual(self.save(essential,'',not_applicable='on',reason='Not applicable to us').status_code,400)
        conditional=self.item('Key deposit')
        self.assertEqual(self.save(conditional,'',not_applicable='on',reason='No').status_code,400)
        self.assertEqual(self.save(conditional,'',not_applicable='on',reason='No separate key deposit is proposed.').status_code,302)
        optional=self.snapshot_module.ITEMS['sc-10']
        self.assertEqual(self.save(optional,'',not_applicable='on',reason='No extra conditions').status_code,400)
        response=self.client.post(f"/billing/leases/property/26/snapshot/{essential['key']}",
            data={'draft_id':str(self.draft),'version':'1','value':'Must not overwrite'})
        self.assertEqual(response.status_code,409)
        self.assertNotIn('Must not overwrite',json.dumps(self.sql('SELECT items FROM bil_lease_draft').scalar()))

    def test_upload_download_view_permissions_and_metadata(self):
        response=self.upload();self.assertEqual(response.status_code,302)
        row=self.sql('SELECT * FROM bil_lease_document').mappings().one()
        self.assertEqual(row['state'],'available')
        self.assertIn(f"property-26/draft-{self.draft}",row['storage_key'])
        self.assertEqual(set(call[1] for call in self.calls),{'ait-lease-controlled-private'})
        url=f"/billing/leases/documents/{row['id']}"
        downloaded=self.client.get(url);self.assertEqual(downloaded.data,self.pdf())
        self.assertIn('attachment',downloaded.headers['Content-Disposition'])
        self.assertIn('inline',self.client.get(url+'?view=1').headers['Content-Disposition'])
        html=self.client.get(f'/billing/leases/property/26/documents?draft_id={self.draft}').get_data(as_text=True)
        self.assertNotIn(row['storage_key'],html);self.assertNotIn('r2.dev',html)
        count=len(self.calls);self.sign_in(8)
        self.assertEqual(self.client.get(url).status_code,404)
        self.assertEqual(self.client.get(f'/billing/leases/property/27/documents?draft_id={self.draft}').status_code,404)
        self.assertEqual(self.upload().status_code,404)
        self.assertEqual(len(self.calls),count)

    def test_upload_validation_private_bucket_and_failure(self):
        for payload,name in [(b'not pdf','lease.pdf'),(b'bad','lease.exe'),(b'not zip','lease.docx'),(b'x'*(10*1024*1024+1),'big.pdf')]:
            self.assertEqual(self.upload(payload,name).status_code,400)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_document').scalar(),0)
        self.assertFalse(self.calls)
        self.app.config['PM_LEASE_PRIVATE_STORAGE_APPROVED']=False
        self.assertEqual(self.upload().status_code,503);self.assertFalse(self.calls)
        self.app.config['PM_LEASE_PRIVATE_STORAGE_APPROVED']=True
        with patch.dict(os.environ,{'SACE_HOME_R2_BUCKET':'ait-lease-controlled-private'}):
            self.assertEqual(self.upload().status_code,503);self.assertFalse(self.calls)
        self.deny=True
        self.assertEqual(self.upload().status_code,503)
        record=self.sql('SELECT id,state FROM bil_lease_document').one()
        self.assertEqual(record.state,'pending')
        self.assertEqual(self.client.get(f'/billing/leases/documents/{record.id}').status_code,404)

    def test_docx_validation_download_and_failed_final_commit(self):
        from zipfile import ZipFile
        document=io.BytesIO()
        with ZipFile(document,'w') as archive:
            archive.writestr('[Content_Types].xml','<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/></Types>')
            archive.writestr('word/document.xml','<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p/></w:body></w:document>')
        payload=document.getvalue()
        self.assertEqual(self.upload(payload,'lease.docx').status_code,302)
        record=self.sql('SELECT id FROM bil_lease_document').scalar()
        response=self.client.get(f'/billing/leases/documents/{record}?view=1')
        self.assertEqual(response.data,payload)
        self.assertIn('attachment',response.headers['Content-Disposition'])
        original=self.session.commit;commits=[]
        def fail_final():
            commits.append(1)
            if len(commits)==2:raise __import__('sqlalchemy').exc.SQLAlchemyError('synthetic final commit failure')
            return original()
        with patch.object(self.session,'commit',side_effect=fail_final):
            self.assertEqual(self.upload().status_code,503)
        states=self.sql('SELECT state FROM bil_lease_document ORDER BY created_at').scalars().all()
        self.assertEqual(states,['available','pending'])
        pending=self.sql("SELECT id FROM bil_lease_document WHERE state='pending'").scalar()
        self.assertEqual(self.client.get(f'/billing/leases/documents/{pending}').status_code,404)

    def test_corrupt_object_and_manager_reassignment_fail_closed(self):
        self.assertEqual(self.upload().status_code,302)
        record=self.sql('SELECT id,storage_key FROM bil_lease_document').one()
        self.objects[record.storage_key]=b'corrupt bytes'
        self.assertEqual(self.client.get(f'/billing/leases/documents/{record.id}').status_code,503)
        self.sql('UPDATE bil_property SET manager_id=8 WHERE id=26');self.session.commit()
        count=len(self.calls)
        self.assertEqual(self.client.get(f'/billing/leases/documents/{record.id}').status_code,404)
        self.assertEqual(len(self.calls),count)

    def test_catalog_date_validation_and_prepared_migration_safety(self):
        self.assertEqual(len(self.snapshot_module.CATALOG['items']),60)
        self.assertEqual(len(self.snapshot_module.ITEMS),60)
        self.assertTrue(self.snapshot_module.ITEMS['sc-01']['placeholders'])
        self.assertEqual(self.save(self.item('Commencement date'),'2026-10-15').status_code,302)
        self.assertEqual(self.save(self.item('Expiry date'),'2026-09-01').status_code,400)
        self.assertEqual(self.save(self.snapshot_module.ITEMS['sc-10'],'').status_code,302)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease').scalar(),2)
        self.assertEqual(self.sql('SELECT name FROM bil_property WHERE id=26').scalar(),'Almond')
        self.assertTrue((properties.ROOT/'migrations/versions/pm_lease_001.py').exists())  # Prepared only; never applied automatically.
        self.sign_in(8)
        self.assertEqual(self.client.get(f'/billing/leases/property/26/snapshot/s1-01?draft_id={self.draft}').status_code,404)

    def test_default_off_schema_gate_and_no_storage_calls(self):
        self.app.config['PM_LEASE_DRAFT_WRITES_APPROVED']=False
        self.assertEqual(self.upload().status_code,503)
        item=self.item('Agreement reference')
        self.assertEqual(self.save(item).status_code,503)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),{})
        self.assertFalse(self.calls)

if __name__=='__main__':unittest.main()
