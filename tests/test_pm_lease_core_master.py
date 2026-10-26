"""Core master administration, immutable private versions and draft pins; isolated PostgreSQL/R2."""
import copy,io,json,sys,types,uuid,unittest
from unittest.mock import patch
from flask import request,abort,current_app,redirect,url_for,flash,send_file,render_template
from sqlalchemy import text,func,cast,String,column
from lxml import html
import test_pm_lease_addendum_pdf_search as fixture
from test_pm_lease_lessee_persistence import functions

class CoreMasterTests(unittest.TestCase):
    setUp_base=fixture.FullLeaseSelectionTests.setUp
    stored=fixture.FullLeaseSelectionTests.stored
    payload=fixture.FullLeaseSelectionTests.payload
    sql=fixture.FullLeaseSelectionTests.sql
    signed=fixture.FullLeaseSelectionTests.signed
    pdf=fixture.FullLeaseSelectionTests.pdf
    register_url=fixture.FullLeaseSelectionTests.register_url
    def setUp(self):
        self.setUp_base();self.core_registry.stop()
        self.session.execute(text("ALTER TABLE bil_lease_draft ALTER COLUMN status SET DEFAULT 'draft', ALTER COLUMN version SET DEFAULT 1"));self.session.commit()
        from app.program_billing import lease_core_master as core,lease_storage as storage
        self.master=core;self.storage=storage;self.allowed=True
        self.user.is_authenticated=True;self.user.is_active=True;self.user.email='synthetic-admin@example.test';self.user.has_role=lambda role:True
        class Query:
            def __init__(q,value):q.value=value
            def filter_by(q,**kw):return q
            def filter(q,*args):return q
            def populate_existing(q):return q
            def with_for_update(q):return q
            def first(q):return q.value()
        auth=types.ModuleType('app.models.auth');auth.User=type('User',(),{});auth.ApprovedAdmin=types.SimpleNamespace(email=column('email'),active=column('active'),query=Query(lambda:object() if self.allowed else None))
        authpatch=patch.dict(sys.modules,{'app.models.auth':auth});authpatch.start();self.addCleanup(authpatch.stop)
        proxy=types.SimpleNamespace(execute=self.session.execute,commit=self.session.commit,rollback=self.session.rollback,query=lambda model:Query(lambda:self.user))
        db=types.SimpleNamespace(session=proxy,func=func,cast=cast,String=String)
        guard=patch.object(core,'db',db);guard.start();self.addCleanup(guard.stop)
        listing=patch.object(core,'list_keys',side_effect=lambda prefix:[key for key in self.objects if key.startswith(prefix)]);listing.start();self.addCleanup(listing.stop)
        def immutable(key,data,**kw):
            if key in self.objects and self.objects[key]!=data:raise ValueError('Immutable key collision')
            self.objects[key]=data;return key
        self.r2.upload_bytes_to_r2.side_effect=immutable
        env=dict(__name__='app.program_billing.lease_core_admin',__package__='app.program_billing',billing_bp=types.SimpleNamespace(route=lambda *a,**kw:lambda f:f,get=lambda *a,**kw:lambda f:f,after_request=lambda f:f),require_admin=core.require_admin,publications=core.publications,working_copies=core.working_copies,prepare=core.prepare,publish=core.publish,update_draft=core.update_draft,read_object=core.read_object,DOCX=core.DOCX,storage=storage,db=db,BytesIO=io.BytesIO,abort=abort,request=request,current_app=current_app,send_file=send_file,redirect=redirect,url_for=url_for,flash=flash,render_pm_template=render_template)
        functions('app/program_billing/lease_core_admin.py',{'admin','file','private'},env);self.app._got_first_request=False
        self.app.add_url_rule('/billing/admin/core-lease-master',endpoint='billing_bp.lease_core_master',view_func=env['admin'],methods=['GET','POST'])
        self.app.add_url_rule('/billing/admin/core-lease-master/<int:version>/<kind>',endpoint='billing_bp.lease_core_master_file',view_func=env['file'])
        self.app.add_url_rule('/billing/dashboard/admin',endpoint='billing_bp.admin_dashboard',view_func=lambda:'Admin')
        self.app.after_request(env['private']);self.url='/billing/admin/core-lease-master'
    def word(self):
        from app.program_billing.lease_sc_pdf import converted_word
        return converted_word(self.pdf())
    def publish(self):
        with self.app.test_request_context('/'):
            from werkzeug.datastructures import FileStorage
            work=self.master.prepare(FileStorage(stream=io.BytesIO(self.word()),filename='Working.docx'))
            return self.master.publish(work['id'],FileStorage(stream=io.BytesIO(self.pdf()),filename='Published.pdf'))
    def test_platform_admin_only_current_view_and_working_copy_does_not_publish(self):
        before=copy.deepcopy(dict(self.stored()));page=self.client.get(self.url);self.assertEqual(page.status_code,200,page.text);tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//nav[@aria-label="Core Lease Master"]/a/text()'),['View Core Lease','Edit Core Lease','Publish New Version']);self.assertTrue(tree.xpath('//*[@data-viewer-prefix="core-master"]'))
        pdf=self.client.get(self.url+'/1/pdf');self.assertEqual(pdf.status_code,200);self.assertIn('inline',pdf.headers['Content-Disposition']);self.assertIn('no-store',pdf.headers['Cache-Control'])
        uploaded=self.client.post(self.url,data={'csrf_token':self.csrf,'action':'prepare','working_docx':(io.BytesIO(self.word()),'Working.docx')},content_type='multipart/form-data');self.assertEqual(uploaded.status_code,302,uploaded.text)
        self.assertEqual(self.master.current()['version'],1);self.assertEqual(len(self.master.working_copies()),1);self.assertEqual(dict(self.stored()),before)
        self.allowed=False
        for method,path,data in [('get',self.url,None),('get',self.url+'/1/pdf',None),('post',self.url,{'csrf_token':self.csrf,'action':'publish'})]:self.assertEqual(getattr(self.client,method)(path,data=data).status_code,403)
        self.user.is_authenticated=False;self.assertEqual(self.client.get(self.url).status_code,403)
    def test_publish_preserves_history_new_draft_pins_and_legacy_drafts_keep_original(self):
        self.signed();original=copy.deepcopy(dict(self.stored()));objects=copy.deepcopy(self.objects);published=self.publish();self.assertEqual(published['version'],2);self.assertEqual(published['published_by'],7);self.assertTrue(published['published_at']);self.assertEqual(self.master.current()['version'],2)
        self.assertEqual(dict(self.stored()),original);self.assertEqual({key:self.objects[key] for key in objects},objects);self.assertEqual(self.master.pinned(self.stored())['version'],1)
        from app.program_billing import lease_snapshot as snapshot
        env=dict(__package__='app.program_billing',request=request,uuid=uuid,snapshot=snapshot,deepcopy=copy.deepcopy,db=self.db,text=text,current_user=self.user,json=json,abort=abort)
        functions('app/program_billing/lease_workflow.py',{'create_parties_draft'},env)
        token=str(uuid.uuid4())
        with self.app.test_request_context('/',method='POST',data={'registration_token':token}):new=env['create_parties_draft'](26,{'_parties':{'lessees':[]}})
        self.assertEqual(new['items']['_core_master']['version'],2)
        from app.program_billing import lease_kit_pdf
        with self.app.app_context():self.assertEqual(lease_kit_pdf.build_pdf('core',{'lease_draft':new}),self.pdf())
        with self.app.test_request_context('/'):
            self.assertEqual(self.master.core_for(new),self.pdf());self.assertNotEqual(self.master.core_for(self.stored()),self.pdf())
        second=self.publish();self.assertEqual(second['version'],3);self.assertEqual(len(self.master.publications()),3)
        with self.app.test_request_context('/',method='POST',data={'registration_token':token}):same=env['create_parties_draft'](26,{'_parties':{'lessees':[]}})
        self.assertEqual(same['items']['_core_master']['version'],2)
        with self.app.test_request_context('/',method='POST',data={'registration_token':str(uuid.uuid4())}):latest=env['create_parties_draft'](26,{'_parties':{'lessees':[]}})
        self.assertEqual(latest['items']['_core_master']['version'],3);self.assertEqual(dict(self.stored()),original)
    def test_publish_route_version_race_and_explicit_eligible_draft_update(self):
        with self.app.test_request_context('/'):
            from werkzeug.datastructures import FileStorage
            work=self.master.prepare(FileStorage(stream=io.BytesIO(self.word()),filename='Working.docx'))
        response=self.client.post(self.url,data={'csrf_token':self.csrf,'action':'publish','working_id':work['id'],'published_pdf':(io.BytesIO(self.pdf()),'Core.pdf')},content_type='multipart/form-data');self.assertEqual(response.status_code,302,response.text)
        race=self.client.post(self.url,data={'csrf_token':self.csrf,'action':'publish','working_id':work['id'],'published_pdf':(io.BytesIO(self.pdf()),'Core.pdf')},content_type='multipart/form-data');self.assertEqual(race.status_code,400);self.assertEqual(self.master.current()['version'],2)
        with self.app.test_request_context('/'):
            from werkzeug.datastructures import FileStorage
            with patch.object(self.master,'current',side_effect=self.master.legacy):
                with self.assertRaises(ValueError):self.master.publish(work['id'],FileStorage(stream=io.BytesIO(self.pdf()),filename='Concurrent.pdf'))
        self.assertEqual(self.master.current()['version'],2)
        before=copy.deepcopy(self.stored()['items']);version=self.stored()['version']
        updated=self.client.post(self.url,data={'csrf_token':self.csrf,'action':'update_draft','property_id':26,'draft_id':str(self.draft),'version':version});self.assertEqual(updated.status_code,302,updated.text);self.assertEqual(self.stored()['items']['_core_master']['version'],2)
        stale=self.client.post(self.url,data={'csrf_token':self.csrf,'action':'update_draft','property_id':26,'draft_id':str(self.draft),'version':version});self.assertEqual(stale.status_code,409)
        after=self.stored()['items'];self.assertEqual({k:v for k,v in after.items() if k not in ('_core_master','_lease_history')},{k:v for k,v in before.items() if k not in ('_core_master','_lease_history')});self.assertEqual(after['_lease_history']['core_master_updates'][-1]['updated_by'],7)
        self.signed();signed=copy.deepcopy(dict(self.stored()));denied=self.client.post(self.url,data={'csrf_token':self.csrf,'action':'update_draft','property_id':26,'draft_id':str(self.draft),'version':self.stored()['version']});self.assertEqual(denied.status_code,409);self.assertEqual(dict(self.stored()),signed)

    def test_snapshot_creation_pins_current_and_registry_failure_does_not_fall_back(self):
        self.publish()
        from app.program_billing import lease_snapshot as snapshot
        env=dict(__package__='app.program_billing',billing_bp=types.SimpleNamespace(post=lambda *a,**kw:lambda f:f),endpoint=snapshot.endpoint,authorised_property=snapshot.authorised_property,require_particulars_schema=lambda:None,request=request,uuid=uuid,db=self.db,text=text,current_user=self.user,json=json,abort=abort,CATALOG=snapshot.CATALOG,flash=flash,redirect=redirect,url_for=url_for)
        functions('app/program_billing/lease_snapshot.py',{'create_draft'},env)
        with self.app.test_request_context('/',method='POST',headers=self.headers,data={'registration_token':str(uuid.uuid4())}):response=env['create_draft'](26)
        self.assertEqual(response.status_code,302)
        from urllib.parse import urlparse,parse_qs
        identifier=parse_qs(urlparse(response.location).query)['draft_id'][0]
        row=self.session.execute(text('SELECT items FROM bil_lease_draft WHERE id=:id'),{'id':identifier}).scalar_one();self.assertEqual(row['_core_master']['version'],2)
        with patch.object(self.master,'list_keys',side_effect=PermissionError('Synthetic R2 listing denied')):
            denied=self.client.get(self.url);self.assertEqual(denied.status_code,503);self.assertIn('read, write and list permissions',denied.text)
        self.assertEqual(self.master.pinned(self.stored())['version'],1)
