"""Property master migration/upload integration on isolated local PostgreSQL + R2 doubles."""
import copy,io,uuid,unittest,importlib.util,os,threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from zipfile import ZipFile
from pathlib import Path
import sqlalchemy as sa
from sqlalchemy.orm import scoped_session,sessionmaker
from alembic.operations import Operations
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from alembic.runtime.migration import HeadMaintainer
from lxml import html
import test_pm_lease_snapshot as fixture
import test_pm_property_management as properties

ROOT=properties.ROOT

class PropertyLeaseMasterTests(unittest.TestCase):
    load_lease_workspace=True
    sign_in=fixture.LeaseSnapshotTests.sign_in
    sql=fixture.LeaseSnapshotTests.sql
    payload=fixture.LeaseSnapshotTests.payload
    assign=fixture.LeaseSnapshotTests.assign
    create=fixture.LeaseSnapshotTests.create

    def setUp(self):
        if self._testMethodName=='test_https_upload_preserves_real_csrf_and_property_authorisation':
            from flask import Flask
            from flask_wtf import CSRFProtect
            original_init=Flask.__init__
            def initialise_with_csrf(app,*args,**kwargs):
                original_init(app,*args,**kwargs)
                CSRFProtect(app)
                app.config['WTF_CSRF_ENABLED']=False  # Seed fixture draft only.
            with patch.object(Flask,'__init__',initialise_with_csrf):
                fixture.LeaseSnapshotTests.setUp(self)
        else:
            fixture.LeaseSnapshotTests.setUp(self)
        spec=importlib.util.spec_from_file_location('local_pm_lease_master_001',ROOT/'migrations/versions/pm_lease_master_001.py')
        self.migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.migration)
        self.sql('SELECT 1');self.session.commit()
        before=[r[0] for r in self.sql('SELECT version_num FROM alembic_version')];self.session.commit()
        with self.conn.begin():
            self.migration.op=Operations(MigrationContext.configure(self.conn));self.migration.upgrade()
            self.conn.exec_driver_sql("INSERT INTO alembic_version VALUES('pm_lease_master_001')")
        self.assertEqual(set(r[0] for r in self.sql('SELECT version_num FROM alembic_version')),set(before)|{'pm_lease_master_001'})
        self.session.commit()

    def url(self,property_id=26):return f'/billing/leases/property/{property_id}/masters'
    def docx(self,label='Synthetic master one'):
        stream=io.BytesIO()
        with ZipFile(stream,'w') as z:
            z.writestr('[Content_Types].xml','<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/></Types>')
            z.writestr('word/document.xml','<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>'+label+'</w:t></w:r></w:p></w:body></w:document>')
        return stream.getvalue()
    def upload(self,data=None,name='Original Master.docx',property_id=26):
        return self.client.post(self.url(property_id),data={'master_document':(io.BytesIO(self.docx() if data is None else data),name)},content_type='multipart/form-data')
    def rows(self):return self.sql('SELECT * FROM bil_property_lease_master ORDER BY property_id,version').mappings().all()
    def download_url(self,row,property_id=None):return self.url(property_id or row['property_id'])+'/'+str(row['id'])+'/download'

    def test_two_versions_retrieve_history_and_no_draft_changes(self):
        self.app.config['PM_LEASE_DRAFT_WRITES_APPROVED']=False
        properties_before=[dict(r) for r in self.sql('SELECT * FROM bil_property ORDER BY id').mappings()]
        drafts_before=[dict(r) for r in self.sql('SELECT * FROM bil_lease_draft ORDER BY id').mappings()]
        first=self.docx();second=self.docx('Synthetic master two')
        self.assertEqual(self.upload(first).status_code,302)
        self.assertEqual(self.upload(second,'Second Master.docx').status_code,302)
        rows=self.rows();self.assertEqual([r['version'] for r in rows],[1,2])
        for row,payload in zip(rows,(first,second)):
            self.assertEqual(row['state'],'available');self.assertIsNotNone(row['verified_at'])
            self.assertEqual(row['uploaded_by'],7);self.assertIsNotNone(row['uploaded_at'])
            response=self.client.get(self.download_url(row));self.assertEqual(response.status_code,200);self.assertEqual(response.data,payload)
            self.assertIn('attachment',response.headers['Content-Disposition']);self.assertIn('no-store',response.headers['Cache-Control'])
        page=self.client.get(self.url()+'?draft_id='+str(self.draft));self.assertEqual(page.status_code,200)
        tree=html.fromstring(page.get_data(as_text=True))
        self.assertIn('Property: Almond',tree.text_content());self.assertIn('Version 2',tree.text_content())
        self.assertIn('Download version 1',tree.text_content());self.assertIn('Download version 2',tree.text_content())
        for row in rows:self.assertNotIn(row['storage_key'],page.get_data(as_text=True))
        self.assertNotIn('r2.dev',page.get_data(as_text=True))
        self.assertEqual(set(call[1] for call in self.calls),{'ait-lease-controlled-private'})
        self.assertEqual([dict(r) for r in self.sql('SELECT * FROM bil_property ORDER BY id').mappings()],properties_before)
        self.assertEqual([dict(r) for r in self.sql('SELECT * FROM bil_lease_draft ORDER BY id').mappings()],drafts_before)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_document').scalar(),0)
        from app.program_billing.lease_generation import load_master,MASTER_SHA256
        import hashlib
        self.assertEqual(hashlib.sha256(load_master()).hexdigest(),MASTER_SHA256)
        self.assertNotEqual(load_master(),second)

    def test_invalid_files_size_package_and_storage_configuration(self):
        for data,name in [(b'bad','bad.docx'),(self.docx(),'wrong.pdf'),(b'','empty.docx'),(b'x'*(10*1024*1024+1),'large.docx')]:
            self.assertEqual(self.upload(data,name).status_code,400)
        stream=io.BytesIO()
        with ZipFile(stream,'w') as z:
            z.writestr('[Content_Types].xml','<!DOCTYPE test><Types/>');z.writestr('word/document.xml','<x/>')
        self.assertEqual(self.upload(stream.getvalue()).status_code,400)
        self.assertEqual(len(self.rows()),0);self.assertFalse(self.calls)
        self.app.config['PM_LEASE_PRIVATE_STORAGE_APPROVED']=False
        self.assertEqual(self.upload().status_code,503);self.assertFalse(self.calls)
        self.app.config['PM_LEASE_PRIVATE_STORAGE_APPROVED']=True
        with patch.dict(os.environ,{'SACE_HOME_R2_BUCKET':'ait-lease-controlled-private'}):self.assertEqual(self.upload().status_code,503)
        self.assertEqual(len(self.rows()),0)

    def test_failed_verification_does_not_replace_current_or_destroy_old_file(self):
        first=self.docx();self.assertEqual(self.upload(first).status_code,302)
        with patch.object(self.r2,'read_file_from_r2',return_value=b'corrupt'):
            failure=self.upload(self.docx('Failed next master'))
        self.assertEqual(failure.status_code,503);self.assertIn('not current',failure.get_data(as_text=True))
        rows=self.rows();self.assertEqual([r['state'] for r in rows],['available','failed'])
        page=self.client.get(self.url()).get_data(as_text=True);self.assertIn('Version 1',page)
        self.assertEqual(self.client.get(self.download_url(rows[0])).data,first)
        self.assertEqual(self.client.get(self.download_url(rows[1])).status_code,404)
        self.assertEqual(self.upload(self.docx('Successful third version')).status_code,302)
        self.assertEqual([r['version'] for r in self.rows()],[1,2,3])

    def test_property_isolation_authorization_and_storage_scope(self):
        self.assertEqual(self.upload().status_code,302);row=self.rows()[0];calls=len(self.calls)
        self.assertEqual(self.client.get(self.download_url(row,28)).status_code,404)
        self.assertEqual(self.client.get(self.download_url(row,27)).status_code,404)
        self.assertEqual(self.upload(property_id=27).status_code,404);self.assertEqual(len(self.calls),calls)
        self.assertEqual(self.upload(self.docx('Other property master'),property_id=28).status_code,302)
        self.assertEqual([r['version'] for r in self.rows()],[1,1])
        self.sign_in(8)
        self.assertEqual(self.client.get(self.url()).status_code,404)
        self.assertEqual(self.client.get(self.download_url(row)).status_code,404)
        self.assertEqual(self.upload().status_code,404)
        self.sign_in(7)
        self.session.execute(sa.text("UPDATE bil_property_lease_master SET storage_key='private/leases/property-28/masters/invalid.docx' WHERE id=:id"),{'id':row['id']});self.session.commit()
        self.assertEqual(self.client.get(self.download_url(row)).status_code,503)

    def test_reassignment_during_upload_fails_closed(self):
        original=self.storage.upload
        def transfer(*args):
            result=original(*args)
            self.sql('UPDATE bil_property SET manager_id=8 WHERE id=26');self.session.commit()
            return result
        with patch.object(self.storage,'upload',side_effect=transfer):self.assertEqual(self.upload().status_code,404)
        self.assertEqual(self.rows()[0]['state'],'failed')

    def test_schema_gate_and_migration_constraints_preserve_foundation(self):
        self.sql("DELETE FROM alembic_version WHERE version_num='pm_lease_master_001'");self.session.commit()
        page=self.client.get(self.url());self.assertEqual(page.status_code,200)
        self.assertIn('not recorded in Alembic history',page.get_data(as_text=True))
        self.assertEqual(self.upload().status_code,503);self.assertFalse(self.calls)
        with self.assertRaises(RuntimeError):self.migration.downgrade()
        self.assertIsNone(self.migration.down_revision)
        scripts=ScriptDirectory(str(ROOT/'migrations'))
        heads=('home_sace_003','pm_owner_002','reading_sace_002','reading_sace_audit_001','uip_p57','pm_lease_001')
        steps=scripts._upgrade_revs('pm_lease_master_001',heads)
        self.assertEqual([s.revision.revision for s in steps],['pm_lease_master_001'])
        output=io.StringIO();context=MigrationContext.configure(dialect_name='postgresql',opts={'as_sql':True,'output_buffer':output})
        maintainer=HeadMaintainer(context,set(heads))
        for step in steps:maintainer.update_to_step(step)
        self.assertEqual(maintainer.heads,set(heads)|{'pm_lease_master_001'})
        self.assertNotIn('UPDATE alembic_version',output.getvalue());self.assertNotIn('DELETE FROM alembic_version',output.getvalue())

    def test_concurrent_uploads_have_distinct_versions_and_both_files(self):
        # Separate permanent schema in this disposable loopback test database:
        # thread connections cannot share the fixture's pg_temp namespace.
        schema='pm_master_concurrency_'+uuid.uuid4().hex
        self.assertEqual(self.engine.url.database,'ait_local_db');self.assertIn(self.engine.url.host,('127.0.0.1','localhost','::1'))
        engine=sa.create_engine(self.engine.url,connect_args={'options':'-csearch_path='+schema})
        with self.engine.begin() as conn:conn.exec_driver_sql('CREATE SCHEMA '+schema)
        try:
            with engine.begin() as conn:
                conn.exec_driver_sql('CREATE TABLE "user"(id integer PRIMARY KEY,is_active integer)')
                conn.exec_driver_sql('CREATE TABLE bil_property(id integer PRIMARY KEY,name varchar(100),address varchar(200),manager_id integer,is_archived boolean)')
                conn.exec_driver_sql('INSERT INTO "user" VALUES(7,1)')
                conn.exec_driver_sql("INSERT INTO bil_property VALUES(26,'Almond','Synthetic address',7,false)")
                conn.exec_driver_sql('CREATE TABLE alembic_version(version_num varchar(32) PRIMARY KEY)')
                self.migration.op=Operations(MigrationContext.configure(conn));self.migration.upgrade()
                conn.exec_driver_sql("INSERT INTO alembic_version VALUES('pm_lease_master_001')")
            from app.extensions import db
            original_session=db.session;thread_session=scoped_session(sessionmaker(bind=engine));db.session=thread_session
            barrier=threading.Barrier(2);original_upload=self.storage.upload
            def overlap(*args):barrier.wait(timeout=10);return original_upload(*args)
            def worker(i):
                try:
                    client=self.app.test_client()
                    with client.session_transaction() as session:session['_user_id']='7';session['_fresh']=True
                    return client.post(self.url(),data={'master_document':(io.BytesIO(self.docx('Concurrent '+str(i))),'parallel.docx')},content_type='multipart/form-data').status_code
                finally:thread_session.remove()
            try:
                with patch.object(self.storage,'upload',side_effect=overlap),ThreadPoolExecutor(max_workers=2) as pool:
                    self.assertEqual(sorted(pool.map(worker,(1,2))),[302,302])
                with engine.connect() as conn:
                    rows=conn.exec_driver_sql('SELECT version,state,storage_key FROM bil_property_lease_master ORDER BY version').all()
                self.assertEqual([(r.version,r.state) for r in rows],[(1,'available'),(2,'available')])
                self.assertNotEqual(rows[0].storage_key,rows[1].storage_key)
                self.assertTrue(all(row.storage_key in self.objects for row in rows))
            finally:db.session=original_session;thread_session.remove()
        finally:
            engine.dispose()
            with self.engine.begin() as conn:conn.exec_driver_sql('DROP SCHEMA '+schema+' CASCADE')

    def test_database_constraints_reject_invalid_metadata(self):
        self.assertEqual(self.upload().status_code,302);baseline=dict(self.rows()[0])
        for changes in ({'version':1},{'version':0},{'size_bytes':10485761},{'sha256':'invalid'},
                        {'mime_type':'application/pdf'},{'verified_at':None},{'uploaded_by':999},{'property_id':999}):
            row=dict(baseline,id=uuid.uuid4(),version=2,storage_key='private/unique/'+uuid.uuid4().hex+'.docx')
            row.update(changes)
            columns=list(row)
            query='INSERT INTO bil_property_lease_master ('+','.join(columns)+') VALUES ('+','.join(':'+c for c in columns)+')'
            with self.assertRaises(sa.exc.IntegrityError),self.session.begin_nested():self.session.execute(sa.text(query),row)
        self.assertEqual(len(self.rows()),1)

    def test_migration_ddl_rollback_is_transactional(self):
        schema='pm_master_rollback_'+uuid.uuid4().hex
        self.assertEqual(self.engine.url.database,'ait_local_db');self.assertIn(self.engine.url.host,('127.0.0.1','localhost','::1'))
        engine=sa.create_engine(self.engine.url,connect_args={'options':'-csearch_path='+schema})
        with self.engine.begin() as conn:conn.exec_driver_sql('CREATE SCHEMA '+schema)
        try:
            with engine.begin() as conn:
                conn.exec_driver_sql('CREATE TABLE "user"(id integer PRIMARY KEY)')
                conn.exec_driver_sql('CREATE TABLE bil_property(id integer PRIMARY KEY,manager_id integer)')
                conn.exec_driver_sql('INSERT INTO "user" VALUES(7)');conn.exec_driver_sql('INSERT INTO bil_property VALUES(26,7)')
            with self.assertRaisesRegex(RuntimeError,'synthetic transaction failure'):
                with engine.begin() as conn:
                    self.migration.op=Operations(MigrationContext.configure(conn));self.migration.upgrade()
                    raise RuntimeError('synthetic transaction failure')
            with engine.connect() as conn:
                self.assertFalse(sa.inspect(conn).has_table('bil_property_lease_master'))
                self.assertEqual(conn.exec_driver_sql('SELECT id,manager_id FROM bil_property').one(),(26,7))
        finally:
            engine.dispose()
            with self.engine.begin() as conn:conn.exec_driver_sql('DROP SCHEMA '+schema+' CASCADE')

    def test_pending_state_change_cannot_report_false_success(self):
        original=self.storage.upload
        def fail_during_storage(*args):
            result=original(*args)
            self.sql("UPDATE bil_property_lease_master SET state='failed' WHERE state='pending'");self.session.commit()
            return result
        with patch.object(self.storage,'upload',side_effect=fail_during_storage):
            response=self.upload()
        self.assertEqual(response.status_code,409)
        self.assertEqual(self.rows()[0]['state'],'failed')

    def test_property_upload_does_not_require_or_create_a_draft(self):
        self.sql('DELETE FROM bil_lease_draft');self.session.commit()
        content=self.docx()
        self.assertEqual(self.upload(content).status_code,302)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),0)
        self.assertEqual(len(self.rows()),1)
        self.assertEqual(self.client.get(self.download_url(self.rows()[0])).data,content)

    def test_upload_screen_has_only_back_navigation_and_preserves_functions(self):
        self.assertEqual(self.upload().status_code,302)
        self.assertEqual(self.upload(self.docx('Version two')).status_code,302)
        response=self.client.get(self.url()+'?draft_id='+str(self.draft));self.assertEqual(response.status_code,200)
        tree=html.fromstring(response.get_data(as_text=True))
        self.assertFalse(tree.xpath('//nav[@aria-label="Lease submenu"]'))
        self.assertFalse(tree.xpath('//a[text()="Generate Word"]'))
        self.assertFalse(tree.xpath('//a[contains(@href,"/particulars/") or contains(@href,"/generate-pdf") or contains(@href,"/review?")]'))
        back=tree.xpath('//a[text()="Back to Lease submenu"]');self.assertEqual(len(back),1)
        self.assertEqual(back[0].get('href'),'/billing/leases?property_id=26&draft_id='+str(self.draft))
        self.assertEqual(self.client.get(back[0].get('href')).status_code,200)
        self.assertIn('Almond',tree.xpath('//h2')[0].text_content())
        self.assertIn('text-xl',tree.xpath('//h2')[0].get('class'))
        self.assertIn('Version 2',tree.text_content());self.assertIn('Upload history',tree.text_content())
        self.assertEqual(len(tree.xpath('//input[@name="master_document"]')),1)
        self.assertTrue(tree.xpath('//button[text()="Upload Master Document" and not(@disabled)]'))
        self.assertTrue(tree.xpath('//input[@name="master_document" and not(@disabled)]'))
        self.assertEqual(len(tree.xpath('//a[contains(@href,"/download")]')),3)
        for row in self.rows():self.assertNotIn(row['storage_key'],response.get_data(as_text=True))
        without_draft=self.client.get(self.url());self.assertEqual(without_draft.status_code,200)
        self.assertNotIn('Start a persistent draft',without_draft.get_data(as_text=True))
        self.app.config['PM_LEASE_PRIVATE_STORAGE_APPROVED']=False
        blocked=self.client.get(self.url());self.assertEqual(blocked.status_code,200)
        blocked_tree=html.fromstring(blocked.get_data(as_text=True))
        self.assertTrue(blocked_tree.xpath('//button[text()="Upload Master Document" and @disabled]'))
        self.assertTrue(blocked_tree.xpath('//input[@name="master_document" and @disabled]'))
        self.assertIn('has not been verified and approved',blocked_tree.text_content())
        self.assertEqual(self.upload().status_code,503)

    def test_inherited_navigation_targets_have_no_local_route_failures(self):
        from urllib.parse import urlparse,parse_qs,urlencode
        before=[dict(row) for row in self.sql('SELECT * FROM bil_lease_draft').mappings()]
        response=self.client.get('/billing/leases?property_id=26&draft_id='+str(self.draft))
        tree=html.fromstring(response.get_data(as_text=True))
        self.assertFalse(tree.xpath('//nav[@aria-label="Property Management"]'))
        back=tree.xpath('//a[text()="Back to Property Management"]')
        self.assertEqual(len(back),1)
        self.assertEqual(back[0].get('href'),'/billing/dashboard')
        links=tree.xpath('//nav[@aria-label="Lease submenu"]/a')
        self.assertEqual(len(links),8)
        for link in links:
            target=link.get('href')
            self.assertEqual(self.client.get(target).status_code,200,target)
        word=tree.xpath('//a[text()="Generate Word"]')[0].get('href')
        review=self.client.get(word);self.assertEqual(review.status_code,200)
        self.assertTrue(html.fromstring(review.get_data(as_text=True)).xpath('//form/fieldset[@disabled]'))
        # Same routes with no explicit draft: forms render but remain gated;
        # View Lease rejects missing context instead of creating a draft.
        for link in links:
            parsed=urlparse(link.get('href'));query=parse_qs(parsed.query);query.pop('draft_id',None);query.pop('revision',None)
            target=parsed.path+('?' + urlencode(query,doseq=True) if query else '')
            result=self.client.get(target)
            expected=400 if '/view' in parsed.path else 200
            self.assertEqual(result.status_code,expected,target)
        self.assertEqual([dict(row) for row in self.sql('SELECT * FROM bil_lease_draft').mappings()],before)

    def test_upload_file_picker_explains_missing_r2_or_schema_without_bypass(self):
        for field in ('PM_LEASE_R2_ENDPOINT_URL','PM_LEASE_R2_ACCESS_KEY','PM_LEASE_R2_SECRET_KEY'):
            with self.subTest(field=field),patch.dict(os.environ,{field:''}):
                response=self.client.get(self.url());self.assertEqual(response.status_code,200)
                tree=html.fromstring(response.get_data(as_text=True))
                self.assertTrue(tree.xpath('//input[@name="master_document" and @disabled]'))
                self.assertTrue(tree.xpath('//button[text()="Upload Master Document" and @disabled]'))
                self.assertIn('Shared R2 credentials are not used',tree.text_content())
                self.assertEqual(self.upload().status_code,503)
        self.sql("DELETE FROM alembic_version WHERE version_num='pm_lease_master_001'");self.session.commit()
        response=self.client.get(self.url());self.assertEqual(response.status_code,200)
        tree=html.fromstring(response.get_data(as_text=True))
        self.assertTrue(tree.xpath('//input[@name="master_document" and @disabled]'))
        self.assertIn('pm_lease_master_001 is not recorded in Alembic history',tree.text_content())
        self.assertEqual(self.upload().status_code,503)
        self.assertFalse(self.calls)
        self.assertEqual(len(self.rows()),0)

    def test_https_upload_preserves_real_csrf_and_property_authorisation(self):
        from flask_wtf.csrf import generate_csrf
        from werkzeug.middleware.proxy_fix import ProxyFix
        # Real global CSRF is installed during fixture application construction.
        # Match the production proxy configuration without invoking app startup.
        self.app.config.update(WTF_CSRF_ENABLED=True,WTF_CSRF_SSL_STRICT=True)
        self.app.jinja_env.globals['csrf_token']=generate_csrf
        self.app.wsgi_app=ProxyFix(self.app.wsgi_app,x_proto=1,x_host=1)
        headers={'X-Forwarded-Proto':'https','X-Forwarded-Host':'lease.example.test'}
        page=self.client.get(self.url(),headers=headers)
        self.assertEqual(page.status_code,200)
        self.assertEqual(page.headers['Referrer-Policy'],'same-origin')
        token=html.fromstring(page.get_data(as_text=True)).xpath('//input[@name="csrf_token"]/@value')[0]
        def post(token_value=token,referrer='https://lease.example.test'+self.url(),property_id=26):
            request_headers=dict(headers)
            if referrer is not None:request_headers['Referer']=referrer
            data={'master_document':(io.BytesIO(self.docx()),'Master.docx')}
            if token_value is not None:data['csrf_token']=token_value
            return self.client.post(self.url(property_id),headers=request_headers,data=data,content_type='multipart/form-data')
        missing_referrer=post(referrer=None)
        self.assertEqual(missing_referrer.status_code,400)
        self.assertIn('The referrer header is missing',missing_referrer.get_data(as_text=True))
        self.assertEqual(post(referrer='https://attacker.example/').status_code,400)
        self.assertEqual(post(token_value=None).status_code,400)
        self.assertEqual(post(token_value='invalid').status_code,400)
        self.assertFalse(self.rows());self.assertFalse(self.calls)
        self.assertEqual(post(property_id=27).status_code,404)
        self.assertFalse(self.rows());self.assertFalse(self.calls)
        self.assertEqual(post().status_code,302)
        self.assertEqual(len(self.rows()),1)
        download=self.client.get(self.download_url(self.rows()[0]),headers=headers)
        self.assertEqual(download.status_code,200)
        self.assertEqual(download.headers['Referrer-Policy'],'no-referrer')
        self.assertIn('no-store',download.headers['Cache-Control'])
