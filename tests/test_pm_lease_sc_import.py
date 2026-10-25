"""Strict in-memory Word preview/confirm on temporary local PostgreSQL + private R2 double."""
import copy,io,json,sys,types,unittest,uuid
from unittest.mock import Mock,patch
from sqlalchemy import text
from flask import request,abort
import test_pm_lease_sc_persistence as fixtures

class SpecialConditionsImportTests(unittest.TestCase):
    stored=fixtures.SpecialConditionsPersistenceTests.stored
    payload=fixtures.SpecialConditionsPersistenceTests.payload
    def setUp(self):
        fixtures.SpecialConditionsPersistenceTests.setUp(self)
        self.session.execute(text('UPDATE bil_lease_draft SET registration_token=:r'),{'r':uuid.uuid4()})
        self.session.execute(text('ALTER TABLE bil_lease_draft ALTER COLUMN registration_token SET NOT NULL'))
        self.session.execute(text('CREATE UNIQUE INDEX ON bil_lease_draft(manager_id,registration_token)'))
        self.session.execute(text('CREATE TEMP TABLE bil_lease_document(id uuid PRIMARY KEY,draft_id uuid,property_id integer,manager_id integer,item_key text,storage_key text,filename text,mime_type text,size_bytes integer,sha256 text,state text,verified_at timestamptz)'))
        self.session.commit()
        self.assertEqual(self.client.post(self.url,headers={'X-Test-Owner':'yes'},data=self.payload([('First Heading','1.1 Original first.'),('Second Heading','2.1 Original second.')])).status_code,302)
        extensions=types.ModuleType('app.extensions');extensions.db=self.db
        r2=types.ModuleType('app.program_billing.lease_r2');r2.settings=Mock(return_value={'bucket':'ait-lease-controlled-private'})
        self.objects={}
        def upload(key,data,**kw):self.objects[key]=data;return key
        r2.upload_bytes_to_r2=Mock(side_effect=upload);r2.read_file_from_r2=Mock(side_effect=lambda key,**kw:self.objects[key])
        self.r2=r2;self.env=patch.dict(sys.modules,{'app.extensions':extensions,r2.__name__:r2});self.env.start();self.addCleanup(self.env.stop)
        from app.program_billing import lease_sc_import as importer,lease_sc_export as exporter
        self.importer=importer;self.exporter=exporter
        snap=sys.modules['app.program_billing.lease_snapshot']
        def context(property_id):return snap.authorised_property(property_id),snap.draft_row(property_id,request.args.get('draft_id'))
        # Execute the actual history action dispatch with its existing endpoint guard.
        env={'__name__':'app.program_billing.lease_history','__package__':'app.program_billing','billing_bp':types.SimpleNamespace(route=lambda *a,**k:lambda f:f),'snapshot':snap,'context':context,'db':self.db,'request':request,'redirect':__import__('flask').redirect,'url_for':__import__('flask').url_for,'render_pm_template':__import__('flask').render_template}
        fixtures.fixtures.functions('app/program_billing/lease_history.py',{'history'},env)
        self.app._got_first_request=False  # Isolated fixture route registration, never production setup.
        self.app.add_url_rule('/billing/leases/property/<int:property_id>/history',view_func=env['history'],methods=['GET','POST'])
        self.endpoint='/billing/leases/property/26/history?draft_id='+str(self.identifier)
        self.headers={'X-Test-Owner':'yes'}
    def document(self,clauses=None):
        from datetime import datetime,timezone
        items=copy.deepcopy(self.stored()['items'])
        if clauses is not None:items['_supplementary_conditions']['clauses']=clauses
        return self.exporter.build_word({'id':26,'name':'Almond'},{'id':self.identifier,'version':self.stored()['version'],'items':items},datetime.now(timezone.utc))
    def post(self,action,payload=None,**data):
        data.update(action=action,csrf_token=self.csrf,source=(io.BytesIO(payload or self.document()),'Conditions.docx'))
        return self.client.post(self.endpoint,headers=self.headers,data=data,content_type='multipart/form-data')
    def test_preview_no_writes_confirm_forks_new_version_and_preserves_original(self):
        before=copy.deepcopy(dict(self.stored()));clauses=copy.deepcopy(before['items']['_supplementary_conditions']['clauses']);clauses[0]['text']='1.1 Proposed wording.\n1.2 Unchanged numbering.'
        payload=self.document(clauses);preview=self.post('sc_import_preview',payload)
        self.assertEqual(preview.status_code,200);data=preview.get_json();self.assertEqual(data['comparison'][0]['existing_text'],'1.1 Original first.');self.assertIn('Proposed wording',data['comparison'][0]['proposed_text'])
        self.assertEqual(dict(self.stored()),before);self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_lease_draft')).scalar(),1);self.r2.upload_bytes_to_r2.assert_not_called()
        response=self.post('sc_import_confirm',payload,token=data['token'],confirmed='yes');self.assertEqual(response.status_code,200)
        saved=response.get_json();self.assertNotEqual(saved['draft_id'],str(self.identifier));self.assertEqual(saved['version'],before['version']+1)
        self.assertEqual(dict(self.stored()),before)
        row=self.session.execute(text('SELECT * FROM bil_lease_draft WHERE id=:id'),{'id':saved['draft_id']}).mappings().one();self.assertEqual(row['status'],'draft');self.assertEqual(row['items']['_supplementary_conditions']['clauses'][0]['text'],clauses[0]['text'])
        self.assertEqual({k:v for k,v in row['items'].items() if k not in ('_supplementary_conditions','_sc_import','_lease_history')},{k:v for k,v in before['items'].items() if k not in ('_supplementary_conditions','_sc_import','_lease_history')})
        record=self.session.execute(text('SELECT * FROM bil_lease_document')).mappings().one();self.assertEqual(str(record['draft_id']),saved['draft_id']);self.assertEqual(record['state'],'available');self.assertEqual(self.objects[record['storage_key']],payload);self.assertTrue(record['storage_key'].startswith('private/leases/property-26/draft-'))
        self.assertEqual(self.post('sc_import_confirm',payload,token=data['token'],confirmed='yes').status_code,200);self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_lease_draft')).scalar(),2)
    def test_invalid_numbering_missing_duplicates_tables_and_fields_are_rejected(self):
        original=copy.deepcopy(dict(self.stored()));base=copy.deepcopy(original['items']['_supplementary_conditions']['clauses'])
        for clauses in (base[:1],base+[base[0]],[base[0],base[0]],[{**base[0],'number':'SC-02'},base[1]],list(reversed([{**base[0],'number':'SC-01'},{**base[1],'number':'SC-02'}]))):
            self.assertEqual(self.post('sc_import_preview',self.document(clauses)).status_code,400)
        from zipfile import ZipFile,ZIP_DEFLATED
        for fragment in ('<w:tbl/>','<w:p><w:r><w:instrText>FIELD</w:instrText></w:r></w:p>','<w:p><w:pPr><w:numPr/></w:pPr></w:p>'):
            source=self.document();output=io.BytesIO()
            with ZipFile(io.BytesIO(source)) as old,ZipFile(output,'w',ZIP_DEFLATED) as new:
                for name in old.namelist():new.writestr(name,old.read(name).replace(b'</w:body>',fragment.encode()+b'</w:body>') if name=='word/document.xml' else old.read(name))
            self.assertEqual(self.post('sc_import_preview',output.getvalue()).status_code,400)
        self.assertEqual(dict(self.stored()),original);self.r2.upload_bytes_to_r2.assert_not_called()
    def test_confirmation_version_file_authentication_and_storage_failures(self):
        payload=self.document();preview=self.post('sc_import_preview',payload).get_json();before=copy.deepcopy(dict(self.stored()))
        self.assertEqual(self.post('sc_import_confirm',payload,token=preview['token']).status_code,400)
        self.assertEqual(self.post('sc_import_confirm',payload,token='forged',confirmed='yes').status_code,409)
        changed=copy.deepcopy(before['items']['_supplementary_conditions']['clauses']);changed[0]['text']='Changed after comparison'
        self.assertEqual(self.post('sc_import_confirm',self.document(changed),token=preview['token'],confirmed='yes').status_code,409)
        self.r2.upload_bytes_to_r2.side_effect=RuntimeError('private storage failure')
        self.assertEqual(self.post('sc_import_confirm',payload,token=preview['token'],confirmed='yes').status_code,503);self.assertEqual(dict(self.stored()),before)
        self.session.execute(text('UPDATE bil_lease_draft SET version=version+1 WHERE id=:id'),{'id':self.identifier});self.session.commit()
        self.assertEqual(self.post('sc_import_confirm',payload,token=preview['token'],confirmed='yes').status_code,409)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_lease_draft')).scalar(),1)
        self.headers={};self.assertEqual(self.post('sc_import_preview',payload).status_code,403)
        self.headers={'X-Test-Owner':'yes'};self.endpoint=self.endpoint.replace('/26/','/27/');self.assertEqual(self.post('sc_import_preview',payload).status_code,404)
    def test_preview_requires_csrf_and_failed_publication_preserves_original(self):
        payload=self.document();before=copy.deepcopy(dict(self.stored()))
        response=self.client.post(self.endpoint,headers=self.headers,data={'action':'sc_import_preview','source':(io.BytesIO(payload),'Conditions.docx')},content_type='multipart/form-data')
        self.assertEqual(response.status_code,400)
        token=self.post('sc_import_preview',payload).get_json()['token']
        execute=self.session.execute
        def fail_document(statement,*args,**kwargs):
            if 'INSERT INTO bil_lease_document' in str(statement):raise RuntimeError('synthetic publication failure')
            return execute(statement,*args,**kwargs)
        with patch.object(self.session,'execute',side_effect=fail_document):self.assertEqual(self.post('sc_import_confirm',payload,token=token,confirmed='yes').status_code,503)
        self.assertEqual(dict(self.stored()),before);self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_lease_draft')).scalar(),1)
    def test_signed_or_approved_drafts_cannot_be_imported(self):
        items=copy.deepcopy(self.stored()['items']);items['_lease_history']['documents']=[{'kind':'signed_original'}]
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb) WHERE id=:id'),{'id':self.identifier,'items':json.dumps(items)});self.session.commit()
        self.assertEqual(self.post('sc_import_preview').status_code,409);self.r2.upload_bytes_to_r2.assert_not_called()
        items['_lease_history']['documents']=[];items['_generations']=[{'signature_or_issuance_approved':True}]
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb) WHERE id=:id'),{'id':self.identifier,'items':json.dumps(items)});self.session.commit()
        self.assertEqual(self.post('sc_import_preview').status_code,409)

if __name__=='__main__':unittest.main()
