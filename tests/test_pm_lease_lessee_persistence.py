"""Actual capture/save/reopen against session-temporary local PostgreSQL fixtures. No migrations."""
import ast,copy,json,os,re,sys,types,unittest,uuid
from pathlib import Path
from datetime import datetime,timezone
from functools import wraps
from unittest.mock import patch
from flask import Flask,Blueprint,request,abort,url_for,redirect,flash,jsonify,render_template
from flask_wtf.csrf import CSRFProtect,generate_csrf
from jinja2 import ChoiceLoader,DictLoader,FileSystemLoader
from sqlalchemy import create_engine,text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError,IntegrityError
from werkzeug.exceptions import HTTPException
from dotenv import dotenv_values
from lxml import html
ROOT=Path(__file__).resolve().parents[1]

def functions(file,names,env):
    tree=ast.parse((ROOT/file).read_text(encoding='utf-8-sig'))
    nodes=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in names]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),file,'exec'),env)

class LesseePersistenceTests(unittest.TestCase):
    def setUp(self):
        raw=os.environ.get('PM_LEASE_TEST_DATABASE_URL') or dotenv_values(ROOT.parents[1]/'.env').get('DATABASE_URL','')
        if not raw:self.skipTest('Verified local PostgreSQL configuration required')
        target=make_url(raw.replace('postgres://','postgresql://',1))
        self.assertEqual(target.database,'ait_local_db');self.assertIn(target.host,('localhost','127.0.0.1','::1'))
        self.engine=create_engine(target,connect_args={'connect_timeout':5});self.addCleanup(self.engine.dispose)
        self.conn=self.engine.connect();self.addCleanup(self.conn.close)
        self.assertEqual(self.conn.execute(text('SELECT current_database()')).scalar(),'ait_local_db')
        self.conn.execute(text('CREATE TEMP TABLE bil_property (id integer PRIMARY KEY,name text,address text,manager_id integer,is_archived boolean)'))
        self.conn.execute(text('CREATE TEMP TABLE bil_lease_draft (id uuid PRIMARY KEY,property_id integer,manager_id integer,status text,source_version text,items jsonb,version integer,updated_at timestamptz)'))
        self.conn.execute(text('CREATE TEMP TABLE bil_tenant (id integer PRIMARY KEY,manager_id integer,property_id integer,sectional_unit_id integer,party_type text,name text,identity_number text,address text,email text,phone text)'))
        self.conn.execute(text('CREATE TEMP TABLE bil_tenant_identity (id integer,tenant_id integer,manager_id integer,legal_name text,identity_number text,email text,phone text)'))
        self.conn.execute(text('SET search_path TO pg_temp'))
        self.conn.execute(text("INSERT INTO bil_property VALUES (26,'Synthetic property','Synthetic address',7,false)"))
        self.conn.execute(text("INSERT INTO bil_tenant(id,manager_id,property_id,party_type,name,identity_number) VALUES (11,7,26,'natural_person','Natural Lessee','NAT-11'),(22,7,26,'juristic_person','Entity Lessee','REG-22')"))
        self.conn.execute(text("INSERT INTO bil_tenant_identity VALUES (33,22,7,'Entity Representative','REP-33','rep@example.com','000')"))
        self.catalog=json.loads((ROOT/'app/program_billing/lease_snapshot_catalog.json').read_text(encoding='utf-8-sig'))
        self.identifier=uuid.uuid4();self.initial={'_contracting_lessors':{'identities':[{'type':'individual','name':'Owner','identity_number':'OWNER-1'}]},'_certificate_authorities':{'sentinel':{'confirmed':False}},'_parties':{'lessees':[]}}
        self.initial['_lease_history']={'documents':[],'emails':[{'recipient':'audit@example.com','status':'submitted'}]}
        self.conn.execute(text("INSERT INTO bil_lease_draft VALUES (:id,26,7,'draft',:source,CAST(:items AS jsonb),1,now())"),{'id':self.identifier,'source':self.catalog['version'],'items':json.dumps(self.initial)});self.conn.commit()
        self.session=Session(self.conn);self.addCleanup(lambda:self.session.close())
        self.db=types.SimpleNamespace(session=self.session)
        self.user=types.SimpleNamespace(id=7)
        self.user_patch=patch('flask_login.current_user',self.user);self.user_patch.start();self.addCleanup(self.user_patch.stop)
        mods={name:types.ModuleType(name) for name in ('app','app.program_billing','app.program_billing.lease_snapshot','app.program_billing.lease_workflow','app.program_billing.lease_property_payments','app.program_billing.tenant_register','app.program_billing.lease_management','app.program_billing.lease_certificate_view')}
        mods['app'].__path__=[str(ROOT/'app')];mods['app.program_billing'].__path__=[str(ROOT/'app/program_billing')]
        self.mods=patch.dict(sys.modules,mods);self.mods.start();self.addCleanup(self.mods.stop)
        from app.program_billing import lease_certificate as certificate,lease_certificate_form as form,lease_supplementary_conditions as supplementary,lease_kit as kit
        self.kit=kit;self.cert=certificate;self.form=form
        app=Flask('real-local-lessee-persistence',root_path=str(ROOT/'app'));app.secret_key='synthetic-local-test';app.config['TESTING']=True
        CSRFProtect(app);app.jinja_env.globals['csrf_token']=generate_csrf
        app.jinja_loader=ChoiceLoader([DictLoader({'layout.html':'{% block content %}{% endblock %}','partials/flash_messages.html':''}),FileSystemLoader(ROOT/'templates')])
        self.app=app;bp=Blueprint('billing_bp',__name__)
        snap=mods['app.program_billing.lease_snapshot'];workflow=mods['app.program_billing.lease_workflow'];tenant=mods['app.program_billing.tenant_register'];payments=mods['app.program_billing.lease_property_payments']
        def require_user():
            if request.headers.get('X-Test-Owner')!='yes':abort(403)
        snap.__dict__.update(db=self.db,text=text,request=request,abort=abort,uuid=uuid,current_user=self.user,CATALOG=self.catalog,wraps=wraps,HTTPException=HTTPException,SQLAlchemyError=SQLAlchemyError,require_pm_user=require_user)
        functions('app/program_billing/lease_snapshot.py',{'authorised_property','draft_row','endpoint'},snap.__dict__)
        snap.require_particulars_schema=lambda:None
        workflow.__dict__.update(db=self.db,text=text,request=request,abort=abort,current_user=self.user,json=json)
        functions('app/program_billing/lease_workflow.py',{'save_items'},workflow.__dict__);workflow.owners_for=lambda property_id:[]
        tenant_tree=ast.parse((ROOT/'app/program_billing/tenant_register.py').read_text())
        select=next(node for node in tenant_tree.body if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='SELECT' for t in node.targets))
        tenant.__dict__.update(db=self.db,text=text,abort=abort,current_user=self.user,readiness=lambda:None)
        exec(compile(ast.Module(body=[select],type_ignores=[]),'real-tenant-select','exec'),tenant.__dict__)
        functions('app/program_billing/tenant_register.py',{'rows','selected'},tenant.__dict__)
        payments.signed=lambda draft:any(row.get('kind')=='signed_original' for row in draft['items'].get('_lease_history',{}).get('documents',[]))
        mods['app.program_billing.lease_management'].render_pm_template=render_template
        env={'__name__':'app.program_billing.lease_certificate_view','__package__':'app.program_billing',
             'db':self.db,'snapshot':snap,'certificate':certificate,'form':form,'supplementary':supplementary,
             'request':request,'abort':abort,'url_for':url_for,'redirect':redirect,'flash':flash,'jsonify':jsonify,
             'deepcopy':copy.deepcopy,'current_user':self.user,'uuid':uuid,'re':re,'datetime':datetime,'timezone':timezone,'IntegrityError':IntegrityError}
        functions('app/program_billing/lease_certificate_view.py',{'context','edit'},env)
        bp.add_url_rule('/capture/<int:property_id>/<section_key>',endpoint='lease_capture_section',view_func=snap.endpoint(env['edit']),methods=['GET','POST'])
        mods['app.program_billing.lease_certificate_view'].context=env['context']
        bp.add_url_rule('/view/<int:property_id>',endpoint='lease_live_view',view_func=snap.endpoint(kit.view))
        bp.add_url_rule('/certificate/<int:property_id>',endpoint='lease_certificate_edit',view_func=snap.endpoint(lambda property_id:env['edit'](property_id,'parties')))
        bp.add_url_rule('/kit-email/<int:property_id>',endpoint='lease_kit_email',view_func=lambda **kw:'')
        pdf_env={'__name__':'app.program_billing.lease_preview','__package__':'app.program_billing','billing_bp':bp,'snapshot':snap,'BytesIO':__import__('io').BytesIO,'abort':abort}
        functions('app/program_billing/lease_preview.py',{'kit_pdf','kit_email_signer','kit_email_selection'},pdf_env)
        for endpoint in ('pm_lease_management',):
            bp.add_url_rule('/stub/'+endpoint+'/<int:property_id>',endpoint=endpoint,view_func=lambda **kw:'')
        app.register_blueprint(bp);app.add_url_rule('/csrf',view_func=generate_csrf)
        self.client=app.test_client();self.csrf=self.client.get('/csrf').text
        self.url='/capture/26/parties?draft_id='+str(self.identifier)

    def stored(self):
        return self.session.execute(text('SELECT items,version FROM bil_lease_draft WHERE id=:id'),{'id':self.identifier}).mappings().one()

    def save_both(self,**changes):
        version=self.stored()['version']
        data={'csrf_token':self.csrf,'version':str(version),'lessor_count':'1','lessee_count':'2','action':'save',
              'tenant_register_id_1':'11','LESSEE_1_PARTY_TYPE':'natural_person','tenant_register_id_2':'22','LESSEE_2_PARTY_TYPE':'juristic_person'}
        data.update(changes)
        data.setdefault('lease_selection_version','1')
        data.setdefault('lease_selected_tenant_id',[data['tenant_register_id_'+str(index)] for index in range(1,int(data['lessee_count'])+1) if data.get('tenant_register_id_'+str(index))])
        return self.client.post(self.url,headers={'X-Test-Owner':'yes'},data=data)

    def test_both_types_persist_and_reopen_as_real_distinct_lessees(self):
        self.assertEqual(self.save_both().status_code,302)
        # Close the ORM session and reload from PostgreSQL, not the submitted form objects.
        self.session.close();self.session=Session(self.conn);self.db.session=self.session
        saved=self.stored()['items'];parties=saved['_parties']['lessees']
        self.assertEqual([p['tenant_id'] for p in parties],[11,22]);self.assertEqual([p['party_type'] for p in parties],['natural_person','juristic_person'])
        self.assertEqual([p['identity_number'] for p in parties],['NAT-11','REG-22'])
        self.assertFalse(parties[0].get('signatory_name'));self.assertEqual(parties[1]['signatory_name'],'Entity Representative')
        self.assertEqual(saved['_certificate_authorities'],self.initial['_certificate_authorities'])
        page=self.client.get(self.url,headers={'X-Test-Owner':'yes'});self.assertEqual(page.status_code,200)
        tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//*[@id="field_LESSEE_1_LEGAL_NAME"]/text()'),['Natural Lessee'])
        self.assertEqual(tree.xpath('//*[@id="field_LESSEE_2_LEGAL_NAME"]/text()'),['Entity Lessee'])
        from app.program_billing.lease_certificate_parties import blocks
        rows=blocks(saved)[1]['parties'];self.assertEqual(len(rows),2)
        self.assertEqual([r['name'] for r in rows],['Natural Lessee','Entity Lessee'])
        values=self.cert.document_values(saved)
        sections=self.kit.certificate_display_rows(self.form.certificate_layout(*self.form.counts(saved)),values,saved)
        with self.app.app_context():certificate_html=render_template('program_billing/lease_certificate_document.html',sections=sections,preview=True)
        certificate_tree=html.fromstring(certificate_html)
        self.assertEqual(len(certificate_tree.xpath('//tr[@class="certificate-party"][th="Lessee"]')),2)
        for name in ('Natural Lessee','Entity Lessee','Entity Representative'):self.assertIn(name,certificate_html)
        self.assertNotIn('certificate-party-warning',certificate_html)

        self.assertEqual(self.save_both(LESSEE_2_PARTY_TYPE='natural_person').status_code,302);self.assertEqual(len(self.stored()['items']['_parties']['lessees']),2)
        self.assertEqual(self.stored()['items']['_parties']['lessees'][1]['party_type'],'juristic_person')

    def test_missing_juristic_representative_and_stale_save_are_rejected(self):
        self.session.execute(text('DELETE FROM bil_tenant_identity'));self.session.commit()
        self.assertEqual(self.save_both().status_code,409)
        self.assertEqual(self.stored()['items'],self.initial)
        self.assertEqual(self.save_both(version='0').status_code,409)
        self.assertEqual(self.stored()['items'],self.initial)


    def test_selection_replacement_removes_previously_selected_parties(self):
        self.assertEqual(self.save_both().status_code,302)
        for identifier,kind,name in (('22','juristic_person','Entity Lessee'),('11','natural_person','Natural Lessee')):
            self.assertEqual(self.save_both(lessee_count='1',tenant_register_id_1=identifier,LESSEE_1_PARTY_TYPE=kind).status_code,302)
            saved=self.stored()['items'];rows=self.cert.selected_tenants(saved)
            self.assertEqual([str(row['tenant_id']) for row in rows],[identifier])
            from app.program_billing.lease_certificate_parties import blocks
            self.assertEqual([row['name'] for row in blocks(saved)[1]['parties']],[name])
            page=self.client.get(self.url,headers={'X-Test-Owner':'yes'})
            self.assertEqual(html.fromstring(page.data).xpath('//*[@id="field_LESSEE_1_LEGAL_NAME"]/text()'),[name])
        self.assertEqual(self.save_both(lessee_count='1',tenant_register_id_1='',LESSEE_1_PARTY_TYPE='',LESSEE_1_LEGAL_NAME='',LESSEE_1_IDENTITY_NUMBER='').status_code,302)
        self.assertEqual(self.cert.selected_tenants(self.stored()['items']),[])
        self.assertNotIn('lessee',self.stored()['items']['_parties'])

    def test_signing_particulars_save_reopen_amend_and_remain_identity_bound(self):
        self.assertEqual(self.save_both().status_code,302)
        from app.program_billing import lease_signing as signing
        parties=signing.rows(self.stored()['items']);self.assertEqual(len(parties),3)
        url=self.url.replace('/parties?','/signing?')
        data={'csrf_token':self.csrf,'version':str(self.stored()['version'])}
        for index,party in enumerate(parties):
            data['signing_place_'+party['key']]='Place '+str(index)
            data['signing_date_'+party['key']]='2026-11-'+str(index+10)
        self.assertEqual(self.client.post(url,headers={'X-Test-Owner':'yes'},data=data).status_code,302)
        self.session.close();self.session=Session(self.conn);self.db.session=self.session
        saved=self.stored()['items'];rows=signing.rows(saved)
        self.assertEqual([row['place'] for row in rows],['Place 0','Place 1','Place 2'])
        self.assertEqual([row['date'] for row in rows],['2026-11-10','2026-11-11','2026-11-12'])
        self.assertEqual(self.session.execute(text('SELECT status FROM bil_lease_draft')).scalar(),'draft')
        page=self.client.get(url,headers={'X-Test-Owner':'yes'});self.assertEqual(page.status_code,200)
        self.assertIn('value="Place 1"',page.text)
        data={'csrf_token':self.csrf,'version':str(self.stored()['version']),'signing_place_'+rows[1]['key']:'Amended place','signing_date_'+rows[1]['key']:'2026-12-01'}
        self.assertEqual(self.client.post(url,headers={'X-Test-Owner':'yes'},data=data).status_code,302)
        saved=self.stored()['items'];rows=signing.rows(saved)
        self.assertEqual((rows[1]['place'],rows[1]['date']),('Amended place','2026-12-01'))
        with self.app.app_context():
            certificate=render_template('program_billing/lease_signing_particulars.html',signing_rows=rows)
            signature=render_template('program_billing/lease_signature_document.html',signing_rows=rows)
        for text_value in ('Amended place','2026-12-01','Place 2','2026-11-12'):
            self.assertIn(text_value,certificate);self.assertIn(text_value,signature)
        self.assertEqual(self.save_both(lessee_count='1',tenant_register_id_1='22',LESSEE_1_PARTY_TYPE='juristic_person').status_code,302)
        retained=signing.rows(self.stored()['items']);self.assertEqual(retained[1]['place'],'Place 2')
        removed_key=rows[1]['key']
        data={'csrf_token':self.csrf,'version':str(self.stored()['version']),'signing_place_'+removed_key:'Wrong party'}
        self.assertEqual(self.client.post(url,headers={'X-Test-Owner':'yes'},data=data).status_code,400)
        current=signing.rows(self.stored()['items'])[1]
        data={'csrf_token':self.csrf,'version':str(self.stored()['version']),'signing_date_'+current['key']:'2026-02-30'}
        self.assertEqual(self.client.post(url,headers={'X-Test-Owner':'yes'},data=data).status_code,400)
        self.assertEqual(self.client.get(url).status_code,403)
        frozen=copy.deepcopy(self.stored()['items']);frozen['_lease_history']={'documents':[{'kind':'signed_original'}]}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(frozen)});self.session.commit()
        data={'csrf_token':self.csrf,'version':str(self.stored()['version']),'signing_place_'+current['key']:'Blocked'}
        self.assertEqual(self.client.post(url,headers={'X-Test-Owner':'yes'},data=data).status_code,409)
        self.assertEqual(self.stored()['items'],frozen)


    def test_rendered_form_deselection_persists_and_matches_certificate_pdf(self):
        from urllib.parse import urlsplit,parse_qs
        import fitz
        self.session.execute(text("UPDATE bil_tenant SET name=CASE id WHEN 11 THEN 'Marelise van der Merwe' ELSE 'WBH NPC' END"));self.session.commit()
        self.assertEqual(self.save_both().status_code,302)
        for selected_ids,expected in ((['11'],['Marelise van der Merwe']),(['22'],['WBH NPC']),(['11','22'],['Marelise van der Merwe','WBH NPC']),([],[])):
            # Start with both saved; submit real rendered controls, leaving stale hidden
            # party fields in place while the visible checkbox selection changes.
            self.assertEqual(self.save_both().status_code,302)
            page=self.client.get(self.url,headers={'X-Test-Owner':'yes'});tree=html.fromstring(page.data)
            self.assertEqual(tree.xpath('//input[@data-lease-tenant-selection and @checked]/@value'),['11','22'])
            data={}
            form_node=tree.get_element_by_id('certificate-form')
            for element in form_node.xpath('.//input[@name and not(ancestor::template)] | .//textarea[@name and not(ancestor::template)] | .//select[@name and not(ancestor::template)]'):
                name=element.get('name')
                if element.get('type')=='checkbox':continue
                if element.tag=='textarea':value=element.text or ''
                elif element.tag=='select':value=(element.xpath('./option[@selected]/@value') or element.xpath('./option/@value') or [''])[0]
                else:value=element.get('value','')
                data[name]=value
            data['lease_selected_tenant_id']=selected_ids
            self.assertEqual(self.client.post(self.url,headers={'X-Test-Owner':'yes'},data=data).status_code,302)
            self.session.close();self.session=Session(self.conn);self.db.session=self.session
            saved=self.stored()['items']
            self.assertEqual([row['legal_name'] for row in self.cert.selected_tenants(saved)],expected)
            reopened=html.fromstring(self.client.get(self.url,headers={'X-Test-Owner':'yes'}).data)
            self.assertEqual(reopened.xpath('//input[@data-lease-tenant-selection and @checked]/@value'),selected_ids)
            view_link=reopened.xpath('//a[normalize-space()="View Lease"]')[0]
            self.assertEqual(view_link.getnext().text_content(),'Back')
            self.assertFalse(view_link.xpath('ancestor-or-self::*[@hidden]'))
            target=view_link.get('href');query=parse_qs(urlsplit(target).query)
            self.assertEqual(query['draft_id'],[str(self.identifier)]);self.assertIn('/26',target)
            view=self.client.get(target+'&component=certificate',headers={'X-Test-Owner':'yes'})
            self.assertEqual(view.status_code,200);certificate=html.fromstring(view.data)
            party_rows=certificate.xpath('//tr[@class="certificate-party"][th="Lessee"]')
            self.assertEqual(len(party_rows),len(expected))
            pdf=self.client.get('/billing/leases/property/26/kit.pdf?draft_id='+str(self.identifier)+'&component=certificate',headers={'X-Test-Owner':'yes'})
            self.assertEqual(pdf.status_code,200)
            pdf_text=''.join(page.get_text() for page in fitz.open(stream=pdf.data,filetype='pdf'))
            for name in ('Marelise van der Merwe','WBH NPC'):
                self.assertEqual(name in certificate.text_content(),name in expected)
                self.assertEqual(name in pdf_text,name in expected)
            capture_link=certificate.xpath('//*[@id="lease-header-actions"]/a[normalize-space()="Data Capture"]')[0]
            self.assertEqual(capture_link.getnext().text_content(),'Back')
            self.assertFalse(capture_link.xpath('ancestor-or-self::*[@hidden]'))
            capture_target=capture_link.get('href');self.assertIn('/26',capture_target)
            self.assertEqual(parse_qs(urlsplit(capture_target).query)['draft_id'],[str(self.identifier)])
            returned=self.client.get(capture_target,headers={'X-Test-Owner':'yes'})
            self.assertEqual(returned.status_code,200);self.assertIn('View Lease',returned.text)
            self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant')).scalar(),2)
            self.assertEqual(saved['_certificate_authorities'],self.initial['_certificate_authorities'])
            self.assertEqual(saved['_lease_history'],self.initial['_lease_history'])
        for asset,selector in (('lease_capture.css','.capture-header .lease-view-link'),('lease_kit.css','#lease-header-actions > .lease-capture-link')):
            css=self.client.get('/static/css/'+asset)
            self.assertEqual(css.status_code,200);self.assertIn(selector,css.text);css.close()
        self.assertIn('lease-cross-actions',view.text)
        saved_view=self.save_both(lease_selected_tenant_id=[],capture_return='view')
        self.assertEqual(saved_view.status_code,302);self.assertIn('/view/26?',saved_view.location)
        self.assertEqual(self.client.get(saved_view.location,headers={'X-Test-Owner':'yes'}).status_code,200)
        stale=self.save_both(lease_selection_version='')
        self.assertEqual(stale.status_code,400)
        self.assertEqual(self.cert.selected_tenants(self.stored()['items']),[])
        self.assertEqual(self.save_both(lease_selected_tenant_id=['999']).status_code,400)

    def test_signed_draft_edit_is_still_rejected(self):
        frozen=copy.deepcopy(self.initial);frozen['_lease_history']={'documents':[{'kind':'signed_original'}]}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(frozen)});self.session.commit()
        self.assertEqual(self.save_both().status_code,409)
        self.assertEqual(self.stored()['items'],frozen)

if __name__=='__main__':unittest.main()
