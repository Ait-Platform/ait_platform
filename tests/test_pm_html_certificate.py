"""HTML certificate workflow on session-temporary local PostgreSQL tables."""
import copy
import json
from pathlib import Path
from unittest.mock import patch
from lxml import html
from sqlalchemy import text
import importlib.util
from alembic.operations import Operations
from alembic.migration import MigrationContext
import test_pm_approved_certificate as existing


class HTMLCertificateTests(existing.CertificateRouteTests):
    def open_form(self, draft=True):
        url='/billing/leases/property/26/certificate'+('?draft_id='+str(self.draft) if draft else '')
        response=self.client.get(url)
        self.assertEqual(response.status_code,200)
        tree=html.fromstring(response.data)
        fields={node.get('name'):node.get('value','') for node in tree.xpath('//form[@id="certificate-form"]//input[@name]')}
        fields.update({node.get('name'):node.text or '' for node in tree.xpath('//form[@id="certificate-form"]//textarea')})
        fields.update({node.get('name'):(node.xpath('./option[@selected]/@value') or [''])[0]
                       for node in tree.xpath('//form[@id="certificate-form"]//select[@name]')})
        fields['action']='save'
        return url,fields,response

    def test_almond_open_save_reopen_preview_and_print_without_converter(self):
        from app.program_billing import lease_certificate as cert,lease_certificate_form as form
        from app.program_billing import lease_certificate_render as renderer
        self.assertEqual(len(form.tokens(form.layout(3,1))),69)
        self.assertEqual(set(form.tokens(form.layout(3,1))),set(cert.TOKEN.findall(''.join(cert.xml(cert.load_master()).itertext()))))
        for i,name in enumerate(('Synthetic Almond Lessor One','Synthetic Almond Lessor Two','Synthetic Almond Lessor Three'),1):
            self.assertEqual(self.assign(name=name,identity_number='TEST-'+str(i)).status_code,200)
        before=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar())
        url,data,page=self.open_form()
        self.assertEqual(data['PROPERTY_NAME'],'Almond')
        data.update(LESSOR_1_NAME='Synthetic Almond Lessor One',LESSOR_1_IDENTITY_NUMBER='TEST-1',
            LESSOR_2_NAME='Synthetic Almond Lessor Two',LESSOR_2_IDENTITY_NUMBER='TEST-2',
            LESSOR_3_NAME='Synthetic Almond Lessor Three',LESSOR_3_IDENTITY_NUMBER='TEST-3',
            LESSOR_SIGNATORY_NAME='Synthetic Lessor Representative',LESSOR_SIGNATORY_IDENTITY_NUMBER='TEST-4',
            LESSEE_1_LEGAL_NAME='WBH NPC',LESSEE_1_IDENTITY_NUMBER='TEST-5',
            LESSEE_1_SIGNATORY_NAME='Synthetic Lessee Representative',LESSEE_1_SIGNATORY_IDENTITY_NUMBER='TEST-6',
            PROPERTY_ADDRESS='21 Almond Road, Warner Beach, 4126',COMMENCEMENT_DATE='2026-10-01',
            DURATION_MONTHS='6',EXPIRY_DATE='2027-03-31',RENEWAL_NOTICE_DATE='2026-12-31',
            LEASE_REFERENCE='LOCAL-ALMOND-HTML',PAYMENT_DAY_PARTICULARS='')
        with patch.object(cert,'document',side_effect=AssertionError('Interactive workflow used Word')),patch.object(renderer,'render_pdf',side_effect=AssertionError('Interactive workflow used converter')):
            saved=self.client.post(url,data=data)
            self.assertEqual(saved.status_code,302)
            reopened=self.client.get(saved.location)
            self.assertEqual(reopened.status_code,200)
            self.assertIn('Synthetic Almond Lessor Three',reopened.get_data(as_text=True))
            preview=self.client.get(url.replace('/certificate','/view')+'&component=certificate')
            self.assertEqual(preview.status_code,200)
            text=preview.get_data(as_text=True)
            for value in ('Synthetic Almond Lessor One','Synthetic Almond Lessor Two','Synthetic Almond Lessor Three',
                'WBH NPC','Synthetic Lessor Representative','Synthetic Lessee Representative',
                '2026-10-01','2027-03-31','2026-12-31','[MONTHLY_RENT]','[ACCOUNT_NUMBER]','Unconfirmed','window.print()'):
                self.assertIn(value,text)
            tree=html.fromstring(preview.data)
            self.assertEqual(len(tree.xpath('//section[contains(@class,"certificate-section")]')),6)
            self.assertEqual(len(tree.xpath('//article[contains(@class,"certificate-page")]')),2)
            self.assertFalse(tree.xpath('//iframe|//textarea|//form'))
            self.assertEqual(preview.headers['Cache-Control'],'private, no-store')
        after=self.sql('SELECT items FROM bil_lease_draft').scalar()
        for key in ('_lease_history','_generations','sc-01','s1-40'):
            self.assertEqual(after.get(key),before.get(key))
        self.assertEqual(after['_parties']['lessee'],after['_parties']['lessees'][0])
        self.assertNotIn('_ait_standard',after)
        self.assertEqual(self.client.post(url,data=data).status_code,409)
        self.sign_in(8)
        self.assertEqual(self.client.post(url,data=data).status_code,404)

    def test_repeatable_rows_partial_first_save_and_validation_preserves_input(self):
        from app.program_billing import lease_certificate_form as form
        url,data,_=self.open_form(False)
        count=self.sql('SELECT count(*) FROM bil_lease_draft').scalar()
        data.update(action='add_lessee',LESSOR_1_NAME='Unsaved first lessor')
        expanded=self.client.post(url,data=data)
        self.assertEqual(expanded.status_code,200)
        self.assertIn('name="LESSEE_2_LEGAL_NAME"',expanded.get_data(as_text=True))
        self.assertNotIn('Unsaved first lessor',expanded.get_data(as_text=True))
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),count)
        data.update(action='save',lessee_count='2',LESSEE_2_LEGAL_NAME='Additional local Lessee')
        saved=self.client.post(url,data=data)
        self.assertEqual(saved.status_code,302)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),count+1)
        repeat=self.client.post(url,data=data)
        self.assertEqual(repeat.status_code,302)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),count+1)
        page=self.client.get(saved.location).get_data(as_text=True)
        self.assertIn('Additional local Lessee',page)
        self.assertIn('[MONTHLY_RENT]',page)
        url,data,_=self.open_form()
        original=copy.deepcopy(self.session.execute(text('SELECT items FROM bil_lease_draft WHERE id=:id'),{'id':self.draft}).scalar())
        data.update(COMMENCEMENT_DATE='2026-99-99',LESSOR_1_NAME='Retain entered name')
        invalid=self.client.post(url,data=data)
        self.assertEqual(invalid.status_code,400)
        self.assertNotIn('Retain entered name',invalid.get_data(as_text=True))
        self.assertEqual(self.session.execute(text('SELECT items FROM bil_lease_draft WHERE id=:id'),{'id':self.draft}).scalar(),original)
        # Extra parties, long content and HTML are rendered as text, never markup.
        data.update(COMMENCEMENT_DATE='',lessor_count='5',lessee_count='3',
                    LESSOR_5_NAME='Ignore unregistered owner',LESSEE_3_LEGAL_NAME='Third Lessee <script>alert(1)</script>'+' extended '*50)
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        preview=self.client.get(url.replace('/certificate','/view')+'&component=certificate').get_data(as_text=True)
        self.assertIn('Third Lessee',preview)
        self.assertIn('&lt;script&gt;alert(1)&lt;/script&gt;',preview)
        self.assertNotIn('<script>alert(1)</script>',preview)

    def test_linked_bank_explicit_snapshot_and_signed_draft_are_preserved(self):
        self.migrate()
        spec=importlib.util.spec_from_file_location('html_bank_gate',Path('migrations/versions/pm_bank_register_002.py'))
        migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        migration.op=Operations(MigrationContext.configure(self.session.connection()));migration.upgrade()
        self.sql("UPDATE alembic_version SET version_num='pm_bank_register_002' WHERE version_num='pm_lease_payments_001'")
        self.session.commit()
        self.assertEqual(self.account().status_code,302)
        bank=self.sql('SELECT id FROM bil_bank_detail').scalar()
        url,data,page=self.open_form()
        # Register accounts are not selectable until linked to this property.
        data['bank_detail_id']=str(bank)
        self.assertEqual(self.client.post(url,data=data).status_code,400)
        self.assertEqual(self.client.post(self.url(),data={'action':'allocate_existing','bank_detail_id':str(bank)}).status_code,302)
        url,data,_=self.open_form()
        self.assertEqual(data['bank_detail_id'],'')
        data['bank_detail_id']=str(bank)
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        snapshot=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar()['_certificate_banking'])
        self.sql("UPDATE bil_bank_detail SET bank_name='Later register edit'");self.session.commit()
        url,data,_=self.open_form()
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar()['_certificate_banking'],snapshot)
        self.assertEqual(self.signed().status_code,302)
        original=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar())
        url,data,page=self.open_form()
        self.assertIn('read-only',page.get_data(as_text=True))
        data.update(lessor_count='3',lessee_count='1',LESSOR_1_NAME='Must not overwrite signed original')
        self.assertEqual(self.client.post(url,data=data).status_code,409)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),original)
    def test_global_csrf_rejects_missing_token_and_accepts_real_form_token(self):
        from flask import Flask
        from flask_wtf import CSRFProtect
        secure=Flask('certificate-csrf-check')
        secure.config.update(TESTING=True,SECRET_KEY='local-csrf-test')
        secure.jinja_loader=self.app.jinja_loader
        self.app.login_manager.init_app(secure)
        CSRFProtect(secure)
        secure.register_blueprint(self.app.blueprints['billing_bp'])
        client=secure.test_client()
        with client.session_transaction() as session:
            session['_user_id']='7';session['_fresh']=True
        url='/billing/leases/property/26/certificate?draft_id='+str(self.draft)
        page=client.get(url)
        self.assertEqual(page.status_code,200)
        tree=html.fromstring(page.data)
        token=tree.xpath('//input[@name="csrf_token"]/@value')[0]
        before=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar())
        fields={'lessor_count':'3','lessee_count':'1','version':self.version(),'action':'save','LESSOR_1_NAME':'CSRF protected name'}
        self.assertEqual(client.post(url,data=fields).status_code,400)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),before)
        fields['csrf_token']=token
        self.assertEqual(client.post(url,data=fields).status_code,302)

    def test_editing_representatives_never_confirms_authority(self):
        from app.program_billing import lease_certificate_form as form,lease_certificate as cert
        items={'_contracting_lessors':{'identities':[{'name':'Owner'}]},
               '_parties':{'lessor':{'signatory_name':'Existing representative'}},
               '_ait_standard':{'source':{'preserve':'unchanged'},'authorities':{},'generations':[]},
               '_lease_history':{'documents':[{'kind':'unrelated-note','text':'preserve'}]},'sc-01':{'value':'Do not change'}}
        original=copy.deepcopy(items)
        merged=form.merge(items,{'[LESSOR_SIGNATORY_NAME]':'Updated representative',
                                '[LESSOR_SIGNATORY_CAPACITY]':'User supplied capacity',
                                '[LESSOR_AUTHORITY_EVIDENCE]':'User supplied evidence'},1,1)
        self.assertEqual(cert.values(merged)['[LESSOR_AUTHORITY_CONFIRMED]'],'Unconfirmed')
        self.assertEqual(merged['_ait_standard'],original['_ait_standard'])
        self.assertEqual(merged['_lease_history'],original['_lease_history'])
        self.assertEqual(merged['sc-01'],original['sc-01'])
        self.assertEqual(items,original)

    def test_blank_flag_does_not_erase_other_data_or_bypass_issued_lock(self):
        initial={'sc-01':{'value':'Preserved condition'},'_lease_history':{'documents':[]},'_premises':{'excluded_areas':'Keep excluded areas'}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(initial)});self.session.commit()
        url,data,_=self.open_form()
        data['LESSOR_1_NAME']='Partial edit'
        self.assertEqual(self.client.post(url+'&blank=1',data=data).status_code,302)
        saved=self.sql('SELECT items FROM bil_lease_draft').scalar()
        self.assertEqual(saved['sc-01'],initial['sc-01'])
        self.assertEqual(saved['_lease_history'],initial['_lease_history'])
        saved['_generations']=[{'signature_or_issuance_approved':True}]
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(saved)});self.session.commit()
        data['version']=self.version()
        self.assertEqual(self.client.post(url+'&blank=1',data=data).status_code,409)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),saved)

    def test_cleared_bank_selection_cannot_reappear_from_legacy_snapshot(self):
        from app.program_billing import lease_certificate as cert
        items={'_banking_utilities':{'schema_version':2,'property_id':26,'property_accounts':{
            'rental':{'bank_detail_id':99,'bank_name':'Historical explicitly selected bank'}}}}
        self.assertEqual(cert.values(items)['[BANK_NAME]'],'Historical explicitly selected bank')
        items['_certificate_banking']={'schema_version':1,'property_id':26,'bank_detail_id':None}
        self.assertNotIn('[BANK_NAME]',cert.values(items))
