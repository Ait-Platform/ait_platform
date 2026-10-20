"""Sectioned lease capture on temporary local PostgreSQL fixtures."""
import copy,json,importlib.util
from pathlib import Path
from unittest.mock import patch
from sqlalchemy import text
from lxml import html
from alembic.operations import Operations
from alembic.migration import MigrationContext
import test_pm_approved_certificate as existing


class HTMLCertificateTests(existing.CertificateRouteTests):
    def open_form(self,draft=True,section='parties'):
        url='/billing/leases/property/26/certificate/'+section+('?draft_id='+str(self.draft) if draft else '')
        response=self.client.get(url);self.assertEqual(response.status_code,200)
        tree=html.fromstring(response.data)
        fields={node.get('name'):node.get('value','') for node in tree.xpath('//form[@id="certificate-form"]//input[@name and not(ancestor::template)]')}
        fields.update({node.get('name'):node.text or '' for node in tree.xpath('//form[@id="certificate-form"]//textarea[not(ancestor::template)]')})
        fields.update({node.get('name'):(node.xpath('./option[@selected]/@value') or [''])[0] for node in tree.xpath('//form[@id="certificate-form"]//select[@name and not(ancestor::template)]')})
        fields['action']='save'
        return url,fields,response

    def preview_url(self):return '/billing/leases/property/26/view?draft_id='+str(self.draft)+'&component=certificate'

    def bank_setup(self):
        self.migrate()
        spec=importlib.util.spec_from_file_location('capture_bank_gate',Path('migrations/versions/pm_bank_register_002.py'))
        migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        migration.op=Operations(MigrationContext.configure(self.session.connection()));migration.upgrade()
        self.sql("UPDATE alembic_version SET version_num='pm_bank_register_002' WHERE version_num='pm_lease_payments_001'");self.session.commit()

    def test_almond_open_save_reopen_preview_and_print_without_converter(self):
        from app.program_billing import lease_certificate as cert,lease_certificate_form as form,lease_certificate_render as renderer
        self.assertEqual(len(form.tokens(form.layout(3,1))),69)
        self.assertEqual(set(form.tokens(form.layout(3,1))),set(cert.TOKEN.findall(''.join(cert.xml(cert.load_master()).itertext()))))
        for i in (1,2,3):self.assertEqual(self.assign(name='Synthetic Almond Lessor '+str(i),identity_number='TEST-'+str(i)).status_code,200)
        before=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar())
        entries={'parties':{'LESSEE_1_LEGAL_NAME':'WBH NPC','LESSEE_1_SIGNATORY_NAME':'Synthetic tenant representative','LESSOR_SIGNATORY_NAME':'Synthetic lessor representative'},
            'premises':{'PROPERTY_ADDRESS':'21 Almond Road, Warner Beach, 4126'},
            'period':{'COMMENCEMENT_DATE':'2026-10-01','DURATION_MONTHS':'6','EXPIRY_DATE':'2027-03-31','RENEWAL_NOTICE_DATE':'2026-12-31'},
            'financial':{'MONTHLY_RENT':'1000.00'},'banking':{},'additional':{'PAYMENT_DAY_PARTICULARS':'User supplied payment day'}}
        with patch.object(cert,'document',side_effect=AssertionError('Word used')),patch.object(renderer,'render_pdf',side_effect=AssertionError('Converter used')):
            for section,updates in entries.items():
                url,data,response=self.open_form(section=section)
                tree=html.fromstring(response.data)
                self.assertEqual(tree.xpath('//form//button[@type="submit"]/text()'),['Save'])
                self.assertEqual(tree.xpath('//form//div[@class="capture-actions"]/a/text()'),['Back'])
                self.assertFalse(tree.xpath('//iframe|//a[contains(@href,"view")]'))
                data.update(updates)
                self.assertEqual(self.client.post(url,data=data).status_code,302)
                _,reopened,_=self.open_form(section=section)
                for key,value in updates.items():self.assertEqual(reopened[key],value)
                self.assertEqual(self.client.post(url,data=data).status_code,409)
            preview=self.client.get(self.preview_url()).get_data(as_text=True)
            for value in ('WBH NPC','Synthetic tenant representative','Synthetic lessor representative','2026-10-01','2027-03-31','2026-12-31','[ACCOUNT_NUMBER]','Unconfirmed','window.print()'):
                self.assertIn(value,preview)
            for i in (1,2,3):self.assertIn('Synthetic Almond Lessor '+str(i),preview)
        after=self.sql('SELECT items FROM bil_lease_draft').scalar()
        for key in ('_lease_history','_generations','sc-01','s1-40'):self.assertEqual(after.get(key),before.get(key))
        self.sign_in(8);self.assertEqual(self.client.get(self.preview_url()).status_code,404)

    def test_repeatable_rows_partial_first_save_and_validation_preserves_input(self):
        url,data,_=self.open_form(False)
        count=self.sql('SELECT count(*) FROM bil_lease_draft').scalar()
        data.update(lessee_count='2',LESSEE_1_LEGAL_NAME='First local tenant',LESSEE_2_LEGAL_NAME='Second local tenant')
        saved=self.client.post(url,data=data);self.assertEqual(saved.status_code,302)
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),count+1)
        self.assertIn('Second local tenant',self.client.get(saved.location).get_data(as_text=True))
        url,data,_=self.open_form(section='period')
        original=copy.deepcopy(self.session.execute(text('SELECT items FROM bil_lease_draft WHERE id=:d'),{'d':self.draft}).scalar())
        data.update(COMMENCEMENT_DATE='2026-99-99',DURATION_MONTHS='6')
        invalid=self.client.post(url,data=data);self.assertEqual(invalid.status_code,400)
        self.assertEqual(html.fromstring(invalid.data).xpath('//textarea[@name="DURATION_MONTHS"]/text()'),['6'])
        self.assertEqual(self.session.execute(text('SELECT items FROM bil_lease_draft WHERE id=:d'),{'d':self.draft}).scalar(),original)
        url,data,_=self.open_form()
        data.update(lessee_count='3',LESSEE_3_LEGAL_NAME='Third Lessee <script>alert(1)</script>'+' extended '*50)
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        preview=self.client.get(self.preview_url()).get_data(as_text=True)
        self.assertIn('&lt;script&gt;alert(1)&lt;/script&gt;',preview);self.assertNotIn('<script>alert(1)</script>',preview)

    def test_linked_bank_explicit_snapshot_and_signed_draft_are_preserved(self):
        self.bank_setup();self.assertEqual(self.account().status_code,302)
        bank=self.sql('SELECT id FROM bil_bank_detail').scalar()
        url,data,_=self.open_form(section='banking');data['bank_detail_id']=str(bank)
        rejected=self.client.post(url,data=data);self.assertEqual(rejected.status_code,400)
        self.assertNotIn(str(bank),html.fromstring(rejected.data).xpath('//select[@name="bank_detail_id"]/option[@selected]/@value'))
        self.assertEqual(self.client.post(self.url(),data={'action':'allocate_existing','bank_detail_id':str(bank)}).status_code,302)
        url,data,_=self.open_form(section='banking');self.assertEqual(data['bank_detail_id'],'')
        data['bank_detail_id']=str(bank);self.assertEqual(self.client.post(url,data=data).status_code,302)
        snapshot=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar()['_certificate_banking'])
        self.sql("UPDATE bil_bank_detail SET bank_name='Later register edit'");self.session.commit()
        url,data,_=self.open_form(section='period');data['DURATION_MONTHS']='6'
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar()['_certificate_banking'],snapshot)
        # A removed link must retain the saved snapshot rather than select another account.
        self.sql('DELETE FROM bil_property_payment_account');self.session.commit()
        url,data,page=self.open_form(section='banking')
        self.assertIn('Preserve saved account snapshot',page.get_data(as_text=True))
        self.assertEqual(data['bank_detail_id'],str(bank))
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar()['_certificate_banking'],snapshot)
        self.assertEqual(self.signed().status_code,302)
        original=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar())
        url,data,page=self.open_form(section='banking');self.assertIn('read-only',page.get_data(as_text=True))
        self.assertEqual(self.client.post(url,data=data).status_code,409)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),original)

    def test_global_csrf_rejects_missing_token_and_accepts_real_form_token(self):
        from flask import Flask
        from flask_wtf import CSRFProtect
        secure=Flask('section-csrf-check');secure.config.update(TESTING=True,SECRET_KEY='local-csrf-test')
        secure.jinja_loader=self.app.jinja_loader;self.app.login_manager.init_app(secure);CSRFProtect(secure)
        secure.register_blueprint(self.app.blueprints['billing_bp']);client=secure.test_client()
        with client.session_transaction() as session:session['_user_id']='7';session['_fresh']=True
        url='/billing/leases/property/26/certificate/parties?draft_id='+str(self.draft)
        token=html.fromstring(client.get(url).data).xpath('//input[@name="csrf_token"]/@value')[0]
        fields={'lessor_count':'3','lessee_count':'1','version':self.version(),'action':'save','LESSEE_1_LEGAL_NAME':'CSRF protected tenant'}
        before=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar())
        self.assertEqual(client.post(url,data=fields).status_code,400);self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),before)
        fields['csrf_token']=token;self.assertEqual(client.post(url,data=fields).status_code,302)

    def test_editing_representatives_never_confirms_authority(self):
        from app.program_billing import lease_certificate_form as form,lease_certificate as cert
        items={'_contracting_lessors':{'identities':[{'name':'Owner'}]},'_parties':{'lessor':{'signatory_name':'Existing representative'}},'_ait_standard':{'source':{'preserve':'unchanged'},'authorities':{},'generations':[]},'_lease_history':{'documents':[]},'sc-01':{'value':'Do not change'}}
        original=copy.deepcopy(items)
        merged=form.merge_section(items,items,{'[LESSOR_SIGNATORY_NAME]':'Updated representative','[LESSOR_AUTHORITY_EVIDENCE]':'User supplied evidence'},1,1,'parties')
        self.assertEqual(cert.values(merged)['[LESSOR_AUTHORITY_CONFIRMED]'],'Unconfirmed')
        for key in ('_ait_standard','_lease_history','sc-01'):self.assertEqual(merged[key],original[key])
        self.assertEqual(items,original)

    def test_blank_flag_does_not_erase_other_data_or_bypass_issued_lock(self):
        initial={'sc-01':{'value':'Preserved condition'},'_lease_history':{'documents':[]},'_premises':{'excluded_areas':'Keep excluded areas'}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(initial)});self.session.commit()
        url,data,_=self.open_form();data['LESSEE_1_LEGAL_NAME']='Partial edit'
        self.assertEqual(self.client.post(url+'&blank=1',data=data).status_code,302)
        saved=self.sql('SELECT items FROM bil_lease_draft').scalar()
        for key in initial:self.assertEqual(saved[key],initial[key])
        saved['_generations']=[{'signature_or_issuance_approved':True}]
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(saved)});self.session.commit()
        data['version']=self.version();self.assertEqual(self.client.post(url+'&blank=1',data=data).status_code,409)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),saved)

    def test_cleared_bank_selection_cannot_reappear_from_legacy_snapshot(self):
        from app.program_billing import lease_certificate as cert
        items={'_banking_utilities':{'schema_version':2,'property_id':26,'property_accounts':{'rental':{'bank_detail_id':99,'bank_name':'Historical selected bank'}}}}
        self.assertEqual(cert.values(items)['[BANK_NAME]'],'Historical selected bank')
        items['_certificate_banking']={'schema_version':1,'property_id':26,'bank_detail_id':None}
        self.assertNotIn('[BANK_NAME]',cert.values(items))
