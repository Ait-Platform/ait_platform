"""Focused tests for approved Word master, safe mapping and local banking snapshots."""
import copy,importlib.util,io,json,unittest
from pathlib import Path
from zipfile import ZipFile
from unittest.mock import patch
from lxml import etree as ET
from alembic.operations import Operations
from alembic.migration import MigrationContext
import test_pm_bank_register as fixture

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('pure_approved_certificate',ROOT/'app/program_billing/lease_certificate.py')
cert=importlib.util.module_from_spec(spec);spec.loader.exec_module(cert)


def visible(data):
    return '\n'.join(cert.xml(data).itertext())


class WordMappingTests(unittest.TestCase):
    def test_blank_approved_master_preserves_all_package_parts_and_layout(self):
        master=cert.load_master();output,missing=cert.document()
        self.assertIn('[LESSOR_3_NAME]',missing)
        self.assertIn('[LESSEE_1_SIGNATORY_NAME]',missing)
        self.assertNotIn('BANK_DETAIL_ID',visible(output))
        with ZipFile(io.BytesIO(master)) as source,ZipFile(io.BytesIO(output)) as target:
            for name in source.namelist():
                if name!='word/document.xml':self.assertEqual(source.read(name),target.read(name))
        root=cert.xml(output)
        size=root.find('.//{'+cert.W+'}pgSz')
        self.assertEqual((size.get('{'+cert.W+'}w'),size.get('{'+cert.W+'}h')),('11906','16838'))
        self.assertEqual(len(root.findall('.//{'+cert.W+'}tbl')),6)

    def test_representatives_structured_period_and_missing_money(self):
        items={'_contracting_lessors':{'identities':[{'name':'Test Lessor '+str(i),'identity_number':str(i)} for i in (1,2,3)]},
            '_parties':{'schema_version':3,'lessor':{'signatory_name':'Test Landlord Representative','capacity_proposal':'Proposed only'},
                'lessee':{'tenant_id':9,'legal_name':'WBH NPC','party_type':'juristic_person','signatory_name':'Test Tenant Representative'},
                'lessees':[{'tenant_id':9,'legal_name':'WBH NPC','party_type':'juristic_person'}]},
            '_period':{'commencement_date':'2026-01-01','duration_months':12,'expiry_date':'2026-12-31','renewal_notice_date':'2026-09-30'},
            's1-18':{'value':'1990-01-01'},'s1-17':{'value':'99 months'},'_amounts':{}}
        before=copy.deepcopy(items);out,missing=cert.document(items);text=visible(out)
        for name in ('Test Lessor 1','Test Lessor 2','Test Lessor 3','WBH NPC','Test Landlord Representative','Test Tenant Representative'):
            self.assertIn(name,text)
        self.assertIn('Unconfirmed',text);self.assertNotIn('Proposed only',text)
        self.assertIn('2026-01-01',text);self.assertNotIn('1990-01-01',text)
        self.assertIn('[MONTHLY_RENT]',missing);self.assertIn('[ACCOUNT_NUMBER]',missing)
        self.assertEqual(items,before)
        conflict=copy.deepcopy(items);conflict['_parties']['lessees'][0]['tenant_id']=10
        self.assertFalse(cert.selected_tenants(conflict)[0].get('signatory_name'))
        self.assertEqual(cert.values({'s1-17':{'value':'2 years'}})['[DURATION_MONTHS]'],'24')

    def test_additional_parties_long_text_and_explicit_snapshot_only(self):
        items={'_contracting_lessors':{'identities':[{'name':'Long synthetic owner '+('extended name '*18)+str(i)} for i in range(5)]},
            '_parties':{'lessees':[{'legal_name':'Synthetic Tenant '+str(i),'party_type':'juristic_person','signatory_name':'Synthetic Representative '+str(i)} for i in range(3)]},
            '_banking_utilities':{'bank_name':'Legacy free text must not be selected','account_number':'0001111111'}}
        items['_premises']={'property_address':'Synthetic first line\nSynthetic second line'}
        out,missing=cert.document(items);text=visible(out)
        self.assertTrue(cert.xml(out).findall('.//{'+cert.W+'}br'))
        self.assertIn('Synthetic Tenant 2',text);self.assertIn('LESSORS \u2014 5',text)
        self.assertIn('[ACCOUNT_NUMBER]',missing);self.assertNotIn('Legacy free text must not be selected',text)
        items['_certificate_banking']={'bank_detail_id':11,'bank_name':'Selected Snapshot Bank','account_number':'0002222222'}
        out,missing=cert.document(items)
        self.assertIn('Selected Snapshot Bank',visible(out));self.assertNotIn('[ACCOUNT_NUMBER]',missing)


class CertificateRouteTests(unittest.TestCase):
    load_lease_workspace=True
    setUp=fixture.BankRegisterCheck.setUp
    sign_in=fixture.BankRegisterCheck.sign_in
    sql=fixture.BankRegisterCheck.sql
    payload=fixture.BankRegisterCheck.payload
    assign=fixture.BankRegisterCheck.assign
    create=fixture.BankRegisterCheck.create
    version=fixture.BankRegisterCheck.version
    signed=fixture.BankRegisterCheck.signed
    url_history=fixture.BankRegisterCheck.url_history
    ledger=fixture.BankRegisterCheck.ledger
    docx=fixture.BankRegisterCheck.docx
    url=fixture.BankRegisterCheck.url
    migrate=fixture.BankRegisterCheck.migrate
    account=fixture.BankRegisterCheck.account

    def test_blank_view_before_capture_authorization_native_pdf_and_existing_cover_order(self):
        view='/billing/leases/property/26/view'
        response=self.client.get(view);self.assertEqual(response.status_code,200)
        self.assertIn('certificate-form',response.get_data(as_text=True))
        self.assertNotIn('<iframe',response.get_data(as_text=True))
        word=self.client.get('/billing/leases/property/26/certificate.docx')
        self.assertEqual(word.status_code,200);self.assertIn('[MONTHLY_RENT]',visible(word.data))
        from app.program_billing import lease_certificate as loaded
        with patch.object(loaded,'render_pdf',return_value=b'%PDF-1.4\nsynthetic-converter-double') as render:
            response=self.client.get('/billing/leases/property/26/certificate.pdf')
            self.assertEqual(response.status_code,200);self.assertEqual(response.mimetype,'application/pdf')
            self.assertIn('[PROPERTY_NAME]',visible(render.call_args[0][0]))
        # Existing cover is extracted only from the unchanged existing source builder.
        self.sql("UPDATE bil_lease_draft SET items='{\"_ait_standard\":{\"source\":{}}}'::jsonb");self.session.commit()
        from app.program_billing import lease_standard as standard,lease_preview as preview
        with patch.object(standard,'source_master',return_value=({},b'existing-source')),patch.object(standard,'build_document',return_value=b'existing-built'),patch.object(preview,'extract_blocks',return_value=[
            {'kind':'paragraph','text':'EXISTING COVER'},{'kind':'page_break'},{'kind':'paragraph','text':'REFERENCE BODY'}]):
            page=self.client.get(view+'?draft_id='+str(self.draft))
            text=page.get_data(as_text=True)
            self.assertEqual(page.status_code,200)
            self.assertLess(text.index('id="existing-cover"'),text.index('id="approved-certificate"'))
            self.assertNotIn('REFERENCE BODY',text)
        self.sign_in(8)
        self.assertEqual(self.client.get(view).status_code,404)
        self.assertEqual(self.client.get('/billing/leases/property/26/certificate.docx').status_code,404)

    def test_explicit_linked_bank_selection_snapshot_survives_register_edit_and_signed_lease(self):
        self.migrate()
        spec=importlib.util.spec_from_file_location('register_gate',ROOT/'migrations/versions/pm_bank_register_002.py')
        migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        migration.op=Operations(MigrationContext.configure(self.session.connection()));migration.upgrade()
        self.sql("UPDATE alembic_version SET version_num='pm_bank_register_002' WHERE version_num='pm_lease_payments_001'");self.session.commit()
        self.assertEqual(self.account(for_rental='',for_utilities='').status_code,302)
        bank=self.sql('SELECT id FROM bil_bank_detail').scalar()
        data={'action':'allocate_existing','bank_detail_id':str(bank),'use_for_lease':'1','version':self.version()}
        self.assertEqual(self.client.post(self.url(),data=data).status_code,400)
        self.assertEqual(self.client.post(self.url(),data={'action':'allocate_existing','bank_detail_id':str(bank)}).status_code,302)
        self.assertEqual(self.client.post(self.url(),data=data).status_code,302)
        saved=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar())
        snapshot=saved['_certificate_banking']
        self.assertEqual(snapshot['bank_detail_id'],bank)
        self.sql("UPDATE bil_bank_detail SET bank_name='Register changed later'");self.session.commit()
        out,_=cert.document(saved)
        self.assertIn(snapshot['bank_name'],visible(out));self.assertNotIn('Register changed later',visible(out))
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),saved)
        self.assertEqual(self.signed().status_code,302)
        signed=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar())
        self.assertEqual(self.client.post(self.url(),data=dict(data,version=self.version())).status_code,409)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),signed)
