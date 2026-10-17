"""Parties-only integration tests on session-temporary local PostgreSQL fixtures."""
import hashlib
import json
import re
import unittest
from pathlib import Path
from unittest.mock import patch
from zipfile import ZipFile
from xml.etree import ElementTree as ET
from sqlalchemy.exc import SQLAlchemyError
import test_pm_lease_workflow as workspace

class StructuredPartiesTests(unittest.TestCase):
    load_lease_workspace=True
    setUp=workspace.LeaseWorkflowTests.setUp
    sign_in=workspace.LeaseWorkflowTests.sign_in
    sql=workspace.LeaseWorkflowTests.sql
    payload=workspace.LeaseWorkflowTests.payload
    assign=workspace.LeaseWorkflowTests.assign
    create=workspace.LeaseWorkflowTests.create
    stored=workspace.LeaseWorkflowTests.stored
    route=workspace.LeaseWorkflowTests.route

    def details(self, **overrides):
        owner=self.assign(name='Individual Owner',identity_number='OWNER-1').get_json()['owners'][0]
        data=dict(draft_id=str(self.draft),version=self.sql('SELECT version FROM bil_lease_draft').scalar(),
            parties_form_version='1',lessor_id=str(owner['owner_id']),confirm_lessors='on',
            lessor_signatory_name='Independent Agent',lessor_signatory_capacity='Authorised agent',
            lessor_service_address='Nominated lessor service address',lessor_email='agent@example.com',lessor_phone='01234',
            lessee_legal_name='Individual Lessee',lessee_entity_type='individual',lessee_identity_number='LESSEE-ID-1',
            lessee_email='lessee@example.com',lessee_phone='05678',lessee_physical_address='Lessee physical address',
            lessee_service_address='Different lessee service address',lessee_signatory_name='Individual Lessee',
            lessee_signatory_capacity='Personal capacity',signatures_landlord_place='',signatures_landlord_date='',
            signatures_tenant_place='',signatures_tenant_date='')
        data.update(overrides);return data

    def post(self,data):return self.client.post('/billing/leases/property/26/particulars/parties',data=data)

    def test_individual_lessors_lessee_save_reload_mapping_and_blank_signatures(self):
        data=self.details();before=self.sql('SELECT count(*) FROM bil_owner_identity').scalar()
        self.assertEqual(self.post(data).status_code,302)
        stored=self.stored();self.assertEqual(stored['_parties']['lessee']['entity_type'],'individual')
        self.assertEqual(stored['_parties']['lessor']['signatory_name'],'Independent Agent')
        values=self.workflow_module.party_placeholder_values(stored)
        self.assertEqual(values['[S1 LEGAL LANDLORD(S)]'],'Individual Owner')
        self.assertEqual(values['[LANDLORD SIGNATORY NAME AND CAPACITY]'],'Independent Agent - Authorised agent')
        self.assertEqual(values['[TENANT REGISTRATION OR ID]'],'LESSEE-ID-1')
        self.assertIn('Different lessee service address',values['[S1 TENANT SERVICE ADDRESS AND CONTACT PARTICULARS]'])
        self.assertNotIn('Lessee physical address',values['[S1 TENANT SERVICE ADDRESS AND CONTACT PARTICULARS]'])
        for token in self.workflow_module.SIGNATURE_TOKENS:self.assertEqual(values[token],'')
        self.assertEqual(stored['s1-41']['value'],'')
        html=self.client.get(self.route('particulars/parties')).get_data(as_text=True)
        for value in ('Independent Agent','LESSEE-ID-1','Lessee physical address','Different lessee service address'):
            self.assertIn(value,html)
        self.assertEqual(html.count('<dialog'),1)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),before)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease').scalar(),2)
        # These fixtures deliberately contain no tenant or sectional-unit tables.
        self.assertIsNone(self.sql("SELECT to_regclass('pg_temp.bil_tenant')").scalar())
        self.assertIsNone(self.sql("SELECT to_regclass('pg_temp.bil_sectional_unit')").scalar())

    def test_multiple_lessors_preserve_existing_identity_snapshots(self):
        data=self.details()
        second=self.assign(name='Trust Owner',type='trust',identity_number='TRUST-2').get_json()['owners'][-1]
        data['lessor_id']=[data['lessor_id'],str(second['owner_id'])]
        self.assertEqual(self.post(data).status_code,302)
        original=self.stored()['_contracting_lessors']
        self.assertEqual(len(original['identities']),2)
        self.assertEqual(self.workflow_module.party_placeholder_values(self.stored())['[S1 LEGAL LANDLORD(S)]'],'Individual Owner\nTrust Owner')
        self.sql("UPDATE bil_owner_identity SET email='new@example.com' WHERE identity_number='OWNER-1'");self.session.commit()
        data['version']=2;data['lessee_phone']='0999'
        self.assertEqual(self.post(data).status_code,302)
        self.assertEqual(self.stored()['_contracting_lessors'],original)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership').scalar(),2)

    def test_company_npc_trust_and_other_lessees_keep_legal_party_and_signatory_separate(self):
        data=self.details(lessee_legal_name='Entity Lessee',lessee_signatory_name='Entity Representative',lessee_signatory_capacity='Authorised representative')
        for kind in ('company','npc','trust','other'):
            data.update(lessee_entity_type=kind,version=self.sql('SELECT version FROM bil_lease_draft').scalar())
            self.assertEqual(self.post(data).status_code,302)
            stored=self.stored();self.assertEqual(stored['_parties']['lessee']['entity_type'],kind)
            mapping=self.workflow_module.party_placeholder_values(stored)
            self.assertEqual(mapping['[S1 LEGAL TENANT]'],'Entity Lessee')
            self.assertEqual(mapping['[TENANT SIGNATORY NAME AND CAPACITY]'],'Entity Representative - Authorised representative')

    def test_missing_registration_dates_lengths_and_email_fail_without_persistence(self):
        data=self.details()
        for overrides in ({'lessee_identity_number':''},{'lessee_entity_type':'npc','lessee_identity_number':''},{'lessee_entity_type':'company','lessee_identity_number':''},{'lessee_entity_type':'invalid'},
                          {'signatures_landlord_date':'2026-02-30'},{'lessee_email':'invalid'},
                          {'lessee_legal_name':'X'*201},{'lessor_signatory_name':''}):
            with self.subTest(overrides=overrides):
                response=self.post(dict(data,**overrides));self.assertEqual(response.status_code,400)
                self.assertEqual(self.stored(),{})
                html=response.get_data(as_text=True);self.assertIn('role="alert"',html)
                self.assertIn('Different lessee service address',html)
        self.assertIn('identity number is required',self.post(dict(data,lessee_identity_number='')).get_data(as_text=True))

    def test_owner_read_with_writes_disabled_does_not_allow_owner_edits(self):
        data=self.details();owner_id=int(data['lessor_id'])
        self.app.config['PM_OWNER_WRITES_APPROVED']=False
        html=self.client.get(self.route('particulars/parties')).get_data(as_text=True)
        self.assertIn('Individual Owner',html)
        self.assertEqual(self.post(data).status_code,302)
        response=self.client.patch(f'/billing/property-owner/owners/{owner_id}',json={'name':'Unauthorized overwrite'})
        self.assertEqual(response.status_code,503)
        self.assertEqual(self.sql('SELECT display_name FROM bil_owner_identity').scalar(),'Individual Owner')
        self.assertIs(self.app.config['PM_OWNER_WRITES_APPROVED'],False)

    def test_authorization_and_reassigned_property(self):
        data=self.details()
        self.assertEqual(self.client.post('/billing/leases/property/27/particulars/parties',data=data).status_code,404)
        self.assertEqual(self.post(dict(data,lessor_id='99999')).status_code,400)
        self.assertEqual(self.client.get(self.route('particulars/parties').replace('/26/','/27/')).status_code,404)
        self.sql('UPDATE bil_property SET manager_id=8 WHERE id=26');self.session.commit()
        self.assertEqual(self.post(data).status_code,404)
        self.assertEqual(self.stored(),{})

    def test_anonymous_and_inactive_users_cannot_open_or_save_parties(self):
        data=self.details()
        self.sql('UPDATE "user" SET is_active=0 WHERE id=7');self.session.commit()
        self.assertEqual(self.client.get(self.route('particulars/parties')).status_code,403)
        self.assertEqual(self.post(data).status_code,403)
        with self.client.session_transaction() as session:session.clear()
        self.assertEqual(self.client.get(self.route('particulars/parties')).status_code,403)
        self.assertEqual(self.post(data).status_code,403)
        self.assertEqual(self.stored(),{})

    def test_readiness_and_schema_fail_closed_with_useful_reasons(self):
        data=self.details();self.app.config['PM_LEASE_PARTICULARS_WRITES_APPROVED']=False
        response=self.post(data);self.assertEqual(response.status_code,503)
        self.assertEqual(self.stored(),{})
        html=self.client.get(self.route('particulars/parties')).get_data(as_text=True)
        self.assertIn('Structured lease particulars are not approved',html);self.assertIn('<fieldset disabled',html)
        self.app.config['PM_LEASE_PARTICULARS_WRITES_APPROVED']=True
        self.sql("DELETE FROM alembic_version WHERE version_num='pm_owner_002'");self.session.commit()
        html=self.client.get(self.route('particulars/parties')).get_data(as_text=True)
        self.assertIn('ownership migration history is not verified',html)
        self.assertNotIn('Individual Owner',html)

    def test_save_preserves_parts_two_to_five_and_failed_commit_rolls_back(self):
        data=self.details()
        untouched={'s1-10':{'value':'Saved premises'},'s1-18':{'value':'2026-01-01'},'s1-21':{'value':'Saved rent'},
                   'sc-01':{'value':'Saved condition'},'annexure-a':{'value':'Saved plan'},'_additional_clauses':[{'number':29,'text':'Saved condition'}]}
        self.session.execute(__import__('sqlalchemy').text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb)'),{'items':json.dumps(untouched)});self.session.commit()
        with patch.object(self.session,'commit',side_effect=SQLAlchemyError('simulated commit failure')):
            self.assertEqual(self.post(data).status_code,503)
        self.assertEqual(self.stored(),untouched)
        self.assertEqual(self.post(data).status_code,302)
        stored=self.stored()
        for key,value in untouched.items():self.assertEqual(stored[key],value)

    def test_signature_details_exact_mapping_and_master_unchanged(self):
        data=self.details(signatures_landlord_place='Durban',signatures_landlord_date='2026-11-01',
                          signatures_tenant_place='Pinetown',signatures_tenant_date='2026-11-02')
        master=Path(workspace.properties.ROOT)/'app/program_billing/private_templates/Lease_Master_Stage5_Review.docx'
        before=hashlib.sha256(master.read_bytes()).hexdigest()
        self.assertEqual(before,self.workflow_module.PARTIES_MASTER_SHA256)
        self.assertEqual(self.post(data).status_code,302)
        mapping=self.workflow_module.party_placeholder_values(self.stored())
        self.assertEqual(mapping['[LANDLORD SIGNATURE PLACE]'],'Durban')
        self.assertEqual(mapping['[LANDLORD SIGNATURE DATE]'],'2026-11-01')
        self.assertEqual(mapping['[TENANT SIGNATURE PLACE]'],'Pinetown')
        self.assertEqual(mapping['[TENANT SIGNATURE DATE]'],'2026-11-02')
        xml=ET.fromstring(ZipFile(master).read('word/document.xml'));n={'w':'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}
        placeholders=set(re.findall(r'\[[^\[\]]+\]',''.join(x.text or '' for x in xml.findall('.//w:t',n))))
        self.assertTrue(set(mapping)<=placeholders);self.assertEqual(len(mapping),11)
        self.assertEqual(hashlib.sha256(master.read_bytes()).hexdigest(),before)
