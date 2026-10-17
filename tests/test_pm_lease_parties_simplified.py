"""Revised Part A on disposable PostgreSQL; old drafts and master stay compatible."""
import copy,hashlib,io,json,re,unittest
from pathlib import Path
from zipfile import ZipFile
from xml.etree import ElementTree as ET
from html.parser import HTMLParser
from unittest.mock import patch
from sqlalchemy.exc import SQLAlchemyError
import test_pm_lease_database_workflow as database
import test_pm_lease_parties_structured as legacy
import test_pm_lease_workflow as workspace

class Tags(HTMLParser):
    def __init__(self,html):super().__init__();self.tags=[];self.feed(html)
    def handle_starttag(self,tag,attrs):self.tags.append((tag,dict(attrs)))

class SimplifiedPartiesTests(unittest.TestCase):
    load_lease_workspace=True
    setUp=workspace.LeaseWorkflowTests.setUp
    sign_in=workspace.LeaseWorkflowTests.sign_in
    sql=workspace.LeaseWorkflowTests.sql
    payload=workspace.LeaseWorkflowTests.payload
    assign=workspace.LeaseWorkflowTests.assign
    create=workspace.LeaseWorkflowTests.create
    stored=workspace.LeaseWorkflowTests.stored
    route=workspace.LeaseWorkflowTests.route
    post=database.DatabaseLeaseWorkflowTests.post
    complete=database.DatabaseLeaseWorkflowTests.complete
    generate=database.DatabaseLeaseWorkflowTests.generate

    def owner(self,name='First Owner',identity='SYNTHETIC-OWNER-1',kind='individual'):
        result=self.assign(name=name,identity_number=identity,type=kind)
        self.assertEqual(result.status_code,200,result.get_data(as_text=True))
        return next(o for o in result.get_json()['owners'] if o['name']==name)

    def form(self,ids=None,**changes):
        if ids is None:ids=[str(self.owner()['owner_id'])]
        data={'parties_form_version':'2','lessor_id':ids,
              'lessor_signatory_name':'Synthetic Representative','lessor_signatory_identity_number':'SYNTHETIC-REP-1',
              'lessor_signatory_capacity':'Authorised representative','lessor_service_address':'Synthetic lessor notices address',
              'lessee_party_type':'natural_person','lessee_legal_name':'Synthetic Natural Lessee','lessee_identity_number':'SYNTHETIC-PERSON-1',
              'lessee_address':'Synthetic lessee address','lessee_email':'lessee@example.com','lessee_phone':'0100000000'}
        data.update(changes);return data

    def test_one_owner_and_natural_lessee_save_reopen_without_representative(self):
        data=self.form();data.update(lessee_signatory_name='Ignored representative',lessee_signatory_identity_number='IGNORED-ID',lessee_signatory_capacity='Ignored capacity');before=self.sql('SELECT count(*) FROM bil_owner_identity').scalar()
        self.app.config['PM_OWNER_WRITES_APPROVED']=False
        self.assertEqual(self.post('parties',data).status_code,302)
        saved=self.stored();self.assertEqual(saved['_parties']['schema_version'],2)
        self.assertEqual(len(saved['_contracting_lessors']['identities']),1)
        self.assertEqual(saved['_parties']['lessee']['signatory_name'],'')
        mapping=self.workflow_module.party_placeholder_values(saved)
        self.assertIn('First Owner - ID / registration number: SYNTHETIC-OWNER-1',mapping['[S1 LEGAL LANDLORD(S)]'])
        self.assertIn('SYNTHETIC-REP-1',mapping['[LANDLORD SIGNATORY NAME AND CAPACITY]'])
        self.assertEqual(mapping['[TENANT SIGNATORY NAME AND CAPACITY]'],'Synthetic Natural Lessee - In personal capacity')
        html=self.client.get(self.route('particulars/parties')).get_data(as_text=True);tags=Tags(html)
        rep=next(attrs for tag,attrs in tags.tags if attrs.get('id')=='lessee-representative-fields')
        self.assertIn('hidden',rep);self.assertIn('disabled',rep)
        for text in ('Full legal name','ID / registration number','Representative','Natural person','Juristic person','Synthetic Natural Lessee'):self.assertIn(text,html)
        names=[attrs.get('name') for tag,attrs in tags.tags]
        self.assertNotIn('confirm_lessors',names)
        self.assertFalse(any(name and name.startswith('signatures_') for name in names))
        self.assertNotIn('Lessor signatory and nominated service details',html)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),before)
        self.assertIsNone(self.sql("SELECT to_regclass('pg_temp.bil_tenant')").scalar())

    def test_three_owners_add_remove_and_owner_as_representative(self):
        owners=[self.owner(),self.owner('Second Owner','SYNTHETIC-OWNER-2','company'),self.owner('Third Owner','SYNTHETIC-OWNER-3','trust')]
        ids=[str(o['owner_id']) for o in owners]
        data=self.form(ids,lessor_signatory_name=owners[0]['name'],lessor_signatory_identity_number=owners[0]['identity_number'],lessor_signatory_capacity='Owner acting for all lessors')
        self.assertEqual(self.post('parties',data).status_code,302)
        saved=self.stored();self.assertEqual(len(saved['_contracting_lessors']['identities']),3)
        mapping=self.workflow_module.party_placeholder_values(saved)
        for owner in owners:
            self.assertIn(owner['name'],mapping['[S1 LEGAL LANDLORD(S)]']);self.assertIn(owner['identity_number'],mapping['[S1 LEGAL LANDLORD(S)]'])
        self.assertIn(owners[0]['identity_number'],mapping['[LANDLORD SIGNATORY NAME AND CAPACITY]'])
        data['lessor_id']=[ids[0],ids[2]]
        self.assertEqual(self.post('parties',data).status_code,302)
        self.assertEqual(len(self.stored()['_contracting_lessors']['identities']),2)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership').scalar(),3)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),3)
        data['lessor_id']=ids;self.assertEqual(self.post('parties',data).status_code,302)
        self.assertEqual(len(self.stored()['_contracting_lessors']['identities']),3)

    def test_juristic_lessee_has_distinct_representative(self):
        data=self.form(lessee_party_type='juristic_person',lessee_legal_name='Synthetic Entity',lessee_identity_number='SYNTHETIC-REG-1',
            lessee_signatory_name='Synthetic Entity Representative',lessee_signatory_identity_number='SYNTHETIC-ENTITY-REP-1',lessee_signatory_capacity='Director')
        self.assertEqual(self.post('parties',data).status_code,302)
        mapping=self.workflow_module.party_placeholder_values(self.stored())
        self.assertEqual(mapping['[S1 LEGAL TENANT]'],'Synthetic Entity')
        self.assertEqual(mapping['[TENANT SIGNATORY NAME AND CAPACITY]'],'Synthetic Entity Representative (ID number: SYNTHETIC-ENTITY-REP-1) - Director')
        html=self.client.get(self.route('particulars/parties')).get_data(as_text=True);tags=Tags(html)
        rep=next(attrs for _,attrs in tags.tags if attrs.get('id')=='lessee-representative-fields')
        self.assertNotIn('hidden',rep);self.assertNotIn('disabled',rep)
        self.assertIn('Registered entity name',html);self.assertIn('Registration number',html)
        self.assertIn('SYNTHETIC-ENTITY-REP-1',html)

    def test_saved_v1_company_npc_trust_details_and_signatures_are_preserved(self):
        data=legacy.StructuredPartiesTests.details(self,lessee_entity_type='npc',lessee_legal_name='Existing NPC',
            signatures_landlord_place='Saved place',signatures_landlord_date='2026-11-01',signatures_tenant_place='Other saved place',signatures_tenant_date='2026-11-02')
        self.assertEqual(self.post('parties',data).status_code,302);old=copy.deepcopy(self.stored()['_parties'])
        original=self.stored()['_contracting_lessors']
        for kind in ('npc','company','trust'):
            current=self.stored();current['_parties']['lessee']['entity_type']=kind
            self.session.execute(__import__('sqlalchemy').text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb)'),{'items':json.dumps(current)});self.session.commit()
            revised=self.form([data['lessor_id']],lessor_service_address=old['lessor']['service_address'],lessee_party_type='juristic_person',lessee_legal_name='Existing NPC',
                lessee_identity_number=old['lessee']['identity_number'],lessee_address=old['lessee']['service_address'],
                lessee_signatory_name=old['lessee']['signatory_name'],lessee_signatory_identity_number='SYNTHETIC-NEW-REP-ID',lessee_signatory_capacity=old['lessee']['signatory_capacity'])
            self.assertEqual(self.post('parties',revised).status_code,302)
            saved=self.stored();self.assertEqual(saved['_parties']['lessee']['entity_type'],kind)
            self.assertEqual(saved['_parties']['signatures'],old['signatures'])
            self.assertEqual(saved['_parties']['lessor']['email'],old['lessor']['email'])
            self.assertEqual(saved['_parties']['lessor']['phone'],old['lessor']['phone'])
            self.assertEqual(saved['_parties']['lessor']['service_address'],old['lessor']['service_address'])
            self.assertEqual(saved['_contracting_lessors'],original)
            self.assertEqual(saved['_legacy_parties'],old)

    def test_missing_ids_invalid_owner_and_validation_retain_details(self):
        data=self.form()
        for changes in ({'lessor_signatory_identity_number':''},{'lessee_identity_number':''},{'lessor_id':[]},
            {'lessee_party_type':'juristic_person'},{'lessee_party_type':'unknown'},{'lessee_email':'invalid'},
            {'lessee_party_type':'juristic_person','lessee_signatory_name':'Person','lessee_signatory_capacity':'Director','lessee_signatory_identity_number':''}):
            response=self.post('parties',dict(data,**changes));self.assertEqual(response.status_code,400,response.get_data(as_text=True))
            self.assertEqual(self.stored(),{});self.assertIn('Synthetic lessee address',response.get_data(as_text=True))
        self.assertEqual(self.post('parties',dict(data,lessor_id=['99999'])).status_code,400)
        self.assertEqual(self.post('parties',dict(data,lessor_id=[data['lessor_id'][0]]*2)).status_code,400)
        self.sql("UPDATE bil_owner_identity SET identity_number=NULL");self.session.commit()
        response=self.post('parties',data);self.assertEqual(response.status_code,400);self.assertIn('Every selected owner requires',response.get_data(as_text=True))

    def test_authorization_disabled_flag_and_transaction_rollback(self):
        data=self.form()
        self.assertEqual(self.client.post('/billing/leases/property/27/particulars/parties',data=dict(data,draft_id=str(self.draft),version='1')).status_code,404)
        with patch.object(self.session,'commit',side_effect=SQLAlchemyError('synthetic rollback')):
            self.assertEqual(self.post('parties',data).status_code,503)
        self.assertEqual(self.stored(),{})
        self.app.config['PM_LEASE_PARTICULARS_WRITES_APPROVED']=False
        self.assertEqual(self.post('parties',data).status_code,503)
        self.app.config['PM_LEASE_PARTICULARS_WRITES_APPROVED']=True
        self.sign_in(8);self.assertEqual(self.post('parties',data).status_code,404)

    def word_case(self,blank_address,juristic):
        self.complete();before=copy.deepcopy(self.stored())
        # Restore the test-only owner-write permission solely to prepare synthetic owners.
        self.app.config['PM_OWNER_WRITES_APPROVED']=True
        owners=[self.owner(),self.owner('Second Owner','SYNTHETIC-OWNER-2','company'),self.owner('Third Owner','SYNTHETIC-OWNER-3','trust')]
        data=self.form([str(o['owner_id']) for o in owners],lessor_service_address='' if blank_address else 'Confirmed synthetic notices address')
        if juristic:data.update(lessee_party_type='juristic_person',lessee_legal_name='Synthetic Juristic Lessee',lessee_identity_number='SYNTHETIC-ENTITY-REG',lessee_signatory_name='Synthetic Director',lessee_signatory_identity_number='SYNTHETIC-DIRECTOR-ID',lessee_signatory_capacity='Director')
        self.app.config['PM_OWNER_WRITES_APPROVED']=False
        self.assertEqual(self.post('parties',data).status_code,302)
        saved=self.stored()
        for key,value in before.items():
            if key not in {'_parties','_contracting_lessors','s1-03','s1-04','s1-05','s1-06','s1-07','s1-08','s1-09','s1-41'}:self.assertEqual(saved[key],value)
        self.assertEqual(self.workflow_module.stage5_completion(saved),[])
        html=self.client.get(self.route('review')).get_data(as_text=True)
        if blank_address:self.assertIn('Lessor service address not supplied',html);self.assertIn('INCOMPLETE PARTICULARS',html)
        response=self.generate();self.assertEqual(response.status_code,200,response.get_data(as_text=True) if response.status_code != 200 else '')
        with ZipFile(io.BytesIO(response.data)) as archive:
            root=ET.fromstring(archive.read('word/document.xml'));visible=''.join(root.itertext())
            self.assertNotRegex(visible,r'\[[^\[\]]+\]');self.assertEqual(len(root.findall('.//{http://schemas.openxmlformats.org/wordprocessingml/2006/main}tbl')),3)
            for owner in owners:self.assertIn(owner['name'],visible);self.assertIn(owner['identity_number'],visible)
            self.assertIn('SYNTHETIC-REP-1',visible)
            self.assertIn('DRAFT \u2014 INCOMPLETE PARTICULARS' if blank_address else 'DRAFT \u2014 FOR REVIEW',visible)
            if blank_address:self.assertIn('Lessor service address not supplied \u2014 complete before signing.',visible)
            if juristic:self.assertIn('SYNTHETIC-DIRECTOR-ID',visible)
            else:self.assertIn('Synthetic Natural Lessee - In personal capacity',visible)
            headings=[]
            for paragraph in root.iter('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}p'):
                match=re.match(r'^(\d+)\.?\s+([A-Z][A-Z &\x27,/-]*)$',''.join(paragraph.itertext()).strip())
                if match and 1<=int(match[1])<=28:headings.append(int(match[1]))
            self.assertEqual(headings,list(range(1,29)))
        master=Path(workspace.properties.ROOT)/'app/program_billing/private_templates/Lease_Master_Stage5_Review.docx'
        self.assertEqual(hashlib.sha256(master.read_bytes()).hexdigest(),self.workflow_module.PARTIES_MASTER_SHA256)
        record=self.stored()['_generations'][-1];self.assertEqual(record['incomplete_particulars'],blank_address)
        self.assertEqual(len(record['merged_values']),37)
        self.assertEqual(len(record['warnings']),1 if blank_address else 0)
        output=Path(workspace.properties.ROOT)/'output/pm_part_a_review';output.mkdir(parents=True,exist_ok=True)
        name='Lease_PartA_Natural_Incomplete.docx' if blank_address else 'Lease_PartA_Juristic_Complete.docx'
        (output/name).write_bytes(response.data)

    def test_word_natural_lessee_blank_address_explicitly_marked_incomplete(self):self.word_case(True,False)
    def test_word_juristic_lessee_complete_address_and_all_parts_preserved(self):self.word_case(False,True)
