"""Part A reuses current registered owners; no owner writes or issuance actions."""
import copy,io,json,re,unittest
from zipfile import ZipFile
from xml.etree import ElementTree as E
from sqlalchemy import text
import test_pm_lease_parties_simplified as previous
import test_pm_lease_premises_offline as premises
import test_pm_lease_period_offline as period
import test_pm_lease_amounts_offline as financial
import test_pm_lease_part5_offline as utilities

class RegisteredOwnerPartiesTests(unittest.TestCase):
    load_lease_workspace=True
    setUp=previous.SimplifiedPartiesTests.setUp
    sign_in=previous.SimplifiedPartiesTests.sign_in
    sql=previous.SimplifiedPartiesTests.sql
    payload=previous.SimplifiedPartiesTests.payload
    assign=previous.SimplifiedPartiesTests.assign
    create=previous.SimplifiedPartiesTests.create
    stored=previous.SimplifiedPartiesTests.stored
    route=previous.SimplifiedPartiesTests.route
    post=previous.SimplifiedPartiesTests.post
    generate=previous.SimplifiedPartiesTests.generate

    def owners(self,missing_address=False,mixed=False):
        output=[]
        for i,name in enumerate(('Alexandra Jane Example','Benjamin Mark Example','Charlotte Ruth Example'),1):
            result=self.assign(name=name,identity_number=f'SYNTHETIC-OWNER-{i}',type=('company' if i==2 else 'trust' if i==3 else 'individual') if mixed else 'individual',
                residential_address='' if missing_address and i==1 else f'{i} Synthetic Residential Road',postal_address=f'PO BOX {i}',email=f'owner{i}@example.com')
            self.assertEqual(result.status_code,200)
            output.append(next(o for o in result.get_json()['owners'] if o['name']==name))
        self.app.config['PM_OWNER_WRITES_APPROVED']=False
        return output

    def form(self,owner_id,**changes):
        data={'parties_form_version':'3','representative_owner_id':owner_id if isinstance(owner_id,list) else str(owner_id),
            'lessee_party_type':'natural_person','lessee_legal_name':'Synthetic Lessee',
            'lessee_identity_number':'SYNTHETIC-LESSEE','lessee_address':'Synthetic lessee address','lessee_email':'lessee@example.com','lessee_phone':'0100000000'}
        data.update(changes);return data

    def update_items(self,items):
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb)'),{'items':json.dumps(items)});self.session.commit()

    def complete(self,owner_id,**changes):
        self.assertEqual(self.post('parties',self.form(owner_id,**changes)).status_code,302)
        for section,data in [('premises',premises.PremisesOfflineTests.form(self)),('period',period.PeriodOfflineTests.form(self)),
            ('amounts',financial.AmountsOfflineTests.form(self)),('amounts',utilities.Part5OfflineTests.form(self))]:
            self.assertEqual(self.post(section,data).status_code,302)

    def test_load_all_owners_no_duplicate_inputs_and_authoritative_mapping(self):
        owners=self.owners();before=self.sql('SELECT count(*) FROM bil_owner_identity').scalar()
        html=self.client.get(self.route('particulars/parties')).get_data(as_text=True)
        tags=previous.Tags(html)
        checkboxes=[attrs for tag,attrs in tags.tags if attrs.get('name')=='representative_owner_id']
        self.assertEqual(len(checkboxes),3)
        names={attrs.get('name') for _,attrs in tags.tags}
        for forbidden in ('lessor_id','lessor_signatory_name','lessor_signatory_identity_number','lessor_signatory_capacity','lessor_service_address'):self.assertNotIn(forbidden,names)
        self.assertNotIn('Add Owner',html);self.assertNotIn('Remove Owner',html);self.assertNotIn('lease-owner-choice',html)
        for owner in owners:self.assertIn(owner['name'],html);self.assertIn(owner['identity_number'],html)
        data=self.form(owners[0]['owner_id'],lessor_id='999',lessor_signatory_name='FORGED',lessor_signatory_identity_number='FORGED',lessor_signatory_capacity='FORGED',lessor_service_address='FORGED')
        self.assertEqual(self.post('parties',data).status_code,302)
        saved=self.stored();self.assertEqual(len(saved['_contracting_lessors']['identities']),3)
        rep=saved['_parties']['lessor'];self.assertEqual(rep['signatory_name'],owners[0]['name']);self.assertEqual(rep['signatory_identity_number'],owners[0]['identity_number'])
        self.assertEqual(rep['service_address'],'1 Synthetic Residential Road');self.assertEqual(rep['email'],'owner1@example.com')
        self.assertEqual(rep['signatory_capacity'],'');self.assertFalse(rep['capacity_confirmed'])
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),before)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership').scalar(),3)
        self.assertIsNone(self.sql("SELECT to_regclass('pg_temp.bil_tenant')").scalar())

    def test_exactly_one_current_individual_owner_is_required(self):
        owners=self.owners(mixed=True)
        for selected in ('', ['1','2'], '99999',str(owners[1]['owner_id'])):
            response=self.post('parties',self.form(selected));self.assertEqual(response.status_code,400,response.get_data(as_text=True));self.assertEqual(self.stored(),{})
        self.assertEqual(self.post('parties',self.form(owners[0]['owner_id'])).status_code,302)
        self.sign_in(8);self.assertEqual(self.post('parties',self.form(owners[0]['owner_id'])).status_code,404)

    def test_capacity_is_only_a_proposal_until_explicitly_confirmed(self):
        owner=self.owners()[0]
        self.assertEqual(self.post('parties',self.form(owner['owner_id'])).status_code,302)
        saved=self.stored();self.assertEqual(saved['_parties']['lessor']['signatory_capacity'],'')
        self.assertIn('not confirmed',self.workflow_module.party_placeholder_values(saved)['[LANDLORD SIGNATORY NAME AND CAPACITY]'])
        self.assertEqual(self.post('parties',self.form(owner['owner_id'],confirm_representative_capacity='on')).status_code,302)
        saved=self.stored();self.assertEqual(saved['_parties']['lessor']['signatory_capacity'],self.workflow_module.OWNER_CAPACITY_PROPOSAL)
        self.assertTrue(saved['_parties']['lessor']['capacity_confirmed'])
        self.assertNotIn('_generations',saved)
        html=self.client.get(self.route('particulars/parties')).get_data(as_text=True)
        confirmation=next(a for _,a in previous.Tags(html).tags if a.get('id')=='owner-capacity-confirmation');self.assertIn('checked',confirmation)

    def test_missing_residential_address_never_uses_postal_address(self):
        owners=self.owners(missing_address=True)
        self.assertEqual(self.post('parties',self.form(owners[0]['owner_id'])).status_code,302)
        saved=self.stored();self.assertEqual(saved['_parties']['lessor']['service_address'],'')
        value=self.workflow_module.party_placeholder_values(saved)['[S1 LANDLORD SERVICE ADDRESS AND CONTACT PARTICULARS]']
        self.assertTrue(value.startswith('________________'));self.assertNotIn('PO BOX',value)
        self.assertEqual(len(self.workflow_module.parties_generation_warnings(saved)),2)

    def test_missing_owner_id_is_reported_not_invented(self):
        owners=self.owners();self.sql('UPDATE bil_owner_identity SET identity_number=NULL WHERE id='+str(owners[1]['owner_id']));self.session.commit()
        response=self.post('parties',self.form(owners[0]['owner_id']));self.assertEqual(response.status_code,400)
        self.assertIn('missing a legal name or ID',response.get_data(as_text=True));self.assertEqual(self.stored(),{})

    def test_saved_capacity_and_other_parts_preserved_on_conversion(self):
        owners=self.owners();self.complete(owners[0]['owner_id'],confirm_representative_capacity='on');saved=self.stored()
        saved['_parties']['schema_version']=2;saved['_parties']['lessor']['signatory_capacity']='Previously recorded co-owner capacity'
        saved['_parties']['signatures']['landlord_place']='Previously saved signature place';self.update_items(saved)
        self.assertEqual(self.post('parties',self.form(owners[0]['owner_id'])).status_code,302)
        converted=self.stored();self.assertEqual(converted['_parties']['lessor']['signatory_capacity'],'Previously recorded co-owner capacity')
        self.assertEqual(converted['_legacy_parties'],saved['_parties']);self.assertEqual(converted['_legacy_contracting_lessors'],saved['_contracting_lessors'])
        for key in ('_premises','_amounts','_banking_utilities','s1-18','s1-19','s1-20'):self.assertEqual(converted[key],saved[key])
        self.assertEqual(converted['_parties']['signatures'],saved['_parties']['signatures'])
        self.assertEqual(self.post('parties',self.form(owners[1]['owner_id'])).status_code,302)
        self.assertEqual(self.stored()['_parties']['lessor']['signatory_capacity'],'')

    def test_review_word_generation_works_with_missing_capacity_and_address(self):
        owners=self.owners(missing_address=True);self.complete(owners[0]['owner_id'])
        self.assertEqual(self.workflow_module.stage5_completion(self.stored()),[])
        html=self.client.get(self.route('review')).get_data(as_text=True)
        self.assertIn('capacity not confirmed',html);self.assertIn('service address not supplied',html)
        result=self.generate();self.assertEqual(result.status_code,200)
        with ZipFile(io.BytesIO(result.data)) as archive:
            root=E.fromstring(archive.read('word/document.xml'));visible=''.join(root.itertext())
            self.assertIn('INCOMPLETE PARTICULARS',visible);self.assertIn('capacity not confirmed',visible);self.assertIn('service address not supplied',visible)
            self.assertNotIn(self.workflow_module.OWNER_CAPACITY_PROPOSAL,visible);self.assertNotRegex(visible,r'\[[^\[\]]+\]')
            for owner in owners:self.assertIn(owner['name'],visible);self.assertIn(owner['identity_number'],visible)
            self.assertEqual(len(root.findall('.//{http://schemas.openxmlformats.org/wordprocessingml/2006/main}tbl')),3)
        record=self.stored()['_generations'][-1];self.assertTrue(record['incomplete_particulars']);self.assertEqual(len(record['warnings']),2)
        self.assertFalse(record['signature_or_issuance_approved'])
        from pathlib import Path
        target=Path('tmp/pm_owner_reuse_review.docx');target.write_bytes(result.data)

    def test_approved_representative_change_preserves_original_and_flags_review(self):
        owners=self.owners();self.complete(owners[0]['owner_id'],confirm_representative_capacity='on')
        self.assertEqual(self.generate().status_code,200)
        original=self.stored();original['_generations'][-1]['signature_or_issuance_approved']=True;self.update_items(original)
        response=self.post('parties',self.form(owners[1]['owner_id'],confirm_representative_capacity='on'));self.assertEqual(response.status_code,302)
        saved=self.stored()
        for key in ('_parties','_contracting_lessors','_generations','s1-03','s1-04','s1-05'):self.assertEqual(saved[key],original[key])
        request=saved['_representative_change_requests'][-1]
        self.assertTrue(request['addendum_required']);self.assertTrue(request['lessee_notice_required']);self.assertFalse(request['notice_sent']);self.assertFalse(request['addendum_issued'])
        self.assertEqual(request['proposed_representative']['owner_id'],owners[1]['owner_id'])
        self.assertIn('pending review',self.client.get(self.route('particulars/parties')).get_data(as_text=True))
        self.assertFalse(self.calls);self.assertEqual(self.sql('SELECT count(*) FROM bil_lease').scalar(),2)

    def test_review_generation_is_not_misclassified_as_issuance(self):
        owners=self.owners();self.complete(owners[0]['owner_id'],confirm_representative_capacity='on');self.assertEqual(self.generate().status_code,200)
        original=copy.deepcopy(self.stored()['_generations'])
        self.assertEqual(self.post('parties',self.form(owners[1]['owner_id'])).status_code,302)
        self.assertEqual(self.stored()['_parties']['lessor']['owner_id'],owners[1]['owner_id'])
        self.assertEqual(self.stored()['_generations'],original);self.assertNotIn('_representative_change_requests',self.stored())
