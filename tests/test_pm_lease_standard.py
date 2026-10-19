"""Local AIT standard development workspace; synthetic R01 and private R2 doubles."""
import copy,hashlib,io,json,unittest
from zipfile import ZipFile
from lxml import html
from sqlalchemy import event,text
from unittest.mock import patch
import test_pm_property_lease_master as fixture


class StandardLeaseTests(fixture.PropertyLeaseMasterTests):
    def standard_url(self,suffix=''):
        return '/billing/leases/property/26/standard'+suffix+'?draft_id='+str(self.draft)

    def reference(self,label='Reference one'):
        with ZipFile(io.BytesIO(self.cover_master(label))) as archive:
            entries={name:archive.read(name) for name in archive.namelist()}
        clauses=''.join('<w:p><w:r><w:t>'+str(i)+'. Clause '+str(i)+' unchanged.</w:t></w:r></w:p>' for i in range(1,29))
        entries['word/document.xml']=entries['word/document.xml'].replace(b'</w:body>',clauses.encode()+b'</w:body>')
        stream=io.BytesIO()
        with ZipFile(stream,'w') as archive:
            for name,data in entries.items():archive.writestr(name,data)
        return stream.getvalue()

    def start(self):
        payload=self.reference()
        self.assertEqual(self.upload(payload,'Lease_Review_Draft_R01.docx').status_code,302)
        return payload

    def standard_state(self):
        return self.sql('SELECT items FROM bil_lease_draft').scalar()['_ait_standard']

    def acknowledge(self,**extra):
        data={'acknowledge':'on','version':str(self.sql('SELECT version FROM bil_lease_draft').scalar())}
        data.update(extra)
        return self.client.post(self.standard_url(),data=data)

    def test_disclaimer_required_saved_and_reopened_with_all_mandatory_schedule(self):
        self.start()
        before=self.sql('SELECT items FROM bil_lease_draft').scalar()
        response=self.client.post(self.standard_url(),data={'version':'1'})
        self.assertEqual(response.status_code,400)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),before)
        self.assertEqual(self.acknowledge().status_code,302)
        state=self.standard_state()
        self.assertEqual(state['source']['version'],1)
        self.assertEqual(state['applicability']['excluded'],[])
        self.assertEqual(state['acknowledgement']['by'],7)
        self.assertEqual(len(state['acknowledgements']),1)
        self.assertEqual(state['status'],'draft')
        page=self.client.get(self.standard_url());self.assertEqual(page.status_code,200)
        self.assertIn('Independent legal advice should be obtained where appropriate.',page.get_data(as_text=True))
        self.assertIn('All source clauses are mandatory.',page.get_data(as_text=True))
        tree=html.fromstring(page.get_data(as_text=True))
        self.assertTrue(tree.xpath('//button[@disabled and contains(text(),"Ready for Signature")]'))

    def test_view_and_word_use_same_deterministic_builder_preserve_28_clauses_and_reference(self):
        original=self.start();self.assertEqual(self.acknowledge().status_code,302)
        from app.program_billing import lease_standard as standard
        state=self.standard_state();items=self.sql('SELECT items FROM bil_lease_draft').scalar()
        with self.app.app_context():source,payload=standard.source_master(26,state['source'])
        output=standard.build_document(items,state,source,payload)
        original_hash=hashlib.sha256(original).hexdigest()
        visible='\n'.join(standard.preview.block_text(block) for block in standard.preview.extract_blocks(output))
        for heading in ('AIT Standard Lease','Lease Certificate','AIT Core Terms and Conditions','Clause Applicability Schedule'):
            self.assertIn(heading,visible)
        for i in range(1,29):self.assertIn(str(i)+'. Clause '+str(i)+' unchanged.',visible)
        self.assertIn('[UNKNOWN AUTHORITY]',visible)
        statements=[]
        def capture(conn,cursor,statement,parameters,context,executemany):statements.append(statement)
        event.listen(self.conn,'before_cursor_execute',capture)
        try:view=self.client.get(self.standard_url('/view'))
        finally:event.remove(self.conn,'before_cursor_execute',capture)
        self.assertEqual(view.status_code,200)
        self.assertFalse(any(s.lstrip().upper().startswith(('INSERT','UPDATE','DELETE','CREATE','ALTER','DROP')) for s in statements))
        response=self.client.post(self.standard_url('/word'),data={'version':str(self.sql('SELECT version FROM bil_lease_draft').scalar())})
        self.assertEqual(response.status_code,200);self.assertEqual(response.data,output)
        self.assertEqual(self.standard_state()['generations'][0]['output_sha256'],hashlib.sha256(output).hexdigest())
        self.assertFalse(self.standard_state()['generations'][0]['signature_or_issuance_approved'])
        self.assertEqual(self.standard_state()['generations'][0]['particulars_snapshot'],{k:v for k,v in items.items() if k not in ('_ait_standard','_generations')})
        with self.app.app_context():self.assertEqual(standard.source_master(26,state['source'])[1],original)
        self.assertEqual(hashlib.sha256(original).hexdigest(),original_hash)
        # Retain one local synthetic artifact for Word/visual review; no client data.
        artifact=fixture.ROOT/'tmp'/'ait-standard-synthetic-DRAFT.docx';artifact.parent.mkdir(exist_ok=True)
        artifact.write_bytes(output)

    def test_pin_survives_later_upload_and_no_other_master_is_substituted(self):
        self.start();self.assertEqual(self.acknowledge().status_code,302)
        self.assertEqual(self.upload(self.reference('Reference two'),'Lease_Review_Draft_R01.docx').status_code,302)
        view=self.client.get(self.standard_url('/view'));self.assertEqual(view.status_code,200)
        self.assertIn('Reference one',view.get_data(as_text=True));self.assertNotIn('Reference two',view.get_data(as_text=True))
        self.sql("UPDATE bil_property_lease_master SET state='failed',verified_at=NULL WHERE version=1");self.session.commit()
        self.assertEqual(self.client.get(self.standard_url('/view')).status_code,503)

    def test_exclusions_and_signature_readiness_cannot_be_forged(self):
        self.start()
        self.assertEqual(self.acknowledge(exclude_clauses=['11']).status_code,400)
        self.assertEqual(self.acknowledge(request_ready='on').status_code,400)
        self.assertEqual(self.acknowledge().status_code,302)
        from app.program_billing import lease_standard as standard
        state=self.standard_state();items=self.sql('SELECT items FROM bil_lease_draft').scalar()
        with self.app.app_context():source,payload=standard.source_master(26,state['source'])
        state['applicability']['excluded']=['34']
        with self.assertRaisesRegex(ValueError,'All clauses are mandatory'):standard.build_document(items,state,source,payload)
        state['applicability']['excluded']=[];state['applicability']['terms_sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'exact terms version'):standard.build_document(items,state,source,payload)

    def test_saved_particular_changes_require_new_ack_and_preserve_other_sections(self):
        self.start();self.assertEqual(self.acknowledge().status_code,302)
        items=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar())
        items['sc-01']={'value':'Synthetic agreed special condition'}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb)'),{'items':json.dumps(items)});self.session.commit()
        self.assertEqual(self.client.get(self.standard_url('/view')).status_code,400)
        self.assertEqual(self.acknowledge().status_code,302)
        view=self.client.get(self.standard_url('/view'));self.assertEqual(view.status_code,200)
        self.assertIn('Synthetic agreed special condition',view.get_data(as_text=True))
        after=self.sql('SELECT items FROM bil_lease_draft').scalar()
        self.assertEqual({k:v for k,v in after.items() if k!='_ait_standard'},{k:v for k,v in items.items() if k!='_ait_standard'})
        self.assertEqual(len(after['_ait_standard']['acknowledgements']),2)

    def test_authority_is_separate_not_invented_and_bound_to_representative(self):
        self.start()
        parties={'schema_version':2,'lessor':{'signatory_name':'Natural Representative','signatory_identity_number':'TEST-ID',
            'signatory_capacity':'Agent'},'lessees':[{'legal_name':'Company Tenant','party_type':'juristic_person',
            'identity_number':'TEST-COMPANY','signatory_name':'Tenant Representative','signatory_identity_number':'TEST-REP',
            'signatory_capacity':'Director'}]}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb)'),{'items':json.dumps({'_parties':parties,
            '_contracting_lessors':{'identities':[{'name':'Owner One','identity_number':'OWNER-1'},{'name':'Owner Two','identity_number':'OWNER-2'}]}})});self.session.commit()
        self.assertEqual(self.acknowledge(authority_landlords='Mandate TEST-1',confirm_landlords='on').status_code,302)
        state=self.standard_state();self.assertTrue(state['authorities']['landlords']['confirmed'])
        self.assertFalse(state['authorities']['tenant-0']['confirmed'])
        view=self.client.get(self.standard_url('/view'));self.assertEqual(view.status_code,200)
        content=view.get_data(as_text=True)
        for expected in ('Owner One','Owner Two','Natural Representative','Company Tenant','Tenant Representative','Mandate TEST-1','[AUTHORITY NOT CONFIRMED]'):
            self.assertIn(expected,content)
        from app.program_billing import lease_standard as standard
        changed={'_parties':copy.deepcopy(parties)};changed['_parties']['lessor']['signatory_name']='Different Representative'
        self.assertNotEqual(standard.authority_parties(changed)[0]['fingerprint'],state['authorities']['landlords']['party_fingerprint'])

    def test_stale_save_rollback_unauthorised_access_and_missing_source_fail_safely(self):
        self.start();self.assertEqual(self.acknowledge().status_code,302)
        before=self.sql('SELECT items FROM bil_lease_draft').scalar()
        self.assertEqual(self.acknowledge(version='0').status_code,409)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),before)
        self.deny=True;self.assertEqual(self.client.get(self.standard_url('/view')).status_code,503);self.deny=False
        self.sign_in(8)
        for suffix in ('','/view'):self.assertEqual(self.client.get(self.standard_url(suffix)).status_code,404)
        self.assertEqual(self.client.post(self.standard_url('/word'),data={'version':'1'}).status_code,404)

    def test_certificate_merges_every_dictionary_field_without_assumed_authority(self):
        original=self.start();self.assertEqual(self.acknowledge().status_code,302)
        from app.program_billing import lease_standard as standard
        state=self.standard_state();items=self.sql('SELECT items FROM bil_lease_draft').scalar()
        with self.app.app_context():source,payload=standard.source_master(26,state['source'])
        values={field['token']:'Synthetic value '+str(i) for i,field in enumerate(standard.DICTIONARY['fields'],1)}
        with patch.object(standard.workflow,'stage5_placeholder_values',return_value=values):
            output=standard.build_document(items,state,source,payload)
        rendered='\n'.join(standard.preview.block_text(b) for b in standard.preview.extract_blocks(output))
        for field in standard.DICTIONARY['fields']:
            self.assertIn(values[field['token']],rendered)
            self.assertNotIn(field['token'],rendered)
        self.assertIn('[UNKNOWN AUTHORITY]',rendered)
        self.assertIn('[AUTHORITY NOT CONFIRMED]',rendered)

    def test_complete_dictionary_and_unapproved_release_cannot_claim_ready(self):
        from app.program_billing import lease_standard as standard,lease_generation as generation
        tokens=set(standard.inventory(generation.load_master())['tokens'])
        self.assertEqual(tokens,{f['token'] for f in standard.DICTIONARY['fields']})
        self.assertEqual(len(tokens),37)
        self.assertFalse(standard.RELEASE['terms_approved']);self.assertFalse(standard.RELEASE['disclaimer_approved'])
        self.assertFalse(standard.RELEASE['clause_policy_approved']);self.assertEqual(standard.RELEASE['excludable_clauses'],[])
        self.start();self.assertEqual(self.acknowledge().status_code,302)
        page=self.client.get(self.standard_url());self.assertEqual(page.status_code,200)
        self.assertIn('await legal release approval',page.get_data(as_text=True))

if __name__=='__main__':unittest.main()

class StandardPolicyIntegrityTests(unittest.TestCase):
    def test_notice_text_matches_supplied_development_notice(self):
        # Importing the module requires the isolated application fixture; source
        # AST verifies approved prose without running Flask startup.
        import ast
        source=(fixture.ROOT/'app/program_billing/lease_standard.py').read_text(encoding='utf-8')
        tree=ast.parse(source)
        assignments={n.targets[0].id:ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id in ('DISCLAIMER_TITLE','DISCLAIMER')}
        self.assertEqual(assignments['DISCLAIMER_TITLE'],'AIT STANDARD LEASE \u2014 IMPORTANT NOTICE')
        self.assertEqual(assignments['DISCLAIMER'],
            'The AIT Standard Lease is provided as a general contractual template. AIT does not guarantee its suitability for every property, tenant or transaction.\n'
            'The user is responsible for verifying the accuracy of all particulars, confirming the identity and authority of contracting parties, reviewing the agreement and ensuring compliance with applicable law.\n'
            'Independent legal advice should be obtained where appropriate.\n'
            'Generating a lease through AIT does not, by itself, establish that the agreement is legally valid or enforceable.')
