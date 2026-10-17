"""Permanent lease SQL paths on disposable PostgreSQL fixtures, never production."""
import copy, hashlib, io, json, unittest
from pathlib import Path
from unittest.mock import patch
from zipfile import ZipFile
from lxml import etree as ET
from sqlalchemy.exc import SQLAlchemyError
import test_pm_lease_workflow as workspace
import test_pm_lease_parties_structured as parties
import test_pm_lease_premises_offline as premises
import test_pm_lease_period_offline as period
import test_pm_lease_amounts_offline as financial
import test_pm_lease_part5_offline as ancillary

class DatabaseLeaseWorkflowTests(unittest.TestCase):
    load_lease_workspace=True
    setUp=workspace.LeaseWorkflowTests.setUp
    sign_in=workspace.LeaseWorkflowTests.sign_in
    sql=workspace.LeaseWorkflowTests.sql
    payload=workspace.LeaseWorkflowTests.payload
    assign=workspace.LeaseWorkflowTests.assign
    create=workspace.LeaseWorkflowTests.create
    stored=workspace.LeaseWorkflowTests.stored
    route=workspace.LeaseWorkflowTests.route

    def post(self,section,data):
        data=dict(data,draft_id=str(self.draft),version=str(self.sql('SELECT version FROM bil_lease_draft WHERE id=\''+str(self.draft)+'\'').scalar()))
        return self.client.post('/billing/leases/property/26/particulars/'+section,data=data)

    def complete(self):
        self.app.config.update(PM_LEASE_DRAFT_WRITES_APPROVED=False,PM_LEASE_PARTICULARS_WRITES_APPROVED=True)
        data=parties.StructuredPartiesTests.details(self)
        self.app.config['PM_OWNER_WRITES_APPROVED']=False
        for section,data in [('parties',data),('premises',premises.PremisesOfflineTests.form(self)),
                             ('period',period.PeriodOfflineTests.form(self)),('amounts',financial.AmountsOfflineTests.form(self)),
                             ('amounts',ancillary.Part5OfflineTests.form(self))]:
            response=self.post(section,data);self.assertEqual(response.status_code,302,response.get_data(as_text=True))
        self.assertEqual(self.workflow_module.stage5_completion(self.stored()),[])

    def generate(self,**extra):
        version=self.sql('SELECT version FROM bil_lease_draft WHERE id=\''+str(self.draft)+'\'').scalar()
        data=dict(draft_id=str(self.draft),version=str(version),review_draft_confirmation='on');data.update(extra)
        return self.client.post('/billing/leases/property/26/generate',data=data)

    def test_all_five_save_reload_edit_and_generate_without_shared_flag(self):
        self.complete();before=copy.deepcopy(self.stored())
        for section in ('parties','premises','period','amounts'):
            self.assertEqual(self.client.get(self.route('particulars/'+section)).status_code,200)
        review=self.client.get(self.route('review'))
        self.assertEqual(review.status_code,200)
        self.assertIn('Generate review Word document',review.get_data(as_text=True))
        response=self.generate();self.assertEqual(response.status_code,200)
        self.assertIn('private, no-store',response.headers['Cache-Control'])
        self.assertIn('DRAFT-FOR-REVIEW.docx',response.headers['Content-Disposition'])
        stored=self.stored();record=stored['_generations'][0]
        self.assertEqual(record['merged_values'],self.workflow_module.stage5_placeholder_values(before))
        self.assertEqual(record['output_sha256'],hashlib.sha256(response.data).hexdigest())
        self.assertEqual(len(record['merged_values']),37)
        for token in self.workflow_module.SIGNATURE_TOKENS:self.assertEqual(record['merged_values'][token],'________________')
        self.assertEqual(before['_parties']['signatures']['landlord_date'],'')
        self.assertFalse(record['signature_or_issuance_approved'])
        for key,value in before.items():self.assertEqual(stored[key],value)
        with ZipFile(io.BytesIO(response.data)) as archive:
            doc=ET.fromstring(archive.read('word/document.xml'))
            visible=''.join(doc.itertext())
            self.assertIn('DRAFT \u2014 FOR REVIEW',visible);self.assertIn('NOT FOR SIGNATURE OR ISSUANCE',visible)
            self.assertNotRegex(visible,r'\[[^\[\]]+\]')
            self.assertEqual(len(doc.findall('.//{http://schemas.openxmlformats.org/wordprocessingml/2006/main}tbl')),3)
        output=workspace.properties.ROOT/'tmp/pm-lease-db-rehearsal/application-review.docx'
        output.parent.mkdir(exist_ok=True,parents=True);output.write_bytes(response.data)
        # An edit does not rewrite the previous generation snapshot or other sections.
        changed=ancillary.Part5OfflineTests.form(self,part5_branch_name='Corrected synthetic branch')
        self.assertEqual(self.post('amounts',changed).status_code,302)
        self.assertEqual(self.stored()['_generations'][0],record)
        self.assertEqual(self.stored()['_parties'],before['_parties'])
        self.assertEqual(self.stored()['_premises'],before['_premises'])
        self.assertEqual(self.generate().status_code,200)
        self.assertEqual(len(self.stored()['_generations']),2)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease').scalar(),2)
        self.assertFalse(self.calls)

    def test_scoped_flag_does_not_enable_legacy_snapshot_upload_or_annexure_writes(self):
        self.app.config.update(PM_LEASE_DRAFT_WRITES_APPROVED=False,PM_LEASE_PARTICULARS_WRITES_APPROVED=True)
        new=self.client.post('/billing/leases/property/26/drafts',data={'registration_token':str(__import__('uuid').uuid4())})
        self.assertEqual(new.status_code,302)
        self.assertEqual(self.client.post('/billing/leases/property/26/snapshot/s1-01',data={'draft_id':str(self.draft),'value':'must not save','version':'1'}).status_code,503)
        self.assertEqual(self.client.post('/billing/leases/property/26/documents',data={'draft_id':str(self.draft)}).status_code,503)
        self.assertEqual(self.client.post('/billing/leases/property/26/supporting',data={'draft_id':str(self.draft)}).status_code,503)
        self.assertEqual(self.stored(),{});self.assertFalse(self.calls)

    def test_other_manager_cannot_view_save_or_generate(self):
        self.complete();before=self.stored();self.sign_in(8)
        for path in [self.route('review'),self.route('particulars/parties')]:self.assertEqual(self.client.get(path).status_code,404)
        self.assertEqual(self.post('amounts',ancillary.Part5OfflineTests.form(self)).status_code,404)
        self.assertEqual(self.generate().status_code,404)
        self.assertEqual(self.stored(),before)
        self.assertEqual(self.client.get(self.route('review').replace('/26/','/27/')).status_code,404)

    def test_disabled_scope_and_inactive_user_fail_closed(self):
        self.complete();before=self.stored()
        self.app.config['PM_LEASE_PARTICULARS_WRITES_APPROVED']=False
        self.assertEqual(self.generate().status_code,503)
        self.app.config['PM_LEASE_PARTICULARS_WRITES_APPROVED']=True
        self.sql('UPDATE "user" SET is_active=0 WHERE id=7');self.session.commit()
        self.assertEqual(self.generate().status_code,403)
        self.assertEqual(self.stored(),before)

    def test_incomplete_unconfirmed_and_stale_generation_do_not_save_audit(self):
        self.assertEqual(self.generate().status_code,400)
        self.complete();before=self.stored()
        self.assertEqual(self.generate(review_draft_confirmation='').status_code,400)
        self.assertEqual(self.generate(version='0').status_code,409)
        self.assertEqual(self.stored(),before)

    def test_generation_commit_failure_rolls_back(self):
        self.complete();before=self.stored()
        with patch.object(self.session,'commit',side_effect=SQLAlchemyError('synthetic generation commit failure')):
            self.assertEqual(self.generate().status_code,503)
        self.assertEqual(self.stored(),before)

    def test_master_corruption_and_extra_conditions_block_generation(self):
        self.complete();before=self.stored()
        service=__import__('sys').modules['app.program_billing.lease_generation']
        with patch.object(service,'load_master',side_effect=ValueError('synthetic wrong master')):
            self.assertEqual(self.generate().status_code,503)
        self.assertEqual(self.stored(),before)
        self.session.execute(__import__('sqlalchemy').text("UPDATE bil_lease_draft SET items=jsonb_set(items,'{_additional_clauses}',CAST(:conditions AS jsonb))"),{'conditions':json.dumps([{'number':29,'text':'Unmapped contractual condition'}])});self.session.commit()
        self.assertEqual(self.generate().status_code,400)
