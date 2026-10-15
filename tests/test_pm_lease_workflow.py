"""Five-section Lease integration on disposable PostgreSQL; no live writes."""
import importlib.util
import hashlib
import io
import json
import unittest
from pathlib import Path
from unittest.mock import patch
from zipfile import ZipFile
from lxml import etree as ET
import test_pm_lease_snapshot as snapshots
import test_pm_property_management as properties

class LeaseWorkflowTests(unittest.TestCase):
    load_lease_workspace=True
    setUp=snapshots.LeaseSnapshotTests.setUp
    sign_in=snapshots.LeaseSnapshotTests.sign_in
    sql=snapshots.LeaseSnapshotTests.sql
    payload=snapshots.LeaseSnapshotTests.payload
    assign=snapshots.LeaseSnapshotTests.assign
    create=snapshots.LeaseSnapshotTests.create

    def route(self, section):
        return f'/billing/leases/property/26/{section}?draft_id={self.draft}'

    def post_group(self, group, **values):
        data={key:'' for key in self.workflow_module.GROUP_KEYS[group]}
        data.update(draft_id=str(self.draft),version=self.sql('SELECT version FROM bil_lease_draft').scalar())
        data.update(values)
        return self.client.post(f'/billing/leases/property/26/particulars/{group}',data=data)

    def stored(self):
        return self.sql('SELECT items FROM bil_lease_draft').scalar()

    def test_five_navigation_group_mapping_authorization_and_property_retention(self):
        html=self.client.get('/billing/leases?property_id=26').get_data(as_text=True)
        headings=['1. Select Property','2. Lease Particulars','3. Master Lease Document','4. Supporting Documents and Annexures','5. Generate / Review Lease']
        self.assertEqual(sorted(headings,key=html.index),headings)
        self.assertNotIn('/snapshot/',html)
        self.assertNotIn('>Select Property</button>',html)
        page=self.client.get(self.route('particulars')).get_data(as_text=True)
        for group,(title,_) in self.workflow_module.GROUPS.items():
            self.assertIn(title,page)
            response=self.client.get(self.route('particulars/'+group))
            self.assertEqual(response.status_code,200)
            self.assertNotIn('aria-label="Property Management"',response.get_data(as_text=True))
            self.assertEqual(response.headers['Cache-Control'],'private, no-store')
        keys=[key for group in self.workflow_module.GROUP_KEYS.values() for key in group if key.startswith('s1-')]
        self.assertEqual(len(keys),41);self.assertEqual(len(set(keys)),41)
        self.assertIn('s1-16',self.workflow_module.GROUP_KEYS['premises'])
        self.assertIn('s1-23',self.workflow_module.GROUP_KEYS['amounts'])
        self.assertEqual(self.client.get('/billing/leases').status_code,200)
        self.assertIn('value="26" selected',self.client.get('/billing/leases').get_data(as_text=True))
        for route in ('particulars','particulars/parties','master','supporting','review'):
            self.assertEqual(self.client.get(self.route(route).replace('/26/','/27/')).status_code,404)
        self.assertEqual(self.client.post('/billing/leases/property/27/particulars/period',data={'draft_id':str(self.draft)}).status_code,404)

    def test_lessee_modal_lessors_snapshot_and_saved_owner_changes_do_not_rewrite_contract(self):
        self.assign(name='Contracting Owner',identity_number='SNAPSHOT-OWNER',email='owner@example.com')
        owner_id=self.sql('SELECT id FROM bil_owner_identity').scalar()
        html=self.client.get(self.route('particulars/parties')).get_data(as_text=True)
        self.assertEqual(html.count('<dialog'),1);self.assertIn('id="lessee-dialog"',html)
        self.assertIn('Contracting Owner',html);self.assertIn('owner@example.com',html)
        self.assertIn('[UNRESOLVED - DO NOT ASSUME INDIVIDUAL OR ENTITY]',html)
        response=self.post_group('parties',lessor_id=str(owner_id),confirm_lessors='on',**{'s1-05':'Expressly nominated service address','s1-06':'Lessee Company','s1-07':'Company / REG-7','s1-08':'Director / written authority','s1-09':'Lessee service address'})
        self.assertEqual(response.status_code,302);self.assertIn('/particulars?',response.location)
        record=self.stored();self.assertEqual(record['s1-06']['value'],'Lessee Company')
        self.assertEqual(record['s1-05']['value'],'Expressly nominated service address')
        self.assertEqual(record['_contracting_lessors']['identities'][0]['email'],'owner@example.com')
        self.sql("UPDATE bil_owner_identity SET email='changed@example.com' WHERE id="+str(owner_id));self.session.commit()
        html=self.client.get(self.route('particulars/parties')).get_data(as_text=True)
        self.assertIn('owner@example.com',html);self.assertNotIn('changed@example.com',html)
        self.assertEqual(self.stored(),record)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property WHERE id=26').scalar(),1)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease').scalar(),2)

    def test_lessors_require_confirmation_and_reject_other_manager_owner(self):
        self.assertEqual(self.post_group('parties').status_code,400)
        self.assertNotIn('_contracting_lessors',self.stored())
        self.assertEqual(self.post_group('parties',lessor_id='999999',confirm_lessors='on').status_code,400)
        self.assertEqual(self.stored(),{})

    def test_period_validation_partial_save_refresh_and_optimistic_lock(self):
        response=self.post_group('period',**{'s1-17':'12 months','s1-18':'2026-01-01','s1-19':'2026-01-01','s1-20':'2026-12-31'})
        self.assertEqual(response.status_code,302)
        self.assertIn('2026-12-31',self.client.get(self.route('particulars/period')).get_data(as_text=True))
        before=self.stored()
        for values in ({'s1-17':'12 months','s1-18':'2026-01-01','s1-20':'2025-12-31'},
                       {'s1-17':'6 months','s1-18':'2026-01-01','s1-20':'2026-12-31'},
                       {'s1-17':'12 months','s1-18':'invalid'}):
            self.assertEqual(self.post_group('period',**values).status_code,400)
            self.assertEqual(self.stored(),before)
        self.assertEqual(self.post_group('amounts',**{'s1-21':'15000 / fifteen thousand'}).status_code,302)
        self.assertEqual(self.stored()['s1-18']['value'],'2026-01-01')
        self.assertEqual(self.client.post('/billing/leases/property/26/particulars/amounts',data={'draft_id':str(self.draft),'version':1}).status_code,409)

    def test_special_conditions_numbering_reference_alerts_and_no_silent_renumbering(self):
        response=self.post_group('conditions',additional_clauses='29 | Inspect safety records under Core 8.1.1 and Annexure A.\n30 | Review Schedule 99 and clause 999.')
        self.assertEqual(response.status_code,302)
        clauses=self.stored()['_additional_clauses'];self.assertEqual([c['number'] for c in clauses],[29,30])
        html=self.client.get(self.route('review')).get_data(as_text=True)
        self.assertIn('Unknown Schedule 99',html);self.assertIn('Unknown master clause reference 999',html)
        self.assertIn('referenced Annexure A is missing',html)
        self.assertIn('/particulars/conditions?',html)
        self.assertEqual(self.post_group('conditions',additional_clauses='31 | Wrong identifier').status_code,400)
        self.assertEqual(self.stored()['_additional_clauses'],clauses)
        self.assertTrue(self.workflow_module.reference_alerts({'sc-01':{'value':'See clause above and SC-99.'}}))

    def test_supporting_requirements_na_and_completion_links(self):
        data={'draft_id':str(self.draft),'version':1,'annexure-a_requirement':'Conditional','annexure-a_na':'on','annexure-a_reason':'No plan required for this draft'}
        self.assertEqual(self.client.post('/billing/leases/property/26/supporting',data=data).status_code,302)
        self.assertTrue(self.stored()['annexure-a']['not_applicable'])
        html=self.client.get(self.route('review')).get_data(as_text=True)
        self.assertIn('essential',html.lower());self.assertIn('Annexure B',html)
        self.assertIn('/supporting?',html);self.assertNotIn('>Generate</button>',html)
        data['version']=2;data['annexure-a_requirement']='Optional'
        self.assertEqual(self.client.post('/billing/leases/property/26/supporting',data=data).status_code,400)
        self.assertEqual(self.stored()['_annexure_requirements']['annexure-a'],'Conditional')
        self.assertIn('Upload supporting document',self.client.get(self.route('supporting')).get_data(as_text=True))

    def test_master_and_generation_disabled_even_with_draft_and_storage_flags(self):
        html=self.client.get(self.route('master')).get_data(as_text=True)
        self.assertIn('master uploads are currently disabled',html.lower())
        with patch.object(properties.Identity,'has_role',return_value=True,create=True):
            html=self.client.get(self.route('master')).get_data(as_text=True)
            self.assertIn('<fieldset disabled',html);self.assertIn('accept=".docx"',html)
        self.assertEqual(self.client.post('/billing/leases/property/26/master',data={}).status_code,405)
        self.assertEqual(self.client.post('/billing/leases/property/26/review',data={}).status_code,405)
        self.assertEqual(self.calls,[])
        self.app.config['PM_LEASE_DRAFT_WRITES_APPROVED']=False
        self.assertEqual(self.post_group('amounts',**{'s1-21':'500'}).status_code,503)
        self.assertEqual(self.stored(),{})

    def test_completion_checks_essential_conditional_placeholders_dates_and_documents(self):
        items={key:{'value':'Captured particulars'} for keys in self.workflow_module.GROUP_KEYS.values() for key in keys}
        items.update({'s1-17':{'value':'1 year'},'s1-18':{'value':'2026-01-01'},'s1-20':{'value':'2026-12-31'},'s1-19':{'value':'2026-01-01'},'_contracting_lessors':{'confirmed':True}})
        files=[{'item_key':key,'state':'available'} for key in self.snapshot_module.ITEMS if key.startswith('annexure-')]
        self.assertEqual(self.workflow_module.completion_alerts(items,files),[])
        items['s1-21']={'value':'[MONTHLY RENT]'}
        issues=self.workflow_module.completion_alerts(items,[])
        self.assertTrue(any('placeholder' in message for _,_,message in issues))
        self.assertTrue(any('supporting'==group for group,_,_ in issues))
        items['s1-04']={'not_applicable':True,'reason':'no'}
        self.assertTrue(any('invalid Not Applicable' in message for _,_,message in self.workflow_module.completion_alerts(items,files)))

