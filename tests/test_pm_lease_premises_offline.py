"""Premises route/validation/master-mapping checks with no database or app startup."""
import copy, importlib.util, json, re, sys, types, unittest
from pathlib import Path
from unittest.mock import patch
from zipfile import ZipFile
from lxml import etree as E
from flask import Flask, Blueprint, abort, render_template
from jinja2 import ChoiceLoader, DictLoader, FileSystemLoader
ROOT=Path(__file__).resolve().parents[1]

class PremisesOfflineTests(unittest.TestCase):
    def setUp(self):
        self.app=Flask('premises-offline');self.app.config.update(TESTING=True,SECRET_KEY='synthetic')
        self.app.jinja_loader=ChoiceLoader([DictLoader({'layout.html':'{% block content %}{% endblock %}','partials/flash_messages.html':''}),FileSystemLoader(ROOT/'templates')])
        self.app.jinja_env.globals['csrf_token']=lambda:'synthetic'
        bp=Blueprint('billing_bp',__name__)
        modules={name:types.ModuleType(name) for name in ['app','app.extensions','app.program_billing','app.program_billing.routes','app.program_billing.lease_snapshot','app.program_billing.lease_management']}
        for module in modules.values():module.__path__=[]
        snapshot=modules['app.program_billing.lease_snapshot']
        snapshot.CATALOG=json.loads((ROOT/'app/program_billing/lease_snapshot_catalog.json').read_text(encoding='utf-8-sig'))
        snapshot.ITEMS={i['key']:i for i in snapshot.CATALOG['items']}
        snapshot.endpoint=lambda function:function
        self.prop={'id':26,'name':'Almond','address':'Saved property address'}
        self.items={'_parties':{'lessee':{'legal_name':'Existing lessee'}},'_contracting_lessors':{'identities':[{'owner_id':1,'name':'Existing owner'}]},'s1-18':{'value':'Saved Part 3'},'s1-21':{'value':'Saved Part 4'},'sc-01':{'value':'Saved Part 5'}}
        self.draft={'id':'synthetic-draft','version':1,'items':self.items}
        self.blocked=False;self.saved=None
        def authorised(identifier,lock=False):
            if identifier!=26:abort(404)
            return self.prop
        snapshot.authorised_property=authorised
        snapshot.readiness=lambda:'Draft storage is not approved.' if self.blocked else None
        snapshot.particulars_readiness=snapshot.readiness
        snapshot.draft_row=lambda *args:self.draft
        def require():
            if self.blocked:abort(503)
        snapshot.require_schema=require
        snapshot.require_particulars_schema=require
        modules['app.extensions'].db=types.SimpleNamespace(session=None)
        modules['app.program_billing.routes'].billing_bp=bp
        modules['app.program_billing.lease_management'].render_pm_template=render_template
        modules['app.program_billing'].lease_snapshot=snapshot
        self.patch=patch.dict(sys.modules,modules);self.patch.start();self.addCleanup(self.patch.stop)
        spec=importlib.util.spec_from_file_location('premises_isolated',ROOT/'app/program_billing/lease_workflow.py')
        self.m=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.m)
        def save(draft,items):self.saved=copy.deepcopy(items)
        self.m.save_items=save
        self.app.register_blueprint(bp);self.client=self.app.test_client()
        self.route='/billing/leases/property/26/particulars/premises?draft_id=synthetic-draft'

    def form(self,**changes):
        data=dict(premises_form_version='1',premises_lease_scope='part_only',premises_property_name='Almond',premises_property_address='Saved property address',premises_property_legal_description='',premises_property_extent_sqm='1200',premises_leased_area_sqm='300',premises_unit_count='2',premises_leased_portion_description='Ground floor units 1 and 2 only',premises_excluded_areas='Garden and units 3-4',premises_plan_a_identifier='TEST-SITE-A Rev 1',premises_plan_b_identifier='TEST-UNIT-B Rev 2',draft_id='synthetic-draft',version='1')
        data.update(changes);return data

    def test_authorized_prefill_and_no_scope_or_extent_assumption(self):
        response=self.client.get(self.route);self.assertEqual(response.status_code,200)
        html=response.get_data(as_text=True)
        self.assertIn('Saved property address',html);self.assertIn('value="Almond"',html)
        self.assertIn('name="premises_lease_scope"',html)
        self.assertNotIn('value="whole_property" selected',html)
        self.assertNotIn('aria-label="Property Management"',html)
        self.assertEqual(self.client.get(self.route.replace('/26/','/27/')).status_code,404)
        self.assertEqual(self.client.post(self.route.replace('/26/','/27/'),data=self.form()).status_code,404)

    def test_structured_save_preserves_parties_other_parts_and_register(self):
        before=copy.deepcopy(self.items);prop=copy.deepcopy(self.prop)
        self.assertEqual(self.client.post(self.route,data=self.form()).status_code,302)
        for key,value in before.items():self.assertEqual(self.saved[key],value)
        self.assertEqual(self.prop,prop)
        self.assertEqual(self.saved['_premises']['lease_scope'],'part_only')
        self.assertEqual(self.saved['s1-10']['value'],'Almond')
        self.draft['items']=self.saved
        html=self.client.get(self.route).get_data(as_text=True)
        self.assertIn('TEST-SITE-A Rev 1',html);self.assertIn('Ground floor units 1 and 2 only',html)

    def test_invalid_values_retain_input_without_save(self):
        for changes in [dict(premises_lease_scope=''),dict(premises_leased_area_sqm='0'),dict(premises_property_extent_sqm='NaN'),dict(premises_unit_count='1.5'),dict(premises_plan_a_identifier=''),dict(premises_plan_b_identifier='[PLAN B IDENTIFIER]'),dict(premises_property_name='X'*201)]:
            response=self.client.post(self.route,data=self.form(**changes));self.assertEqual(response.status_code,400)
            self.assertIsNone(self.saved)
            self.assertIn('TEST-UNIT-B Rev 2' if 'premises_plan_b_identifier' not in changes else '[PLAN B IDENTIFIER]',response.get_data(as_text=True))

    def test_areas_independent_and_no_automatic_entire_property(self):
        state,errors=self.m.premises_from_form(self.form(premises_leased_area_sqm='1800'))
        self.assertFalse(errors) # Multi-floor floor area can exceed site extent.
        self.assertEqual(state['property_extent_sqm'],'1200')
        self.assertEqual(state['leased_area_sqm'],'1800')
        _,errors=self.m.premises_from_form(self.form(premises_leased_area_sqm='300 m2'))
        self.assertTrue(errors)

    def test_readiness_gates_preserved(self):
        self.blocked=True
        html=self.client.get(self.route).get_data(as_text=True)
        self.assertIn('<fieldset disabled',html)
        self.assertEqual(self.client.post(self.route,data=self.form()).status_code,503)
        self.assertIsNone(self.saved)

    def test_legacy_values_preserved_without_parsing_unknown_scope(self):
        state=self.m.premises_state({'s1-10':{'value':'Historic name'},'s1-12':{'value':'Unstructured portion'}},self.prop)
        self.assertEqual(state['property_name'],'Historic name')
        for field in ['lease_scope','leased_area_sqm','unit_count']:self.assertEqual(state[field],'')

    def test_seven_exact_master_tokens_and_unchanged_part1_mapping(self):
        state,errors=self.m.premises_from_form(self.form());self.assertFalse(errors)
        items=copy.deepcopy(self.items);items['_premises']=state;before=copy.deepcopy(items)
        values=self.m.premises_placeholder_values(items)
        self.assertEqual(len(values),7)
        self.assertEqual(values['[S1 EXTENT]'],'1200');self.assertEqual(values['[LEASED PREMISES AREA]'],'300')
        self.assertEqual(self.m.party_placeholder_values(items),self.m.party_placeholder_values(self.items))
        self.assertEqual(items,before)
        master=ROOT/'app/program_billing/private_templates/Lease_Master_Stage5_Review.docx'
        import hashlib
        self.assertEqual(hashlib.sha256(master.read_bytes()).hexdigest(),self.m.PARTIES_MASTER_SHA256)
        with ZipFile(master) as z:tree=E.fromstring(z.read('word/document.xml'))
        tokens=set(re.findall(r'\[[^\[\]]+\]',''.join(tree.xpath('//w:t/text()',namespaces={'w':'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}))))
        self.assertTrue(set(values)<=tokens)
        self.assertTrue(set(values).isdisjoint(self.m.party_placeholder_values(items)))

    def test_other_sections_keep_their_existing_forms(self):
        self.m.owners_for=lambda property_id:[]
        for group in ['parties','period','amounts','conditions']:
            response=self.client.get(self.route.replace('/premises?','/'+group+'?'))
            self.assertEqual(response.status_code,200)
            self.assertNotIn('name="premises_form_version"',response.get_data(as_text=True))
        self.assertIsNone(self.saved)

    def test_template_escapes_untrusted_premises_and_requires_form_version(self):
        response=self.client.post(self.route,data=self.form(premises_property_address='<script>alert(1)</script>',premises_form_version=''))
        self.assertEqual(response.status_code,400)
        html=response.get_data(as_text=True)
        self.assertIn('&lt;script&gt;',html);self.assertNotIn('<script>alert(1)</script>',html)
        self.assertIsNone(self.saved)
