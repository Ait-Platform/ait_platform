"""Part 5 routes and merge mapping: synthetic data and in-memory doubles only."""
import copy, hashlib, re, types, unittest
from unittest.mock import patch
from zipfile import ZipFile
from lxml import etree as E
import test_pm_lease_premises_offline as premises
ROOT=premises.ROOT

class Part5OfflineTests(unittest.TestCase):
    def setUp(self):
        premises.PremisesOfflineTests.setUp(self)
        self.route=self.route.replace('/premises?','/amounts?')
        self.items.update({'_amounts':{'monthly_rent':'10000.00','deposit_amount':'10000.00'},'_premises':{'property_name':'Almond'},'s1-38':{'value':'Unverified legacy bank value - must not prefill'}})
        actor=patch.object(self.m,'current_user',types.SimpleNamespace(id=7));actor.start();self.addCleanup(actor.stop)

    def form(self,**changes):
        data={'workspace_section':'banking_utilities','part5_form_version':'1','draft_id':'synthetic-draft','version':'1',
              'part5_utility_allocation_method':'Synthetic allocation by verified leased-portion sub-meter readings against supplier invoices.',
              'part5_utility_reference_mode':'provided','part5_utility_references':'TEST-METER-001 / TEST-ACCOUNT-001 (synthetic)',
              'part5_utility_no_references_reason':'','part5_utilities_confirmed':'on','part5_bank_name':'Synthetic Test Bank - NOT FOR PAYMENT',
              'part5_branch_name':'Synthetic Test Branch','part5_branch_code':'000000','part5_account_holder':'SYNTHETIC TEST LANDLORD - DO NOT PAY',
              'part5_account_type':'current','part5_account_type_other':'','part5_account_number':'0000000000','part5_swift':'','part5_banking_confirmed':'on'}
        data.update(changes);return data

    def test_save_reload_preserves_parts_one_to_four_and_records_confirmer(self):
        before=copy.deepcopy(self.items)
        self.assertEqual(self.client.post(self.route,data=self.form()).status_code,302)
        for key,value in before.items():
            if key not in ('s1-36','s1-38'):self.assertEqual(self.saved[key],value)
        state=self.saved['_banking_utilities']
        self.assertEqual(state['confirmed_by'],7);self.assertIn('+00:00',state['confirmed_at'])
        self.assertEqual(state['account_number'],'0000000000')
        self.draft['items']=self.saved
        html=self.client.get(self.route).get_data(as_text=True)
        self.assertIn('value="0000000000"',html);self.assertIn('Synthetic Test Bank',html)
        self.assertNotIn('name="part5_confirmed_by"',html)

    def test_no_unverified_prefill(self):
        state=self.m.banking_utilities_state(self.items)
        self.assertEqual(state['bank_name'],'');self.assertEqual(state['account_number'],'')
        self.assertFalse(state['banking_confirmed'])
        html=self.client.get(self.route).get_data(as_text=True)
        self.assertIn('name="part5_account_number"',html)
        self.assertIn('value=""',html)
        # Legacy values are review-only; they are not assigned to structured inputs.
        self.assertEqual(self.m.banking_utilities_placeholder_values(self.items),dict.fromkeys(self.m.ANCILLARY_TOKENS,''))

    def test_explicit_confirmations_required_and_input_retained(self):
        for key in ['part5_utilities_confirmed','part5_banking_confirmed','part5_form_version']:
            response=self.client.post(self.route,data=self.form(**{key:''}))
            self.assertEqual(response.status_code,400);self.assertIsNone(self.saved)
            self.assertIn('Synthetic Test Bank',response.get_data(as_text=True))

    def test_required_and_format_validation(self):
        for changes in [dict(part5_bank_name=''),dict(part5_account_holder=''),dict(part5_branch_code='12X456'),
                        dict(part5_account_number='123-456'),dict(part5_account_number='12345'),dict(part5_account_type='invalid'),
                        dict(part5_swift='INVALID!'),dict(part5_utility_allocation_method=''),dict(part5_utility_references=''),
                        dict(part5_utility_references='[UTILITY METER OR ACCOUNT REFERENCES]')]:
            _,errors=self.m.banking_utilities_from_form(self.form(**changes));self.assertTrue(errors)
        for swift in ['', 'TESTZA00','TESTZA00XXX']:
            _,errors=self.m.banking_utilities_from_form(self.form(part5_swift=swift));self.assertFalse(errors)

    def test_no_reference_case_requires_explicit_explanation(self):
        data=self.form(part5_utility_reference_mode='none',part5_utility_references='',part5_utility_no_references_reason='Synthetic test: no separate leased-portion references supplied.')
        state,errors=self.m.banking_utilities_from_form(data);self.assertFalse(errors)
        values=self.m.banking_utilities_placeholder_values({'_banking_utilities':state})
        self.assertTrue(values['[UTILITY METER OR ACCOUNT REFERENCES]'].startswith('No separate references supplied:'))
        _,errors=self.m.banking_utilities_from_form(dict(data,part5_utility_no_references_reason=''));self.assertTrue(errors)
        _,errors=self.m.banking_utilities_from_form(dict(data,part5_utility_references='Contradictory meter'));self.assertTrue(errors)

    def test_other_account_type_and_optional_swift_are_not_invented(self):
        data=self.form(part5_account_type='other',part5_account_type_other='Synthetic permitted account')
        state,errors=self.m.banking_utilities_from_form(data);self.assertFalse(errors)
        bank=self.m.banking_utilities_placeholder_values({'_banking_utilities':state})[self.m.BANKING_TOKEN]
        self.assertIn('Synthetic permitted account',bank);self.assertNotIn('SWIFT',bank)
        _,errors=self.m.banking_utilities_from_form(dict(data,part5_account_type_other=''));self.assertTrue(errors)

    def test_authorization_and_write_gates_preserved(self):
        self.assertEqual(self.client.post(self.route.replace('/26/','/27/'),data=self.form()).status_code,404)
        self.blocked=True
        self.assertEqual(self.client.post(self.route,data=self.form()).status_code,503);self.assertIsNone(self.saved)
        html=self.client.get(self.route).get_data(as_text=True)
        self.assertEqual(html.count('<fieldset disabled'),2)

    def test_xss_and_client_audit_tampering(self):
        data=self.form(part5_bank_name='<script>synthetic</script>',part5_form_version='',part5_confirmed_by='8')
        response=self.client.post(self.route,data=data)
        self.assertEqual(response.status_code,400)
        self.assertIn('&lt;script&gt;',response.get_data(as_text=True));self.assertIsNone(self.saved)
        self.assertEqual(self.client.post(self.route,data=self.form(part5_confirmed_by='8')).status_code,302)
        self.assertEqual(self.saved['_banking_utilities']['confirmed_by'],7)

    def test_three_exact_master_tokens_and_non_mutating_mapping(self):
        state,errors=self.m.banking_utilities_from_form(self.form());self.assertFalse(errors)
        before=copy.deepcopy(state);values=self.m.banking_utilities_placeholder_values({'_banking_utilities':state})
        self.assertEqual(state,before);self.assertEqual(len(values),3);self.assertTrue(all(values.values()))
        self.assertIn('0000000000',values[self.m.BANKING_TOKEN])
        master=ROOT/'app/program_billing/private_templates/Lease_Master_Stage5_Review.docx'
        self.assertEqual(hashlib.sha256(master.read_bytes()).hexdigest(),self.m.PARTIES_MASTER_SHA256)
        with ZipFile(master) as z:tree=E.fromstring(z.read('word/document.xml'))
        all_tokens=re.findall(r'\[[^\[\]]+\]',''.join(tree.xpath('//w:t/text()',namespaces={'w':'http://schemas.openxmlformats.org/wordprocessingml/2006/main'})))
        self.assertTrue(set(values)<=set(all_tokens));self.assertEqual(sum(all_tokens.count(token) for token in values),5)
        state['banking_confirmed']=False
        self.assertFalse(any(self.m.banking_utilities_placeholder_values({'_banking_utilities':state}).values()))

    def test_missing_part4_does_not_require_reentry(self):
        # Part 5 is an independent submission on the existing authorised page.
        self.items.pop('_amounts')
        self.assertEqual(self.client.post(self.route,data=self.form()).status_code,302)
        self.assertNotIn('_amounts',self.saved)
        self.assertIn('_banking_utilities',self.saved)

    def test_part4_submission_does_not_replace_part5(self):
        import test_pm_lease_amounts_offline as financial
        state,errors=self.m.banking_utilities_from_form(self.form());self.assertFalse(errors)
        state.update(confirmed_by=7,confirmed_at='synthetic audit timestamp')
        self.items['_banking_utilities']=copy.deepcopy(state)
        self.items.update({'s1-17':{'value':'24 months'},'s1-18':{'value':'2027-01-01'},'s1-19':{'value':'2027-01-15'},'s1-20':{'value':'2028-12-31'}})
        self.assertEqual(self.client.post(self.route,data=financial.AmountsOfflineTests.form(self)).status_code,302)
        self.assertEqual(self.saved['_banking_utilities'],state)
