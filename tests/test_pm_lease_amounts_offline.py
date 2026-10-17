"""Financial Particulars tests: isolated routes, no database or application startup."""
import copy, hashlib, re, unittest
from decimal import Decimal
from zipfile import ZipFile
from lxml import etree as E
import test_pm_lease_premises_offline as premises
ROOT=premises.ROOT

class AmountsOfflineTests(unittest.TestCase):
    def setUp(self):
        premises.PremisesOfflineTests.setUp(self)
        self.route=self.route.replace('/premises?','/amounts?')
        self.items.update({'s1-17':{'value':'24 months'},'s1-18':{'value':'2027-01-01'},'s1-19':{'value':'2027-01-15'},'s1-20':{'value':'2028-12-31'},'_premises':{'property_name':'Almond'},'s1-36':{'value':'Existing Part 5 utility allocation'},'s1-38':{'value':'Existing Part 5 banking'},'s1-31':{'value':'Legacy preparation fee retained'}})

    def form(self,**changes):
        data={'amounts_form_version':'1','amounts_monthly_rent':'10000.00','amounts_monthly_rent_words':'Ten thousand rand and zero cents',
              'amounts_rent_effective_date':'2027-01-01','amounts_deposit_amount':'10000.00','amounts_parking_applicability':'allocated',
              'amounts_parking_bay_count':'2','amounts_parking_bay_identifiers':'TEST-P1\nTEST-P2','amounts_parking_rent_per_bay':'250.00','draft_id':'synthetic-draft','version':'1'}
        for i,day,amount in [(1,'2027-07-01','10500.00'),(2,'2028-01-01','11000.00'),(3,'2028-07-01','11500.00')]:
            data.update({f'amounts_adjustment_{i}_applicability':'applies',f'amounts_adjustment_{i}_date':day,f'amounts_adjustment_{i}_amount':amount})
        data.update(changes);return data

    def test_save_reload_preserves_parts_one_to_three_and_part_five(self):
        before=copy.deepcopy(self.items)
        self.assertEqual(self.client.post(self.route,data=self.form()).status_code,302)
        for key,value in before.items():
            if key not in ('s1-21','s1-22','s1-23','s1-24','s1-28'):self.assertEqual(self.saved[key],value)
        self.assertEqual(self.saved['_amounts']['deposit_amount'],'10000.00')
        self.assertEqual(self.saved['_amounts']['currency'],'ZAR')
        self.draft['items']=self.saved
        html=self.client.get(self.route).get_data(as_text=True)
        self.assertIn('value="10000.00"',html);self.assertIn('2027-07-01',html)
        self.assertNotIn('name="s1-23"',html);self.assertNotIn('aria-label="Property Management"',html)

    def test_money_format_nonnegative_decimal_and_explicit_deposit(self):
        for raw in ['-1','1e4','NaN','Infinity','R 10000','10,000.00','10000.001','', '1000000000000']:
            with self.assertRaises(ValueError):self.m.zar_amount(raw)
        self.assertEqual(self.m.zar_amount('0'),Decimal('0.00'))
        self.assertEqual(self.m.zar_amount('12.5'),Decimal('12.50'))
        for changes in [dict(amounts_monthly_rent='-1'),dict(amounts_deposit_amount=''),dict(amounts_deposit_amount='9000.00')]:
            response=self.client.post(self.route,data=self.form(**changes));self.assertEqual(response.status_code,400);self.assertIsNone(self.saved)
        state,_=self.m.amounts_from_form(self.form(amounts_deposit_amount=''),self.items)
        self.assertEqual(state['deposit_amount'],'')

    def test_invalid_dates_and_six_month_chronology(self):
        for changes in [dict(amounts_rent_effective_date='2026-12-31'),dict(amounts_rent_effective_date='2029-01-01'),
                        dict(amounts_adjustment_1_date='2028-01-01'),dict(amounts_adjustment_2_date='2028-02-30'),
                        dict(amounts_adjustment_3_date='2029-07-01'),dict(amounts_rent_effective_date='20270101')]:
            self.assertEqual(self.client.post(self.route,data=self.form(**changes)).status_code,400);self.assertIsNone(self.saved)

    def test_agreed_amounts_not_annual_percentages_or_forced_increases(self):
        data=self.form(amounts_adjustment_1_amount='9500',amounts_adjustment_2_amount='9500',amounts_adjustment_3_amount='0')
        state,errors=self.m.amounts_from_form(data,self.items)
        self.assertFalse(errors)
        self.assertEqual(state['adjustment_1_amount'],'9500');self.assertEqual(state['deposit_amount'],'10000.00')
        self.assertNotIn('percentage',state)
        _,errors=self.m.amounts_from_form(self.form(amounts_adjustment_1_amount='5%'),self.items)
        self.assertTrue(errors)

    def test_after_expiry_only_with_explicit_applicability(self):
        self.items.update({'s1-17':{'value':'6 months'},'s1-20':{'value':'2027-06-30'}})
        data=self.form()
        for i in (1,2,3):data.update({f'amounts_adjustment_{i}_applicability':'after_expiry',f'amounts_adjustment_{i}_date':'',f'amounts_adjustment_{i}_amount':''})
        state,errors=self.m.amounts_from_form(data,self.items);self.assertFalse(errors)
        values=self.m.amounts_placeholder_values({'_amounts':state})
        self.assertEqual(values['[RENT ADJUSTMENT 1 DATE]'],'Not applicable (after expiry)')
        self.items.update({'s1-17':{'value':'24 months'},'s1-20':{'value':'2028-12-31'}})
        _,errors=self.m.amounts_from_form(data,self.items);self.assertTrue(errors)

    def test_master_schedule_capacity_is_not_silently_extended(self):
        self.items.update({'s1-17':{'value':'3 years'},'s1-20':{'value':'2029-12-31'}})
        _,errors=self.m.amounts_from_form(self.form(),self.items)
        self.assertTrue(any('three adjustment rows' in e for e in errors))

    def test_parking_conditional_validation_and_no_allocation(self):
        for changes in [dict(amounts_parking_bay_count='3'),dict(amounts_parking_bay_identifiers='TEST-P1\nTEST-P1'),dict(amounts_parking_rent_per_bay='-1')]:
            _,errors=self.m.amounts_from_form(self.form(**changes),self.items);self.assertTrue(errors)
        data=self.form(amounts_parking_applicability='none',amounts_parking_bay_count='',amounts_parking_bay_identifiers='',amounts_parking_rent_per_bay='')
        state,errors=self.m.amounts_from_form(data,self.items);self.assertFalse(errors)
        values=self.m.amounts_placeholder_values({'_amounts':state})
        self.assertEqual(values['[PARKING BAY COUNT]'],'0');self.assertEqual(values['[S1 PARKING RENT PER BAY]'],'R 0.00')
        _,errors=self.m.amounts_from_form(dict(data,amounts_parking_rent_per_bay='50'),self.items);self.assertTrue(errors)

    def test_authorization_readiness_and_failed_input_preservation(self):
        self.assertEqual(self.client.post(self.route.replace('/26/','/27/'),data=self.form()).status_code,404)
        self.blocked=True
        self.assertEqual(self.client.post(self.route,data=self.form()).status_code,503);self.assertIsNone(self.saved)
        self.assertIn('<fieldset disabled',self.client.get(self.route).get_data(as_text=True))
        self.blocked=False
        response=self.client.post(self.route,data=self.form(amounts_adjustment_1_amount='-1'))
        self.assertEqual(response.status_code,400);self.assertIn('value="-1"',response.get_data(as_text=True))
        self.assertIsNone(self.saved)

    def test_exact_thirteen_master_tokens_and_part_five_excluded(self):
        state,errors=self.m.amounts_from_form(self.form(),self.items);self.assertFalse(errors)
        before=copy.deepcopy(state);values=self.m.amounts_placeholder_values({'_amounts':state})
        self.assertEqual(state,before);self.assertEqual(len(values),13)
        self.assertEqual(values['[MONTHLY RENT INITIAL]'],'R 10 000.00')
        self.assertEqual(values['[RENT ADJUSTMENT 1 DATE]'],'2027-07-01')
        self.assertEqual(values['[DEPOSIT AMOUNT]'],'R 10 000.00')
        self.assertFalse(any('BANK' in k or 'UTILITY' in k for k in values))
        master=ROOT/'app/program_billing/private_templates/Lease_Master_Stage5_Review.docx'
        self.assertEqual(hashlib.sha256(master.read_bytes()).hexdigest(),self.m.PARTIES_MASTER_SHA256)
        with ZipFile(master) as z:tree=E.fromstring(z.read('word/document.xml'))
        tokens=set(re.findall(r'\[[^\[\]]+\]',''.join(tree.xpath('//w:t/text()',namespaces={'w':'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}))))
        self.assertTrue(set(values)<=tokens)
        self.assertTrue(set(values).isdisjoint(self.m.party_placeholder_values(self.items)))
        self.assertTrue(set(values).isdisjoint(self.m.premises_placeholder_values(self.items)))
        self.assertTrue(set(values).isdisjoint(self.m.period_placeholder_values(self.items)))

    def test_period_required_and_no_legacy_amount_guessing(self):
        self.items['s1-18']={'value':''}
        _,errors=self.m.amounts_from_form(self.form(),self.items);self.assertTrue(any('Part 3' in e for e in errors))
        state=self.m.amounts_state({'s1-21':{'value':'R 9999 and uncertain words'}})
        self.assertEqual(state['monthly_rent'],'');self.assertEqual(state['rent_effective_date'],'')

    def test_structured_completion_does_not_require_obsolete_percentage_fields(self):
        state,errors=self.m.amounts_from_form(self.form(),self.items);self.assertFalse(errors)
        data=copy.deepcopy(self.items);data['_amounts']=state
        alerts=self.m.completion_alerts(data,[])
        obsolete={'s1-21','s1-22','s1-23','s1-24','s1-25','s1-26','s1-27','s1-28','s1-29','s1-30','s1-31','s1-32','s1-33','s1-34','s1-35'}
        self.assertFalse(any(group=='amounts' and key in obsolete for group,key,message in alerts))
        data['_amounts']['adjustment_1_date']='2028-01-01'
        self.assertTrue(any('six-month' in message for group,key,message in self.m.completion_alerts(data,[]) if group=='amounts'))
