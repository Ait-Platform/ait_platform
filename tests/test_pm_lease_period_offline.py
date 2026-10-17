"""Period route and master mapping regressions with no database connections."""
import copy, hashlib, re, unittest
from zipfile import ZipFile
from lxml import etree as E
import test_pm_lease_premises_offline as premises
ROOT=premises.ROOT

class PeriodOfflineTests(unittest.TestCase):
    def setUp(self):
        premises.PremisesOfflineTests.setUp(self)
        self.route=self.route.replace('/premises?','/period?')
        self.items['_premises']={'property_name':'Almond','leased_area_sqm':'300'}

    def form(self,**changes):
        data={'s1-17':'24 months','s1-18':'2027-01-01','s1-19':'2027-01-15','s1-20':'2028-12-31','draft_id':'synthetic-draft','version':'1'}
        data.update(changes);return data

    def items_from(self,data):return {key:{'value':value} for key,value in data.items() if key.startswith('s1-')}

    def test_distinct_dates_save_reload_and_preserve_parts_one_two_four_five(self):
        original=copy.deepcopy(self.items)
        self.assertEqual(self.client.post(self.route,data=self.form()).status_code,302)
        for key,value in original.items():
            if key not in ['s1-17','s1-18','s1-19','s1-20']:self.assertEqual(self.saved[key],value)
        self.assertEqual(self.saved['s1-18']['value'],'2027-01-01')
        self.assertEqual(self.saved['s1-19']['value'],'2027-01-15')
        self.draft['items']=self.saved
        html=self.client.get(self.route).get_data(as_text=True)
        for value in ['2027-01-01','2027-01-15','2028-12-31']:self.assertIn(value,html)
        self.assertNotIn('aria-label="Property Management"',html)

    def test_three_dates_required_and_no_occupation_autofill(self):
        for key in ['s1-18','s1-19','s1-20']:
            response=self.client.post(self.route,data=self.form(**{key:''}))
            self.assertEqual(response.status_code,400);self.assertIsNone(self.saved)
        html=self.client.get(self.route).get_data(as_text=True)
        self.assertIn('value=""',html)
        self.assertIn('occupation is not copied',html)

    def test_formats_leap_dates_and_chronology(self):
        invalid=[{'s1-18':'20270101'},{'s1-18':'01/01/2027'},{'s1-19':'2027-02-29'},
                 {'s1-20':'2026-12-31'},{'s1-19':'2029-01-01'},{'s1-19':'2026-12-31'}]
        for changes in invalid:
            response=self.client.post(self.route,data=self.form(**changes))
            self.assertEqual(response.status_code,400);self.assertIsNone(self.saved)
        self.assertFalse(self.m.period_alerts(self.items_from(self.form(**{'s1-19':'2028-02-29'})),True))

    def test_optional_duration_and_consistency(self):
        self.assertEqual(self.client.post(self.route,data=self.form(**{'s1-17':''})).status_code,302)
        self.saved=None
        for term in ['12 months','0 months','1201 months','two years']:
            self.assertEqual(self.client.post(self.route,data=self.form(**{'s1-17':term})).status_code,400)
            self.assertIsNone(self.saved)
        fields=self.m.period_field({'key':'s1-17','requirement':'Essential','placeholders':['[TERM TO CONFIRM]']})
        self.assertEqual(fields['requirement'],'Optional');self.assertFalse(fields['placeholders'])
        alerts=self.m.completion_alerts(self.items_from(self.form(**{'s1-17':''})),[])
        self.assertFalse(any(group=='period' for group,key,message in alerts))

    def test_authorization_and_schema_write_gates_unchanged(self):
        self.assertEqual(self.client.post(self.route.replace('/26/','/27/'),data=self.form()).status_code,404)
        self.blocked=True
        self.assertEqual(self.client.post(self.route,data=self.form()).status_code,503)
        self.assertIsNone(self.saved)
        self.assertIn('<fieldset disabled',self.client.get(self.route).get_data(as_text=True))

    def test_exact_master_tokens_and_no_mutation_or_rent_date_inference(self):
        items=self.items_from(self.form());before=copy.deepcopy(items)
        values=self.m.period_placeholder_values(items)
        self.assertEqual(values,{'[COMMENCEMENT DATE]':'2027-01-01','[OCCUPATION DATE]':'2027-01-15','[EXPIRY DATE]':'2028-12-31'})
        self.assertEqual(items,before)
        self.assertNotIn('[RENT EFFECTIVE DATE]',values)
        master=ROOT/'app/program_billing/private_templates/Lease_Master_Stage5_Review.docx'
        self.assertEqual(hashlib.sha256(master.read_bytes()).hexdigest(),self.m.PARTIES_MASTER_SHA256)
        with ZipFile(master) as z:tree=E.fromstring(z.read('word/document.xml'))
        tokens=set(re.findall(r'\[[^\[\]]+\]',''.join(tree.xpath('//w:t/text()',namespaces={'w':'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}))))
        self.assertTrue(set(values)<=tokens)
        self.assertNotIn('[TERM TO CONFIRM]',tokens)
        self.assertEqual(self.m.period_field({'key':'s1-20'})['placeholders'],['[EXPIRY DATE]'])

    def test_single_day_lease_and_calendar_bounds(self):
        data=self.form(**{'s1-17':'','s1-18':'2028-02-29','s1-19':'2028-02-29','s1-20':'2028-02-29'})
        self.assertFalse(self.m.period_alerts(self.items_from(data),True))
        data=self.form(**{'s1-17':'2 years','s1-18':'9999-01-01','s1-19':'9999-01-01','s1-20':'9999-12-31'})
        self.assertTrue(self.m.period_alerts(self.items_from(data),True))
