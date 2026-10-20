"""Focused section boundaries and independent banking selections per lease draft."""
import copy,json
from urllib.parse import urlparse,parse_qs
from sqlalchemy import text
from lxml import html
import test_pm_html_certificate as capture_cases


class CaptureSectionTests(capture_cases.HTMLCertificateTests):
    def test_selector_navigation_and_each_form_has_only_save_and_back(self):
        selector='/billing/leases/property/26/certificate?draft_id='+str(self.draft)
        page=self.client.get(selector);self.assertEqual(page.status_code,200)
        tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//a[@data-capture-section]/@data-capture-section'),['parties','premises','period','financial','banking','additional'])
        nav=tree.xpath('//div[@id="lease-header-actions"]/nav')[0]
        self.assertEqual(nav.xpath('./a/text()'),['Data Capture','View Lease','Banking Register','Addendum','Lease History'])
        self.assertEqual(nav.getprevious().text,'Back')
        self.assertFalse(tree.xpath('//textarea'))
        for key in ('parties','premises','period','financial','banking','additional'):
            _,_,response=self.open_form(section=key)
            form=html.fromstring(response.data).xpath('//form[@id="certificate-form"]')[0]
            self.assertEqual(form.xpath('.//button/text()'),['Save'])
            self.assertEqual(form.xpath('.//a/text()'),['Back'])
            self.assertEqual(form.xpath('.//a/@href'),[selector])
            self.assertFalse(form.xpath('.//a[contains(@href,"view") or contains(@href,".docx")]'))
        home=self.client.get('/billing/leases?property_id=26&draft_id='+str(self.draft))
        self.assertEqual(home.status_code,200)
        tree=html.fromstring(home.data)
        self.assertIn('Lease Management',tree.xpath('//h1/text()'))
        self.assertNotIn('DIY Lease Kit',home.get_data(as_text=True))
        self.assertEqual(tree.xpath('//*[@id="lease-management-actions"]/nav/a/text()'),['Data Capture','View Lease','Banking Register','Addendum','Lease History'])
        self.assertEqual(self.client.get('/billing/leases/property/26/certificate/not-a-section').status_code,404)
        self.assertEqual(self.client.post(selector,data={}).status_code,405)

    def test_saving_sections_preserves_all_other_sections_exactly(self):
        from app.program_billing import lease_certificate_form as model
        original={'_parties':{'lessee':{'legal_name':'Existing tenant','extra':'Keep legacy shape'}},
            '_premises':{'property_name':'Almond','excluded_areas':'Keep exclusions'},
            '_period':{'commencement_date':'2026-10-01','expiry_date':'2027-03-31'},
            '_amounts':{'monthly_rent':'999.00','unrelated_metadata':'Keep finance metadata'},
            '_certificate_banking':{'schema_version':1,'property_id':26,'bank_detail_id':77,'bank_name':'Saved snapshot bank','account_number':'LOCAL-TEST-ONLY'},
            '_lease_history':{'documents':[],'emails':[]},'_generations':[],
            '_ait_standard':{'authorities':{},'acknowledgement':{'preserve':'unchanged'}},
            'sc-01':{'value':'Keep condition'},'s1-40':{'value':'Keep original special particulars'},
            's1-25':{'value':'Existing payment day'},'custom_future_key':{'keep':[1,2,3]}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(original)});self.session.commit()
        cases={'premises':{'PROPERTY_ADDRESS':'Updated address'},'period':{'OCCUPATION_DATE':'2026-10-03','DURATION_MONTHS':'6'},
            'financial':{'DEPOSIT_AMOUNT':'1234.00'},'additional':{'VAT_TREATMENT':'User supplied VAT particulars'},
            'banking':{},'parties':{'LESSEE_1_SIGNATORY_NAME':'User supplied representative'}}
        for section,edits in cases.items():
            before=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar())
            url,data,_=self.open_form(section=section);data.update(edits)
            data['bank_detail_id']=data.get('bank_detail_id','999')
            if section!='parties':data['LESSEE_1_LEGAL_NAME']='Ignore cross-section injection'
            self.assertEqual(self.client.post(url,data=data).status_code,302)
            after=self.sql('SELECT items FROM bil_lease_draft').scalar()
            owned=set(model.capture_section(section)[3])
            self.assertEqual({k:v for k,v in before.items() if k not in owned},{k:v for k,v in after.items() if k not in owned},section)
            _,restored,_=self.open_form(section=section)
            for field,value in edits.items():self.assertEqual(restored[field],value)

    def test_three_leases_choose_accounts_independently_without_bank_duplicates(self):
        self.bank_setup()
        for number,name in (('0001234567','Synthetic account one'),('0001234568','Synthetic account two'),('0001234569','Unlinked account')):
            self.assertEqual(self.account(account_number=number,bank_name=name).status_code,302)
        register=html.fromstring(self.client.get(self.url()).data)
        self.assertFalse(register.xpath('//button[@name="use_for_lease"]'))
        self.assertIn('Link Bank Account',register.text_content())
        accounts=list(self.sql('SELECT id FROM bil_bank_detail ORDER BY id').scalars())
        for bank in accounts[:2]:self.assertEqual(self.client.post(self.url(),data={'action':'allocate_existing','bank_detail_id':str(bank)}).status_code,302)
        url,data,_=self.open_form();data['LESSEE_1_LEGAL_NAME']='Tenant A'
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        leases=[self.draft]
        for name in ('Tenant B','Tenant C'):
            url,data,_=self.open_form(False);data['LESSEE_1_LEGAL_NAME']=name
            created=self.client.post(url,data=data);self.assertEqual(created.status_code,302)
            leases.append(parse_qs(urlparse(created.location).query)['draft_id'][0])
        original_id=self.draft
        before_property=dict(self.sql('SELECT * FROM bil_property WHERE id=26').mappings().one())
        before_links=[dict(row) for row in self.sql('SELECT * FROM bil_property_payment_account ORDER BY id').mappings()]
        for index,(lease_id,bank_id) in enumerate(zip(leases,(accounts[0],accounts[1],accounts[0]))):
            self.draft=lease_id
            url,data,page=self.open_form(section='banking')
            self.assertEqual(data['bank_detail_id'],'')
            ids=html.fromstring(page.data).xpath('//select[@name="bank_detail_id"]/option/@value')
            self.assertNotIn(str(accounts[2]),ids)
            data['bank_detail_id']=str(bank_id)
            self.assertEqual(self.client.post(url,data=data).status_code,302)
            _,restored,_=self.open_form(section='banking');self.assertEqual(restored['bank_detail_id'],str(bank_id))
            expected='Synthetic account two' if index==1 else 'Synthetic account one'
            preview=self.client.get(self.preview_url()).get_data(as_text=True)
            self.assertIn(expected,preview)
            self.assertIn(('Tenant A','Tenant B','Tenant C')[index],preview)
        saved={str(row['id']):copy.deepcopy(row['items']) for row in self.sql('SELECT id,items FROM bil_lease_draft').mappings()}
        self.draft=leases[0]
        url,data,_=self.open_form(section='banking');data['bank_detail_id']=str(accounts[1])
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        after={str(row['id']):row['items'] for row in self.sql('SELECT id,items FROM bil_lease_draft').mappings()}
        for lease_id in leases[1:]:self.assertEqual(after[str(lease_id)],saved[str(lease_id)])
        self.assertEqual(after[str(leases[0])]['_certificate_banking']['bank_detail_id'],accounts[1])
        self.assertEqual(self.sql('SELECT count(*) FROM bil_bank_detail').scalar(),3)
        self.assertEqual(dict(self.sql('SELECT * FROM bil_property WHERE id=26').mappings().one()),before_property)
        self.assertEqual([dict(row) for row in self.sql('SELECT * FROM bil_property_payment_account ORDER BY id').mappings()],before_links)
        self.draft=original_id
