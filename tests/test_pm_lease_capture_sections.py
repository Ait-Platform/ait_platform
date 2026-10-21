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
        self.assertFalse(tree.xpath('//nav[contains(@class,"lease-main-navigation")]'))
        self.assertTrue(tree.xpath('//form[@id="certificate-form"]'))
        for key in ('parties','premises','period','financial','banking','additional'):
            _,_,response=self.open_form(section=key)
            tree=html.fromstring(response.data)
            form=tree.xpath('//form[@id="certificate-form"]')[0]
            self.assertEqual(tree.xpath('//button[@form="certificate-form"]/text()'),['Save'])
            self.assertEqual(tree.xpath('//a[@data-capture-back]/text()'),['Back'])
            self.assertEqual(tree.xpath('//a[@data-capture-back]/@href'),['/billing/leases?property_id=26&draft_id='+str(self.draft)])
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
        cases={'premises':{'PROPERTY_ADDRESS':'Updated address'},'period':{'DURATION_MONTHS':'6'},
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

    def test_https_proxy_csrf_all_six_tabs_and_reference(self):
        from flask import Flask
        from flask_wtf import CSRFProtect
        from werkzeug.middleware.proxy_fix import ProxyFix
        secure=Flask('capture-https-proxy')
        secure.config.update(TESTING=True,SECRET_KEY='local-test-only')
        secure.jinja_loader=self.app.jinja_loader
        self.app.login_manager.init_app(secure);CSRFProtect(secure)
        secure.wsgi_app=ProxyFix(secure.wsgi_app,x_proto=1,x_host=1)
        secure.register_blueprint(self.app.blueprints['billing_bp']);client=secure.test_client()
        with client.session_transaction() as session:session['_user_id']='7';session['_fresh']=True
        proxy={'X-Forwarded-Proto':'https','X-Forwarded-Host':'localhost'}
        for section in ('parties','premises','period','financial','banking','additional'):
            url='/billing/leases/property/26/certificate/'+section+'?draft_id='+str(self.draft)
            page=client.get(url,headers=proxy);self.assertEqual(page.status_code,200)
            self.assertEqual(page.headers['Referrer-Policy'],'same-origin')
            tree=html.fromstring(page.data)
            self.assertEqual(tree.xpath('//nav[@class="capture-tabs"]/a[@aria-current]/@data-capture-section'),[section])
            data={node.get('name'):node.get('value','') for node in tree.xpath('//form[@id="certificate-form"]//input[@name]')}
            data['action']='save'
            before=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar())
            # Valid token without a referrer still fails: strict HTTPS protection is intact.
            missing=client.post(url,data=data,headers=proxy)
            self.assertEqual(missing.status_code,400)
            self.assertIn(b'The referrer header is missing',missing.data)
            self.assertEqual(client.post(url,data=data,headers=dict(proxy,Referer='https://foreign.invalid/')).status_code,400)
            invalid=dict(data,csrf_token='invalid')
            headers=dict(proxy,Referer='https://localhost'+url)
            self.assertEqual(client.post(url,data=invalid,headers=headers).status_code,400)
            self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),before)
            saved=client.post(url,data=data,headers=headers);self.assertEqual(saved.status_code,302)
            self.assertEqual(client.get(saved.location,headers=proxy).status_code,200)
            self.assertEqual(client.post(url,data=data,headers=headers).status_code,409)
        items=self.sql('SELECT items FROM bil_lease_draft').scalar()
        self.assertEqual(items['s1-01']['value'],'LEASE-'+str(self.draft))

    def test_parties_simple_fields_preserve_evidence_and_existing_reference(self):
        original={'s1-01':{'value':'Historical reference'},'_parties':{'lessor':{'signatory_name':'Existing name'}},
            '_certificate_authorities':{'landlords':{'evidence':'Existing evidence','confirmed':False}}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(original)});self.session.commit()
        url,data,page=self.open_form()
        tree=html.fromstring(page.data)
        self.assertFalse(tree.xpath('//*[@name="LEASE_REFERENCE" or contains(@name,"AUTHORITY")]'))
        self.assertNotIn('Historical reference',page.get_data(as_text=True))
        data.update(LEASE_REFERENCE='Injected reference',LESSOR_AUTHORITY_EVIDENCE='Injected evidence',LESSOR_SIGNATORY_NAME='Updated name')
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        after=self.sql('SELECT items FROM bil_lease_draft').scalar()
        self.assertEqual(after['s1-01'],original['s1-01'])
        self.assertEqual(after['_certificate_authorities'],original['_certificate_authorities'])
        self.assertEqual(after['_parties']['lessor'],original['_parties']['lessor'])
        url,data,_=self.open_form(False);data['LESSEE_1_LEGAL_NAME']='New local tenant'
        saved=self.client.post(url,data=data);self.assertEqual(saved.status_code,302)
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        identifier=parse_qs(urlparse(saved.location).query)['draft_id'][0]
        created=self.session.execute(text('SELECT items FROM bil_lease_draft WHERE id=:i'),{'i':identifier}).scalar()
        self.assertEqual(created['s1-01']['value'],'LEASE-'+identifier)

    def test_removed_inputs_preserve_legacy_values_and_narratives(self):
        original={'_premises':{'lease_scope':'Historical scope','leased_area_sqm':'400','plan_a_identifier':'Original plan'},
            '_parties':{'lessor':{'signatory_capacity':'Historical capacity'},'lessee':{'legal_name':'Saved tenant','signatory_capacity':'Historical tenant capacity'}},
            '_amounts':{'monthly_rent':'0','monthly_rent_words':'Old manual words','rent_effective_date':'2025-01-01',
                'adjustment_2_amount':'-123.45','adjustment_3_date':'2025-03-01','parking_details':'Original parking agreement'},
            's1-26':{'value':'Original renewal clause'},'_lease_history':{'documents':[]}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(original)});self.session.commit()
        cases={'parties':{'LESSOR_SIGNATORY_CAPACITY':'forged'},'premises':{'LEASE_SCOPE':'forged'},'period':{'RENEWAL_PROVISION':'forged'},
            'financial':{'MONTHLY_RENT_WORDS':'forged','RENT_EFFECTIVE_DATE':'2029-01-01','ADJUSTMENT_2_AMOUNT':'200'},'additional':{}}
        for key,forged in cases.items():
            url,data,page=self.open_form(section=key)
            tree=html.fromstring(page.data)
            for field in forged:self.assertFalse(tree.xpath('//*[@name="'+field+'"]'))
            data.update(forged)
            if key=='additional':data.update(RENTAL_ADJUSTMENTS_TEXT='User rental terms',PARKING_OTHER_CHARGES_TEXT='User parking terms',UTILITIES_ALLOCATION_TEXT='User utility terms')
            self.assertEqual(self.client.post(url,data=data).status_code,302)
        after=self.sql('SELECT items FROM bil_lease_draft').scalar()
        for name,value in original['_premises'].items():self.assertEqual(after['_premises'][name],value)
        for name,value in original['_amounts'].items():self.assertEqual(after['_amounts'][name],value)
        self.assertEqual(after['s1-26'],original['s1-26'])
        self.assertEqual(after['_parties']['lessor']['signatory_capacity'],'Historical capacity')
        preview=self.client.get(self.preview_url()).get_data(as_text=True)
        for value in ('User rental terms','User parking terms','User utility terms','Original parking agreement','minus one hundred and twenty-three rand and forty-five cents'):
            self.assertIn(value,preview)
        _,data,page=self.open_form(section='financial')
        self.assertEqual(data['MONTHLY_RENT'],'0')
        self.assertNotIn('RENTAL_ADJUSTMENTS_TEXT',data)
        self.assertIn('Previously saved financial particulars',page.get_data(as_text=True))
        _,data,page=self.open_form(section='additional')
        self.assertEqual(data['RENTAL_ADJUSTMENTS_TEXT'],'User rental terms')

    def tenant_fixture(self):
        self.sql('CREATE TEMP TABLE bil_tenant (id integer PRIMARY KEY,manager_id integer,property_id integer,sectional_unit_id integer,party_type text,name text,identity_number text,address text,email text,phone text)')
        self.sql('CREATE TEMP TABLE bil_tenant_identity (id integer,tenant_id integer,manager_id integer,legal_name text,identity_number text,email text,phone text)')
        self.sql("INSERT INTO bil_tenant VALUES (71,7,26,NULL,'juristic_person','Saved tenant','REG-71','Test address','test@example.invalid','000'),(72,8,27,NULL,'natural_person','Other manager tenant','REG-72','Other address','','')")
        self.sql("INSERT INTO bil_tenant_identity VALUES (1,71,7,'Saved representative','REP-71','','')");self.session.commit()

    def test_select_existing_tenant_persists_scoped_identity_and_representative(self):
        self.tenant_fixture()
        url,data,page=self.open_form()
        tree=html.fromstring(page.data)
        self.assertTrue(tree.xpath('//button[@id="add-lessee"]'))
        self.assertEqual(tree.xpath('//select[@name="tenant_register_id_1" and not(ancestor::template)]/option/@value'),['','71'])
        data['tenant_register_id_1']='72'
        self.assertEqual(self.client.post(url,data=data).status_code,404)
        data['tenant_register_id_1']='71'
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        _,restored,page=self.open_form()
        self.assertEqual(restored['tenant_register_id_1'],'71')
        for key,value in (('LEGAL_NAME','Saved tenant'),('IDENTITY_NUMBER','REG-71'),('SIGNATORY_NAME','Saved representative'),('SIGNATORY_IDENTITY_NUMBER','REP-71')):
            self.assertEqual(restored['LESSEE_1_'+key],value)
        saved=self.sql('SELECT items FROM bil_lease_draft').scalar()
        self.assertEqual(saved['_parties']['lessee']['tenant_id'],71)
        self.assertEqual(saved['_parties']['lessee']['address'],'Test address')
        self.assertNotIn('confirmed',saved['_parties']['lessee'])

    def test_id_only_bank_snapshot_repaired_and_complete_snapshot_preserved(self):
        self.bank_setup();self.assertEqual(self.account(branch_name='Synthetic branch').status_code,302)
        bank=self.sql('SELECT id FROM bil_bank_detail').scalar()
        self.assertEqual(self.client.post(self.url(),data={'action':'allocate_existing','bank_detail_id':str(bank)}).status_code,302)
        original={'_certificate_banking':{'schema_version':1,'property_id':26,'bank_detail_id':bank},'_lease_history':{'documents':[]}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(original)});self.session.commit()
        url,data,page=self.open_form(section='banking')
        self.assertIn('[ACCOUNT_NUMBER]',page.get_data(as_text=True))
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        snapshot=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar()['_certificate_banking'])
        self.assertEqual(snapshot['branch_name'],'Synthetic branch')
        _,_,page=self.open_form(section='banking')
        self.assertEqual(html.fromstring(page.data).xpath('//*[@data-bank-field="branch_name"]/text()'),['Synthetic branch'])
        self.assertIn('Synthetic branch',self.client.get(self.preview_url()).get_data(as_text=True))
        self.assertIn('All banking particulars restored.',page.get_data(as_text=True))
        self.assertTrue(html.fromstring(page.data).xpath('//fieldset[@id="bank-add-operation"]'))
        self.sql("UPDATE bil_bank_detail SET branch_name='Later register branch'");self.session.commit()
        url,data,_=self.open_form(section='banking')
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar()['_certificate_banking'],snapshot)
        url,data,_=self.open_form(section='banking');data['refresh_bank_snapshot']='1'
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar()['_certificate_banking']['branch_name'],'Later register branch')

    def test_money_wording_is_render_only_and_supports_signed_decimal_zero(self):
        from app.program_billing import lease_certificate as cert
        examples={'0':'zero rand and zero cents','1.01':'one rand and one cent','0.09':'zero rand and nine cents',
            '-123.45':'minus one hundred and twenty-three rand and forty-five cents',
            '1000.10':'one thousand rand and ten cents','1000000001.99':'one billion and one rand and ninety-nine cents'}
        for raw,expected in examples.items():self.assertEqual(cert.money_words(raw),expected)
        for raw in ('NaN','Infinity','1.999','1e3','1,000','R10'):
            with self.assertRaises(ValueError):cert.money_amount(raw)
        items={'_amounts':{'monthly_rent':'0','deposit_amount':'-123.45','monthly_rent_words':'Preserve manual legacy words'}}
        before=copy.deepcopy(items);values=cert.document_values(items)
        self.assertEqual(values['[MONTHLY_RENT]'],'R0.00')
        self.assertEqual(values['[MONTHLY_RENT_WORDS]'],'(zero rand and zero cents)')
        self.assertEqual(values['[DEPOSIT_AMOUNT]'],'-R123.45 (minus one hundred and twenty-three rand and forty-five cents)')
        self.assertEqual(cert.values(items)['[DEPOSIT_AMOUNT]'],'-123.45')
        self.assertEqual(items,before)
        items['_amounts'].update(rental_adjustments_text='Separate new rental narrative',parking_other_charges_text='Separate new parking narrative')
        output,_=cert.document(items)
        rendered=''.join(cert.xml(output).itertext())
        self.assertIn('Additional particulars: Separate new rental narrative',rendered)
        self.assertIn('Separate new parking narrative',rendered)
        self.assertIn('-R123.45 (minus one hundred and twenty-three rand and forty-five cents)',rendered)
        self.assertIn('[ADJUSTMENT_1_APPLICABILITY]',rendered)

    def test_final_layout_owner_modal_unit_and_top_controls(self):
        self.assign(name='Existing Lessor',identity_number='OWNER-UNIT')
        from app.program_billing import lease_workflow as workflow
        owner=self.sql('SELECT id AS owner_id,display_name AS name,identity_number FROM bil_owner_identity ORDER BY display_name LIMIT 1').mappings().one()
        original={'_parties':{'lessor':{'owner_id':str(owner['owner_id']),'signatory_identity_number':owner['identity_number']}},'_certificate_authorities':{'landlords':{'evidence':'Preserve evidence','confirmed':False}}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(original)});self.session.commit()
        home=html.fromstring(self.client.get('/billing/leases?property_id=26&draft_id='+str(self.draft)).data)
        self.assertTrue(home.xpath('//*[@class="lease-management-header"]/h1'))
        for text_value in (str(self.draft),'R01','Revision','DRAFT'):self.assertNotIn(text_value,home.text_content())
        for key in ('parties','premises','period','financial','banking','additional'):
            _,_,page=self.open_form(section=key);tree=html.fromstring(page.data)
            self.assertEqual(tree.xpath('//button[@form="certificate-form"]/text()'),['Save'])
            self.assertEqual(tree.xpath('//a[@data-capture-back]/text()'),['Back'])
            self.assertFalse(tree.xpath('//form//div[@class="capture-actions"]'))
        url,data,page=self.open_form()
        tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//*[@class="party-controls"]//button/text()'),['Show Lessors','Select Existing Tenant','Add Lessee'])
        self.assertFalse(tree.xpath('//textarea[starts-with(@name,"LESSOR_")]'))
        self.assertIn('Existing Lessor',tree.xpath('//dialog[@id="lessors-dialog"]')[0].text_content())
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar()['_certificate_authorities'],original['_certificate_authorities'])
        url,data,_=self.open_form(section='premises');data['LEASED_PORTION_DESCRIPTION']='Unit 4B'
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        self.assertEqual(self.open_form(section='premises')[1]['LEASED_PORTION_DESCRIPTION'],'Unit 4B')
        self.assertIn('Unit 4B',self.client.get(self.preview_url()).get_data(as_text=True))

    def test_calculated_dates_conflicts_and_save_back_use_normal_validation(self):
        original={'_period':{'commencement_date':'2026-10-01','duration_months':'6','expiry_date':'2027-04-01','renewal_notice_date':'2026-12-30'},'s1-19':{'value':'2026-10-02'}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(original)});self.session.commit()
        url,data,page=self.open_form(section='period')
        self.assertNotIn('OCCUPATION_DATE',data);self.assertNotIn('EXPIRY_DATE',data)
        self.assertIn('saved 2027-04-01; calculated 2027-03-31',page.get_data(as_text=True))
        data.update(EXPIRY_DATE='2099-01-01',RENEWAL_NOTICE_DATE='2099-01-01',capture_return='back')
        self.assertEqual(self.client.post(url,data=data).status_code,400)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),original)
        data['confirm_period_dates']='1'
        saved=self.client.post(url,data=data);self.assertEqual(saved.status_code,302)
        self.assertIn('/billing/leases?property_id=26',saved.location)
        self.assertEqual(self.client.post(url,data=data).status_code,409)
        after=self.sql('SELECT items FROM bil_lease_draft').scalar()
        self.assertEqual(after['_period']['expiry_date'],'2027-03-31')
        self.assertEqual(after['_period']['renewal_notice_date'],'2026-12-31')
        self.assertEqual(after['s1-19'],original['s1-19'])
        before=copy.deepcopy(after)
        calculated=self.client.get(url+'&calculate=1&COMMENCEMENT_DATE=2024-02-01&DURATION_MONTHS=1')
        self.assertEqual(calculated.json['dates']['expiry_date'],'2024-02-29')
        self.assertEqual(calculated.json['dates']['renewal_notice_date'],'2023-11-30')
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),before)
        url,data,_=self.open_form(section='period');data.update(DURATION_MONTHS='nonsense',capture_return='back')
        self.assertEqual(self.client.post(url,data=data).status_code,400)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),before)

    def test_additional_conditions_move_without_rewriting_financial_records(self):
        original={'_amounts':{'monthly_rent':'100.50','deposit_amount':'0','adjustment_2_amount':'-10.25','rental_adjustments_text':'Existing rental conditions','parking_other_charges_text':'Existing parking conditions','utilities_allocation_text':'Existing allocation conditions'}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(original)});self.session.commit()
        url,data,_=self.open_form(section='financial')
        self.assertNotIn('RENTAL_ADJUSTMENTS_TEXT',data)
        data['RENTAL_ADJUSTMENTS_TEXT']='Forged financial-tab edit'
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar()['_amounts'],original['_amounts'])
        url,data,page=self.open_form(section='additional')
        self.assertIn('Additional Conditions',page.get_data(as_text=True))
        self.assertEqual(data['RENTAL_ADJUSTMENTS_TEXT'],'Existing rental conditions')
        data['PARKING_OTHER_CHARGES_TEXT']='Updated parking conditions'
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        after=self.sql('SELECT items FROM bil_lease_draft').scalar()['_amounts']
        self.assertEqual({k:v for k,v in after.items() if k!='parking_other_charges_text'},{k:v for k,v in original['_amounts'].items() if k!='parking_other_charges_text'})
        self.assertIn('Updated parking conditions',self.client.get(self.preview_url()).get_data(as_text=True))

    def test_inline_bank_add_reuses_register_and_does_not_select_account(self):
        self.bank_setup();self.assertEqual(self.account().status_code,302)
        bank=self.sql('SELECT id FROM bil_bank_detail').scalar()
        self.assertEqual(self.client.post(self.url(),data={'action':'allocate_existing','bank_detail_id':str(bank)}).status_code,302)
        url,data,_=self.open_form(section='banking');data['bank_detail_id']=str(bank)
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        original=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar()['_certificate_banking'])
        for repeat in range(2):
            url,data,_=self.open_form(section='banking')
            data.update(bank_operation='add',bank_name='New synthetic bank',branch_name='New branch',account_holder='New holder',account_number='0009876543',account_type='current',branch_code='123456')
            self.assertEqual(self.client.post(url,data=data).status_code,302)
            self.assertEqual(self.client.post(url,data=data).status_code,409)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_bank_detail').scalar(),2)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_payment_account').scalar(),2)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar()['_certificate_banking'],original)
        url,data,_=self.open_form(section='banking');data.update(bank_operation='add',account_number='invalid')
        self.assertEqual(self.client.post(url,data=data).status_code,400)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_bank_detail').scalar(),2)
