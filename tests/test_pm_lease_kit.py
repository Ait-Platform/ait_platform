"""Document-only Lease Kit workflow; no production records or mail sends."""
import copy,json
from unittest.mock import patch
from sqlalchemy import text
from lxml import html
from test_pm_html_certificate import HTMLCertificateTests


class LeaseKitTests(HTMLCertificateTests):
    def view_url(self,key='cover'):
        return '/billing/leases/property/26/view?draft_id='+str(self.draft)+'&component='+key

    def test_six_selections_controls_and_no_word_operations(self):
        from app.program_billing import lease_standard as standard,lease_certificate as cert,lease_certificate_render as renderer
        before=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar())
        with patch.object(standard,'source_master',side_effect=AssertionError('Source read')),patch.object(standard,'build_document',side_effect=AssertionError('Word build')),patch.object(cert,'document',side_effect=AssertionError('Word certificate')),patch.object(renderer,'render_pdf',side_effect=AssertionError('LibreOffice')):
            for key,printable in (('cover',True),('certificate',True),('core',False),('special_conditions',False),('signature',False),('lease',False)):
                response=self.client.get(self.view_url(key))
                self.assertEqual(response.status_code,200)
                tree=html.fromstring(response.data)
                self.assertEqual(tree.xpath('//nav[@id="lease-kit-selectors"]/a/text()'),['Cover','Certificate','Core','Special Conditions','Signature','Lease'])
                self.assertFalse(tree.xpath('//form|//textarea|//input|//iframe'))
                self.assertFalse(tree.xpath('//a[contains(@href,".docx") or contains(text(),"Word")]'))
                actions=tree.xpath('//div[@id="lease-header-actions"]')[0]
                self.assertEqual(actions.xpath('./a/text()'),['Back'])
                self.assertEqual(actions.xpath('./div[@id="lease-kit-actions"]/button/text()'),['Email','Print'])
                self.assertEqual(bool(tree.xpath('//button[@id="lease-kit-print" and not(@disabled)]')),printable)
                self.assertTrue(tree.xpath('//button[@id="lease-kit-email" and @disabled]'))
                if key=='cover':self.assertTrue(tree.xpath('//*[@id="existing-cover"]'));self.assertFalse(tree.xpath('//*[@id="approved-certificate"]'))
                if key=='certificate':self.assertTrue(tree.xpath('//*[@id="approved-certificate"]'));self.assertFalse(tree.xpath('//*[@id="existing-cover"]'))
                if key in ('core','signature','lease'):self.assertIn('unavailable',response.get_data(as_text=True))
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),before)
        self.assertEqual(self.client.post(self.view_url(),data={}).status_code,405)
        self.assertEqual(self.client.get(self.view_url('other')).status_code,404)

    def test_owner_corrections_are_live_in_working_previews_but_not_signed_snapshots(self):
        from app.program_billing import lease_workflow as workflow
        owners=[{'owner_id':str(i),'type':'company','name':'Synthetic Owner '+str(i),'identity_number':'ID-'+str(i)} for i in (1,2,3)]
        items={'_contracting_lessors':{'identities':copy.deepcopy(owners)},'_parties':{'lessee':{'legal_name':'WBH NPC','signatory_name':'Saved tenant representative'}},'_period':{'commencement_date':'2026-10-01'},'sc-01':{'value':'Preserve saved condition'}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(items)});self.session.commit()
        with patch.object(workflow,'owners_for',side_effect=lambda _:copy.deepcopy(owners)):
            first=self.client.get(self.view_url()).get_data(as_text=True)
            for owner in owners:self.assertIn(owner['name'],first)
            owners[0]['name']='Corrected Synthetic Owner'
            self.assertIn('Corrected Synthetic Owner',self.client.get(self.view_url()).get_data(as_text=True))
            certificate=self.client.get(self.view_url('certificate')).get_data(as_text=True)
            self.assertIn('Corrected Synthetic Owner',certificate)
            self.assertIn('WBH NPC',certificate)
            self.assertIn('[MONTHLY_RENT]',certificate)
            self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),items)
            signed=copy.deepcopy(items);signed['_lease_history']={'documents':[{'kind':'signed_original'}]}
            self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(signed)});self.session.commit()
            frozen=self.client.get(self.view_url('certificate')).get_data(as_text=True)
            self.assertIn('Synthetic Owner 1',frozen);self.assertNotIn('Corrected Synthetic Owner',frozen)
            self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),signed)
            blank_flag=self.client.get(self.view_url('certificate')+'&blank=1').get_data(as_text=True)
            self.assertIn('Synthetic Owner 1',blank_flag);self.assertNotIn('Corrected Synthetic Owner',blank_flag)

    def test_saved_conditions_draft_and_complete_lease_gate(self):
        from app.program_billing import lease_kit
        items={'sc-01':{'value':'User-supplied condition only; no invented terms.'}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(items)});self.session.commit()
        conditions=self.client.get(self.view_url('special_conditions')).get_data(as_text=True)
        self.assertIn(items['sc-01']['value'],conditions)
        self.assertIn('draft text only',conditions)
        self.assertNotIn('id="approved-certificate"',conditions)
        incomplete=self.client.get(self.view_url('lease')).get_data(as_text=True)
        self.assertNotIn('id="existing-cover"',incomplete)
        self.assertNotIn('id="approved-certificate"',incomplete)
        # Only fixture-approved independently supplied components permit full assembly.
        states=lease_kit.component_states(items)
        for key in ('core','special_conditions','signature'):
            states[key].update(available=True,printable=True,text='Fixture approved '+key,reason='')
        states['lease'].update(available=True,printable=True)
        with patch.object(lease_kit,'component_states',return_value=states):
            complete=self.client.get(self.view_url('lease')).get_data(as_text=True)
            positions=[complete.index(value) for value in ('id="existing-cover"','id="approved-certificate"','Fixture approved core','Fixture approved special_conditions','Fixture approved signature')]
            self.assertEqual(positions,sorted(positions))
            tree=html.fromstring(complete)
            self.assertTrue(tree.xpath('//button[@id="lease-kit-print" and not(@disabled)]'))

    def test_current_owners_populate_blank_cover_without_creating_draft(self):
        from app.program_billing import lease_workflow as workflow
        count=self.sql('SELECT count(*) FROM bil_lease_draft').scalar()
        owners=[{'owner_id':'1','type':'company','name':'Saved property owner','identity_number':'TEST-ID'}]
        with patch.object(workflow,'owners_for',return_value=owners):
            page=self.client.get('/billing/leases/property/26/view').get_data(as_text=True)
            self.assertIn('Saved property owner',page)
            self.assertNotIn('<form',page)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease_draft').scalar(),count)

    def test_owners_are_captured_only_in_property_management(self):
        self.assertEqual(self.assign(name='Registered owner only',identity_number='SOURCE-OWNER').status_code,200)
        url,data,page=self.open_form()
        tree=html.fromstring(page.data)
        self.assertFalse(tree.xpath('//textarea[starts-with(@name,"LESSOR_1_")]'))
        self.assertFalse(tree.xpath('//button[@value="add_lessor" or @value="remove_lessor"]'))
        data.update(LESSOR_1_NAME='Forged owner edit',LESSOR_1_IDENTITY_NUMBER='WRONG',lessor_count='5')
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        saved=self.sql('SELECT items FROM bil_lease_draft').scalar()['_contracting_lessors']['identities']
        self.assertEqual(len(saved),1)
        self.assertEqual(saved[0]['name'],'Registered owner only')
        self.assertEqual(saved[0]['identity_number'],'SOURCE-OWNER')
        self.assertEqual(self.sql('SELECT display_name FROM bil_owner_identity').scalar(),'Registered owner only')

    def test_representative_correction_does_not_retain_previous_owner_link(self):
        self.assertEqual(self.assign(name='Registered Owner',identity_number='OWNER-REP').status_code,200)
        owner_id=str(self.sql('SELECT id FROM bil_owner_identity').scalar())
        items={'_parties':{'lessor':{'owner_id':owner_id,'signatory_name':'Registered Owner',
            'signatory_identity_number':'OWNER-REP','signatory_capacity':'Prior saved capacity',
            'identity_source':'property_owner_register','capacity_confirmed':True}}}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:i AS jsonb)'),{'i':json.dumps(items)});self.session.commit()
        url,data,_=self.open_form()
        data.update(LESSOR_SIGNATORY_NAME='New representative supplied by user',LESSOR_SIGNATORY_IDENTITY_NUMBER='NEW-REP',LESSOR_SIGNATORY_CAPACITY='')
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        preview=self.client.get(self.view_url('certificate')).get_data(as_text=True)
        self.assertIn('New representative supplied by user',preview)
        self.assertIn('Unconfirmed',preview)
        saved=self.sql('SELECT items FROM bil_lease_draft').scalar()['_parties']['lessor']
        self.assertNotIn('owner_id',saved)
        self.assertFalse(saved['capacity_confirmed'])
        self.assertEqual(self.sql('SELECT display_name FROM bil_owner_identity').scalar(),'Registered Owner')
