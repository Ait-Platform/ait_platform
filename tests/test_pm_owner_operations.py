"""Enabled owner operations on local temporary fixtures using prepared revisions."""
import importlib.util
import unittest
from unittest.mock import patch
import sqlalchemy as sa
from alembic.operations import Operations
from alembic.migration import MigrationContext
import test_pm_property_management as property_tests

class OwnerOperationsTests(unittest.TestCase):
    sign_in=property_tests.PropertyManagementTests.sign_in
    def setUp(self):
        property_tests.PropertyManagementTests.setUp(self)
        migration_root=property_tests.ROOT.parent/'pm-owner-stage2'/'migrations'/'versions'
        if not (migration_root/'pm_owner_002.py').exists():self.skipTest('Prepared migration worktree required for enabled integration tests')
        self.session.rollback()
        with self.conn.begin():
            self.conn.exec_driver_sql('CREATE TEMP TABLE "user"(id integer PRIMARY KEY)')
            self.conn.exec_driver_sql('INSERT INTO "user" VALUES (7),(8)')
            for revision in ('pm_owner_001','pm_owner_002'):
                spec=importlib.util.spec_from_file_location(revision,migration_root/(revision+'.py'));mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
                mod.op=Operations(MigrationContext.configure(self.conn));mod.upgrade()
            self.conn.exec_driver_sql("CREATE TEMP TABLE alembic_version(version_num varchar(32)); INSERT INTO alembic_version VALUES ('pm_owner_002')")
            self.conn.exec_driver_sql("INSERT INTO bil_property(id,name,address,manager_id,is_archived) VALUES (28,'Property Two','Other address',7,false)")
        self.app.config['PM_OWNER_WRITES_APPROVED']=True

    def sql(self,query):return self.session.execute(sa.text(query))
    def payload(self,**overrides):
        data=dict(type='individual',name='Owner One',identity_number='ID001',email='one@example.invalid',phone='123',residential_address='Owner home',postal_address='PO Box',percentage='50')
        data.update(overrides);return data
    def assign(self,property_id=26,**overrides):return self.client.post(f'/billing/property-owner/{property_id}/owners',json=self.payload(**overrides))

    def test_new_owner_atomic_save_sync_and_almond_preserved(self):
        response=self.assign(postal_same=True)
        self.assertEqual(response.status_code,200)
        owners=response.get_json()['owners'];self.assertEqual(len(owners),1);self.assertEqual(owners[0]['postal_address'],'Owner home')
        self.assertEqual(self.sql('SELECT name,address FROM bil_property WHERE id=26').one(),('Almond','Original address'))
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),1)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership').scalar(),1)

    def test_reuse_identity_across_properties_and_retry_without_duplicates(self):
        response=self.assign();owner_id=response.get_json()['owners'][0]['owner_id']
        self.assertEqual(self.assign().status_code,200)
        self.assertEqual(self.client.post('/billing/property-owner/28/owners',json={'owner_id':owner_id,'percentage':'100'}).status_code,200)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),1)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership').scalar(),2)
        self.assertEqual(len(self.client.get('/billing/property-owner/owners?q=Owner').get_json()['owners']),1)

    def test_multiple_owner_types_assigned_to_almond(self):
        for kind,number in [('individual','ID1'),('company','REG2'),('trust','TR3')]:self.assertEqual(self.assign(type=kind,identity_number=number).status_code,200)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),3)
        html=self.client.get('/billing/property-owner/26/owners/edit').get_data(as_text=True)
        for value in ('ID1','REG2','TR3','Owner home','PO Box'):self.assertIn(value,html)

    def test_shared_owner_edit_preserves_assignment_snapshots(self):
        owner=self.assign().get_json()['owners'][0];owner_id=owner['owner_id']
        self.client.post('/billing/property-owner/28/owners',json={'owner_id':owner_id})
        original=self.sql('SELECT owner_identity_snapshot FROM bil_property_ownership ORDER BY id').scalars().all()
        response=self.client.patch(f'/billing/property-owner/owners/{owner_id}',json=self.payload(name='Corrected Owner',postal_address='New postal'))
        self.assertEqual(response.status_code,200)
        self.assertEqual(self.sql('SELECT display_name,postal_address FROM bil_owner_identity').one(),('Corrected Owner','New postal'))
        self.assertEqual(self.sql('SELECT owner_identity_snapshot FROM bil_property_ownership ORDER BY id').scalars().all(),original)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership').scalar(),2)

    def test_percentage_edit_and_removal_only_affect_selected_property(self):
        owner=self.assign().get_json()['owners'][0]
        self.client.post('/billing/property-owner/28/owners',json={'owner_id':owner['owner_id'],'percentage':'100'})
        url=f"/billing/property-owner/26/assignments/{owner['assignment_id']}"
        changed=self.client.patch(url,json={'percentage':'25'});self.assertEqual(changed.status_code,200)
        current=changed.get_json()['owners'][0]
        response=self.client.delete(f"/billing/property-owner/26/assignments/{current['assignment_id']}")
        self.assertEqual(response.status_code,200);self.assertEqual(response.get_json()['owners'],[])
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),1)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership WHERE property_id=28').scalar(),1)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership WHERE property_id=26').scalar(),3)

    def test_cross_manager_operations_are_denied(self):
        owner=self.assign().get_json()['owners'][0]
        self.sign_in(8)
        self.assertEqual(self.client.post('/billing/property-owner/26/owners',json=self.payload()).status_code,404)
        self.assertEqual(self.client.post('/billing/property-owner/27/owners',json={'owner_id':owner['owner_id']}).status_code,404)
        self.assertEqual(self.client.patch(f"/billing/property-owner/owners/{owner['owner_id']}",json=self.payload()).status_code,404)
        self.assertEqual(self.client.delete(f"/billing/property-owner/26/assignments/{owner['assignment_id']}").status_code,404)
        self.assertEqual(self.client.get('/billing/property-owner/owners?q=Owner').get_json()['owners'],[])

    def test_assignment_failure_rolls_back_new_identity(self):
        original=self.session.execute
        def failed(statement,*args,**kwargs):
            if 'INSERT INTO bil_property_ownership' in str(statement):raise sa.exc.SQLAlchemyError('synthetic assignment failure')
            return original(statement,*args,**kwargs)
        with patch.object(self.session,'execute',side_effect=failed):response=self.assign()
        self.assertEqual(response.status_code,503)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),0)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership').scalar(),0)

    def test_commit_failure_rolls_back_both_records(self):
        with patch.object(self.session,'commit',side_effect=sa.exc.SQLAlchemyError('synthetic commit failure')):response=self.assign()
        self.assertEqual(response.status_code,503);self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),0)

    def test_three_individual_owners_edit_and_page_reload(self):
        ids=[]
        for i in range(3):
            response=self.assign(name=f'Individual {i}',identity_number=f'ID-{i}',percentage='25')
            self.assertEqual(response.status_code,200)
            ids.append(next(o['owner_id'] for o in response.get_json()['owners'] if o['identity_number']==f'ID-{i}'))
        for i,owner_id in enumerate(ids):
            response=self.client.patch(f'/billing/property-owner/owners/{owner_id}',json=self.payload(name=f'Edited {i}',identity_number=f'ID-{i}'))
            self.assertEqual(response.status_code,200)
            page=self.client.get('/billing/property-owner')
            self.assertEqual(page.status_code,200)
            html=page.get_data(as_text=True)
            for j in range(3):
                self.assertIn(f'Edited {j}' if j<=i else f'Individual {j}',html)
            self.assertIn('data-owner-edit=',html)
            self.assertIn('Add Owner',html)
            # The property owner cell contains clickable identities, not percentages.
            import re
            cell=re.search(r'data-property-owners="26">(.*?)</td>',html,re.S).group(1)
            self.assertNotIn('%',cell)
            self.assertEqual(cell.count('data-owner-edit='),3)
        rows=self.sql('SELECT display_name FROM bil_owner_identity ORDER BY id').scalars().all()
        self.assertEqual(rows,['Edited 0','Edited 1','Edited 2'])
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property WHERE id=26').scalar(),1)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership WHERE property_id=26').scalar(),3)

    def test_portfolio_readonly_and_three_columns(self):
        for i in range(3):self.assign(name=f'Portfolio owner {i}',identity_number=f'PORT-{i}')
        self.assertEqual(self.client.post('/billing/portfolio',data={'property_name':'Forbidden'}).status_code,405)
        html=self.client.get('/billing/portfolio').get_data(as_text=True)
        for value in ('Almond','Original address','Portfolio owner 0','Portfolio owner 1','Portfolio owner 2'):
            self.assertIn(value,html)
        self.assertIn('/billing/portfolio/print',html);self.assertIn('>Email</button>',html);self.assertNotIn('delete_property',html)
        self.assertNotIn('data-owner-edit',html)
        table=self.client.get('/billing/property-owner').get_data(as_text=True)
        self.assertEqual(table.count('<th class="p-3">'),3)
        self.assertIn('href="/billing/property-owner/26/edit"',table)
        self.assertIn('data-address-dialog="address-26"',table)
        self.assertNotIn('>Save Owner</button>',table)
        self.assertIn('Save and Add Another Owner',table)
        self.client.post('/billing/property-owner/26/edit',data={'property_name':'Changed','address':'Saved address','active':'on'})
        refreshed=self.client.get('/billing/portfolio').get_data(as_text=True)
        self.assertIn('Changed',refreshed);self.assertIn('Original address',refreshed)
        self.sign_in(8)
        self.assertNotIn('Portfolio owner',self.client.get('/billing/portfolio').get_data(as_text=True))

    def test_deactivate_reactivate_one_owner_preserves_other_two_and_history(self):
        ids=[]
        for i in range(3):
            result=self.assign(name=f'Status owner {i}',identity_number=f'ST-{i}').get_json()
            ids.append(result['owners'][-1]['owner_id'])
        owner=result['owners'][0]
        url=f"/billing/property-owner/26/assignments/{owner['assignment_id']}"
        response=self.client.patch(url,json={'active':False})
        self.assertEqual(response.status_code,200)
        self.assertEqual(len(response.get_json()['owners']),2)
        current=self.client.get('/billing/property-owner').get_data(as_text=True)
        self.assertNotIn('Status owner 0',current)
        edit=self.client.get('/billing/property-owner/26/owners/edit').get_data(as_text=True)
        self.assertIn('Status owner 0',edit);self.assertIn('Inactive',edit)
        portfolio=self.client.get('/billing/portfolio').get_data(as_text=True)
        self.assertIn('Status owner 0',portfolio);self.assertIn('(Inactive)',portfolio)
        terminal=self.sql('SELECT id FROM bil_property_ownership WHERE owner_id='+str(owner['owner_id'])+' ORDER BY id DESC LIMIT 1').scalar()
        response=self.client.patch(f'/billing/property-owner/26/assignments/{terminal}',json={'active':True})
        self.assertEqual(response.status_code,200)
        self.assertEqual(len(response.get_json()['owners']),3)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),3)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership').scalar(),5)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership WHERE owner_id<>'+str(owner['owner_id'])).scalar(),2)
        self.assertIn('Status owner 0',self.client.get('/billing/property-owner').get_data(as_text=True))

    def test_property_active_reactivation_and_owner_modal_status(self):
        self.client.post('/billing/property-owner/26/edit',data={'property_name':'Almond','address':'Original address'})
        self.assertTrue(self.sql('SELECT is_archived FROM bil_property WHERE id=26').scalar())
        self.assertIn('Almond',self.client.get('/billing/property-owner').get_data(as_text=True))
        self.assertNotIn('Almond',self.client.get('/billing/portfolio').get_data(as_text=True))
        self.client.post('/billing/property-owner/26/edit',data={'property_name':'Almond','address':'Original address','active':'on'})
        self.assertFalse(self.sql('SELECT is_archived FROM bil_property WHERE id=26').scalar())
        owner=self.assign().get_json()['owners'][0]
        payload=self.payload(active=False,property_id=26,assignment_id=owner['assignment_id'])
        response=self.client.patch(f"/billing/property-owner/owners/{owner['owner_id']}",json=payload)
        self.assertEqual(response.status_code,200)
        self.assertNotIn('Owner One',self.client.get('/billing/property-owner').get_data(as_text=True))

    def test_atomic_status_identity_edit_rollback_and_scoped_reactivation(self):
        owner=self.assign().get_json()['owners'][0]
        self.client.post('/billing/property-owner/28/owners',json={'owner_id':owner['owner_id']})
        bad=self.payload(name='Must roll back',active='invalid',property_id=26,assignment_id=owner['assignment_id'])
        response=self.client.patch(f"/billing/property-owner/owners/{owner['owner_id']}",json=bad)
        self.assertEqual(response.status_code,400)
        self.assertEqual(self.sql('SELECT display_name FROM bil_owner_identity').scalar(),'Owner One')
        response=self.client.patch(f"/billing/property-owner/26/assignments/{owner['assignment_id']}",json={'active':False})
        self.assertEqual(response.status_code,200)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership WHERE property_id=28').scalar(),1)
        # Selecting the existing identity reactivates by supersession, not a duplicate identity.
        response=self.client.post('/billing/property-owner/26/owners',json={'owner_id':owner['owner_id']})
        self.assertEqual(len(response.get_json()['owners']),1)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),1)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership WHERE property_id=28').scalar(),1)

    def test_standard_report_email_and_failures(self):
        import sys,types
        from unittest.mock import Mock,patch
        self.assign(name='Sensitive Owner',identity_number='SECRET-ID')
        sender=Mock(return_value=True);renderer=Mock(return_value=b'%PDF-test')
        modules={'app.utils.mailer':types.SimpleNamespace(send_standard_report_email=sender),
            'app.utils.pdf_render':types.SimpleNamespace(html_to_pdf_bytes=renderer)}
        with patch.dict(sys.modules,modules):
            response=self.client.post('/billing/portfolio/email',data={'email':'attacker@example.com'})
            self.assertEqual(response.status_code,302)
            sender.assert_called_once()
            kwargs=sender.call_args.kwargs
            self.assertEqual(kwargs['to_email'],'manager7@example.com')
            self.assertEqual(kwargs['pdf_bytes'],b'%PDF-test')
            rendered=renderer.call_args.args[0]
            for value in ('Sensitive Owner','SECRET-ID','Owner home','PO Box','Original address'):
                self.assertIn(value,rendered)
            self.assertIn('Assignment recorded at',rendered)
            self.assertIn('Effective to (exclusive)',rendered)
            self.assertNotIn('Private address',rendered)
            with self.client.session_transaction() as session:
                self.assertTrue(any(category=='success' for category,message in session.pop('_flashes',[])))
            sender.return_value=False
            self.client.post('/billing/portfolio/email')
            with self.client.session_transaction() as session:
                self.assertFalse(any(category=='success' for category,message in session.pop('_flashes',[])))
            sender.side_effect=RuntimeError('Delivery failed')
            self.client.post('/billing/portfolio/email')
            with self.client.session_transaction() as session:
                self.assertFalse(any(category=='success' for category,message in session.pop('_flashes',[])))
            sender.reset_mock();sender.side_effect=None;renderer.side_effect=RuntimeError('PDF unavailable')
            self.client.post('/billing/portfolio/email');sender.assert_not_called()
            with self.client.session_transaction() as session:
                self.assertFalse(any(category=='success' for category,message in session.pop('_flashes',[])))
            renderer.side_effect=None
            sender.reset_mock();self.app.config['MAIL_SUPPRESS_SEND']=True
            self.client.post('/billing/portfolio/email');sender.assert_not_called()
            self.app.config['MAIL_SUPPRESS_SEND']=False
            self.sign_in(8)
            self.client.post('/billing/portfolio/email')
            self.assertNotIn('SECRET-ID',renderer.call_args.args[0])

    def test_identity_and_assignment_edit_for_all_types(self):
        owners=[]
        for i,kind in enumerate(('individual','company','trust')):
            result=self.assign(type=kind,name=f'Original {kind}',identity_number=f'TYPE-{i}').get_json()
            owners.append(next(o for o in result['owners'] if o['type']==kind))
        for i,owner in enumerate(owners):
            self.client.post('/billing/property-owner/28/owners',json={'owner_id':owner['owner_id'],'percentage':'90'})
            payload=self.payload(type=owner['type'],name=f'Edited {owner["type"]}',identity_number=f'TYPE-{i}',
                percentage='12.5',active=True,property_id=26,assignment_id=owner['assignment_id'])
            response=self.client.patch(f"/billing/property-owner/owners/{owner['owner_id']}",json=payload)
            self.assertEqual(response.status_code,200)
            self.assertEqual(response.get_json()['message'],'Owner changes saved.')
            self.assertEqual(str(self.sql('SELECT ownership_percentage FROM bil_property_ownership WHERE property_id=28 AND owner_id='+str(owner['owner_id'])).scalar()),'90.0000')
            self.assertIn(f'Edited {owner["type"]}',self.client.get('/billing/property-owner').get_data(as_text=True))
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),3)
        # Invalid assignment input must roll back identity changes and report a useful reason.
        owner=owners[-1]
        payload=self.payload(type='trust',name='Should not persist',identity_number='TYPE-2',
            active=True,percentage='101',property_id=26,
            assignment_id=self.sql('SELECT max(id) FROM bil_property_ownership WHERE property_id=26').scalar())
        response=self.client.patch(f"/billing/property-owner/owners/{owner['owner_id']}",json=payload)
        self.assertEqual(response.status_code,400)
        self.assertIn('between 0 and 100',response.get_json()['error'])
        self.assertNotEqual(self.sql('SELECT display_name FROM bil_owner_identity WHERE id='+str(owner['owner_id'])).scalar(),'Should not persist')

    def test_clean_print_report_matches_authorized_email_body(self):
        self.assign(name='Printable identity',identity_number='PRINT-ID')
        page=self.client.get('/billing/portfolio/print')
        self.assertEqual(page.status_code,200)
        html=page.get_data(as_text=True)
        self.assertIn('window.print()',html)
        self.assertIn('PRINT-ID',html)
        self.assertNotIn('<form',html)
        self.assertNotIn('Private address',html)
        self.assertNotIn('METSOA',html)
        self.sign_in(8)
        self.assertNotIn('PRINT-ID',self.client.get('/billing/portfolio/print').get_data(as_text=True))
        with self.client.session_transaction() as session:session.clear()
        self.assertEqual(self.client.post('/billing/portfolio/email').status_code,401)
        self.assertEqual(self.client.get('/billing/portfolio/print').status_code,401)

    def test_disabled_flag_denies_all_owner_operations(self):
        self.app.config['PM_OWNER_WRITES_APPROVED']=False
        with patch.object(self.session,'execute',side_effect=AssertionError('Disabled owner routes must not query database')):
            self.assertEqual(self.client.post('/billing/property-owner/26/owners',json=self.payload()).status_code,503)
            self.assertEqual(self.client.get('/billing/property-owner/owners').status_code,503)

    def test_unapproved_schema_or_invalid_percentage_fails_closed(self):
        self.assertEqual(self.assign(percentage='101').status_code,400)
        self.assertEqual(self.assign(percentage='NaN').status_code,400)
        self.app.config['PM_OWNER_WRITES_APPROVED']='true'
        self.assertEqual(self.assign().status_code,503)

if __name__=='__main__':unittest.main()
