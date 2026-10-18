"""Essential register/save/reopen/authorization and one Word merge smoke check."""
import importlib.util, io, copy, re, unittest
from pathlib import Path
from zipfile import ZipFile
from alembic.operations import Operations
from alembic.migration import MigrationContext
import test_pm_lease_owner_reuse as reuse

class TenantRegisterTests(unittest.TestCase):
    load_lease_workspace=True
    setUp=reuse.RegisteredOwnerPartiesTests.setUp
    sign_in=reuse.RegisteredOwnerPartiesTests.sign_in
    sql=reuse.RegisteredOwnerPartiesTests.sql
    payload=reuse.RegisteredOwnerPartiesTests.payload
    assign=reuse.RegisteredOwnerPartiesTests.assign
    create=reuse.RegisteredOwnerPartiesTests.create
    stored=reuse.RegisteredOwnerPartiesTests.stored
    route=reuse.RegisteredOwnerPartiesTests.route
    post=reuse.RegisteredOwnerPartiesTests.post
    generate=reuse.RegisteredOwnerPartiesTests.generate
    owners=reuse.RegisteredOwnerPartiesTests.owners

    def schema(self, local=False):
        spec=importlib.util.spec_from_file_location('tenant_migration',Path('docs/proposed_migrations/pm_tenant_001.py'))
        migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        migration.op=Operations(MigrationContext.configure(self.session.connection()));migration.upgrade();self.session.commit()
        self.sql("INSERT INTO alembic_version VALUES ('pm_tenant_001')")
        linkage = "property_id integer REFERENCES bil_property(id)" if local else "sectional_unit_id integer NOT NULL REFERENCES bil_sectional_unit(id)"
        if not local:self.sql('CREATE TEMP TABLE bil_sectional_unit(id integer PRIMARY KEY)')
        self.sql('CREATE TEMP TABLE bil_tenant (id serial PRIMARY KEY,name varchar(200) NOT NULL,'+linkage+",email varchar(255),phone varchar(50),address varchar(500),metro_account_no varchar(64),rent_includes_metro integer NOT NULL,email_statements boolean NOT NULL,is_active boolean,notes text)")
        self.session.commit()
        spec=importlib.util.spec_from_file_location('tenant_compatibility',Path('docs/proposed_migrations/pm_tenant_002.py'))
        compatibility=importlib.util.module_from_spec(spec);spec.loader.exec_module(compatibility)
        before=set(self.sql('SELECT version_num FROM alembic_version').scalars())
        compatibility.op=Operations(MigrationContext.configure(self.session.connection()));compatibility.upgrade()
        sql=Path('docs/sql/pm_tenant_002.pgadmin.sql').read_text(encoding='utf-8')
        history=sql.split('-- END MIGRATION DDL',1)[1].rsplit('COMMIT;',1)[0]
        self.session.connection().exec_driver_sql(history);self.session.commit()
        after=set(self.sql('SELECT version_num FROM alembic_version').scalars())
        self.assertEqual(after,(before-{'pm_tenant_001'})|{'pm_tenant_002'})
        return compatibility

    def form(self,**changes):
        values=dict(party_type='natural_person',legal_name='Synthetic Test Tenant',identity_number='TEST-TENANT-1',
            address='1 Synthetic Street',email='test@example.com',phone='0100000000')
        values.update(changes);return values

    def register(self,**changes):
        return self.client.post(self.route('tenants'),data=self.form(**changes))

    def save_party(self,owner,tenant,**changes):
        data=dict(parties_form_version='4',representative_owner_id=str(owner['owner_id']),tenant_id=str(tenant),lessee_legal_name='FORGED')
        data.update(changes);return self.post('parties',data)

    def test_register_reuse_select_reopen_and_word(self):
        self.schema();owners=self.owners()
        self.app.config.update(PM_LEASE_PARTICULARS_WRITES_APPROVED=False,PM_LEASE_DRAFT_WRITES_APPROVED=False)
        from sqlalchemy import inspect
        columns={c['name']:c for c in inspect(self.session.connection()).get_columns('bil_tenant')}
        self.assertNotIn('property_id',columns);self.assertTrue(columns['sectional_unit_id']['nullable'])
        self.assertTrue(inspect(self.session.connection()).get_foreign_keys('bil_tenant'))
        self.assertIn('metro_account_no',columns)
        from test_pm_lease_parties_simplified import Tags
        form=self.client.get(self.route('tenants')).get_data(as_text=True)
        attrs=next(attrs for tag,attrs in Tags(form).tags if attrs.get('id')=='tenant-representative')
        self.assertIn('hidden',attrs);self.assertIn('disabled',attrs)
        self.assertNotIn('Signatory Capacity',form)
        self.assertEqual(self.client.post('/billing/leases/property/26/drafts',data={'registration_token':str(__import__('uuid').uuid4())}).status_code,302)
        # Keep this fixture's original selected draft; it remains independently reopenable.
        self.session.execute(__import__('sqlalchemy').text('DELETE FROM bil_lease_draft WHERE id<>:d'),{'d':self.draft});self.session.commit()
        self.assertEqual(self.register().status_code,302)
        self.assertEqual(self.register(legal_name='Must not overwrite').status_code,302)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_tenant').scalar(),1)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_tenant_identity').scalar(),0)
        tenant=self.sql('SELECT id FROM bil_tenant').scalar()
        self.assertEqual(self.save_party(owners[1],tenant).status_code,302)
        saved=self.stored();self.assertEqual(len(saved['_contracting_lessors']['identities']),3)
        self.assertEqual(saved['_parties']['lessee']['legal_name'],'Synthetic Test Tenant')
        self.assertEqual(saved['_parties']['lessee']['tenant_id'],tenant)
        values=self.workflow_module.party_placeholder_values(saved)
        self.assertIn(owners[1]['name'],values['[LANDLORD SIGNATORY NAME AND CAPACITY]'])
        self.assertIn(owners[1]['residential_address'],values['[S1 LANDLORD SERVICE ADDRESS AND CONTACT PARTICULARS]'])
        html=self.client.get(self.route('particulars/parties')).get_data(as_text=True)
        self.assertIn('Synthetic Test Tenant',html);self.assertIn('value="'+str(tenant)+'" selected',html)
        for removed in ('Selected representative','owner-capacity-confirmation','lessee-dialog'):self.assertNotIn(removed,html)
        import test_pm_lease_premises_offline as premises
        import test_pm_lease_period_offline as period
        import test_pm_lease_amounts_offline as financial
        import test_pm_lease_part5_offline as utilities
        for group,data in [('premises',premises.PremisesOfflineTests.form(self)),('period',period.PeriodOfflineTests.form(self)),('amounts',financial.AmountsOfflineTests.form(self)),('amounts',utilities.Part5OfflineTests.form(self))]:
            self.assertEqual(self.post(group,data).status_code,302)
        all_parts=copy.deepcopy(self.stored())
        self.assertEqual(self.save_party(owners[0],tenant).status_code,302)
        for key in ('_premises','_period','_amounts','_banking_utilities'):
            if key in all_parts:self.assertEqual(self.stored()[key],all_parts[key])
        generated=self.generate();self.assertEqual(generated.status_code,200)
        with ZipFile(io.BytesIO(generated.data)) as archive:
            from lxml import etree
            root=etree.fromstring(archive.read('word/document.xml'));visible=''.join(root.itertext())
            self.assertNotRegex(visible,r'\[[^\[\]]+\]');self.assertIn('Synthetic Test Tenant',visible)
            for owner in owners:self.assertIn(owner['name'],visible);self.assertIn(owner['identity_number'],visible)
            self.assertEqual(len(self.workflow_module.stage5_placeholder_values(self.stored())),37)
        generations=copy.deepcopy(self.stored()['_generations'])
        self.assertEqual(self.register(party_type='juristic_person',legal_name='Synthetic NPC',identity_number='NPC-TEST',representative_name='NPC Representative',representative_identity_number='NPC-REP-ID',representative_phone='0100000099',representative_email='rep@example.com').status_code,302)
        entity_id=self.sql("SELECT id FROM bil_tenant WHERE party_type='juristic_person'").scalar()
        self.assertEqual(self.save_party(owners[0],entity_id).status_code,302)
        merged=self.workflow_module.party_placeholder_values(self.stored())
        self.assertEqual(merged['[S1 LEGAL TENANT]'],'Synthetic NPC')
        self.assertIn('NPC Representative',merged['[TENANT SIGNATORY NAME AND CAPACITY]'])
        self.assertNotIn('capacity',merged['[TENANT SIGNATORY NAME AND CAPACITY]'].lower())
        juristic_doc=self.generate();self.assertEqual(juristic_doc.status_code,200)
        with ZipFile(io.BytesIO(juristic_doc.data)) as archive:
            visible=''.join(__import__('lxml.etree',fromlist=['etree']).fromstring(archive.read('word/document.xml')).itertext())
            self.assertIn('Synthetic NPC',visible);self.assertIn('NPC Representative',visible)
        self.assertEqual(self.save_party(owners[0],tenant).status_code,302)
        self.assertEqual(self.stored()['_generations'][:len(generations)],generations)
        natural_doc=self.generate();self.assertEqual(natural_doc.status_code,200)
        with ZipFile(io.BytesIO(natural_doc.data)) as archive:
            visible=''.join(__import__('lxml.etree',fromlist=['etree']).fromstring(archive.read('word/document.xml')).itertext())
            self.assertNotIn('NPC Representative',visible);self.assertNotIn('NPC-REP-ID',visible)
        self.assertEqual(self.stored()['_parties']['lessee']['representative_id'],None)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_lease').scalar(),2)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_ownership').scalar(),3)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_owner_identity').scalar(),3)

    def test_juristic_atomic_save_authorization_and_removed_switches(self):
        migration=self.schema();owners=self.owners()
        invalid=self.register(party_type='juristic_person');self.assertEqual(invalid.status_code,400)
        from test_pm_lease_parties_simplified import Tags
        attrs=next(attrs for tag,attrs in Tags(invalid.get_data(as_text=True)).tags if attrs.get('id')=='tenant-representative')
        self.assertNotIn('hidden',attrs);self.assertNotIn('disabled',attrs)
        self.assertEqual(self.register(party_type='juristic_person',representative_name='Test Director',representative_identity_number='TEST-DIRECTOR',representative_phone='0100000099',representative_email='director@example.com').status_code,302)
        tenant=self.sql('SELECT id FROM bil_tenant').scalar()
        self.assertEqual(self.save_party(owners[0],tenant).status_code,302)
        self.assertIn('Test Director',self.workflow_module.party_placeholder_values(self.stored())['[TENANT SIGNATORY NAME AND CAPACITY]'])
        before=copy.deepcopy(self.stored())
        self.assertEqual(self.save_party(owners[0],9999).status_code,404)
        self.assertEqual(self.save_party(owners[0],tenant,representative_owner_id=[str(o['owner_id']) for o in owners]).status_code,400)
        self.assertEqual(self.stored(),before)
        self.sign_in(8)
        self.assertEqual(self.client.get(self.route('tenants')).status_code,404)
        import uuid
        other=self.client.post('/billing/leases/property/27/drafts',data={'registration_token':str(uuid.uuid4())})
        self.assertEqual(other.status_code,302)
        other_id=self.sql('SELECT id FROM bil_lease_draft WHERE manager_id=8').scalar()
        self.assertEqual(self.client.post('/billing/leases/property/27/particulars/parties',data=dict(parties_form_version='4',tenant_id=tenant,draft_id=str(other_id),version=1)).status_code,404)
        self.sign_in(7);self.app.config['PM_LEASE_PARTICULARS_WRITES_APPROVED']=False
        self.app.config['PM_OWNER_WRITES_APPROVED']=False
        self.assertEqual(self.register(identity_number='NATURAL-2',representative_name='STALE',representative_identity_number='STALE').status_code,302)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_tenant_identity').scalar(),1)
        self.assertEqual(self.save_party(owners[0],tenant).status_code,302)
        self.assertNotIn('not approved',self.client.get(self.route('particulars/parties')).get_data(as_text=True))
        self.app.config['PM_OWNER_WRITES_APPROVED']=False
        self.assertEqual(self.assign(name='Owner without switch',identity_number='OWNER-NO-FLAG').status_code,200)
        self.sign_in(8)
        self.assertEqual(self.assign(name='Unauthorized',identity_number='NO-ACCESS').status_code,404)
        self.sign_in(7)
        # An actual representative insert failure must roll back its new tenant.
        from unittest.mock import patch
        from sqlalchemy.exc import SQLAlchemyError
        original=self.session.execute
        def fail_representative(statement,*args,**kwargs):
            if 'INSERT INTO bil_tenant_identity' in str(statement):raise SQLAlchemyError('synthetic failure')
            return original(statement,*args,**kwargs)
        count=self.sql('SELECT count(*) FROM bil_tenant').scalar()
        with patch.object(self.session,'execute',side_effect=fail_representative):
            response=self.register(party_type='juristic_person',identity_number='ROLLBACK-ENTITY',representative_name='Rollback Representative',representative_identity_number='ROLLBACK-ID',representative_phone='0100000000',representative_email='rollback@example.com')
        self.assertEqual(response.status_code,400)
        self.assertIn('entered details are retained',response.get_data(as_text=True))
        self.assertEqual(self.sql('SELECT count(*) FROM bil_tenant').scalar(),count)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_tenant_identity').scalar(),1)
        with self.assertRaises(RuntimeError):migration.downgrade()
        self.session.rollback()

    def test_missing_schema_fails_safely_and_empty_rollback(self):
        owners=self.owners();html=self.client.get(self.route('particulars/parties')).get_data(as_text=True)
        self.assertIn('pm_tenant_002',html)
        self.assertEqual(self.register().status_code,503)
        self.assertEqual(self.save_party(owners[0],1).status_code,503)
        self.assertEqual(self.stored(),{})
        migration=self.schema(local=True)
        self.assertEqual(self.register().status_code,302)
        columns={c['name'] for c in __import__('sqlalchemy').inspect(self.session.connection()).get_columns('bil_tenant')}
        self.assertIn('property_id',columns);self.assertNotIn('sectional_unit_id',columns)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_tenant_identity').scalar(),0)
        from sqlalchemy.exc import SQLAlchemyError
        with self.assertRaises(SQLAlchemyError):migration.upgrade()
        self.session.rollback()
        self.assertEqual(self.sql('SELECT count(*) FROM bil_tenant').scalar(),1)
