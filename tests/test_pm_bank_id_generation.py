"""One PostgreSQL insert check using session-temporary bank tables only."""
import importlib.util
import unittest
from pathlib import Path
from alembic.operations import Operations
from alembic.migration import MigrationContext
from sqlalchemy.exc import IntegrityError
import test_pm_bank_register as fixture


class BankIdInsertCheck(unittest.TestCase):
    load_lease_workspace=True
    setUp=fixture.BankRegisterCheck.setUp
    sign_in=fixture.BankRegisterCheck.sign_in
    sql=fixture.BankRegisterCheck.sql
    payload=fixture.BankRegisterCheck.payload
    assign=fixture.BankRegisterCheck.assign
    create=fixture.BankRegisterCheck.create
    version=fixture.BankRegisterCheck.version
    signed=fixture.BankRegisterCheck.signed
    url_history=fixture.BankRegisterCheck.url_history
    ledger=fixture.BankRegisterCheck.ledger
    docx=fixture.BankRegisterCheck.docx
    url=fixture.BankRegisterCheck.url
    migrate=fixture.BankRegisterCheck.migrate
    account=fixture.BankRegisterCheck.account

    def test_missing_generation_repair_and_real_account_insert(self):
        self.migrate()
        def migration(name):
            spec=importlib.util.spec_from_file_location(name,Path('migrations/versions')/(name+'.py'))
            mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
            mod.op=Operations(MigrationContext.configure(self.session.connection()))
            return mod
        migration('pm_bank_register_002').upgrade()
        self.sql("UPDATE alembic_version SET version_num='pm_bank_register_002' WHERE version_num='pm_lease_payments_001'")
        self.sql('ALTER TABLE bil_bank_detail ALTER COLUMN id DROP DEFAULT')
        self.session.commit()
        # Reproduce the observed failure with an omitted primary key.
        with self.session.begin_nested():
            try:
                with self.session.begin_nested():
                    self.sql("INSERT INTO bil_bank_detail(bank_name,account_number) VALUES('Synthetic bank','0000000001')")
            except IntegrityError as error:
                self.assertEqual(error.orig.pgcode,'23502')
                self.assertEqual(error.orig.diag.column_name,'id')
            else:self.fail('Missing ID generation was not reproduced')
        # Existing serial ownership survives a lost default; repair that path first.
        repair=migration('pm_bank_id_003')
        repair.upgrade()
        self.assertIsNotNone(self.sql("SELECT column_default FROM information_schema.columns WHERE table_name='bil_bank_detail' AND column_name='id' AND table_schema LIKE 'pg_temp_%'").scalar())
        # Preserve a legitimately generated record, then simulate a missing sequence too.
        self.sql("INSERT INTO bil_bank_detail(bank_name,account_number) VALUES('Existing synthetic bank','0000000002')")
        original=[dict(row) for row in self.sql('SELECT * FROM bil_bank_detail').mappings()]
        sequence=self.sql("SELECT pg_get_serial_sequence('bil_bank_detail','id')").scalar()
        self.sql('ALTER TABLE bil_bank_detail ALTER COLUMN id DROP DEFAULT')
        quote=self.engine.dialect.identifier_preparer.quote
        self.sql('DROP SEQUENCE '+'.'.join(quote(part) for part in sequence.split('.')))
        repair.upgrade()
        identity=self.sql("SELECT attidentity FROM pg_attribute WHERE attrelid='bil_bank_detail'::regclass AND attname='id'").scalar()
        self.assertEqual(identity,'d')
        self.sql("UPDATE alembic_version SET version_num='pm_bank_id_003' WHERE version_num='pm_bank_register_002'")
        self.session.commit()
        before=self.sql('SELECT items FROM bil_lease_draft').scalar()
        self.assertEqual(self.account(for_rental='',for_utilities='').status_code,302)
        rows=[dict(row) for row in self.sql('SELECT * FROM bil_bank_detail ORDER BY id').mappings()]
        self.assertEqual(len(rows),2)
        self.assertEqual(rows[0],original[0])
        self.assertGreater(rows[1]['id'],rows[0]['id'])
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_payment_account').scalar(),0)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),before)
        # Rerunning the repair preserves the now-healthy identity and its records.
        repair.upgrade();self.session.commit()
        self.assertEqual([dict(row) for row in self.sql('SELECT * FROM bil_bank_detail ORDER BY id').mappings()],rows)
