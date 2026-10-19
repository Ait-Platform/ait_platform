"""One real FK failure checks safe banking diagnostics and complete rollback."""
import copy,unittest,uuid
from unittest.mock import patch
from sqlalchemy import text
import test_pm_lease_property_payments as fixture

class BankingIntegrityDiagnosticTest(unittest.TestCase):
    load_lease_workspace=True
    setUp=fixture.PropertyPaymentsTests.setUp
    sign_in=fixture.PropertyPaymentsTests.sign_in
    sql=fixture.PropertyPaymentsTests.sql
    payload=fixture.PropertyPaymentsTests.payload
    assign=fixture.PropertyPaymentsTests.assign
    create=fixture.PropertyPaymentsTests.create
    migrate=fixture.PropertyPaymentsTests.migrate
    account=fixture.PropertyPaymentsTests.account
    url=fixture.PropertyPaymentsTests.url

    def test_fk_diagnostic_has_only_safe_metadata_rolls_back_and_next_save_reopens(self):
        self.migrate()
        from app.program_billing import lease_property_payments as payments
        before=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar())
        original=payments.add_account
        def rejected(property_id,form):
            original(property_id,form)
            # Fail after both normal inserts, proving the entire save rolls back.
            self.session.execute(text('INSERT INTO bil_property_payment_account(id,property_id,bank_detail_id,for_rental,for_utilities,created_by) VALUES (:id,26,999999,true,false,7)'),{'id':uuid.uuid4()})
        with self.client.session_transaction() as session:session.pop('_flashes',None)
        with patch.object(payments,'add_account',side_effect=rejected),self.assertLogs(self.app.logger,level='ERROR') as captured:
            response=self.account(branch_name='PRIVATE-BRANCH-MARKER',account_number='9988776655')
        self.assertEqual(response.status_code,503)
        logs='\n'.join(captured.output)
        self.assertIn('action=add_account sqlstate=23503',logs)
        self.assertIn('constraint=bil_property_payment_account_bank_detail_id_fkey',logs)
        self.assertIn('table=bil_property_payment_account column=unknown',logs)
        for secret in ('9988776655','PRIVATE-BRANCH-MARKER','Synthetic Bank','Test Owner','INSERT INTO','[parameters','postgresql://'):
            self.assertNotIn(secret,logs)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_bank_detail').scalar(),0)
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_payment_account').scalar(),0)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),before)
        with self.client.session_transaction() as session:self.assertFalse(session.get('_flashes'))
        self.assertEqual(self.account(branch_name='Saved Branch').status_code,302)
        self.assertEqual(self.client.get(self.url()).status_code,200)
        self.assertEqual(self.sql('SELECT branch_name FROM bil_bank_detail').scalar(),'Saved Branch')
        self.assertEqual(self.sql('SELECT count(*) FROM bil_property_payment_account').scalar(),1)
        self.assertEqual(self.sql('SELECT items FROM bil_lease_draft').scalar(),before)
