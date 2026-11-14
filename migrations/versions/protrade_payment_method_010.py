"""Optional structured payment method; preserve all old references and balances."""
from alembic import op
import sqlalchemy as sa
revision='protrade_payment_method_010'
down_revision='protrade_quote_ledger_009'
branch_labels=None
depends_on=None


def upgrade():
    bind=op.get_bind()
    if bind.dialect.name!='postgresql' or op.get_context().as_sql:
        raise RuntimeError('Run payment metadata migration online on verified PostgreSQL')
    if not all(bind.execute(sa.text('SELECT current_database(),current_user')).one()):
        raise RuntimeError('Cannot verify database identity')
    op.execute('LOCK TABLE debtor_ledger IN SHARE ROW EXCLUSIVE MODE')
    op.add_column('debtor_ledger',sa.Column('payment_method',sa.String(16)))
    op.drop_constraint('ck_debtor_payment_credit','debtor_ledger',type_='check')
    op.create_check_constraint('ck_debtor_payment_credit','debtor_ledger',"payment_key IS NULL OR (kind='credit' AND boq_quote_id IS NULL AND amount>0 AND ((ref IS NOT NULL AND length(trim(ref))>0) OR (ref IS NULL AND payment_method IS NOT NULL)))")
    op.create_check_constraint('ck_debtor_payment_method','debtor_ledger',"payment_method IS NULL OR (kind='credit' AND payment_key IS NOT NULL AND payment_method IN ('Cash','EFT','eWallet','Card','Other'))")
    op.drop_index('uq_debtor_payment_fingerprint',table_name='debtor_ledger')
    op.create_index('uq_debtor_payment_fingerprint','debtor_ledger',['debtor_id','txn_date','amount',sa.text("COALESCE(ref, '')"),sa.text("(CASE WHEN ref IS NULL THEN payment_method ELSE '' END)")],unique=True,postgresql_where=sa.text('payment_key IS NOT NULL'))


def downgrade():
    raise RuntimeError('Payment metadata and original references must be preserved')
