"""Allow an owned Price List email without inventing a debtor; preserve all receipts."""
from alembic import op
import sqlalchemy as sa

revision = 'protrade_output_email_022'
down_revision = 'protrade_unified_prices_021'
branch_labels = None
# Order the older historical-email constraint before this broader final guard.
depends_on = 'protrade_local_files_020'


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql' or op.get_context().as_sql:
        raise RuntimeError('Run online on verified PostgreSQL')
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    # Preserve the historical-email guard and all existing receipts.
    # No Folder Management interface or process is enabled here.
    present = bind.execute(sa.text("SELECT 1 FROM pg_constraint WHERE conrelid='protrade_output_email'::regclass AND conname='ck_protrade_email_history_debtor'")).first()
    if present:
        op.drop_constraint('ck_protrade_email_history_debtor', 'protrade_output_email', type_='check')
    op.alter_column('protrade_output_email', 'debtor_id', existing_type=sa.Integer(), nullable=True)
    op.create_check_constraint('ck_protrade_email_history_debtor', 'protrade_output_email',
        "debtor_id IS NOT NULL OR selection LIKE 'history:%' OR selection = 'price_list:all'")


def downgrade():
    raise RuntimeError('Forward-only: preserve debtor-free Price List email receipts')
