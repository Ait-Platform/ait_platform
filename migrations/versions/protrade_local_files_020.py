"""Historical email may lack a debtor; preserve all existing Output receipts."""
from alembic import op
import sqlalchemy as sa
revision='protrade_local_files_020'
down_revision='protrade_trade_codes_019'
branch_labels=None
depends_on=None

def upgrade():
    if op.get_bind().dialect.name!='postgresql':raise RuntimeError('PostgreSQL required')
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.create_check_constraint('ck_protrade_email_history_debtor','protrade_output_email',"debtor_id IS NOT NULL OR selection LIKE 'history:%'")
    op.alter_column('protrade_output_email','debtor_id',existing_type=sa.Integer(),nullable=True)

def downgrade():
    raise RuntimeError('Forward-only: preserve historical email receipts')
