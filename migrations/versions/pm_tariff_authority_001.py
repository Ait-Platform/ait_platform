"""Add verified municipal authority evidence without inventing legacy verification."""
from alembic import op
import sqlalchemy as sa
revision = 'pm_tariff_authority_001'
down_revision = 'pm_meter_exceptions_004'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError('PostgreSQL required')
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.create_table('bil_tariff_verification',
        sa.Column('tariff_id', sa.Integer(), sa.ForeignKey('bil_tariff.id', ondelete='RESTRICT'), primary_key=True),
        sa.Column('municipality', sa.String(200), nullable=False),
        sa.Column('municipal_category', sa.String(200), nullable=False),
        sa.Column('source_reference', sa.String(1000), nullable=False),
        sa.Column('effective_date', sa.Date(), nullable=False),
        sa.Column('verified_by', sa.Integer(), sa.ForeignKey('user.id', ondelete='RESTRICT'), nullable=False),
        sa.Column('verified_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('statement_timestamp()')),
        sa.Column('recorded_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('statement_timestamp()')),
        sa.CheckConstraint("length(btrim(municipality)) > 0 AND length(btrim(municipal_category)) > 0 AND length(btrim(source_reference)) > 0", name='ck_tariff_verification_source'))
    # No legacy rows are inserted, updated or marked verified.


def downgrade():
    raise RuntimeError('Forward-only: preserve municipal verification evidence.')
