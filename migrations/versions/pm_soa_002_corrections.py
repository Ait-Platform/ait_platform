"""Append-only PM ledger correction links and audit metadata."""
from alembic import op
import sqlalchemy as sa

revision='pm_soa_002'
down_revision='pm_soa_001'
branch_labels=None
depends_on=None


def upgrade():
    if op.get_bind().dialect.name!='postgresql':
        raise RuntimeError('PostgreSQL required')
    op.add_column('bil_tenant_ledger',sa.Column('correction_of',sa.Integer(),nullable=True))
    op.add_column('bil_tenant_ledger',sa.Column('correction_role',sa.String(16),nullable=True))
    op.add_column('bil_tenant_ledger',sa.Column('correction_group',sa.String(32),nullable=True))
    op.add_column('bil_tenant_ledger',sa.Column('correction_actor',sa.Integer(),nullable=True))
    op.add_column('bil_tenant_ledger',sa.Column('correction_recorded_at',sa.DateTime(timezone=True),nullable=True))
    op.add_column('bil_tenant_ledger',sa.Column('correction_reason',sa.Text(),nullable=True))
    op.create_index('uq_pm_ledger_id_tenant','bil_tenant_ledger',['id','tenant_id'],unique=True)
    op.create_foreign_key('fk_pm_ledger_correction_tenant','bil_tenant_ledger','bil_tenant_ledger',
        ['correction_of','tenant_id'],['id','tenant_id'],ondelete='RESTRICT')
    op.create_index('uq_pm_ledger_correction_role','bil_tenant_ledger',
        ['correction_of','correction_role'],unique=True,
        postgresql_where=sa.text('correction_of IS NOT NULL'))
    op.create_index('uq_pm_ledger_correction_group','bil_tenant_ledger',
        ['correction_group','correction_role'],unique=True,
        postgresql_where=sa.text('correction_group IS NOT NULL'))
    op.create_check_constraint('ck_pm_ledger_correction_audit','bil_tenant_ledger',"""
        (correction_of IS NULL AND correction_role IS NULL AND correction_group IS NULL
         AND correction_actor IS NULL AND correction_recorded_at IS NULL AND correction_reason IS NULL)
        OR
        (correction_of IS NOT NULL AND correction_of<>id
         AND correction_role IS NOT NULL AND correction_role IN ('reversal','replacement')
         AND correction_group IS NOT NULL AND length(correction_group)=32
         AND kind IS NOT NULL AND correction_actor IS NOT NULL AND correction_recorded_at IS NOT NULL
         AND correction_reason IS NOT NULL AND length(btrim(correction_reason)) BETWEEN 1 AND 2000
         AND ((correction_role='reversal' AND kind='correction_reversal')
           OR (correction_role='replacement' AND kind IN ('correction_replacement','opening_adjustment'))))
    """)


def downgrade():
    raise RuntimeError('No destructive downgrade: preserve correction links and financial audit history.')
