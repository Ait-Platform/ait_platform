"""UIP organization billing scope and entitlements

Revision ID: uip_p51_billing
Revises: uip_p50_governance
Create Date: 2026-09-13 08:24:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector


# revision identifiers, used by Alembic.
revision = 'uip_p51_billing'
down_revision = 'uip_p50_governance'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)
    
    # 1. Add billing_scope to auth_subject
    columns = [c['name'] for c in inspector.get_columns('auth_subject')]
    if 'billing_scope' not in columns:
        op.add_column('auth_subject', sa.Column('billing_scope', sa.String(length=16), server_default='user', nullable=False))

    # 2. Create core_organization_entitlement
    if 'core_organization_entitlement' not in inspector.get_table_names():
        op.create_table('core_organization_entitlement',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('organization_id', sa.Integer(), nullable=False),
        sa.Column('subject_id', sa.Integer(), nullable=False),
        sa.Column('status', sa.String(length=50), nullable=False, server_default='active'),
        sa.Column('start_date', sa.DateTime(), nullable=True),
        sa.Column('end_date', sa.DateTime(), nullable=True),
        sa.Column('is_trial', sa.Boolean(), nullable=True, server_default='false'),
        sa.Column('payment_provenance', sa.String(length=255), nullable=True),
        sa.ForeignKeyConstraint(['organization_id'], ['core_organization.id'], ),
        sa.ForeignKeyConstraint(['subject_id'], ['auth_subject.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('organization_id', 'subject_id', name='uq_org_subject_entitlement')
        )


def downgrade():
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)
    
    if 'core_organization_entitlement' in inspector.get_table_names():
        op.drop_table('core_organization_entitlement')
        
    columns = [c['name'] for c in inspector.get_columns('auth_subject')]
    if 'billing_scope' in columns:
        op.drop_column('auth_subject', 'billing_scope')
