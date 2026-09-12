"""UIP Governance: Add term, member, and meeting agenda

Revision ID: uip_p50_governance
Revises: d3f70f6a0794
Create Date: 2026-09-12 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'uip_p50_governance'
down_revision = 'd3f70f6a0794'
branch_labels = None
depends_on = None

def upgrade():
    # Only create if they don't exist (handled by creating standard tables)
    op.create_table('uip_committee_term',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('organization_id', sa.Integer(), nullable=False),
        sa.Column('term_name', sa.String(length=100), nullable=False),
        sa.Column('created_by', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['created_by'], ['user.id'], ),
        sa.ForeignKeyConstraint(['organization_id'], ['core_organization.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_table('uip_committee_member',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('term_id', sa.Integer(), nullable=False),
        sa.Column('organization_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('email', sa.String(length=255), nullable=False),
        sa.Column('position', sa.String(length=50), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column('created_by', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_by', sa.Integer(), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['created_by'], ['user.id'], ),
        sa.ForeignKeyConstraint(['organization_id'], ['core_organization.id'], ),
        sa.ForeignKeyConstraint(['term_id'], ['uip_committee_term.id'], ),
        sa.ForeignKeyConstraint(['updated_by'], ['user.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    with op.batch_alter_table('uip_committee_member', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_uip_committee_member_email'), ['email'], unique=False)

    # uip_committee_meeting agenda
    try:
        with op.batch_alter_table('uip_committee_meeting', schema=None) as batch_op:
            batch_op.add_column(sa.Column('agenda', sa.Text(), nullable=True))
    except Exception as e:
        print(f"Warning adding agenda column: {e}")

def downgrade():
    with op.batch_alter_table('uip_committee_meeting', schema=None) as batch_op:
        batch_op.drop_column('agenda')
    with op.batch_alter_table('uip_committee_member', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_uip_committee_member_email'))
    op.drop_table('uip_committee_member')
    op.drop_table('uip_committee_term')
