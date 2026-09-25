"""Sub Comm Tools

Revision ID: uip_p55
Revises: uip_p54
Create Date: 2026-09-25 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = 'uip_p55'
down_revision = 'uip_p54'
branch_labels = None
depends_on = None

def upgrade():
    op.create_unique_constraint('uq_uip_subcommittee_org', 'uip_subcommittee', ['id', 'organization_id'])
    op.add_column('uip_proposal', sa.Column('originating_subcommittee_id', sa.Integer(), nullable=True))
    op.add_column('uip_committee_member', sa.Column('user_id', sa.Integer(), nullable=True))
    op.create_foreign_key('fk_uip_committee_member_user', 'uip_committee_member', 'user', ['user_id'], ['id'])
    op.drop_column('uip_proposal', 'originating_subcommittee')
    op.create_foreign_key('fk_uip_proposal_subcommittee_org', 'uip_proposal', 'uip_subcommittee', ['originating_subcommittee_id', 'organization_id'], ['id', 'organization_id'])

def downgrade():
    op.drop_constraint('fk_uip_proposal_subcommittee_org', 'uip_proposal', type_='foreignkey')
    op.drop_constraint('fk_uip_committee_member_user', 'uip_committee_member', type_='foreignkey')
    op.drop_column('uip_committee_member', 'user_id')
    op.drop_constraint('uq_uip_subcommittee_org', 'uip_subcommittee', type_='unique')
    op.add_column('uip_proposal', sa.Column('originating_subcommittee', sa.String(length=255), nullable=True))
    op.drop_column('uip_proposal', 'originating_subcommittee_id')
