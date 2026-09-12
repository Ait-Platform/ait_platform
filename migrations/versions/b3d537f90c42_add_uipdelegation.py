"""add UipDelegation

Revision ID: b3d537f90c42
Revises: 
Create Date: 2026-09-11 08:06:03.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b3d537f90c42'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('uip_delegation',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('organization_id', sa.Integer(), nullable=False),
    sa.Column('delegated_user_id', sa.Integer(), nullable=False),
    sa.Column('appointed_by_user_id', sa.Integer(), nullable=False),
    sa.Column('delegation_type', sa.String(length=50), nullable=False),
    sa.Column('status', sa.String(length=20), nullable=False, server_default='ACTIVE'),
    sa.Column('effective_date', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
    sa.Column('document_id', sa.Integer(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
    sa.ForeignKeyConstraint(['appointed_by_user_id'], ['user.id'], ),
    sa.ForeignKeyConstraint(['delegated_user_id'], ['user.id'], ),
    sa.ForeignKeyConstraint(['document_id'], ['uip_document.id'], ),
    sa.ForeignKeyConstraint(['organization_id'], ['core_organization.id'], ),
    sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('uip_delegation')
