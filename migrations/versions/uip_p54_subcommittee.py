"""uip_subcommittee

Revision ID: uip_p54
Revises: uip_p53
Create Date: 2026-09-25 09:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = 'uip_p54'
down_revision = 'uip_p53_proposals'
branch_labels = None
depends_on = None

def upgrade():
    # Ensure composite FK target is unique
    op.create_unique_constraint('uq_uip_organogram_seat_org', 'uip_organogram_seat', ['id', 'organization_id'])
    
    op.create_table('uip_subcommittee',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('organization_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('establishing_resolution_id', sa.Integer(), nullable=False),
        sa.Column('responsible_seat_id', sa.Integer(), nullable=False),
        sa.Column('reports_to_seat_id', sa.Integer(), nullable=False),
        sa.Column('status', sa.String(length=20), server_default='ACTIVE', nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['establishing_resolution_id', 'organization_id'], ['uip_resolution.id', 'uip_resolution.organization_id'], name='fk_uip_subcommittee_resolution_org'),
        sa.ForeignKeyConstraint(['organization_id'], ['core_organization.id'], name='fk_uip_subcommittee_org'),
        sa.ForeignKeyConstraint(['reports_to_seat_id', 'organization_id'], ['uip_organogram_seat.id', 'uip_organogram_seat.organization_id'], name='fk_uip_subcommittee_reports_org'),
        sa.ForeignKeyConstraint(['responsible_seat_id', 'organization_id'], ['uip_organogram_seat.id', 'uip_organogram_seat.organization_id'], name='fk_uip_subcommittee_responsible_org'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('organization_id', 'name', 'status', name='uq_uip_subcommittee_active_name'),
        sa.CheckConstraint("status IN ('ACTIVE','INACTIVE')", name='ck_uip_subcommittee_status')
    )

def downgrade():
    op.drop_table('uip_subcommittee')
    op.drop_constraint('uq_uip_organogram_seat_org', 'uip_organogram_seat', type_='unique')
