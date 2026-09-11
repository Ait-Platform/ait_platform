"""UIP Stage 3.5 Register Provenance

Revision ID: b3d537f90c43
Revises: b3d537f90c42
Create Date: 2026-09-11 14:15:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'b3d537f90c43'
down_revision = 'b3d537f90c42'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table('uip_register_import',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('organization_id', sa.Integer(), nullable=False),
        sa.Column('source_type', sa.String(length=50), nullable=False),
        sa.Column('source_identifier', sa.String(length=255), nullable=True),
        sa.Column('batch_reference', sa.String(length=100), nullable=True),
        sa.Column('date_received', sa.Date(), nullable=False),
        sa.Column('effective_date', sa.Date(), nullable=False),
        sa.Column('imported_by_user_id', sa.Integer(), nullable=False),
        sa.Column('document_id', sa.Integer(), nullable=True),
        sa.Column('status', sa.String(length=50), nullable=False),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['document_id'], ['uip_document.id'], name="fk_uip_import_document"),
        sa.ForeignKeyConstraint(['imported_by_user_id'], ['user.id'], name="fk_uip_import_user"),
        sa.ForeignKeyConstraint(['organization_id'], ['core_organization.id'], name="fk_uip_import_org"),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_table('uip_register_import_exception',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('import_id', sa.Integer(), nullable=False),
        sa.Column('row_number', sa.Integer(), nullable=True),
        sa.Column('source_reference', sa.String(length=100), nullable=True),
        sa.Column('reason', sa.String(length=255), nullable=False),
        sa.Column('incoming_data', sa.JSON(), nullable=False),
        sa.Column('status', sa.String(length=50), server_default='OPEN', nullable=False),
        sa.Column('resolution_notes', sa.Text(), nullable=True),
        sa.Column('resolved_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('resolved_by_user_id', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['import_id'], ['uip_register_import.id'], name="fk_uip_exception_import"),
        sa.ForeignKeyConstraint(['resolved_by_user_id'], ['user.id'], name="fk_uip_exception_user"),
        sa.PrimaryKeyConstraint('id')
    )
    
    op.add_column('uip_member_profile', sa.Column('record_source', sa.String(length=50), server_default='MANUAL', nullable=False))
    op.add_column('uip_member_profile', sa.Column('last_import_id', sa.Integer(), nullable=True))
    op.create_foreign_key('fk_uip_member_import', 'uip_member_profile', 'uip_register_import', ['last_import_id'], ['id'])
    
    op.add_column('uip_property', sa.Column('record_source', sa.String(length=50), server_default='MANUAL', nullable=False))
    op.add_column('uip_property', sa.Column('last_import_id', sa.Integer(), nullable=True))
    op.create_foreign_key('fk_uip_property_import', 'uip_property', 'uip_register_import', ['last_import_id'], ['id'])
    
    op.add_column('uip_property_member', sa.Column('record_source', sa.String(length=50), server_default='MANUAL', nullable=False))
    op.add_column('uip_property_member', sa.Column('last_import_id', sa.Integer(), nullable=True))
    op.create_foreign_key('fk_uip_property_member_import', 'uip_property_member', 'uip_register_import', ['last_import_id'], ['id'])

def downgrade():
    op.drop_constraint('fk_uip_property_member_import', 'uip_property_member', type_='foreignkey')
    op.drop_column('uip_property_member', 'last_import_id')
    op.drop_column('uip_property_member', 'record_source')
    
    op.drop_constraint('fk_uip_property_import', 'uip_property', type_='foreignkey')
    op.drop_column('uip_property', 'last_import_id')
    op.drop_column('uip_property', 'record_source')
    
    op.drop_constraint('fk_uip_member_import', 'uip_member_profile', type_='foreignkey')
    op.drop_column('uip_member_profile', 'last_import_id')
    op.drop_column('uip_member_profile', 'record_source')
    
    op.drop_table('uip_register_import_exception')
    op.drop_table('uip_register_import')
