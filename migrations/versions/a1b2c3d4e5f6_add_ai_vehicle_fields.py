from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'a1b2c3d4e5f6'
down_revision = '45a4f34e07dc'
branch_labels = None
depends_on = None

def upgrade():
    op.add_column('mech_vehicles', sa.Column('engine_no', sa.String(length=50), nullable=True))
    op.add_column('mech_vehicles', sa.Column('gvm', sa.String(length=20), nullable=True))
    op.add_column('mech_vehicles', sa.Column('tare', sa.String(length=20), nullable=True))
    op.add_column('mech_vehicles', sa.Column('disk_license_no', sa.String(length=50), nullable=True))

def downgrade():
    op.drop_column('mech_vehicles', 'engine_no')
    op.drop_column('mech_vehicles', 'gvm')
    op.drop_column('mech_vehicles', 'tare')
    op.drop_column('mech_vehicles', 'disk_license_no')
