"""Private PM SOA branding metadata; independent of financial migrations."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
revision='pm_soa_brand_001'
down_revision=None
branch_labels=('pm_soa_branding',)
depends_on=None


def upgrade():
    if op.get_bind().dialect.name!='postgresql':raise RuntimeError('PostgreSQL required')
    op.create_table('bil_pm_soa_branding',
        sa.Column('id',postgresql.UUID(as_uuid=False),primary_key=True),
        sa.Column('property_id',sa.Integer(),sa.ForeignKey('bil_property.id'),nullable=False),
        sa.Column('manager_id',sa.Integer(),nullable=False),
        sa.Column('kind',sa.String(16),nullable=False),sa.Column('title',sa.String(120),nullable=False),
        sa.Column('details',postgresql.JSONB(),nullable=False),
        sa.Column('selected',sa.Boolean(),nullable=False,server_default=sa.false()),
        sa.Column('create_key',sa.String(32),nullable=False,unique=True),
        sa.Column('created_at',sa.DateTime(timezone=True),nullable=False,server_default=sa.func.now()),
        sa.CheckConstraint("kind IN ('logo','letterhead')",name='ck_pm_soa_brand_kind'))
    op.create_index('uq_pm_soa_brand_selected','bil_pm_soa_branding',['property_id','manager_id','kind'],unique=True,postgresql_where=sa.text('selected'))


def downgrade():
    raise RuntimeError('Preserve private branding metadata and selection history; no destructive downgrade.')
