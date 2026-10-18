"""Add property-scoped versioned lease masters only; no generation activation.

Independent branch preserves the exact pm_lease_001 readiness marker.
Production execution requires separate preflight/backup/rehearsal approval.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID
revision='pm_lease_master_001'
down_revision=None
branch_labels=('pm_lease_master',)
depends_on=None


def upgrade():
    bind=op.get_bind()
    if bind.dialect.name!='postgresql':raise RuntimeError('PostgreSQL required')
    inspector=sa.inspect(bind)
    if inspector.has_table('bil_property_lease_master'):raise RuntimeError('Existing property-master table requires review')
    for table,required in {'bil_property':{'id','manager_id'},'user':{'id'}}.items():
        if not inspector.has_table(table) or not required<={c['name'] for c in inspector.get_columns(table)}:
            raise RuntimeError('Existing property/user foundation required')
    op.create_table('bil_property_lease_master',
        sa.Column('id',UUID(as_uuid=True),primary_key=True),
        sa.Column('property_id',sa.Integer,sa.ForeignKey('bil_property.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('version',sa.Integer,nullable=False),
        sa.Column('original_filename',sa.String(200),nullable=False),
        sa.Column('mime_type',sa.String(100),nullable=False),
        sa.Column('storage_key',sa.String(500),nullable=False),
        sa.Column('size_bytes',sa.Integer,nullable=False),
        sa.Column('sha256',sa.String(64),nullable=False),
        sa.Column('uploaded_by',sa.Integer,sa.ForeignKey('user.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('uploaded_at',sa.DateTime(timezone=True),nullable=False,server_default=sa.func.now()),
        sa.Column('state',sa.String(16),nullable=False,server_default='pending'),
        sa.Column('verified_at',sa.DateTime(timezone=True)),
        sa.UniqueConstraint('property_id','version',name='uq_pm_property_master_version'),
        sa.UniqueConstraint('storage_key',name='uq_pm_property_master_storage'),
        sa.CheckConstraint('version>0',name='ck_pm_property_master_version'),
        sa.CheckConstraint('size_bytes>0 AND size_bytes<=10485760',name='ck_pm_property_master_size'),
        sa.CheckConstraint("sha256 ~ '^[0-9a-f]{64}$'",name='ck_pm_property_master_digest'),
        sa.CheckConstraint("mime_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document'",name='ck_pm_property_master_mime'),
        sa.CheckConstraint("state IN ('pending','available','failed')",name='ck_pm_property_master_state'),
        sa.CheckConstraint("(state='available' AND verified_at IS NOT NULL) OR (state IN ('pending','failed') AND verified_at IS NULL)",name='ck_pm_property_master_verified'))
    op.create_index('ix_pm_property_master_current','bil_property_lease_master',['property_id','state','version'])


def downgrade():
    raise RuntimeError('Destructive downgrade disabled; preserve uploaded masters and disable the upload feature instead')
