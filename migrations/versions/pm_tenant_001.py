"""Reusable PM tenant identities only. Production execution requires separate approval."""
from alembic import op
import sqlalchemy as sa
revision='pm_tenant_001'
down_revision=None
branch_labels=('pm_tenant',)
depends_on=None

def upgrade():
    bind=op.get_bind()
    if bind.dialect.name!='postgresql':raise RuntimeError('PostgreSQL required')
    inspector=sa.inspect(bind)
    if inspector.has_table('bil_tenant_identity'):raise RuntimeError('Existing tenant identity objects require review')
    if not inspector.has_table('user'):raise RuntimeError('Existing user table required')
    op.create_table('bil_tenant_identity',
        sa.Column('id',sa.Integer,primary_key=True),
        sa.Column('manager_id',sa.Integer,sa.ForeignKey('user.id',ondelete='RESTRICT'),nullable=False),
        sa.Column('party_type',sa.String(32),nullable=False),sa.Column('legal_name',sa.String(200),nullable=False),
        sa.Column('identity_number',sa.String(80),nullable=False),sa.Column('address',sa.Text,nullable=False),
        sa.Column('email',sa.String(255),nullable=False,server_default=''),sa.Column('phone',sa.String(50),nullable=False,server_default=''),
        sa.Column('signatory_name',sa.String(200),nullable=False,server_default=''),
        sa.Column('signatory_identity_number',sa.String(80),nullable=False,server_default=''),
        sa.Column('signatory_capacity',sa.String(200),nullable=False,server_default=''),
        sa.Column('created_at',sa.DateTime(timezone=True),nullable=False,server_default=sa.func.now()),
        sa.CheckConstraint("party_type IN ('natural_person','juristic_person')",name='ck_pm_tenant_type'),
        sa.CheckConstraint("length(trim(legal_name)) > 0 AND length(trim(identity_number)) > 0 AND length(trim(address)) > 0",name='ck_pm_tenant_required'),
        sa.CheckConstraint("party_type = 'natural_person' OR (length(trim(signatory_name)) > 0 AND length(trim(signatory_identity_number)) > 0 AND length(trim(signatory_capacity)) > 0)",name='ck_pm_tenant_representative'))
    op.create_index('uq_pm_tenant_identity','bil_tenant_identity',['manager_id','party_type',sa.text("lower(replace(identity_number,' ',''))")],unique=True)

def downgrade():
    if op.get_bind().execute(sa.text('SELECT EXISTS(SELECT 1 FROM bil_tenant_identity)')).scalar():
        raise RuntimeError('Tenant identities exist; archive and review rather than deleting records')
    op.drop_table('bil_tenant_identity')
