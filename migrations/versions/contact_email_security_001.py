"""Shared PostgreSQL rolling-hour quota for the public contact form."""
from alembic import op
import sqlalchemy as sa

revision = 'contact_email_security_001'
down_revision = None
branch_labels = ('contact_email_security',)
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql':
        raise RuntimeError('Contact email protection requires PostgreSQL')
    if not op.get_context().as_sql:
        if not all(bind.execute(sa.text('SELECT current_database(),current_user')).one()):
            raise RuntimeError('Cannot verify database identity')
    op.create_table('contact_email_attempt',
        sa.Column('id', sa.String(32), primary_key=True),
        sa.Column('ip_hash', sa.String(64), nullable=False),
        sa.Column('attempted_at', sa.DateTime(timezone=True), nullable=False))
    op.create_index('ix_contact_email_ip_time', 'contact_email_attempt', ['ip_hash','attempted_at'])


def downgrade():
    raise RuntimeError('Preserve contact email quota reservations; destructive downgrade disabled')
