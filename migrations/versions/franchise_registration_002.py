"""Rename the global registration table without recreating or rewriting records."""
from alembic import op
import sqlalchemy as sa

revision = 'franchise_registration_002'
down_revision = 'ait_franchisee_001'
branch_labels = None
depends_on = None


def upgrade():
    context = op.get_context()
    config = getattr(context,'config',None)
    approved = context.opts.get('franchise_registration_rename_approved') is True or (
        config is not None and config.attributes.get('franchise_registration_rename_approved') is True)
    if not approved:
        raise RuntimeError('The registration rename requires explicit production migration authorisation')
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql' or context.as_sql:
        raise RuntimeError('Run online on verified PostgreSQL within a guarded transaction')
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute("SET LOCAL statement_timeout='30s'")
    old = bind.scalar(sa.text("SELECT to_regclass('ait_franchisee_registration')::oid"))
    new = bind.scalar(sa.text("SELECT to_regclass('franchise_registration')::oid"))
    if old is None or new is not None:
        raise RuntimeError('Unexpected pre-rename schema; do not create, merge or overwrite a table')
    if bind.scalar(sa.text('SELECT relkind FROM pg_class WHERE oid=:oid'),{'oid':old}) != 'r':
        raise RuntimeError('The legacy registration object must be an ordinary table')
    op.rename_table('ait_franchisee_registration','franchise_registration')
    after = bind.scalar(sa.text("SELECT to_regclass('franchise_registration')::oid"))
    absent = bind.scalar(sa.text("SELECT to_regclass('ait_franchisee_registration')"))
    if after != old or absent is not None:
        raise RuntimeError('Rename verification failed; roll back the transaction')
    # The original table OID, rows, constraints, indexes, grants, FKs, serial
    # default and both existing sequences remain untouched. No licence is dropped.


def downgrade():
    raise RuntimeError('Forward-only: preserve registration identity and reviewed deployment compatibility')
