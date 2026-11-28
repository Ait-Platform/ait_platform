"""Allow account-based registration without collecting or checking an SA ID."""
from alembic import op
import sqlalchemy as sa

revision='franchise_registration_003'
down_revision='franchise_registration_002'
branch_labels=None
depends_on=None


def upgrade():
    context=op.get_context()
    config=getattr(context,'config',None)
    approved=context.opts.get('franchise_registration_simplify_approved') is True or (
        config is not None and config.attributes.get('franchise_registration_simplify_approved') is True)
    if not approved:
        raise RuntimeError('Simplified Franchise registration requires explicit migration authorisation')
    bind=op.get_bind()
    if bind.dialect.name!='postgresql' or context.as_sql:
        raise RuntimeError('Run online on verified PostgreSQL within one guarded transaction')
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute("SET LOCAL statement_timeout='30s'")
    table='franchise_registration'
    if not bind.scalar(sa.text("SELECT to_regclass('franchise_registration') IS NOT NULL AND to_regclass('ait_franchisee_registration') IS NULL")):
        raise RuntimeError('Apply the separately authorised table rename first')
    unique=bind.execute(sa.text("""SELECT k.conname FROM pg_constraint k
      JOIN pg_attribute a ON a.attrelid=k.conrelid AND a.attname='identity_digest'
      WHERE k.conrelid='franchise_registration'::regclass AND k.contype='u'
      AND k.conkey=ARRAY[a.attnum]::smallint[]""")).scalars().all()
    if len(unique)!=1:
        raise RuntimeError('Unexpected identity constraint metadata; stop for review')
    op.drop_constraint('ck_franchisee_submitted',table,type_='check')
    op.create_check_constraint('ck_franchisee_submitted',table,
        "status='Invited' OR (full_name IS NOT NULL AND cellphone IS NOT NULL AND address_ciphertext IS NOT NULL AND submitted_at IS NOT NULL AND invitation_digest IS NULL)")
    op.drop_constraint(unique[0],table,type_='unique')
    # No columns/rows/ciphertext/history/IDs/dates/numbers are rewritten or deleted.
    # Account/email/number uniqueness, FKs and all other constraints remain intact.


def downgrade():
    raise RuntimeError('Forward-only: new account-based registrations legitimately have no identity data')
