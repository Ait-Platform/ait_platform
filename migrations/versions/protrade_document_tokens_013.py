"""Durable ten-token document receipts and immutable generated SOA snapshots."""
from alembic import op
import sqlalchemy as sa
revision='protrade_document_tokens_013'
down_revision='protrade_boq_trade_014'
branch_labels=None
depends_on=None


def upgrade():
    bind=op.get_bind()
    if bind.dialect.name!='postgresql' or op.get_context().as_sql:
        raise RuntimeError('Run document receipt migration online on verified PostgreSQL')
    if not all(bind.execute(sa.text('SELECT current_database(),current_user')).one()):raise RuntimeError('Cannot verify database identity')
    op.create_table('protrade_document',
        sa.Column('id',sa.Integer(),primary_key=True),
        sa.Column('user_id',sa.Integer(),sa.ForeignKey('user.id'),nullable=False),
        sa.Column('shop_id',sa.Integer(),sa.ForeignKey('mech_shops.id')),
        sa.Column('debtor_id',sa.Integer(),sa.ForeignKey('debtor.id')),
        sa.Column('document_key',sa.String(120),nullable=False),sa.Column('kind',sa.String(12),nullable=False),
        sa.Column('token_transaction_id',sa.Integer(),sa.ForeignKey('ait_token_transaction.id'),nullable=False,unique=True),
        sa.Column('tokens',sa.Integer(),nullable=False,server_default='10'),
        sa.Column('snapshot',sa.JSON()),sa.Column('pdf',sa.LargeBinary()),sa.Column('created_at',sa.DateTime(),nullable=False,server_default=sa.text("(now() AT TIME ZONE 'UTC')")),
        sa.UniqueConstraint('user_id','document_key',name='uq_protrade_document_key'),
        sa.CheckConstraint("(tokens=10 AND kind IN ('quote','invoice','soa')) OR (tokens=25 AND kind='boq_section')",name='ck_protrade_document_charge'),
        sa.CheckConstraint("kind<>'soa' OR (shop_id IS NOT NULL AND debtor_id IS NOT NULL AND snapshot IS NOT NULL AND pdf IS NOT NULL)",name='ck_protrade_soa_snapshot'))
    schema=bind.execute(sa.text('SELECT current_schema()')).scalar_one();q=bind.dialect.identifier_preparer.quote_schema(schema)
    op.execute(f"""CREATE FUNCTION {q}.protrade_keep_document_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
        RAISE EXCEPTION 'Completed document receipts and statement snapshots are immutable'; END $$""")
    op.execute(f'CREATE TRIGGER protrade_document_immutable BEFORE UPDATE OR DELETE ON protrade_document FOR EACH ROW EXECUTE FUNCTION {q}.protrade_keep_document_receipt()')
    op.add_column('protrade_boq_quotes',sa.Column('boq_sections',sa.JSON(),nullable=False,server_default='[]'))
    # No historical backfill or retrospective wallet deductions.


def downgrade():
    raise RuntimeError('Preserve token receipts and historical statement PDFs')
