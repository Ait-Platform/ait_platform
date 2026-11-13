"""Per-shop short quotation numbers; existing identifiers and invoices are untouched."""
from alembic import op
import sqlalchemy as sa

revision = 'protrade_quote_numbers_008'
down_revision = 'protrade_output_007'
branch_labels = None
depends_on = None


def upgrade():
    bind=op.get_bind()
    if bind.dialect.name!='postgresql':
        raise RuntimeError('Quotation numbering requires PostgreSQL')
    if op.get_context().as_sql:
        raise RuntimeError('Run this migration online against verified PostgreSQL')
    if not all(bind.execute(sa.text('SELECT current_database(),current_user')).one()):
        raise RuntimeError('Cannot verify database identity')
    # Prevent inserts between the historical ordering and trigger installation.
    op.execute('LOCK TABLE mech_shops,protrade_boq_quotes,mech_job_cards IN ACCESS EXCLUSIVE MODE')
    op.add_column('protrade_boq_quotes',sa.Column('quotation_seq',sa.Integer()))
    op.add_column('mech_job_cards',sa.Column('quotation_seq',sa.Integer()))
    op.create_table('protrade_quote_counter',
        sa.Column('shop_id',sa.Integer(),sa.ForeignKey('mech_shops.id'),primary_key=True),
        sa.Column('last_number',sa.Integer(),nullable=False),
        sa.CheckConstraint('last_number > 0',name='ck_protrade_quote_counter'))
    op.create_table('protrade_quote_number',
        sa.Column('shop_id',sa.Integer(),sa.ForeignKey('mech_shops.id'),primary_key=True),
        sa.Column('number',sa.Integer(),primary_key=True),
        sa.Column('document_kind',sa.String(16),nullable=False),
        sa.Column('document_id',sa.Integer(),nullable=False),
        sa.UniqueConstraint('document_kind','document_id',name='uq_protrade_quote_document'),
        sa.CheckConstraint("number > 0 AND document_kind IN ('boq','mechanic')",name='ck_protrade_quote_number'))
    op.execute("""
        INSERT INTO protrade_quote_number(shop_id,number,document_kind,document_id)
        SELECT shop_id,row_number() OVER(PARTITION BY shop_id ORDER BY created_at NULLS LAST,kind,id)::integer,kind,id
        FROM (
            SELECT shop_id,created_at,'boq'::text AS kind,id FROM protrade_boq_quotes
            UNION ALL
            SELECT shop_id,created_at,'mechanic',id FROM mech_job_cards WHERE shop_id IS NOT NULL
        ) existing
    """)
    for table,kind in [('protrade_boq_quotes','boq'),('mech_job_cards','mechanic')]:
        op.execute(f"UPDATE {table} d SET quotation_seq=n.number FROM protrade_quote_number n WHERE n.document_kind='{kind}' AND n.document_id=d.id")
        op.create_unique_constraint('uq_'+kind+'_shop_quote_number',table,['shop_id','quotation_seq'])
    op.execute('INSERT INTO protrade_quote_counter SELECT shop_id,max(number) FROM protrade_quote_number GROUP BY shop_id')
    # This security-invoker function is installed alongside the tables and resolves only that schema.
    schema=bind.execute(sa.text('SELECT current_schema()')).scalar_one()
    quoted=bind.dialect.identifier_preparer.quote_schema(schema)
    op.execute(f"""
        CREATE FUNCTION {quoted}.protrade_assign_quote_number() RETURNS trigger
        LANGUAGE plpgsql SET search_path = {quoted}, pg_catalog AS $$
        DECLARE allocated integer;
        BEGIN
            IF TG_OP='UPDATE' AND OLD.quotation_seq IS NOT NULL THEN
                IF NEW.quotation_seq IS DISTINCT FROM OLD.quotation_seq OR NEW.shop_id IS DISTINCT FROM OLD.shop_id OR NEW.id IS DISTINCT FROM OLD.id THEN
                    RAISE EXCEPTION 'Assigned quotation identity is immutable';
                END IF;
                RETURN NEW;
            END IF;
            IF NEW.shop_id IS NULL THEN RETURN NEW; END IF;
            IF NEW.quotation_seq IS NOT NULL THEN RAISE EXCEPTION 'Quotation numbers are assigned automatically'; END IF;
            INSERT INTO protrade_quote_counter(shop_id,last_number) VALUES(NEW.shop_id,1)
            ON CONFLICT(shop_id) DO UPDATE SET last_number=protrade_quote_counter.last_number+1
            RETURNING last_number INTO allocated;
            INSERT INTO protrade_quote_number(shop_id,number,document_kind,document_id)
            VALUES(NEW.shop_id,allocated,TG_ARGV[0],NEW.id);
            NEW.quotation_seq=allocated;
            RETURN NEW;
        END $$
    """)
    for table,kind in [('protrade_boq_quotes','boq'),('mech_job_cards','mechanic')]:
        op.execute(f"CREATE TRIGGER protrade_quote_number BEFORE INSERT OR UPDATE ON {table} FOR EACH ROW EXECUTE FUNCTION {quoted}.protrade_assign_quote_number('{kind}')")


def downgrade():
    raise RuntimeError('Quotation numbers are permanent; destructive downgrade disabled')
