"""One Carpentry quotation debit and idempotent payments in the existing ledger."""
from alembic import op
import sqlalchemy as sa
revision = 'protrade_quote_ledger_009'
down_revision = 'protrade_quote_numbers_008'
branch_labels = None
depends_on = None


def upgrade():
    bind=op.get_bind()
    if bind.dialect.name!='postgresql' or op.get_context().as_sql:
        raise RuntimeError('Run quotation ledger migration online on verified PostgreSQL')
    if not all(bind.execute(sa.text('SELECT current_database(), current_user')).one()):
        raise RuntimeError('Cannot verify database identity')
    op.execute('LOCK TABLE protrade_boq_quotes, debtor_ledger, debtor, mech_shops IN SHARE ROW EXCLUSIVE MODE')
    op.alter_column('debtor','opening_balance',server_default=sa.text('0'))
    op.alter_column('debtor_ledger','amount',type_=sa.Numeric(28,0),existing_type=sa.Integer(),postgresql_using='amount::numeric(28,0)')
    op.add_column('debtor_ledger',sa.Column('boq_quote_id',sa.Integer()))
    op.add_column('debtor_ledger',sa.Column('payment_key',sa.String(32)))
    op.create_foreign_key('fk_debtor_ledger_boq_quote','debtor_ledger','protrade_boq_quotes',['boq_quote_id'],['id'])
    op.create_unique_constraint('uq_debtor_ledger_boq_quote','debtor_ledger',['boq_quote_id'])
    op.create_unique_constraint('uq_debtor_payment_key','debtor_ledger',['debtor_id','payment_key'])
    op.create_index('uq_debtor_payment_fingerprint','debtor_ledger',['debtor_id','txn_date','amount','ref'],unique=True,postgresql_where=sa.text('payment_key IS NOT NULL'))
    op.create_check_constraint('ck_debtor_quote_debit','debtor_ledger',"boq_quote_id IS NULL OR (kind='debit' AND payment_key IS NULL AND amount>=0)")
    op.create_check_constraint('ck_debtor_payment_credit','debtor_ledger',"payment_key IS NULL OR (kind='credit' AND boq_quote_id IS NULL AND amount>0 AND ref IS NOT NULL AND length(trim(ref))>0)")
    # Only authorised, permanently shop-linked quotations can be backfilled.
    op.execute("""CREATE TEMP TABLE protrade_quote_backfill ON COMMIT DROP AS
        SELECT q.*, COALESCE('Q-'||lpad(q.quotation_seq::text,greatest(4,length(q.quotation_seq::text)),'0'),q.quote_number) AS ledger_ref
        FROM protrade_boq_quotes q JOIN debtor d ON d.id=q.debtor_id AND d.shop_id=q.shop_id AND d.user_id=q.user_id
        JOIN mech_shops s ON s.id=q.shop_id AND s.user_id=q.user_id WHERE q.trade='carpentry'""")
    # Adopt a single existing, explicitly referenced debit; never guess or double it.
    op.execute("""DO $$ BEGIN
        IF EXISTS(SELECT q.id FROM protrade_quote_backfill q JOIN debtor_ledger l ON l.debtor_id=q.debtor_id AND l.kind='debit' AND l.ref IN(q.ledger_ref,q.quote_number)
                  GROUP BY q.id,q.grand_total HAVING count(*)>1 OR min(l.amount)<>round(q.grand_total*100)) THEN
            RAISE EXCEPTION 'Ambiguous existing quotation charges; reconcile referenced debits before migration';
        END IF;
    END $$""")
    op.execute("""UPDATE debtor_ledger l SET boq_quote_id=q.id FROM protrade_quote_backfill q
        WHERE l.debtor_id=q.debtor_id AND l.kind='debit' AND l.ref IN(q.ledger_ref,q.quote_number) AND l.amount=round(q.grand_total*100)""")
    op.execute("""INSERT INTO debtor_ledger(debtor_id,txn_date,description,kind,amount,ref,created_at,boq_quote_id)
        SELECT q.debtor_id,((COALESCE(q.created_at,now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') AT TIME ZONE 'Africa/Johannesburg')::date,
               'Carpentry quotation '||q.ledger_ref,'debit',round(q.grand_total*100),q.ledger_ref,COALESCE(q.created_at,now() AT TIME ZONE 'UTC'),q.id
        FROM protrade_quote_backfill q WHERE NOT EXISTS(SELECT 1 FROM debtor_ledger l WHERE l.boq_quote_id=q.id)""")
    schema=bind.execute(sa.text('SELECT current_schema()')).scalar_one()
    quoted=bind.dialect.identifier_preparer.quote_schema(schema)
    op.execute(f"""CREATE FUNCTION {quoted}.protrade_sync_quote_debit() RETURNS trigger LANGUAGE plpgsql
        SET search_path={quoted},pg_catalog AS $$ DECLARE reference text; BEGIN
        IF TG_OP='UPDATE' AND EXISTS(SELECT 1 FROM debtor_ledger WHERE boq_quote_id=OLD.id)
           AND (NEW.debtor_id IS DISTINCT FROM OLD.debtor_id OR NEW.shop_id IS DISTINCT FROM OLD.shop_id OR NEW.user_id IS DISTINCT FROM OLD.user_id OR NEW.trade IS DISTINCT FROM OLD.trade) THEN
            RAISE EXCEPTION 'Linked quotation account is immutable'; END IF;
        IF NEW.trade<>'carpentry' OR NEW.debtor_id IS NULL THEN RETURN NEW; END IF;
        IF NOT EXISTS(SELECT 1 FROM debtor d JOIN mech_shops s ON s.id=d.shop_id WHERE d.id=NEW.debtor_id AND d.shop_id=NEW.shop_id AND d.user_id=NEW.user_id AND s.user_id=NEW.user_id) THEN
            RAISE EXCEPTION 'Invalid quotation debtor account'; END IF;
        reference=COALESCE('Q-'||lpad(NEW.quotation_seq::text,greatest(4,length(NEW.quotation_seq::text)),'0'),NEW.quote_number);
        INSERT INTO debtor_ledger(debtor_id,txn_date,description,kind,amount,ref,created_at,boq_quote_id)
        VALUES(NEW.debtor_id,((COALESCE(NEW.created_at,now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') AT TIME ZONE 'Africa/Johannesburg')::date,
               'Carpentry quotation '||reference,'debit',round(NEW.grand_total*100),reference,COALESCE(NEW.created_at,now() AT TIME ZONE 'UTC'),NEW.id)
        ON CONFLICT(boq_quote_id) DO UPDATE SET amount=excluded.amount,ref=excluded.ref,description=excluded.description;
        RETURN NEW; END $$""")
    op.execute(f"""CREATE FUNCTION {quoted}.protrade_guard_quote_debit() RETURNS trigger LANGUAGE plpgsql
        SET search_path={quoted},pg_catalog AS $$ BEGIN
        IF TG_OP='DELETE' THEN
            IF OLD.boq_quote_id IS NOT NULL THEN RAISE EXCEPTION 'Quotation ledger history cannot be deleted'; END IF;
            RETURN OLD;
        END IF;
        IF TG_OP='UPDATE' AND OLD.boq_quote_id IS NOT NULL AND NEW.boq_quote_id IS DISTINCT FROM OLD.boq_quote_id THEN
            RAISE EXCEPTION 'Quotation ledger identity is immutable'; END IF;
        IF NEW.boq_quote_id IS NOT NULL AND NOT EXISTS(
            SELECT 1 FROM protrade_boq_quotes q JOIN debtor d ON d.id=q.debtor_id AND d.shop_id=q.shop_id AND d.user_id=q.user_id
            JOIN mech_shops s ON s.id=q.shop_id AND s.user_id=q.user_id
            WHERE q.id=NEW.boq_quote_id AND q.trade='carpentry' AND q.debtor_id=NEW.debtor_id AND NEW.kind='debit' AND NEW.amount=round(q.grand_total*100)) THEN
            RAISE EXCEPTION 'Invalid quotation ledger debit'; END IF;
        RETURN NEW; END $$""")
    op.execute(f'CREATE TRIGGER protrade_quote_debit AFTER INSERT OR UPDATE OF grand_total,quotation_seq,quote_number,debtor_id,shop_id,user_id,trade ON protrade_boq_quotes FOR EACH ROW EXECUTE FUNCTION {quoted}.protrade_sync_quote_debit()')
    op.execute(f'CREATE TRIGGER protrade_quote_debit_guard BEFORE INSERT OR UPDATE OR DELETE ON debtor_ledger FOR EACH ROW EXECUTE FUNCTION {quoted}.protrade_guard_quote_debit()')


def downgrade():
    raise RuntimeError('Accounting links and payment history must not be destructively downgraded')
