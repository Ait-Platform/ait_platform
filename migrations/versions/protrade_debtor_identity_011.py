"""Reuse debtor identities with independent shop accounts; basic trade quotations."""
from alembic import op
import sqlalchemy as sa

revision = 'protrade_debtor_identity_011'
down_revision = 'protrade_payment_method_010'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql' or op.get_context().as_sql:
        raise RuntimeError('Run debtor identity migration online on verified PostgreSQL')
    if not all(bind.execute(sa.text('SELECT current_database(),current_user')).one()):
        raise RuntimeError('Cannot verify database identity')
    op.execute('LOCK TABLE debtor, protrade_boq_quotes, debtor_ledger IN SHARE ROW EXCLUSIVE MODE')
    op.add_column('debtor', sa.Column('identity_debtor_id',sa.Integer(),sa.ForeignKey('debtor.id',name='fk_debtor_identity')))
    op.create_index('uq_debtor_shop_identity','debtor',['user_id','shop_id',sa.text('(COALESCE(identity_debtor_id,id))')],unique=True,postgresql_where=sa.text('shop_id IS NOT NULL'))
    schema=bind.execute(sa.text('SELECT current_schema()')).scalar_one()
    quoted=bind.dialect.identifier_preparer.quote_schema(schema)
    op.execute(f"""CREATE FUNCTION {quoted}.protrade_guard_debtor_identity() RETURNS trigger LANGUAGE plpgsql
        SET search_path={quoted},pg_catalog AS $$ BEGIN
        IF NEW.identity_debtor_id IS NOT NULL AND (NEW.shop_id IS NULL OR NEW.identity_debtor_id=NEW.id OR NOT EXISTS(
            SELECT 1 FROM debtor d WHERE d.id=NEW.identity_debtor_id AND d.user_id=NEW.user_id AND d.identity_debtor_id IS NULL)) THEN
            RAISE EXCEPTION 'Invalid debtor identity'; END IF;
        IF TG_OP='UPDATE' AND (NEW.user_id IS DISTINCT FROM OLD.user_id OR NEW.shop_id IS DISTINCT FROM OLD.shop_id OR NEW.identity_debtor_id IS DISTINCT FROM OLD.identity_debtor_id)
           AND (OLD.identity_debtor_id IS NOT NULL OR EXISTS(SELECT 1 FROM debtor WHERE identity_debtor_id=OLD.id)) THEN
            RAISE EXCEPTION 'Linked debtor identity and account are immutable'; END IF;
        RETURN NEW; END $$""")
    op.execute(f'CREATE TRIGGER protrade_debtor_identity_guard BEFORE INSERT OR UPDATE ON debtor FOR EACH ROW EXECUTE FUNCTION {quoted}.protrade_guard_debtor_identity()')
    op.drop_constraint('ck_protrade_boq_quote_trade','protrade_boq_quotes',type_='check')
    op.create_check_constraint('ck_protrade_boq_quote_trade','protrade_boq_quotes',"trade <> 'mechanic' AND length(trim(trade)) > 0")
    # Reuse the deployed atomic sync/guard unchanged except for the trade predicate
    # and description. No backfill or recalculation of existing accounting rows.
    for name,replacements in (
        ('protrade_sync_quote_debit',[("NEW.trade<>'carpentry'", "NEW.trade='mechanic'"),("'Carpentry quotation '||reference", "initcap(replace(NEW.trade,'_',' '))||' quotation '||reference")]),
        ('protrade_guard_quote_debit',[("q.trade='carpentry'", "q.trade<>'mechanic'")]),
    ):
        definition=bind.execute(sa.text('SELECT pg_get_functiondef(to_regprocedure(:name))'),{'name':schema+'.'+name+'()'}).scalar_one()
        for old,new in replacements:
            if definition.count(old)!=1:
                raise RuntimeError('Unexpected quotation ledger function; inspect before migrating')
            definition=definition.replace(old,new)
        op.execute(sa.text(definition))


def downgrade():
    raise RuntimeError('Preserve linked debtor identities and all financial history')
