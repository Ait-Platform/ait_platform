"""Verify the BOQ-capable ledger functions already installed by migration 011."""
from alembic import op
import sqlalchemy as sa

revision = 'protrade_boq_trade_014'
down_revision = 'protrade_mechanic_quotes_012'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql' or op.get_context().as_sql:
        raise RuntimeError('Run BOQ trade migration online on PostgreSQL')
    schema = bind.execute(sa.text('SELECT current_schema()')).scalar_one()
    # Revision 011 already supports every non-mechanic trade, including BOQ.
    # Validate its predicates and financial guards without rewriting functions
    # or narrowing the other trades that revision 011 enabled.
    required_fragments = {
        'protrade_sync_quote_debit': [
            "IF NEW.trade='mechanic' OR NEW.debtor_id IS NULL THEN RETURN NEW; END IF;",
            "initcap(replace(NEW.trade,'_',' '))||' quotation '||reference",
            "IF TG_OP='UPDATE' AND EXISTS(SELECT 1 FROM debtor_ledger WHERE boq_quote_id=OLD.id)",
            "NEW.debtor_id IS DISTINCT FROM OLD.debtor_id OR NEW.shop_id IS DISTINCT FROM OLD.shop_id OR NEW.user_id IS DISTINCT FROM OLD.user_id OR NEW.trade IS DISTINCT FROM OLD.trade",
            "RAISE EXCEPTION 'Linked quotation account is immutable'",
            "d.id=NEW.debtor_id AND d.shop_id=NEW.shop_id AND d.user_id=NEW.user_id AND s.user_id=NEW.user_id",
            "RAISE EXCEPTION 'Invalid quotation debtor account'",
            "round(NEW.grand_total*100)",
            "ON CONFLICT(boq_quote_id) DO UPDATE SET amount=excluded.amount,ref=excluded.ref,description=excluded.description",
        ],
        'protrade_guard_quote_debit': [
            "q.trade<>'mechanic'",
            "IF TG_OP='DELETE' THEN",
            "IF OLD.boq_quote_id IS NOT NULL THEN RAISE EXCEPTION 'Quotation ledger history cannot be deleted'; END IF;",
            "IF TG_OP='UPDATE' AND OLD.boq_quote_id IS NOT NULL AND NEW.boq_quote_id IS DISTINCT FROM OLD.boq_quote_id",
            "RAISE EXCEPTION 'Quotation ledger identity is immutable'",
            "d.id=q.debtor_id AND d.shop_id=q.shop_id AND d.user_id=q.user_id",
            "s.id=q.shop_id AND s.user_id=q.user_id",
            "q.debtor_id=NEW.debtor_id AND NEW.kind='debit' AND NEW.amount=round(q.grand_total*100)",
            "RAISE EXCEPTION 'Invalid quotation ledger debit'",
        ],
    }
    for name, fragments in required_fragments.items():
        definition = bind.execute(sa.text(
            "SELECT pg_get_functiondef(p.oid) FROM pg_proc p "
            "JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE n.nspname=:schema AND p.proname=:name AND p.pronargs=0"
        ), dict(schema=schema, name=name)).scalar_one()
        for fragment in fragments:
            if definition.count(fragment) != 1:
                raise RuntimeError('Unexpected quotation ledger function definition for ' + name
                    + ': missing or repeated required fragment ' + repr(fragment))


def downgrade():
    raise RuntimeError('Preserve BOQ quotation accounting history')
