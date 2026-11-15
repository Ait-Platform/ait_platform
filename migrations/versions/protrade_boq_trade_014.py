"""Extend existing quotation ledger triggers to the dedicated BOQ trade."""
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
    replacements = {
        'protrade_sync_quote_debit': [
            ("NEW.trade<>'carpentry'", "NEW.trade NOT IN ('carpentry','boq')"),
            ("'Carpentry quotation '||reference", "CASE WHEN NEW.trade='boq' THEN 'BOQ quotation ' ELSE 'Carpentry quotation ' END||reference"),
        ],
        'protrade_guard_quote_debit': [
            ("q.trade='carpentry'", "q.trade IN ('carpentry','boq')"),
        ],
    }
    for name, changes in replacements.items():
        definition = bind.execute(sa.text(
            "SELECT pg_get_functiondef(p.oid) FROM pg_proc p "
            "JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE n.nspname=:schema AND p.proname=:name AND p.pronargs=0"
        ), dict(schema=schema, name=name)).scalar_one()
        for old, new in changes:
            if definition.count(old) != 1:
                raise RuntimeError('Unexpected quotation ledger function definition for ' + name)
            definition = definition.replace(old, new)
        op.execute(sa.text(definition))


def downgrade():
    raise RuntimeError('Preserve BOQ quotation accounting history')
