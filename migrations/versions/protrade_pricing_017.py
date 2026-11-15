"""Audited ProTrade prices reusing universal token tariffs; no historical deductions."""
import json
from alembic import op
import sqlalchemy as sa

revision = 'protrade_pricing_017'
down_revision = 'protrade_boq_time_016'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql' or op.get_context().as_sql:
        raise RuntimeError('Run pricing migration online on PostgreSQL')
    initial = dict(tokens=dict(quote=10, invoice=10, soa=10, boq_section=25),
        suggested=dict(quote_electronic=1000,quote_printed=1500,soa_electronic=1000,soa_printed=1500),
        boq=dict(contractor_fee_cents=5000,operator_retains_cents=2500))
    existing = bind.execute(sa.text("SELECT action_name,base_token_cost FROM universal_token_tariff WHERE program_slug='protrade'")).all()
    if any(name not in initial['tokens'] or initial['tokens'][name] != cost for name,cost in existing):
        raise RuntimeError('Review existing ProTrade tariffs before installing approved baseline prices')
    op.create_table('protrade_pricing_revision',
        sa.Column('id',sa.Integer(),primary_key=True),
        sa.Column('user_id',sa.Integer(),sa.ForeignKey('user.id')),
        sa.Column('values',sa.JSON(),nullable=False),
        sa.Column('reason',sa.String(400),nullable=False),
        sa.Column('created_at',sa.DateTime(),nullable=False,server_default=sa.text("(now() AT TIME ZONE 'UTC')")),
        sa.Column('transaction_id',sa.BigInteger(),nullable=False,server_default=sa.text('txid_current()')))
    bind.execute(sa.text('INSERT INTO protrade_pricing_revision ("values",reason) VALUES(CAST(:values AS json),:reason)'),
        dict(values=json.dumps(initial),reason='Existing approved ProTrade prices; no financial records changed'))
    for kind,cost in initial['tokens'].items():
        bind.execute(sa.text("INSERT INTO universal_token_tariff(program_slug,action_name,base_token_cost) VALUES('protrade',:kind,:cost) ON CONFLICT(program_slug,action_name) DO NOTHING"),dict(kind=kind,cost=cost))
    op.drop_constraint('ck_protrade_document_charge','protrade_document',type_='check')
    op.create_check_constraint('ck_protrade_document_charge','protrade_document',"tokens > 0 AND kind IN ('quote','invoice','soa','boq_section')")
    schema = bind.execute(sa.text('SELECT current_schema()')).scalar_one()
    q = bind.dialect.identifier_preparer.quote_schema(schema)
    op.execute(f"""CREATE FUNCTION {q}.protrade_keep_pricing_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
        RAISE EXCEPTION 'Pricing approval history is immutable'; END $$""")
    op.execute(f'CREATE TRIGGER protrade_pricing_immutable BEFORE UPDATE OR DELETE ON protrade_pricing_revision FOR EACH ROW EXECUTE FUNCTION {q}.protrade_keep_pricing_history()')
    op.execute(f"""CREATE FUNCTION {q}.protrade_guard_tariff_change() RETURNS trigger LANGUAGE plpgsql SET search_path={q},pg_catalog AS $$ BEGIN
        IF OLD.program_slug='protrade' THEN
            IF TG_OP='DELETE' THEN RAISE EXCEPTION 'ProTrade tariffs cannot be deleted'; END IF;
            IF NEW.program_slug IS DISTINCT FROM OLD.program_slug OR NEW.action_name IS DISTINCT FROM OLD.action_name THEN
                RAISE EXCEPTION 'ProTrade tariff identity is immutable'; END IF;
            IF NEW.base_token_cost IS DISTINCT FROM OLD.base_token_cost AND NOT EXISTS(
                SELECT 1 FROM protrade_pricing_revision r WHERE r.transaction_id=txid_current()
                AND r.user_id IS NOT NULL AND (r.values->'tokens'->>NEW.action_name)::integer=NEW.base_token_cost) THEN
                RAISE EXCEPTION 'ProTrade tariff change requires an approved audit snapshot'; END IF;
        ELSIF TG_OP='UPDATE' AND NEW.program_slug='protrade' THEN
            RAISE EXCEPTION 'ProTrade tariff identity is immutable';
        END IF;
        IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW; END $$""")
    op.execute(f'CREATE TRIGGER protrade_tariff_audit BEFORE UPDATE OR DELETE ON universal_token_tariff FOR EACH ROW EXECUTE FUNCTION {q}.protrade_guard_tariff_change()')


def downgrade():
    raise RuntimeError('Preserve approved price and document history')
