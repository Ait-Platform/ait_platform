"""Explicit VAT profiles, effective-dated statutory rates and frozen converted invoices."""
from alembic import op
import sqlalchemy as sa

revision='protrade_vat_invoices_018'
down_revision='protrade_pricing_017'
branch_labels=None
depends_on=None


def upgrade():
    bind=op.get_bind()
    if bind.dialect.name!='postgresql' or op.get_context().as_sql:
        raise RuntimeError('Run VAT/invoice migration online on PostgreSQL')
    # Unknown existing registration status is never inferred from a historical rate.
    op.add_column('mech_shops',sa.Column('vat_registered',sa.Boolean()))
    op.add_column('protrade_boq_quotes',sa.Column('tax_snapshot',sa.JSON()))
    op.add_column('mech_job_cards',sa.Column('tax_snapshot',sa.JSON()))
    op.create_table('protrade_vat_rate',sa.Column('id',sa.Integer(),primary_key=True),
        sa.Column('effective_date',sa.Date(),nullable=False),sa.Column('rate',sa.Numeric(5,2),nullable=False),
        sa.Column('user_id',sa.Integer(),sa.ForeignKey('user.id')),sa.Column('reason',sa.String(400),nullable=False),
        sa.Column('created_at',sa.DateTime(),nullable=False,server_default=sa.text("(now() AT TIME ZONE 'UTC')")),
        sa.CheckConstraint('rate > 0 AND rate <= 100',name='ck_protrade_vat_rate'))
    # Statutory baseline verified with SARS; runtime calculations read this table.
    op.execute("INSERT INTO protrade_vat_rate(effective_date,rate,reason) VALUES('2018-04-01',15,'SARS standard VAT rate effective 1 April 2018; verified November 2026')")
    op.alter_column('mech_invoices','job_card_id',existing_type=sa.Integer(),nullable=True)
    op.add_column('mech_invoices',sa.Column('boq_quote_id',sa.Integer(),sa.ForeignKey('protrade_boq_quotes.id')))
    op.add_column('mech_invoices',sa.Column('invoice_number',sa.String(50)))
    op.add_column('mech_invoices',sa.Column('source_data',sa.JSON()))
    op.create_unique_constraint('uq_protrade_invoice_number','mech_invoices',['invoice_number'])
    op.create_unique_constraint('uq_protrade_invoice_boq','mech_invoices',['boq_quote_id'])
    op.create_check_constraint('ck_protrade_invoice_source','mech_invoices','(job_card_id IS NULL) <> (boq_quote_id IS NULL)')
    op.create_index('uq_protrade_converted_job','mech_invoices',['job_card_id'],unique=True,postgresql_where=sa.text('invoice_number IS NOT NULL'))
    schema=bind.execute(sa.text('SELECT current_schema()')).scalar_one();q=bind.dialect.identifier_preparer.quote_schema(schema)
    op.execute(f"""CREATE FUNCTION {q}.protrade_keep_vat_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
        RAISE EXCEPTION 'Approved VAT rate history is immutable'; END $$""")
    op.execute(f'CREATE TRIGGER protrade_vat_immutable BEFORE UPDATE OR DELETE ON protrade_vat_rate FOR EACH ROW EXECUTE FUNCTION {q}.protrade_keep_vat_history()')
    op.execute(f"""CREATE FUNCTION {q}.protrade_keep_invoice_snapshot() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
        IF OLD.source_data IS NOT NULL THEN
            IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Issued invoice history cannot be deleted'; END IF;
            IF NEW.job_card_id IS DISTINCT FROM OLD.job_card_id OR NEW.boq_quote_id IS DISTINCT FROM OLD.boq_quote_id
               OR NEW.invoice_number IS DISTINCT FROM OLD.invoice_number OR NEW.source_data::jsonb IS DISTINCT FROM OLD.source_data::jsonb
               OR NEW.subtotal IS DISTINCT FROM OLD.subtotal OR NEW.tax IS DISTINCT FROM OLD.tax OR NEW.total IS DISTINCT FROM OLD.total
               OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN RAISE EXCEPTION 'Issued invoice snapshot is immutable'; END IF;
        END IF;
        IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW; END $$""")
    op.execute(f'CREATE TRIGGER protrade_invoice_immutable BEFORE UPDATE OR DELETE ON mech_invoices FOR EACH ROW EXECUTE FUNCTION {q}.protrade_keep_invoice_snapshot()')
    op.execute(f"""CREATE FUNCTION {q}.protrade_guard_invoiced_quote() RETURNS trigger LANGUAGE plpgsql SET search_path={q},pg_catalog AS $$ BEGIN
        IF TG_TABLE_NAME='protrade_boq_quotes' THEN
            IF EXISTS(SELECT 1 FROM mech_invoices i WHERE i.boq_quote_id=OLD.id AND i.source_data IS NOT NULL)
               AND (NEW.items::jsonb IS DISTINCT FROM OLD.items::jsonb OR NEW.grand_total IS DISTINCT FROM OLD.grand_total
                    OR NEW.tax_snapshot::jsonb IS DISTINCT FROM OLD.tax_snapshot::jsonb) THEN
                RAISE EXCEPTION 'An invoiced quotation requires a new quotation for financial changes'; END IF;
        ELSIF TG_TABLE_NAME='mech_job_cards' THEN
            IF EXISTS(SELECT 1 FROM mech_invoices i WHERE i.job_card_id=OLD.id AND i.source_data IS NOT NULL)
               AND (NEW.vat_rate IS DISTINCT FROM OLD.vat_rate OR NEW.tax_snapshot::jsonb IS DISTINCT FROM OLD.tax_snapshot::jsonb) THEN
                RAISE EXCEPTION 'An invoiced quotation requires a new quotation for financial changes'; END IF;
        ELSE
            IF EXISTS(SELECT 1 FROM mech_invoices i WHERE i.source_data IS NOT NULL AND
                (i.job_card_id=CASE WHEN TG_OP='INSERT' THEN NEW.job_card_id ELSE OLD.job_card_id END
                 OR (TG_OP='UPDATE' AND i.job_card_id=NEW.job_card_id))) THEN
                RAISE EXCEPTION 'Invoiced quotation work items are immutable'; END IF;
        END IF;
        IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW; END $$""")
    for table in ('protrade_boq_quotes','mech_job_cards'):
        op.execute(f'CREATE TRIGGER protrade_invoiced_quote BEFORE UPDATE ON {table} FOR EACH ROW EXECUTE FUNCTION {q}.protrade_guard_invoiced_quote()')
    for table in ('mech_part_lines','mech_labor_lines'):
        op.execute(f'CREATE TRIGGER protrade_invoiced_lines BEFORE INSERT OR UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION {q}.protrade_guard_invoiced_quote()')
    # Existing numbering/ledger functions, receipts and financial rows are untouched.


def downgrade():
    raise RuntimeError('Preserve VAT approval and issued invoice history')
