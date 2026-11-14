"""Unique mechanic quotation charges in the existing debtor ledger."""
from alembic import op
import sqlalchemy as sa
revision='protrade_mechanic_quotes_012'
down_revision='protrade_debtor_identity_011'
branch_labels=None
depends_on=None


def upgrade():
    bind=op.get_bind()
    if bind.dialect.name!='postgresql' or op.get_context().as_sql:
        raise RuntimeError('Run mechanic quotation migration online on verified PostgreSQL')
    if not all(bind.execute(sa.text('SELECT current_database(),current_user')).one()):
        raise RuntimeError('Cannot verify database identity')
    op.execute('LOCK TABLE mech_job_cards,mech_part_lines,mech_labor_lines,debtor,debtor_ledger IN SHARE ROW EXCLUSIVE MODE')
    op.add_column('debtor_ledger',sa.Column('mech_job_card_id',sa.Integer()))
    op.create_foreign_key('fk_debtor_ledger_mech_quote','debtor_ledger','mech_job_cards',['mech_job_card_id'],['id'])
    op.create_unique_constraint('uq_debtor_ledger_mech_quote','debtor_ledger',['mech_job_card_id'])
    op.create_check_constraint('ck_debtor_mech_quote_debit','debtor_ledger',"mech_job_card_id IS NULL OR (boq_quote_id IS NULL AND kind='debit' AND payment_key IS NULL AND amount>=0)")
    # Link only exact client IDs with one known mechanic shop. Never infer by name,
    # move a numbered job between shops, or merge debtor histories.
    op.execute("""CREATE TEMP TABLE mechanic_quote_accounts ON COMMIT DROP AS
        SELECT c.id AS client_id,c.user_id,min(j.shop_id) AS shop_id
        FROM mech_clients c JOIN mech_vehicles v ON v.client_id=c.id JOIN mech_job_cards j ON j.vehicle_id=v.id
        JOIN mech_shops s ON s.id=j.shop_id AND s.user_id=c.user_id AND COALESCE(s.trade,'mechanic')='mechanic'
        WHERE c.shop_id IS NULL OR c.shop_id=j.shop_id
        GROUP BY c.id,c.user_id HAVING count(DISTINCT j.shop_id)=1
        AND NOT EXISTS(SELECT 1 FROM mech_vehicles v2 JOIN mech_job_cards j2 ON j2.vehicle_id=v2.id WHERE v2.client_id=c.id AND (j2.shop_id IS NULL OR j2.shop_id<>min(j.shop_id)))""")
    # Older invoice actions sometimes saved no client reference. An exact job
    # charge can prove the link, but only if every debit identifies this same
    # client and shop and no competing account exists. Names are never matched.
    op.execute("""UPDATE debtor d SET reference_id=c.id,shop_id=j.shop_id
        FROM mech_job_cards j JOIN mech_vehicles v ON v.id=j.vehicle_id JOIN mech_clients c ON c.id=v.client_id
        JOIN mech_shops s ON s.id=j.shop_id AND s.user_id=c.user_id AND COALESCE(s.trade,'mechanic')='mechanic'
        JOIN debtor_ledger charged ON charged.ref='JOB-'||j.job_number AND charged.kind='debit'
        WHERE d.id=charged.debtor_id AND d.user_id=c.user_id AND d.reference_id IS NULL AND d.identity_debtor_id IS NULL
        AND d.slug_reference IN('mechanic','protrade') AND (d.shop_id IS NULL OR d.shop_id=j.shop_id)
        AND NOT EXISTS(SELECT 1 FROM debtor linked WHERE linked.identity_debtor_id=d.id)
        AND NOT EXISTS(SELECT 1 FROM debtor other WHERE other.id<>d.id AND other.user_id=d.user_id AND other.reference_id=c.id AND other.slug_reference IN('mechanic','protrade') AND (other.shop_id IS NULL OR other.shop_id=j.shop_id))
        AND NOT EXISTS(SELECT 1 FROM debtor_ledger old WHERE old.debtor_id=d.id AND old.kind='debit' AND NOT EXISTS(
            SELECT 1 FROM mech_job_cards source JOIN mech_vehicles car ON car.id=source.vehicle_id
            WHERE old.ref='JOB-'||source.job_number AND source.shop_id=j.shop_id AND car.client_id=c.id))""")
    op.execute("""UPDATE debtor d SET shop_id=a.shop_id FROM mechanic_quote_accounts a
        WHERE d.user_id=a.user_id AND d.reference_id=a.client_id AND d.slug_reference IN('mechanic','protrade') AND d.shop_id IS NULL
        AND NOT EXISTS(SELECT 1 FROM debtor linked WHERE linked.identity_debtor_id=d.id)
        AND 1=(SELECT count(*) FROM debtor d2 WHERE d2.user_id=a.user_id AND d2.reference_id=a.client_id AND d2.slug_reference IN('mechanic','protrade'))""")
    op.execute("""INSERT INTO debtor(user_id,shop_id,reference_id,slug_reference,name,phone,email,address,opening_balance,apply_interest,is_active,created_at)
        SELECT a.user_id,a.shop_id,c.id,'mechanic',c.name,c.phone,c.email,c.address,0,true,true,now() AT TIME ZONE 'UTC'
        FROM mechanic_quote_accounts a JOIN mech_clients c ON c.id=a.client_id
        WHERE NOT EXISTS(SELECT 1 FROM debtor d WHERE d.user_id=a.user_id AND d.reference_id=c.id AND d.slug_reference IN('mechanic','protrade'))
        AND NOT EXISTS(SELECT 1 FROM mech_vehicles v JOIN mech_job_cards j ON j.vehicle_id=v.id JOIN debtor_ledger l ON l.ref='JOB-'||j.job_number AND l.kind='debit' JOIN debtor d ON d.id=l.debtor_id WHERE v.client_id=c.id AND d.user_id=a.user_id)""")
    op.execute("""UPDATE mech_clients c SET shop_id=a.shop_id FROM mechanic_quote_accounts a WHERE c.id=a.client_id AND c.shop_id IS NULL""")
    op.execute("""UPDATE mech_job_cards j SET debtor_id=d.id FROM mech_vehicles v,mech_clients c,debtor d,mech_shops s
        WHERE j.vehicle_id=v.id AND v.client_id=c.id AND j.shop_id=s.id AND s.user_id=c.user_id AND COALESCE(s.trade,'mechanic')='mechanic'
        AND j.debtor_id IS NULL AND d.user_id=c.user_id AND d.shop_id=j.shop_id AND d.reference_id=c.id AND d.slug_reference IN('mechanic','protrade')
        AND 1=(SELECT count(*) FROM debtor d2 WHERE d2.user_id=c.user_id AND d2.shop_id=j.shop_id AND d2.reference_id=c.id AND d2.slug_reference IN('mechanic','protrade'))""")
    schema=bind.execute(sa.text('SELECT current_schema()')).scalar_one();q=bind.dialect.identifier_preparer.quote_schema(schema)
    op.execute(f"""CREATE FUNCTION {q}.protrade_mechanic_total(identifier integer) RETURNS numeric LANGUAGE sql STABLE
        SET search_path={q},pg_catalog AS $$ SELECT round((
        COALESCE((SELECT sum(COALESCE(quantity,0)::numeric*COALESCE(markup_price,0)::numeric) FROM mech_part_lines WHERE job_card_id=j.id),0)
        +COALESCE((SELECT sum(COALESCE(hours,0)::numeric*COALESCE(rate_per_hour,0)::numeric) FROM mech_labor_lines WHERE job_card_id=j.id),0))
        *(1+COALESCE(j.vat_rate,0)::numeric/100)*100) FROM mech_job_cards j WHERE j.id=identifier $$""")
    op.execute(f"""CREATE FUNCTION {q}.protrade_sync_mechanic_debit(identifier integer) RETURNS void LANGUAGE plpgsql
        SET search_path={q},pg_catalog AS $$ DECLARE job record; BEGIN
        SELECT j.*,d.user_id INTO job FROM mech_job_cards j JOIN debtor d ON d.id=j.debtor_id AND d.shop_id=j.shop_id
        JOIN mech_shops s ON s.id=j.shop_id AND s.user_id=d.user_id AND COALESCE(s.trade,'mechanic')='mechanic'
        JOIN mech_vehicles v ON v.id=j.vehicle_id JOIN mech_clients c ON c.id=v.client_id AND c.id=d.reference_id AND c.user_id=d.user_id
        WHERE j.id=identifier FOR UPDATE OF j;
        IF NOT FOUND THEN
            IF EXISTS(SELECT 1 FROM debtor_ledger WHERE mech_job_card_id=identifier) THEN RAISE EXCEPTION 'Invalid mechanic quotation account'; END IF;
            RETURN;
        END IF;
        INSERT INTO debtor_ledger(debtor_id,txn_date,description,kind,amount,ref,created_at,mech_job_card_id)
        VALUES(job.debtor_id,((COALESCE(job.created_at,now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') AT TIME ZONE 'Africa/Johannesburg')::date,
        'Mechanic quotation '||COALESCE('Q-'||lpad(job.quotation_seq::text,greatest(4,length(job.quotation_seq::text)),'0'),job.job_number),
        'debit',{q}.protrade_mechanic_total(identifier),'JOB-'||job.job_number,COALESCE(job.created_at,now() AT TIME ZONE 'UTC'),identifier)
        ON CONFLICT(mech_job_card_id) DO UPDATE SET amount=excluded.amount;
        END $$""")
    op.execute("""DO $$ BEGIN IF EXISTS(SELECT 1 FROM mech_job_cards j JOIN mech_vehicles v ON v.id=j.vehicle_id
        JOIN mech_clients c ON c.id=v.client_id JOIN debtor_ledger l ON l.kind='debit' AND l.ref='JOB-'||j.job_number
        JOIN debtor d ON d.id=l.debtor_id AND d.user_id=c.user_id
        WHERE j.debtor_id IS NOT NULL AND d.id<>j.debtor_id) THEN
        RAISE EXCEPTION 'Legacy mechanic charge belongs to another debtor account; reconcile before migrating'; END IF; END $$""")
    # Adopt exactly one correctly valued legacy charge. Ambiguities abort the
    # transaction rather than silently deleting or double charging history.
    op.execute(f"""DO $$ BEGIN IF EXISTS(
        SELECT j.id FROM mech_job_cards j JOIN debtor d ON d.id=j.debtor_id AND d.shop_id=j.shop_id
        JOIN mech_shops s ON s.id=j.shop_id AND s.user_id=d.user_id AND COALESCE(s.trade,'mechanic')='mechanic'
        JOIN debtor_ledger l ON l.debtor_id=d.id AND l.kind='debit' AND l.ref='JOB-'||j.job_number
        GROUP BY j.id HAVING count(*)>1 OR min(l.amount)<>{q}.protrade_mechanic_total(j.id)) THEN
        RAISE EXCEPTION 'Ambiguous or mismatched legacy mechanic charges; reconcile before migrating'; END IF; END $$""")
    op.execute(f"""UPDATE debtor_ledger l SET mech_job_card_id=j.id FROM mech_job_cards j,debtor d,mech_shops s
        WHERE d.id=j.debtor_id AND d.shop_id=j.shop_id AND s.id=j.shop_id AND s.user_id=d.user_id AND COALESCE(s.trade,'mechanic')='mechanic'
        AND l.debtor_id=d.id AND l.kind='debit' AND l.ref='JOB-'||j.job_number AND l.boq_quote_id IS NULL
        AND l.payment_key IS NULL AND l.amount={q}.protrade_mechanic_total(j.id)""")
    op.execute(f"SELECT {q}.protrade_sync_mechanic_debit(id) FROM mech_job_cards WHERE debtor_id IS NOT NULL AND shop_id IS NOT NULL")
    op.execute(f"""CREATE FUNCTION {q}.protrade_mechanic_job_changed() RETURNS trigger LANGUAGE plpgsql
        SET search_path={q},pg_catalog AS $$ BEGIN
        IF TG_OP='UPDATE' AND EXISTS(SELECT 1 FROM debtor_ledger WHERE mech_job_card_id=OLD.id) AND
           (NEW.debtor_id IS DISTINCT FROM OLD.debtor_id OR NEW.shop_id IS DISTINCT FROM OLD.shop_id OR NEW.vehicle_id IS DISTINCT FROM OLD.vehicle_id OR NEW.job_number IS DISTINCT FROM OLD.job_number) THEN
            RAISE EXCEPTION 'Mechanic quotation account is immutable'; END IF;
        PERFORM {q}.protrade_sync_mechanic_debit(NEW.id); RETURN NEW; END $$""")
    op.execute(f"""CREATE FUNCTION {q}.protrade_mechanic_line_changed() RETURNS trigger LANGUAGE plpgsql
        SET search_path={q},pg_catalog AS $$ BEGIN
        IF TG_OP='UPDATE' AND NEW.job_card_id<>OLD.job_card_id THEN RAISE EXCEPTION 'Cannot move a quotation item'; END IF;
        PERFORM {q}.protrade_sync_mechanic_debit(CASE WHEN TG_OP='DELETE' THEN OLD.job_card_id ELSE NEW.job_card_id END);
        RETURN NULL; END $$""")
    op.execute(f"""CREATE FUNCTION {q}.protrade_guard_mechanic_debit() RETURNS trigger LANGUAGE plpgsql
        SET search_path={q},pg_catalog AS $$ DECLARE identifier integer; BEGIN
        IF TG_OP='DELETE' THEN
            IF OLD.mech_job_card_id IS NOT NULL THEN RAISE EXCEPTION 'Mechanic quotation history cannot be deleted'; END IF; RETURN OLD;
        END IF;
        IF TG_OP='UPDATE' AND OLD.mech_job_card_id IS NOT NULL AND (NEW.mech_job_card_id IS DISTINCT FROM OLD.mech_job_card_id OR NEW.debtor_id IS DISTINCT FROM OLD.debtor_id) THEN
            RAISE EXCEPTION 'Mechanic quotation ledger identity is immutable'; END IF;
        IF NEW.mech_job_card_id IS NULL AND NEW.kind='debit' THEN
            SELECT j.id INTO identifier FROM mech_job_cards j WHERE j.debtor_id=NEW.debtor_id AND NEW.ref='JOB-'||j.job_number;
            IF FOUND THEN
                IF EXISTS(SELECT 1 FROM debtor_ledger l WHERE l.mech_job_card_id=identifier AND l.id IS DISTINCT FROM NEW.id) THEN
                    RAISE EXCEPTION 'Mechanic quotation already charged'; END IF;
                NEW.mech_job_card_id=identifier;
            END IF;
        END IF;
        IF NEW.mech_job_card_id IS NOT NULL AND NOT EXISTS(
            SELECT 1 FROM mech_job_cards j JOIN debtor d ON d.id=j.debtor_id AND d.shop_id=j.shop_id
            JOIN mech_shops s ON s.id=j.shop_id AND s.user_id=d.user_id AND COALESCE(s.trade,'mechanic')='mechanic'
            JOIN mech_vehicles v ON v.id=j.vehicle_id JOIN mech_clients c ON c.id=v.client_id AND c.id=d.reference_id AND c.user_id=d.user_id
            WHERE j.id=NEW.mech_job_card_id AND d.id=NEW.debtor_id AND NEW.kind='debit' AND NEW.ref='JOB-'||j.job_number AND NEW.amount={q}.protrade_mechanic_total(j.id)) THEN
            RAISE EXCEPTION 'Invalid mechanic quotation debit'; END IF; RETURN NEW; END $$""")
    op.execute(f'CREATE TRIGGER protrade_mechanic_job_debit AFTER INSERT OR UPDATE ON mech_job_cards FOR EACH ROW EXECUTE FUNCTION {q}.protrade_mechanic_job_changed()')
    for table in ('mech_part_lines','mech_labor_lines'):
        op.execute(f'CREATE TRIGGER protrade_mechanic_line_debit AFTER INSERT OR UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION {q}.protrade_mechanic_line_changed()')
    op.execute(f'CREATE TRIGGER protrade_mechanic_debit_guard BEFORE INSERT OR UPDATE OR DELETE ON debtor_ledger FOR EACH ROW EXECUTE FUNCTION {q}.protrade_guard_mechanic_debit()')


def downgrade():
    raise RuntimeError('Preserve mechanic quotation links and accounting history')
