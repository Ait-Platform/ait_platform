"""Manager-scoped municipal identities through existing property/meter ownership."""
from alembic import op
import sqlalchemy as sa

revision = 'pm_muni_manager_unique_001'
down_revision = 'pm_mar_payment_reference_001'
branch_labels = None
depends_on = None


def upgrade():
    context = op.get_context()
    if not getattr(context, 'config', None) or context.config.attributes.get('pm_muni_manager_unique_approved') is not True:
        raise RuntimeError('Municipal uniqueness migration requires an explicitly approved Alembic context')
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql':
        raise RuntimeError('PostgreSQL required')
    schema = bind.execute(sa.text('SELECT current_schema()')).scalar_one()
    quoted = bind.dialect.identifier_preparer.quote(schema)
    catalog = sa.inspect(bind)
    for table, columns in {
        'bil_property': {'id', 'manager_id'},
        'bil_meter': {'id', 'property_id'},
        'bil_muni_account': {'id', 'account_number', 'property_id', 'water_meter_id', 'elec_meter_id'},
    }.items():
        if not columns <= {column['name'] for column in catalog.get_columns(table, schema=schema)}:
            raise RuntimeError('Municipal ownership schema requires review')
    constraints = catalog.get_unique_constraints('bil_muni_account', schema=schema)
    if not any(item['name'] == 'bil_muni_account_account_number_key' and item['column_names'] == ['account_number'] for item in constraints):
        raise RuntimeError('Expected legacy account-number constraint is missing; review before migration')
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute(f'LOCK TABLE {quoted}.bil_property, {quoted}.bil_meter, {quoted}.bil_muni_account IN ACCESS EXCLUSIVE MODE')
    # Ownership is always derived; no manager column or ownership registry is added.
    op.execute(f"""
        CREATE FUNCTION {quoted}.pm_muni_manager_scope(integer, integer, integer)
        RETURNS integer LANGUAGE sql STABLE SET search_path TO {quoted}, pg_catalog AS $scope$
            SELECT CASE WHEN $1 IS NOT NULL THEN
                (SELECT p.manager_id FROM bil_property p WHERE p.id=$1)
            ELSE (
                SELECT CASE WHEN count(*)>0 AND count(p.manager_id)=count(*)
                                 AND count(DISTINCT p.manager_id)=1
                            THEN min(p.manager_id) END
                FROM bil_meter m LEFT JOIN bil_property p ON p.id=m.property_id
                WHERE m.id=$2 OR m.id=$3
            ) END
        $scope$
    """)
    conflicts = bind.execute(sa.text(f"""
        SELECT {quoted}.pm_muni_manager_scope(property_id,water_meter_id,elec_meter_id) AS manager,
               lower(btrim(account_number)) AS number, count(*) AS records
        FROM {quoted}.bil_muni_account
        WHERE NULLIF(btrim(account_number),'') IS NOT NULL
        GROUP BY 1,2 HAVING count(*)>1 LIMIT 1
    """)).first()
    if conflicts:
        raise RuntimeError('Conflicting municipal account numbers exist within an ownership scope; no records were changed')
    op.execute(f"""
        CREATE FUNCTION {quoted}.pm_muni_unique_lock()
        RETURNS void LANGUAGE plpgsql VOLATILE SET search_path TO {quoted}, pg_catalog AS $lock$
        BEGIN
            -- Join-based uniqueness needs a fresh snapshot after a contended lock.
            IF current_setting('transaction_isolation') NOT IN ('read committed','read uncommitted') THEN
                RAISE EXCEPTION 'Municipal identity changes require READ COMMITTED isolation'
                    USING ERRCODE='25001';
            END IF;
            PERFORM pg_advisory_xact_lock(7643901, 1);
        END $lock$
    """)
    op.execute(f"""
        CREATE FUNCTION {quoted}.pm_muni_check_account_unique()
        RETURNS trigger LANGUAGE plpgsql VOLATILE SET search_path TO {quoted}, pg_catalog AS $account$
        DECLARE scope_manager integer;
        BEGIN
            PERFORM {quoted}.pm_muni_unique_lock();
            IF NULLIF(btrim(NEW.account_number),'') IS NULL THEN RETURN NEW; END IF;
            scope_manager={quoted}.pm_muni_manager_scope(NEW.property_id,NEW.water_meter_id,NEW.elec_meter_id);
            IF EXISTS (
                SELECT 1 FROM bil_muni_account a
                WHERE a.id IS DISTINCT FROM NEW.id
                  AND lower(btrim(a.account_number))=lower(btrim(NEW.account_number))
                  AND {quoted}.pm_muni_manager_scope(a.property_id,a.water_meter_id,a.elec_meter_id)
                      IS NOT DISTINCT FROM scope_manager
            ) THEN
                RAISE EXCEPTION 'Municipal account number already exists within this ownership scope'
                    USING ERRCODE='23505', CONSTRAINT='uq_pm_muni_manager_account';
            END IF;
            RETURN NEW;
        END $account$
    """)
    op.execute(f"""
        CREATE FUNCTION {quoted}.pm_muni_check_scope_changes()
        RETURNS trigger LANGUAGE plpgsql VOLATILE SET search_path TO {quoted}, pg_catalog AS $changes$
        BEGIN
            PERFORM {quoted}.pm_muni_unique_lock();
            IF EXISTS (
                SELECT 1 FROM bil_muni_account a
                WHERE NULLIF(btrim(a.account_number),'') IS NOT NULL
                GROUP BY {quoted}.pm_muni_manager_scope(a.property_id,a.water_meter_id,a.elec_meter_id),
                         lower(btrim(a.account_number)) HAVING count(*)>1
            ) THEN
                RAISE EXCEPTION 'Ownership change would duplicate a municipal account number'
                    USING ERRCODE='23505', CONSTRAINT='uq_pm_muni_manager_account';
            END IF;
            RETURN NEW;
        END $changes$
    """)
    op.execute(f'CREATE INDEX ix_pm_muni_number_lookup ON {quoted}.bil_muni_account (lower(btrim(account_number)))')
    op.execute(f'''CREATE TRIGGER pm_muni_account_unique BEFORE INSERT OR UPDATE OF
        account_number,property_id,water_meter_id,elec_meter_id ON {quoted}.bil_muni_account
        FOR EACH ROW EXECUTE FUNCTION {quoted}.pm_muni_check_account_unique()''')
    op.execute(f'''CREATE TRIGGER pm_muni_property_scope_unique AFTER UPDATE OF manager_id
        ON {quoted}.bil_property FOR EACH ROW WHEN (OLD.manager_id IS DISTINCT FROM NEW.manager_id)
        EXECUTE FUNCTION {quoted}.pm_muni_check_scope_changes()''')
    op.execute(f'''CREATE TRIGGER pm_muni_meter_scope_unique AFTER UPDATE OF property_id
        ON {quoted}.bil_meter FOR EACH ROW WHEN (OLD.property_id IS DISTINCT FROM NEW.property_id)
        EXECUTE FUNCTION {quoted}.pm_muni_check_scope_changes()''')
    enabled = bind.execute(sa.text("""
        SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=:schema AND t.tgname IN
          ('pm_muni_account_unique','pm_muni_property_scope_unique','pm_muni_meter_scope_unique')
          AND NOT t.tgisinternal AND t.tgenabled='O'
    """), {'schema': schema}).scalar_one()
    if enabled != 3:
        raise RuntimeError('Replacement municipal uniqueness triggers were not verified')
    # Last operation, inside the same transaction and table locks as replacement installation.
    op.drop_constraint('bil_muni_account_account_number_key', 'bil_muni_account', type_='unique', schema=schema)


def downgrade():
    raise RuntimeError('Forward-only: preserve municipal identities and history; use a reviewed repair')
