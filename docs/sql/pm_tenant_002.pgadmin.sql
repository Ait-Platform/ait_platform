-- PREPARED ONLY: do not execute without administrator approval and verified backup.
-- Do NOT rerun pm_tenant_001. No psql backslash commands.
BEGIN;
SET LOCAL statement_timeout='60s';
SET LOCAL lock_timeout='5s';
SET LOCAL search_path=public;
DO $identity$
BEGIN
    IF current_database()<>'ait_platform_db' OR current_schema()<>'public' THEN
        RAISE EXCEPTION 'Expected production ait_platform_db, schema public';
    END IF;
END
$identity$;
-- BEGIN MIGRATION DDL
-- Existing tables only. Both must be empty before repurposing identities.
LOCK TABLE alembic_version IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE bil_tenant, bil_tenant_identity IN ACCESS EXCLUSIVE MODE;
DO $checks$
DECLARE cols text[];
BEGIN
    IF EXISTS (SELECT 1 FROM bil_tenant) OR EXISTS (SELECT 1 FROM bil_tenant_identity) THEN
        RAISE EXCEPTION 'Tenant tables are not empty. Stop for a data-preserving conversion review.';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM alembic_version WHERE version_num='pm_tenant_001') THEN
        RAISE EXCEPTION 'Installed pm_tenant_001 must be recorded; do not rerun it.';
    END IF;
    IF EXISTS (SELECT 1 FROM alembic_version WHERE version_num='pm_tenant_002') THEN
        RAISE EXCEPTION 'pm_tenant_002 is already recorded. Inspect before retrying.';
    END IF;
    SELECT array_agg(attname::text) INTO cols FROM pg_attribute
      WHERE attrelid='bil_tenant'::regclass AND attnum>0 AND NOT attisdropped;
    IF NOT ARRAY['id','name','email','phone']::text[] <@ cols THEN
        RAISE EXCEPTION 'Unexpected bil_tenant schema. Required existing fields are missing.';
    END IF;
    IF cols && ARRAY['manager_id','party_type','identity_number']::text[] THEN
        RAISE EXCEPTION 'Tenant identity column collision; inspect existing definitions.';
    END IF;
    SELECT array_agg(attname::text) INTO cols FROM pg_attribute
      WHERE attrelid='bil_tenant_identity'::regclass AND attnum>0 AND NOT attisdropped;
    IF NOT ARRAY['id','manager_id','party_type','legal_name','identity_number','address','email','phone','signatory_name','signatory_identity_number','signatory_capacity']::text[] <@ cols
       OR 'tenant_id'=ANY(cols) THEN
        RAISE EXCEPTION 'Unexpected representative table definition or tenant_id collision.';
    END IF;
    IF (SELECT count(*) FROM pg_constraint WHERE conrelid='bil_tenant_identity'::regclass
        AND conname IN ('ck_pm_tenant_type','ck_pm_tenant_required','ck_pm_tenant_representative') AND convalidated) <> 3 THEN
        RAISE EXCEPTION 'pm_tenant_001 constraints do not match the expected foundation.';
    END IF;
END
$checks$;

-- Production has no address column. Preserve a compatible existing local column.
DO $address$
DECLARE address_type text;
BEGIN
    SELECT format_type(atttypid,atttypmod) INTO address_type FROM pg_attribute
    WHERE attrelid='bil_tenant'::regclass AND attname='address' AND attnum>0 AND NOT attisdropped;
    IF address_type IS NULL THEN
        ALTER TABLE bil_tenant ADD COLUMN address VARCHAR(500);
    ELSIF address_type NOT IN ('character varying(500)','character varying','text') THEN
        RAISE EXCEPTION 'Existing address definition is incompatible; inspect the address column before retrying';
    END IF;
END
$address$;

ALTER TABLE bil_tenant
    ADD COLUMN manager_id INTEGER REFERENCES "user"(id) ON DELETE RESTRICT,
    ADD COLUMN party_type VARCHAR(32),
    ADD COLUMN identity_number VARCHAR(80),
    ADD CONSTRAINT ck_pm_tenant_register_required CHECK (
        manager_id IS NULL OR (
            party_type IS NOT NULL AND party_type IN ('natural_person','juristic_person')
            AND identity_number IS NOT NULL AND length(trim(identity_number))>0
            AND address IS NOT NULL AND length(trim(address))>0
        )
    );
CREATE UNIQUE INDEX uq_pm_tenant_register_identity ON bil_tenant
    (manager_id,party_type,lower(replace(identity_number,' ','')));

-- Both relationships are distinct and optional. Validate compatible existing
-- columns before preserving them; do not replace production foreign keys.
DO $relationships$
DECLARE column_name text; target_table regclass; column_number smallint;
BEGIN
    IF to_regclass('bil_property') IS NULL OR to_regclass('bil_sectional_unit') IS NULL THEN
        RAISE EXCEPTION 'Property and sectional-unit foundations are required';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid='bil_sectional_unit'::regclass
        AND attname='property_id' AND attnum>0 AND NOT attisdropped) THEN
        RAISE EXCEPTION 'Sectional-unit property association must be verified';
    END IF;
    FOREACH column_name IN ARRAY ARRAY['property_id','sectional_unit_id'] LOOP
        target_table := CASE WHEN column_name='property_id' THEN 'bil_property'::regclass ELSE 'bil_sectional_unit'::regclass END;
        SELECT attnum INTO column_number FROM pg_attribute WHERE attrelid='bil_tenant'::regclass
            AND attname=column_name AND attnum>0 AND NOT attisdropped;
        IF column_number IS NULL THEN
            IF column_name='property_id' THEN
                ALTER TABLE bil_tenant ADD COLUMN property_id INTEGER REFERENCES bil_property(id) ON DELETE RESTRICT;
            ELSE
                ALTER TABLE bil_tenant ADD COLUMN sectional_unit_id INTEGER REFERENCES bil_sectional_unit(id) ON DELETE RESTRICT;
            END IF;
        ELSE
            IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid='bil_tenant'::regclass
                AND attnum=column_number AND atttypid='integer'::regtype)
               OR NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='bil_tenant'::regclass
                AND contype='f' AND conkey=ARRAY[column_number] AND confrelid=target_table
                AND confkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=target_table AND attname='id')]::smallint[]
                AND convalidated) THEN
                RAISE EXCEPTION 'Existing tenant relationship type or foreign key requires review';
            END IF;
        END IF;
    END LOOP;
    ALTER TABLE bil_tenant ALTER COLUMN property_id DROP NOT NULL;
    ALTER TABLE bil_tenant ALTER COLUMN sectional_unit_id DROP NOT NULL;
    IF NOT EXISTS (SELECT 1 FROM pg_index WHERE indrelid='bil_tenant'::regclass AND indisvalid
        AND indkey[0]=(SELECT attnum FROM pg_attribute WHERE attrelid='bil_tenant'::regclass AND attname='property_id')
        AND indpred IS NULL) THEN
        CREATE INDEX ix_bil_tenant_property_id ON bil_tenant(property_id);
    END IF;
END
$relationships$;

-- A unit can be assigned only together with its actual property. Lock the unit
-- while validating so a concurrent reassignment cannot race this tenant save.
CREATE FUNCTION pm_tenant_validate_unit() RETURNS trigger LANGUAGE plpgsql AS $function$
DECLARE unit_property integer;
BEGIN
    IF NEW.sectional_unit_id IS NOT NULL THEN
        SELECT property_id INTO unit_property FROM bil_sectional_unit WHERE id=NEW.sectional_unit_id FOR SHARE;
        IF NOT FOUND OR NEW.property_id IS NULL OR unit_property IS NULL OR unit_property<>NEW.property_id THEN
            RAISE EXCEPTION 'The assigned unit must belong to the selected property' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END
$function$;
DO $trigger$
BEGIN
    EXECUTE 'CREATE TRIGGER pm_tenant_unit_property_check BEFORE INSERT OR UPDATE OF property_id,sectional_unit_id ON bil_tenant FOR EACH ROW EXECUTE FUNCTION '
        ||quote_ident(current_schema())||'.pm_tenant_validate_unit()';
END
$trigger$;

ALTER TABLE bil_tenant_identity
    DROP CONSTRAINT ck_pm_tenant_type,
    DROP CONSTRAINT ck_pm_tenant_required,
    DROP CONSTRAINT ck_pm_tenant_representative,
    ADD COLUMN tenant_id INTEGER NOT NULL,
    ADD CONSTRAINT fk_pm_representative_tenant FOREIGN KEY (tenant_id) REFERENCES bil_tenant(id) ON DELETE RESTRICT,
    ADD CONSTRAINT uq_pm_tenant_representative UNIQUE (tenant_id),
    ADD CONSTRAINT ck_pm_representative_required CHECK (
        length(trim(legal_name))>0 AND length(trim(identity_number))>0
        AND length(trim(email))>0 AND length(trim(phone))>0
    ),
    ALTER COLUMN party_type DROP NOT NULL,
    ALTER COLUMN address DROP NOT NULL,
    ALTER COLUMN signatory_name DROP NOT NULL,
    ALTER COLUMN signatory_identity_number DROP NOT NULL,
    ALTER COLUMN signatory_capacity DROP NOT NULL;
DROP INDEX uq_pm_tenant_identity;
-- Legacy columns remain for compatibility; the application no longer stores tenants here.
-- END MIGRATION DDL
-- Advance only this branch head after the exact DDL succeeds.
-- Other programme/branch revision rows are unchanged; the parent remains in the graph.
UPDATE alembic_version SET version_num='pm_tenant_002' WHERE version_num='pm_tenant_001';
COMMIT;
