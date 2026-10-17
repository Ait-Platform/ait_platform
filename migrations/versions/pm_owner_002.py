"""Add owner particulars, shares and property type without business-record backfill."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "pm_owner_002"
down_revision = "pm_owner_001"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        raise RuntimeError("PM ownership migration requires PostgreSQL")
    offline = op.get_context().as_sql
    schema_name = "public" if offline else bind.exec_driver_sql("SELECT current_schema()").scalar_one()
    inspector = None if offline else sa.inspect(bind)
    existing_property_type = False
    if inspector:
        required = {
            "bil_property": {"id", "manager_id", "name", "address"},
            "bil_owner_identity": {"id", "created_by", "identity_type", "display_name", "email", "phone", "linked_user_id"},
            "bil_property_ownership": {"id", "property_id", "owner_id", "created_by", "owner_name_snapshot", "owner_type_snapshot", "supersedes_id"},
        }
        for table, fields in required.items():
            if not inspector.has_table(table, schema=schema_name) or not fields <= {c["name"] for c in inspector.get_columns(table, schema=schema_name)}:
                raise RuntimeError("Required ownership foundation is missing; no DDL applied")
        for table, fields in {
            "bil_owner_identity": {"identity_number", "residential_address", "postal_address"},
            "bil_property_ownership": {"ownership_percentage", "owner_identity_snapshot"},
        }.items():
            if fields & {c["name"] for c in inspector.get_columns(table, schema=schema_name)}:
                raise RuntimeError("Stage 2 columns already exist; inspect migration state before retrying")
        column = next((c for c in inspector.get_columns("bil_property", schema=schema_name) if c["name"] == "property_type"), None)
        if column:
            if not isinstance(column["type"], (sa.String, sa.Text)) or (getattr(column["type"], "length", None) is not None and column["type"].length < 50):
                raise RuntimeError("Existing property_type is incompatible; no DDL applied")
            existing_property_type = True
    # Nullable, no backfill: preserve pre-existing records and ownership snapshots.
    op.add_column("bil_owner_identity", sa.Column("identity_number", sa.String(80)))
    op.add_column("bil_owner_identity", sa.Column("residential_address", sa.Text()))
    op.add_column("bil_owner_identity", sa.Column("postal_address", sa.Text()))
    op.create_check_constraint("ck_pm_owner_identifier", "bil_owner_identity", "identity_number IS NULL OR length(trim(identity_number)) > 0")
    op.create_index("uq_pm_owner_creator_identifier", "bil_owner_identity",
        ["created_by", "identity_type", sa.text("lower(btrim(identity_number))")],
        unique=True, postgresql_where=sa.text("identity_number IS NOT NULL"))
    op.add_column("bil_property_ownership", sa.Column("ownership_percentage", sa.Numeric(7, 4)))
    op.add_column("bil_property_ownership", sa.Column("owner_identity_snapshot", JSONB()))
    op.create_check_constraint("ck_pm_ownership_percentage", "bil_property_ownership", "ownership_percentage IS NULL OR (ownership_percentage >= 0 AND ownership_percentage <= 100)")
    if not existing_property_type:
        op.add_column("bil_property", sa.Column("property_type", sa.String(50)))
    schema = bind.dialect.identifier_preparer.quote_schema(schema_name)
    # Preserve the existing triggers, locks and authorization, enrich new snapshots.
    op.execute(f"""CREATE OR REPLACE FUNCTION {schema}.pm_owner_validate_links() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE property_manager integer; owner_creator integer;
    BEGIN
      SELECT manager_id INTO property_manager FROM {schema}.bil_property
        WHERE id=NEW.property_id FOR UPDATE;
      IF TG_TABLE_NAME = 'bil_property_registration' THEN
        IF property_manager IS DISTINCT FROM NEW.manager_id THEN
          RAISE EXCEPTION 'Registration manager must match property manager';
        END IF;
      ELSE
        IF property_manager IS DISTINCT FROM NEW.created_by THEN
          RAISE EXCEPTION 'Ownership creator must be current property manager';
        END IF;
        SELECT created_by, display_name, identity_type,
          jsonb_build_object('name', display_name, 'type', identity_type,
            'identity_number', identity_number, 'email', email, 'phone', phone,
            'residential_address', residential_address, 'postal_address', postal_address)
          INTO owner_creator, NEW.owner_name_snapshot, NEW.owner_type_snapshot, NEW.owner_identity_snapshot
          FROM {schema}.bil_owner_identity WHERE id=NEW.owner_id FOR SHARE;
        IF NOT FOUND THEN RAISE EXCEPTION 'Owner identity does not exist'; END IF;
        IF owner_creator IS DISTINCT FROM NEW.created_by AND NOT EXISTS (
          SELECT 1 FROM {schema}.bil_property_ownership a JOIN {schema}.bil_property p ON p.id=a.property_id
          WHERE a.owner_id=NEW.owner_id AND p.manager_id=NEW.created_by) THEN
          RAISE EXCEPTION 'Owner record is not accessible to this manager';
        END IF;
        IF NEW.supersedes_id IS NOT NULL AND NOT EXISTS (
          SELECT 1 FROM {schema}.bil_property_ownership WHERE id=NEW.supersedes_id
          AND property_id=NEW.property_id AND owner_id=NEW.owner_id) THEN
          RAISE EXCEPTION 'Supersession must retain property and owner';
        END IF;
      END IF;
      RETURN NEW;
    END; $$""")


def downgrade():
    raise RuntimeError("Destructive ownership downgrade disabled; retain records and roll back application code")
