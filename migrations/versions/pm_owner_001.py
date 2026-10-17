"""Explicit, additive owner-first PM foundation; no backfill or app startup.

Revision ID: pm_owner_001
Independent branch: apply only after reviewing the current database revision set.
"""
from alembic import op
import sqlalchemy as sa

revision = "pm_owner_001"
down_revision = None
branch_labels = ("pm_owner",)
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        raise RuntimeError("PM ownership migration requires PostgreSQL")
    if not op.get_context().as_sql:
        inspector = sa.inspect(bind)
        for table, columns in {"user": {"id"}, "bil_property": {"id", "manager_id"}}.items():
            if not inspector.has_table(table) or not columns <= {c["name"] for c in inspector.get_columns(table)}:
                raise RuntimeError("Required existing PM/user schema is missing; no DDL applied")
        for table in ("bil_owner_identity", "bil_property_ownership", "bil_ownership_verification", "bil_property_registration"):
            if inspector.has_table(table):
                raise RuntimeError("PM ownership table already exists; inspect before retrying")
    schema = "public" if op.get_context().as_sql else bind.exec_driver_sql("SELECT current_schema()").scalar_one()
    schema = bind.dialect.identifier_preparer.quote_schema(schema)
    op.create_table("bil_owner_identity",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("identity_type", sa.String(16), nullable=False),
        sa.Column("display_name", sa.String(200), nullable=False),
        sa.Column("email", sa.String(255)), sa.Column("phone", sa.String(50)),
        sa.Column("linked_user_id", sa.Integer(), sa.ForeignKey("user.id", ondelete="RESTRICT")),
        sa.Column("created_by", sa.Integer(), sa.ForeignKey("user.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("identity_type IN ('individual', 'company', 'trust')", name="ck_pm_owner_type"),
        sa.CheckConstraint("length(trim(display_name)) > 0", name="ck_pm_owner_name"))
    op.create_index("ix_pm_owner_creator", "bil_owner_identity", ["created_by"])
    op.create_table("bil_property_ownership",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("bil_property.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("owner_id", sa.Integer(), sa.ForeignKey("bil_owner_identity.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("owner_name_snapshot", sa.String(200), nullable=False),
        sa.Column("owner_type_snapshot", sa.String(16), nullable=False),
        sa.Column("effective_from", sa.Date()), sa.Column("effective_to", sa.Date()),
        sa.Column("supersedes_id", sa.Integer(), sa.ForeignKey("bil_property_ownership.id", ondelete="RESTRICT"), unique=True),
        sa.Column("created_by", sa.Integer(), sa.ForeignKey("user.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("effective_to IS NULL OR effective_from IS NULL OR effective_to >= effective_from", name="ck_pm_ownership_dates"))
    op.create_index("ix_pm_ownership_property", "bil_property_ownership", ["property_id"])
    op.create_index("ix_pm_ownership_owner", "bil_property_ownership", ["owner_id"])
    op.create_table("bil_ownership_verification",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("ownership_id", sa.Integer(), sa.ForeignKey("bil_property_ownership.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("decision", sa.String(16), nullable=False),
        sa.Column("evidence_reference", sa.Text(), nullable=False),
        sa.Column("recorded_by", sa.Integer(), sa.ForeignKey("user.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("decision IN ('verified', 'rejected', 'revoked')", name="ck_pm_verification_decision"),
        sa.CheckConstraint("length(trim(evidence_reference)) > 0", name="ck_pm_verification_evidence"))
    op.create_index("ix_pm_verification_ownership", "bil_ownership_verification", ["ownership_id", "id"])
    op.create_table("bil_property_registration",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("manager_id", sa.Integer(), sa.ForeignKey("user.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("token", sa.Uuid(), nullable=False),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("bil_property.id", ondelete="RESTRICT"), nullable=False, unique=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("manager_id", "token", name="uq_pm_registration_manager_token"))
    op.execute(f"""CREATE FUNCTION {schema}.pm_owner_preserve_history() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN RAISE EXCEPTION 'PM ownership history is append-only'; END; $$""")
    for table in ("bil_property_ownership", "bil_ownership_verification", "bil_property_registration"):
        op.execute(f"CREATE TRIGGER {table}_immutable BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION {schema}.pm_owner_preserve_history()")
        op.execute(f"CREATE TRIGGER {table}_no_truncate BEFORE TRUNCATE ON {table} FOR EACH STATEMENT EXECUTE FUNCTION {schema}.pm_owner_preserve_history()")
    op.execute(f"""CREATE FUNCTION {schema}.pm_owner_validate_links() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE property_manager integer;
    BEGIN
      -- FOR UPDATE conflicts with manager reassignment, not just key changes.
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
        -- Capture authoritative identity values; ignore supplied snapshots.
        SELECT display_name, identity_type INTO NEW.owner_name_snapshot, NEW.owner_type_snapshot
          FROM {schema}.bil_owner_identity WHERE id=NEW.owner_id FOR SHARE;
        IF NOT FOUND THEN RAISE EXCEPTION 'Owner identity does not exist'; END IF;
        IF NEW.supersedes_id IS NOT NULL AND NOT EXISTS (
          SELECT 1 FROM {schema}.bil_property_ownership WHERE id=NEW.supersedes_id
          AND property_id=NEW.property_id AND owner_id=NEW.owner_id) THEN
          RAISE EXCEPTION 'Supersession must retain property and owner';
        END IF;
      END IF;
      RETURN NEW;
    END; $$""")
    op.execute(f"CREATE TRIGGER pm_registration_validate BEFORE INSERT ON bil_property_registration FOR EACH ROW EXECUTE FUNCTION {schema}.pm_owner_validate_links()")
    op.execute(f"CREATE TRIGGER pm_ownership_validate BEFORE INSERT ON bil_property_ownership FOR EACH ROW EXECUTE FUNCTION {schema}.pm_owner_validate_links()")


def downgrade():
    raise RuntimeError("Automatic destructive downgrade disabled; roll back application code and retain ownership history")
