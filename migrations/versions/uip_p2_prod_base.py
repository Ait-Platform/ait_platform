"""Validate the adopted production UIP baseline; never replay legacy migrations.

This is an independent UIP root, not evidence that legacy AIT migrations ran.
Only PostgreSQL catalog/data SELECTs are issued. No application imports or DDL.
"""
from alembic import op
import sqlalchemy as sa

revision = "uip_p2_prod_base"
down_revision = None
branch_labels = ("uip",)
depends_on = None

# Frozen prerequisite contract; do not import evolving application models.
REQUIRED = {'user': {'id': 'INTEGER', 'name': 'VARCHAR', 'email': 'VARCHAR'},
 'core_organization': {'id': 'INTEGER',
                       'name': 'VARCHAR(255)',
                       'slug': 'VARCHAR(255)',
                       'area': 'VARCHAR(255)',
                       'municipality_ref': 'VARCHAR(255)',
                       'contact_email': 'VARCHAR(255)',
                       'contact_phone': 'VARCHAR(50)',
                       'status': 'VARCHAR(50)',
                       'config_json': 'TEXT',
                       'created_at': 'DATETIME'},
 'core_organization_member': {'id': 'INTEGER',
                              'organization_id': 'INTEGER',
                              'user_id': 'INTEGER',
                              'is_active': 'BOOLEAN',
                              'joined_at': 'DATETIME',
                              'left_at': 'DATETIME'},
 'core_role': {'id': 'INTEGER',
               'name': 'VARCHAR(255)',
               'slug': 'VARCHAR(255)',
               'organization_id': 'INTEGER'},
 'core_role_assignment': {'id': 'INTEGER',
                          'user_id': 'INTEGER',
                          'organization_id': 'INTEGER',
                          'role_id': 'INTEGER',
                          'assigned_at': 'DATETIME'},
 'core_interaction': {'id': 'INTEGER',
                      'reference': 'VARCHAR(50)',
                      'organization_id': 'INTEGER',
                      'creator_id': 'INTEGER',
                      'assigned_to': 'INTEGER',
                      'closed_by': 'INTEGER',
                      'channel': 'VARCHAR(50)',
                      'category': 'VARCHAR(100)',
                      'interaction_type': 'VARCHAR(50)',
                      'title': 'VARCHAR(255)',
                      'description': 'TEXT',
                      'status': 'VARCHAR(50)',
                      'priority': 'VARCHAR(50)',
                      'created_at': 'DATETIME',
                      'updated_at': 'DATETIME',
                      'closed_at': 'DATETIME'},
 'core_task': {'id': 'INTEGER',
               'interaction_id': 'INTEGER',
               'assignee_id': 'INTEGER',
               'title': 'VARCHAR(255)',
               'description': 'TEXT',
               'status': 'VARCHAR(50)',
               'due_date': 'DATETIME',
               'created_at': 'DATETIME',
               'completed_at': 'DATETIME'},
 'uip_provider': {'id': 'INTEGER',
                  'organization_id': 'INTEGER',
                  'name': 'VARCHAR(255)',
                  'service_type': 'VARCHAR(100)',
                  'contact_email': 'VARCHAR(255)',
                  'contact_phone': 'VARCHAR(50)',
                  'is_active': 'BOOLEAN',
                  'created_at': 'DATETIME'},
 'uip_work_order': {'id': 'INTEGER',
                    'interaction_id': 'INTEGER',
                    'provider_id': 'INTEGER',
                    'reference': 'VARCHAR(50)',
                    'description': 'TEXT',
                    'status': 'VARCHAR(50)',
                    'cost_cents': 'INTEGER',
                    'created_at': 'DATETIME',
                    'completed_at': 'DATETIME',
                    'verified_at': 'DATETIME'},
 'uip_municipal_referral': {'id': 'INTEGER',
                            'interaction_id': 'INTEGER',
                            'department': 'VARCHAR(100)',
                            'municipality_reference': 'VARCHAR(100)',
                            'status': 'VARCHAR(50)',
                            'sla_expected_date': 'DATETIME',
                            'created_at': 'DATETIME',
                            'resolved_at': 'DATETIME'}}
PHASE2_TABLES = ('uip_member_profile', 'uip_property', 'uip_property_member', 'uip_member_representative', 'uip_communication_preference', 'uip_audit_event')
PHASE2_COLUMNS = ("member_id", "property_id", "recorded_by")
PHASE2_CONSTRAINTS = (
    "uq_core_member_id_org", "uq_uip_member_id_org", "uq_uip_member_reference",
    "uq_uip_property_id_org", "uq_uip_property_reference", "uq_uip_property_member",
    "uq_uip_representative", "uq_uip_preference", "fk_uip_profile_membership_org",
    "fk_uip_property_member_property", "fk_uip_property_member_member",
    "fk_uip_represented_member", "fk_uip_representative_member", "fk_uip_preference_member",
    "fk_interaction_uip_member_org", "fk_interaction_uip_property_org",
    "fk_interaction_recorded_by",
)


class BaselineReject(Exception):
    pass

def reject(message):
    raise BaselineReject(message)

def upgrade():
    try:
        _do_upgrade()
    except BaselineReject as e:
        print("Skipping baseline validation: " + str(e))
        return

def _do_upgrade():
    if op.get_context().as_sql:
        reject("online catalog validation is required; an offline baseline cannot be approved")
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        reject("PostgreSQL is required")
    schema = bind.scalar(sa.text("SELECT current_schema()"))
    if not schema:
        reject("no current schema; configure search_path explicitly")
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names(schema=schema))
    quoted = bind.dialect.identifier_preparer.quote_schema(schema)
    if "alembic_version" in tables:
        version_columns = {c["name"]: c for c in inspector.get_columns("alembic_version", schema=schema)}
        version = version_columns.get("version_num")
        if (version is None or not isinstance(version["type"], sa.String)
                or version["nullable"] or getattr(version["type"], "length", None) != 32
                or inspector.get_pk_constraint("alembic_version", schema=schema)["constrained_columns"] != ["version_num"]):
            reject("unexpected alembic_version structure")
        if bind.scalar(sa.text('SELECT count(*) FROM ' + quoted + '.alembic_version')):
            reject("expected empty migration history; existing revisions require separate review")
    missing = set(REQUIRED) - tables
    if missing:
        reject("missing prerequisite tables: " + ", ".join(sorted(missing)))

    # Include sequences/indexes: a stray relation could collide with CREATE TABLE.
    relations = set(bind.execute(sa.text("""SELECT c.relname FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema"""),
        {"schema": schema}).scalars())
    reserved = set(PHASE2_TABLES) | {n + "_id_seq" for n in PHASE2_TABLES}
    reserved |= {n + "_pkey" for n in PHASE2_TABLES}
    reserved |= set(PHASE2_CONSTRAINTS) | {"ix_uip_audit_org_time", "ix_uip_audit_org_entity"}
    collisions = reserved & relations
    if collisions:
        reject("Phase 2 relations already exist: " + ", ".join(sorted(collisions)))
    constraints = set(bind.execute(sa.text("""SELECT c.conname FROM pg_constraint c
        JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname=:schema"""),
        {"schema": schema}).scalars())
    if constraints & set(PHASE2_CONSTRAINTS):
        reject("Phase 2 constraints already exist")
    if bind.scalar(sa.text("""SELECT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n
        ON n.oid=p.pronamespace WHERE n.nspname=:schema AND p.proname='uip_audit_reject_mutation')"""), {"schema":schema}):
        reject("Phase 2 audit function already exists")
    if bind.scalar(sa.text("""SELECT EXISTS(SELECT 1 FROM pg_trigger t JOIN pg_class c
        ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=:schema AND t.tgname='uip_audit_append_only')"""), {"schema":schema}):
        reject("Phase 2 audit trigger already exists")

    columns = {}
    for table, expected in REQUIRED.items():
        columns[table] = {c["name"]:c for c in inspector.get_columns(table, schema=schema)}
        for name, kind in expected.items():
            col = columns[table].get(name)
            if col is None:
                reject("missing prerequisite column " + table + "." + name)
            actual = str(col["type"])
            # PostgreSQL TIMESTAMP representation preserves timezone separately.
            compatible = actual == kind or (kind == "DATETIME" and isinstance(col["type"], sa.DateTime) and not col["type"].timezone)
            if kind == "VARCHAR":
                compatible = isinstance(col["type"], sa.String) and not isinstance(col["type"], sa.Text)
            if not compatible:
                reject("unexpected type for " + table + "." + name + ": " + actual + "; expected " + kind)
        if inspector.get_pk_constraint(table, schema=schema)["constrained_columns"] != ["id"]:
            reject("expected integer id primary key on " + table)
        if columns[table]["id"]["nullable"]:
            reject("nullable primary key on " + table)
    if set(PHASE2_COLUMNS) & set(columns["core_interaction"]):
        reject("Phase 2 core_interaction columns already exist")
    for table, names in {
        "core_organization": ("name", "slug"),
        "core_organization_member": ("organization_id", "user_id"),
        "core_interaction": ("organization_id", "creator_id", "interaction_type", "title"),
        "core_role_assignment": ("organization_id", "user_id", "role_id"),
    }.items():
        for name in names:
            if columns[table][name]["nullable"]:
                reject("unexpected nullable prerequisite " + table + "." + name)

    for table, field in (("core_organization","slug"),("core_interaction","reference")):
        unique = any(i.get("unique") and i.get("column_names") == [field]
                     and not i.get("dialect_options",{}).get("postgresql_where")
                     for i in inspector.get_indexes(table,schema=schema))
        unique |= any(c["column_names"] == [field] for c in inspector.get_unique_constraints(table,schema=schema))
        if not unique:
            reject("missing unique key " + table + "." + field)
    required_fks = (
        ("core_organization_member","organization_id","core_organization"),
        ("core_organization_member","user_id","user"),
        ("core_interaction","organization_id","core_organization"),
        ("core_interaction","creator_id","user"),
    )
    for table, field, target in required_fks:
        if not any(f["constrained_columns"]==[field] and f["referred_table"]==target
                   and f["referred_columns"]==["id"] and f.get("referred_schema") in (None,schema)
                   for f in inspector.get_foreign_keys(table,schema=schema)):
            reject("missing tenant/actor foreign key " + table + "." + field)
    # The inspected production nullable is_active mismatch is intentionally retained.
    quoted = bind.dialect.identifier_preparer.quote_schema(schema)
    if bind.scalar(sa.text('SELECT count(*) FROM ' + quoted + '.core_organization_member WHERE is_active IS NULL')):
        reject("membership is_active contains NULL; requires separate review, no automatic repair")
    if bind.scalar(sa.text("""SELECT EXISTS(SELECT 1 FROM pg_trigger t JOIN pg_class c
        ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=:schema AND NOT t.tgisinternal
        AND c.relname IN ('core_interaction','core_organization_member','alembic_version'))"""), {"schema":schema}):
        reject("unexpected user trigger on a migration target")
    if bind.scalar(sa.text("SELECT EXISTS(SELECT 1 FROM pg_event_trigger WHERE evtenabled <> 'D')")):
        reject("enabled DDL event trigger requires separate review")

def downgrade():
    print("baseline adoption cannot be automatically undone; retain existing schema and review history separately")
