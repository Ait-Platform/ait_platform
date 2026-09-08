"""Offline authoring helper: freeze current additive UIP DDL, never connect to SQL.

The generated revision does not import application models at migration time.
"""
import sys
from pathlib import Path
from sqlalchemy.schema import CreateTable, CreateIndex, CreateColumn, AddConstraint
from sqlalchemy.dialects import postgresql

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests" / "uip"))
from bootstrap import db

DIALECT = postgresql.dialect()
NEW_TABLES = (
    "uip_sla_policy", "uip_sla_clock", "uip_follow_up", "uip_referral_event",
    "uip_communication_log", "uip_document_folder", "uip_document_version",
    "uip_quorum_rule", "uip_meeting_participant", "uip_survey",
    "uip_survey_response", "uip_decision_event",
)
EXTENSIONS = {
    "uip_municipal_referral": ("organization_id", "version"),
    "uip_document": ("title", "category", "folder_id", "current_version"),
    "uip_committee_meeting": ("agenda", "eligibility_basis", "quorum_rule", "eligible_count",
        "attendance_count", "required_quorum", "quorum_achieved", "quorum_recorded_at", "quorum_recorded_by"),
    "uip_resolution": ("organization_id", "survey_id", "decision_date", "recorded_by", "responsible_user_id", "result_basis", "supersedes_id"),
}
UNIQUE = {"uq_uip_referral_org", "uq_uip_document_org", "uq_uip_meeting_org", "uq_uip_resolution_org"}
CONSTRAINTS = UNIQUE | {"fk_uip_referral_issue_org", "fk_uip_document_folder_org",
    "fk_uip_resolution_meeting_org", "fk_uip_resolution_survey_org", "fk_uip_resolution_supersedes_org", "ck_uip_resolution_source"}


def main():
    sql = []
    for name, fields in EXTENSIONS.items():
        table = db.metadata.tables[name]
        for field in fields:
            column = table.c[field]
            ddl = str(CreateColumn(column).compile(dialect=DIALECT))
            if field == "organization_id":
                ddl = ddl.replace(" NOT NULL", "")
            sql.append('ALTER TABLE "' + name + '" ADD COLUMN ' + ddl)
    sql.extend([
        "UPDATE uip_municipal_referral r SET organization_id = i.organization_id FROM core_interaction i WHERE i.id = r.interaction_id",
        "UPDATE uip_resolution r SET organization_id = m.organization_id FROM uip_committee_meeting m WHERE m.id = r.meeting_id",
        "ALTER TABLE uip_municipal_referral ALTER COLUMN organization_id SET NOT NULL",
        "ALTER TABLE uip_resolution ALTER COLUMN organization_id SET NOT NULL",
        "ALTER TABLE uip_resolution ALTER COLUMN meeting_id DROP NOT NULL",
    ])
    constraints = [c for name in EXTENSIONS for c in db.metadata.tables[name].constraints if c.name in CONSTRAINTS]
    sql.extend(str(AddConstraint(c).compile(dialect=DIALECT)) for c in constraints if c.name in UNIQUE)
    # The new tables do not reference one another cyclically; existing tables are present.
    order = ("uip_sla_policy", "uip_sla_clock", "uip_follow_up", "uip_referral_event",
        "uip_communication_log", "uip_document_folder", "uip_document_version", "uip_quorum_rule",
        "uip_meeting_participant", "uip_survey", "uip_survey_response", "uip_decision_event")
    for name in order:
        table = db.metadata.tables[name]
        sql.append(str(CreateTable(table).compile(dialect=DIALECT)))
        sql.extend(str(CreateIndex(index).compile(dialect=DIALECT)) for index in sorted(table.indexes, key=lambda i: i.name))
    sql.extend(str(AddConstraint(c).compile(dialect=DIALECT)) for c in constraints if c.name not in UNIQUE)
    # Inline CreateColumn does not emit foreign keys; freeze additive actor/org FKs too.
    for name, fields in EXTENSIONS.items():
        for constraint in db.metadata.tables[name].foreign_key_constraints:
            if len(constraint.columns) == 1 and next(iter(constraint.columns)).name in fields:
                sql.append(str(AddConstraint(constraint).compile(dialect=DIALECT)))
    sql.append("""CREATE FUNCTION uip_p49_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'UIP historical records are immutable'; END $$""")
    for table in ("uip_referral_event", "uip_document_version", "uip_survey_response", "uip_decision_event", "uip_resolution"):
        sql.append(f"CREATE TRIGGER {table}_immutable BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION uip_p49_immutable()")
    sql.append("""CREATE FUNCTION uip_p49_meeting_frozen() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.quorum_recorded_at IS NOT NULL THEN
    RAISE EXCEPTION 'Concluded UIP meeting and quorum are immutable';
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END $$""")
    sql.append("CREATE TRIGGER uip_meeting_frozen BEFORE UPDATE OR DELETE ON uip_committee_meeting FOR EACH ROW EXECUTE FUNCTION uip_p49_meeting_frozen()")
    sql.append("""CREATE FUNCTION uip_p49_attendance_frozen() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE meeting_key integer; org_key integer;
BEGIN
  IF TG_OP = 'DELETE' THEN meeting_key := OLD.meeting_id; org_key := OLD.organization_id;
  ELSE meeting_key := NEW.meeting_id; org_key := NEW.organization_id; END IF;
  PERFORM id FROM uip_committee_meeting WHERE id=meeting_key AND organization_id=org_key FOR UPDATE;
  IF EXISTS (SELECT 1 FROM uip_committee_meeting WHERE id=meeting_key AND organization_id=org_key AND quorum_recorded_at IS NOT NULL) THEN
    RAISE EXCEPTION 'Attendance for a concluded UIP meeting is immutable';
  END IF;
  IF TG_OP = 'UPDATE' AND ROW(NEW.meeting_id, NEW.organization_id, NEW.member_id) IS DISTINCT FROM ROW(OLD.meeting_id, OLD.organization_id, OLD.member_id) THEN
    RAISE EXCEPTION 'UIP attendance identity is immutable';
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END $$""")
    sql.append("CREATE TRIGGER uip_attendance_frozen BEFORE INSERT OR UPDATE OR DELETE ON uip_meeting_participant FOR EACH ROW EXECUTE FUNCTION uip_p49_attendance_frozen()")
    sql.append("""CREATE FUNCTION uip_p49_survey_frozen() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' OR OLD.status = 'FINALIZED' THEN
    RAISE EXCEPTION 'UIP survey history is immutable';
  END IF;
  IF ROW(NEW.organization_id, NEW.title, NEW.purpose, NEW.opens_at, NEW.closes_at, NEW.relationship, NEW.identifiable, NEW.questions::text)
     IS DISTINCT FROM ROW(OLD.organization_id, OLD.title, OLD.purpose, OLD.opens_at, OLD.closes_at, OLD.relationship, OLD.identifiable, OLD.questions::text) THEN
    RAISE EXCEPTION 'UIP survey design cannot change after creation';
  END IF;
  RETURN NEW;
END $$""")
    sql.append("CREATE TRIGGER uip_survey_frozen BEFORE UPDATE OR DELETE ON uip_survey FOR EACH ROW EXECUTE FUNCTION uip_p49_survey_frozen()")
    source = '''"""Add UIP operational capability; only the UIP branch is advanced.

Frozen PostgreSQL DDL. No application factory or production assumptions.
Historical timestamps and file content are never fabricated.
"""
from alembic import op
import sqlalchemy as sa

revision = "uip_p49_operations"
down_revision = "uip_p3_work_orders"
branch_labels = None
depends_on = None

DDL = ''' + "(\n" + "\n".join('    """' + statement.strip() + '""",' for statement in sql) + "\n)" + '''


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        raise RuntimeError("UIP operational migration requires PostgreSQL")
    for statement in DDL:
        op.execute(sa.text(statement))


def downgrade():
    raise RuntimeError("UIP operational history must be preserved; downgrade is refused")
'''
    (ROOT / "migrations" / "versions" / "uip_p49_operations.py").write_text(source, encoding="utf-8")


if __name__ == "__main__":
    main()
