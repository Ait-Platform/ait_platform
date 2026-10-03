"""Independent additive Reading audit branch. No history transfer or authority changes.

Apply this revision explicitly; do not upgrade unrelated heads.
"""
from alembic import op
import sqlalchemy as sa

revision = "reading_sace_audit_001"
down_revision = None
branch_labels = ("reading_sace_audit",)
depends_on = None


def function_name():
    bind = op.get_bind()
    schema = bind.execute(sa.text("SELECT current_schema()")).scalar_one()
    return bind.dialect.identifier_preparer.quote_schema(schema) + ".sace_reading_audit_reject_mutation"


def upgrade():
    op.create_table("sace_reading_audit_event",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("user.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("engagement_id", sa.BigInteger()),
        sa.Column("action", sa.String(100), nullable=False),
        sa.Column("entity_type", sa.String(100)),
        sa.Column("entity_id", sa.Integer()),
        sa.Column("details", sa.Text()),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.Column("ip_address", sa.String(50)),
        sa.Column("created_at", sa.DateTime(), nullable=False))
    op.create_index("ix_sace_reading_audit_event_action", "sace_reading_audit_event", ["action"])
    op.create_index("ix_sace_reading_audit_event_created_at", "sace_reading_audit_event", ["created_at"])
    function = function_name()
    op.execute("""CREATE FUNCTION """ + function + """() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
        RAISE EXCEPTION 'Reading audit events are append-only'; END; $$""")
    op.execute("""CREATE TRIGGER sace_reading_audit_append_only BEFORE UPDATE OR DELETE
        ON sace_reading_audit_event FOR EACH ROW
        EXECUTE FUNCTION """ + function + """()""")


def downgrade():
    if op.get_bind().execute(sa.text("SELECT count(*) FROM sace_reading_audit_event")).scalar_one():
        raise RuntimeError("Preserve Reading audit history; use forward recovery.")
    op.drop_table("sace_reading_audit_event")
    op.execute("DROP FUNCTION " + function_name() + "()")
