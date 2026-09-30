"""Independent HOME endorsement foundation; additive standalone Alembic branch.

No existing table, row, foreign key or programme authority is altered.
Run this revision specifically, not `upgrade heads` (unrelated branches exist).
"""
from alembic import op
import sqlalchemy as sa

revision = "home_sace_001"
down_revision = None
branch_labels = ("home_sace",)
depends_on = None


def pk():
    return sa.Column("id", sa.Integer(), primary_key=True)


def stamp(name="created_at", nullable=False):
    return sa.Column(name, sa.DateTime(timezone=True), nullable=nullable)


def fk(name, table, nullable=False, unique=False):
    return sa.Column(name, sa.Integer(), sa.ForeignKey(table + ".id"), nullable=nullable, unique=unique)


def upgrade():
    op.create_table("sace_home_controller", pk(), fk("user_id", "user", unique=True),
        sa.Column("active", sa.Boolean(), nullable=False), stamp())
    op.create_table("sace_home_provisioning", pk(),
        sa.Column("token_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("email", sa.String(255), nullable=False),
        sa.Column("issued_by", sa.String(255), nullable=False), stamp("expires_at"),
        fk("claimed_by", "user", nullable=True), stamp("claimed_at", True), stamp())
    op.create_table("sace_home_invitation", pk(), fk("controller_id", "sace_home_controller"),
        sa.Column("code_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("status", sa.String(16), nullable=False), stamp("expires_at"), stamp("claimed_at", True), stamp(),
        sa.CheckConstraint("status IN ('unclaimed','claimed','revoked')", name="ck_sace_home_invitation_status"))
    op.create_table("sace_home_pledge", pk(), fk("user_id", "user"),
        sa.Column("role", sa.String(16), nullable=False), sa.Column("signature", sa.String(255), nullable=False),
        sa.Column("version", sa.String(40), nullable=False), sa.Column("text_hash", sa.String(64), nullable=False),
        fk("provisioning_id", "sace_home_provisioning", nullable=True, unique=True),
        fk("invitation_id", "sace_home_invitation", nullable=True, unique=True), stamp("accepted_at"), stamp("recorded_at"),
        sa.CheckConstraint("(role = 'controller' AND provisioning_id IS NOT NULL AND invitation_id IS NULL) OR "
            "(role = 'auditor' AND invitation_id IS NOT NULL AND provisioning_id IS NULL)", name="ck_sace_home_pledge_context"))
    op.create_table("sace_home_assignment", pk(), fk("invitation_id", "sace_home_invitation", unique=True),
        fk("auditor_id", "user"), sa.Column("status", sa.String(16), nullable=False),
        sa.Column("requirements_version", sa.String(40), nullable=False), stamp(), stamp("completed_at", True),
        sa.CheckConstraint("status IN ('active','completed','revoked')", name="ck_sace_home_assignment_status"),
        sa.CheckConstraint("(status = 'completed') = (completed_at IS NOT NULL)", name="ck_sace_home_completion"))
    op.create_index("ix_sace_home_assignment_auditor_id", "sace_home_assignment", ["auditor_id"])
    op.create_table("sace_home_document", pk(), sa.Column("kind", sa.String(60), nullable=False, unique=True),
        sa.Column("title", sa.String(255), nullable=False))
    op.create_table("sace_home_document_version", pk(), fk("document_id", "sace_home_document"),
        sa.Column("version", sa.String(60), nullable=False), sa.Column("storage_key", sa.String(255), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False), sa.Column("source_manifest", sa.JSON(), nullable=False),
        fk("approved_by", "sace_home_controller"), stamp("published_at"),
        sa.UniqueConstraint("document_id", "version", name="uq_sace_home_document_version"))
    op.create_table("sace_home_evidence", pk(), fk("assignment_id", "sace_home_assignment"), fk("actor_id", "user"),
        sa.Column("item", sa.String(60), nullable=False), sa.Column("event", sa.String(40), nullable=False),
        fk("document_version_id", "sace_home_document_version", nullable=True),
        sa.Column("details", sa.JSON(), nullable=False), stamp())
    op.create_index("ix_sace_home_evidence_assignment_id", "sace_home_evidence", ["assignment_id"])


def downgrade():
    for table in ("sace_home_evidence", "sace_home_document_version", "sace_home_document",
                  "sace_home_assignment", "sace_home_pledge", "sace_home_invitation",
                  "sace_home_provisioning", "sace_home_controller"):
        op.drop_table(table)
