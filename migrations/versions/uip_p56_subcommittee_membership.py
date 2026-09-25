"""Specific Resolution-backed Subcommittee memberships; no identity backfill."""
from alembic import op
import sqlalchemy as sa
revision = "uip_p56_submembership"
down_revision = "uip_p55"
branch_labels = None
depends_on = None


def upgrade():
    op.create_unique_constraint("uq_uip_committee_member_org", "uip_committee_member", ["id", "organization_id"])
    op.create_table("uip_subcommittee_membership",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("organization_id", sa.Integer(), sa.ForeignKey("core_organization.id"), nullable=False),
        sa.Column("subcommittee_id", sa.Integer(), nullable=False),
        sa.Column("member_id", sa.Integer(), nullable=False),
        sa.Column("appointing_resolution_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="CURRENT"),
        sa.Column("valid_from", sa.Date(), nullable=False),
        sa.Column("valid_to", sa.Date()),
        sa.Column("recorded_by", sa.Integer(), sa.ForeignKey("user.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["subcommittee_id", "organization_id"], ["uip_subcommittee.id", "uip_subcommittee.organization_id"], name="fk_uip_submembership_sub_org"),
        sa.ForeignKeyConstraint(["member_id", "organization_id"], ["uip_committee_member.id", "uip_committee_member.organization_id"], name="fk_uip_submembership_member_org"),
        sa.ForeignKeyConstraint(["appointing_resolution_id", "organization_id"], ["uip_resolution.id", "uip_resolution.organization_id"], name="fk_uip_submembership_resolution_org"),
        sa.UniqueConstraint("subcommittee_id", "member_id", "appointing_resolution_id", name="uq_uip_submembership_appointment"),
        sa.CheckConstraint("status IN ('CURRENT','FORMER')", name="ck_uip_submembership_status"),
        sa.CheckConstraint("valid_to IS NULL OR valid_to >= valid_from", name="ck_uip_submembership_dates"))


def downgrade():
    if op.get_bind().scalar(sa.text("SELECT EXISTS (SELECT 1 FROM uip_subcommittee_membership)")):
        raise RuntimeError("Refusing to discard Subcommittee appointment history")
    op.drop_table("uip_subcommittee_membership")
    op.drop_constraint("uq_uip_committee_member_org", "uip_committee_member", type_="unique")
