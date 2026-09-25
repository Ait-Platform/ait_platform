"""Shared pre-Resolution Proposal records.

Revision ID: uip_p53_proposals
Revises: uip_p52_billing_data
"""
from alembic import op
import sqlalchemy as sa

revision = "uip_p53_proposals"
down_revision = "uip_p52_billing_data"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table("uip_proposal",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("organization_id", sa.Integer(), sa.ForeignKey("core_organization.id"), nullable=False),
        sa.Column("title", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("motivation", sa.Text(), nullable=False),
        sa.Column("proposed_budget", sa.Numeric(16, 2)),
        sa.Column("originator_id", sa.Integer(), sa.ForeignKey("user.id"), nullable=False),
        sa.Column("originating_capacity", sa.String(50), nullable=False),
        sa.Column("originating_subcommittee", sa.String(255)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(20), server_default="DRAFT", nullable=False),
        sa.Column("resolution_id", sa.Integer(), unique=True),
        sa.Column("converted_by", sa.Integer(), sa.ForeignKey("user.id")),
        sa.Column("converted_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("id", "organization_id", name="uq_uip_proposal_org"),
        sa.ForeignKeyConstraint(["resolution_id", "organization_id"], ["uip_resolution.id", "uip_resolution.organization_id"], name="fk_uip_proposal_resolution_org"),
        sa.CheckConstraint("status IN ('DRAFT','SUBMITTED','CONVERTED')", name="ck_uip_proposal_status"),
        sa.CheckConstraint("proposed_budget IS NULL OR proposed_budget >= 0", name="ck_uip_proposal_budget"),
        sa.CheckConstraint("(status = 'DRAFT' AND submitted_at IS NULL AND resolution_id IS NULL AND converted_by IS NULL AND converted_at IS NULL) OR (status = 'SUBMITTED' AND submitted_at IS NOT NULL AND resolution_id IS NULL AND converted_by IS NULL AND converted_at IS NULL) OR (status = 'CONVERTED' AND submitted_at IS NOT NULL AND resolution_id IS NOT NULL AND converted_by IS NOT NULL AND converted_at IS NOT NULL)", name="ck_uip_proposal_lifecycle"))
    op.create_index("ix_uip_proposal_org_status", "uip_proposal", ["organization_id", "status"])
    op.create_table("uip_proposal_document",
        sa.Column("proposal_id", sa.Integer(), primary_key=True),
        sa.Column("document_id", sa.Integer(), primary_key=True),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["proposal_id", "organization_id"], ["uip_proposal.id", "uip_proposal.organization_id"], name="fk_uip_proposal_document_proposal_org"),
        sa.ForeignKeyConstraint(["document_id", "organization_id"], ["uip_document.id", "uip_document.organization_id"], name="fk_uip_proposal_document_document_org"))


def downgrade():
    connection = op.get_bind()
    if connection.scalar(sa.text("SELECT EXISTS (SELECT 1 FROM uip_proposal)")):
        raise RuntimeError("Refusing to discard existing Proposal records")
    op.drop_table("uip_proposal_document")
    op.drop_index("ix_uip_proposal_org_status", table_name="uip_proposal")
    op.drop_table("uip_proposal")
