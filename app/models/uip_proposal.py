"""Pre-Resolution proposals; neither approval nor spending authority."""
from app.extensions import db


class UipProposal(db.Model):
    __tablename__ = "uip_proposal"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    title = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text, nullable=False)
    motivation = db.Column(db.Text, nullable=False)
    proposed_budget = db.Column(db.Numeric(16, 2))
    originator_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    originating_capacity = db.Column(db.String(50), nullable=False)
    # Reserved attribution only. No current route accepts or infers this relationship.
    originating_subcommittee_id = db.Column(db.Integer)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    submitted_at = db.Column(db.DateTime(timezone=True))
    status = db.Column(db.String(20), nullable=False, default="DRAFT", server_default="DRAFT")
    resolution_id = db.Column(db.Integer, unique=True)
    converted_by = db.Column(db.Integer, db.ForeignKey("user.id"))
    converted_at = db.Column(db.DateTime(timezone=True))
    __table_args__ = (
        db.UniqueConstraint("id", "organization_id", name="uq_uip_proposal_org"),
        db.ForeignKeyConstraint(["resolution_id", "organization_id"], ["uip_resolution.id", "uip_resolution.organization_id"], name="fk_uip_proposal_resolution_org"),
        db.CheckConstraint("status IN ('DRAFT','SUBMITTED','CONVERTED')", name="ck_uip_proposal_status"),
        db.CheckConstraint("proposed_budget IS NULL OR proposed_budget >= 0", name="ck_uip_proposal_budget"),
        db.CheckConstraint("(status = 'DRAFT' AND submitted_at IS NULL AND resolution_id IS NULL AND converted_by IS NULL AND converted_at IS NULL) OR (status = 'SUBMITTED' AND submitted_at IS NOT NULL AND resolution_id IS NULL AND converted_by IS NULL AND converted_at IS NULL) OR (status = 'CONVERTED' AND submitted_at IS NOT NULL AND resolution_id IS NOT NULL AND converted_by IS NOT NULL AND converted_at IS NOT NULL)", name="ck_uip_proposal_lifecycle"),
        db.Index("ix_uip_proposal_org_status", "organization_id", "status"),
        db.ForeignKeyConstraint(["originating_subcommittee_id", "organization_id"], ["uip_subcommittee.id", "uip_subcommittee.organization_id"], name="fk_uip_proposal_subcommittee_org"),
    )

    @property
    def reference(self):
        return f"PROP-{self.id:06d}" if self.id else "PROP-DRAFT"


class UipProposalDocument(db.Model):
    """Links existing protected documents; no new file storage."""
    __tablename__ = "uip_proposal_document"
    proposal_id = db.Column(db.Integer, primary_key=True)
    document_id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(["proposal_id", "organization_id"], ["uip_proposal.id", "uip_proposal.organization_id"], name="fk_uip_proposal_document_proposal_org"),
        db.ForeignKeyConstraint(["document_id", "organization_id"], ["uip_document.id", "uip_document.organization_id"], name="fk_uip_proposal_document_document_org"),
    )

