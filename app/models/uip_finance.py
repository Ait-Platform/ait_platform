"""Additive UIP recorded-finance models; no banking or startup effects."""
from app.extensions import db


def scoped_links(prefix):
    return tuple(db.ForeignKeyConstraint([field, "organization_id"],
        [table + ".id", table + ".organization_id"], name="fk_" + prefix + "_" + field)
        for field, table in (("provider_id", "uip_provider"), ("work_order_id", "uip_work_order"),
                             ("governance_decision_id", "uip_resolution"), ("document_id", "uip_document")))


class UipFinanceCommitment(db.Model):
    __tablename__ = "uip_finance_commitment"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    reference = db.Column(db.String(50), nullable=False, unique=True)
    transaction_date = db.Column(db.Date, nullable=False)
    amount = db.Column(db.Numeric(16, 2), nullable=False)
    category = db.Column(db.String(100), nullable=False)
    description = db.Column(db.Text, nullable=False)
    public_description = db.Column(db.Text, nullable=False)
    public_party = db.Column(db.String(255), nullable=False, default="")
    provider_id = db.Column(db.Integer)
    work_order_id = db.Column(db.Integer)
    governance_decision_id = db.Column(db.Integer)
    document_id = db.Column(db.Integer)
    member_visible = db.Column(db.Boolean, nullable=False, default=False)
    status = db.Column(db.String(20), nullable=False, default="OPEN")
    version = db.Column(db.Integer, nullable=False, default=1)
    recorded_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    recorded_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    request_key = db.Column(db.String(36), nullable=False)
    __table_args__ = scoped_links("uip_fc") + (
        db.UniqueConstraint("id", "organization_id", name="uq_uip_fc_org"),
        db.UniqueConstraint("organization_id", "request_key", name="uq_uip_fc_request"),
        db.CheckConstraint("amount > 0 AND version > 0", name="ck_uip_fc_amount"),
        db.CheckConstraint("status IN ('OPEN','PARTIALLY_PAID','PAID','CANCELLED')", name="ck_uip_fc_status"),
        db.Index("ix_uip_fc_org_date", "organization_id", "transaction_date"),
    )


class UipFinanceTransaction(db.Model):
    __tablename__ = "uip_finance_transaction"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    reference = db.Column(db.String(50), nullable=False, unique=True)
    transaction_date = db.Column(db.Date, nullable=False)
    kind = db.Column(db.String(20), nullable=False)
    amount = db.Column(db.Numeric(16, 2), nullable=False)
    category = db.Column(db.String(100), nullable=False)
    description = db.Column(db.Text, nullable=False)
    party = db.Column(db.String(255), nullable=False, default="")
    public_description = db.Column(db.Text, nullable=False)
    public_party = db.Column(db.String(255), nullable=False, default="")
    provider_id = db.Column(db.Integer)
    work_order_id = db.Column(db.Integer)
    governance_decision_id = db.Column(db.Integer)
    document_id = db.Column(db.Integer)
    commitment_id = db.Column(db.Integer)
    reversal_of_id = db.Column(db.Integer, unique=True)
    correction_of_id = db.Column(db.Integer, unique=True)
    correction_reason = db.Column(db.Text)
    status = db.Column(db.String(20), nullable=False, default="POSTED")
    member_visible = db.Column(db.Boolean, nullable=False, default=False)
    version = db.Column(db.Integer, nullable=False, default=1)
    recorded_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    recorded_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    request_key = db.Column(db.String(36), nullable=False)
    __table_args__ = scoped_links("uip_ft") + (
        db.UniqueConstraint("id", "organization_id", name="uq_uip_ft_org"),
        db.UniqueConstraint("organization_id", "request_key", name="uq_uip_ft_request"),
        db.ForeignKeyConstraint(["commitment_id", "organization_id"], ["uip_finance_commitment.id", "uip_finance_commitment.organization_id"], name="fk_uip_ft_commitment"),
        db.ForeignKeyConstraint(["reversal_of_id", "organization_id"], ["uip_finance_transaction.id", "uip_finance_transaction.organization_id"], name="fk_uip_ft_reversal"),
        db.ForeignKeyConstraint(["correction_of_id", "organization_id"], ["uip_finance_transaction.id", "uip_finance_transaction.organization_id"], name="fk_uip_ft_correction"),
        db.CheckConstraint("kind IN ('INCOME','EXPENDITURE','ADJUSTMENT')", name="ck_uip_ft_kind"),
        db.CheckConstraint("amount <> 0 AND version > 0", name="ck_uip_ft_amount"),
        db.CheckConstraint("(status = 'POSTED' AND reversal_of_id IS NULL AND (amount > 0 OR kind = 'ADJUSTMENT')) OR (status = 'REVERSAL' AND reversal_of_id IS NOT NULL)", name="ck_uip_ft_status"),
        db.CheckConstraint("commitment_id IS NULL OR kind = 'EXPENDITURE'", name="ck_uip_ft_commitment"),
        db.Index("ix_uip_ft_org_date", "organization_id", "transaction_date"),
    )


class UipFinanceBudgetLine(db.Model):
    __tablename__ = "uip_finance_budget_line"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    year_start = db.Column(db.Date, nullable=False)
    category = db.Column(db.String(100), nullable=False)
    __table_args__ = (
        db.UniqueConstraint("id", "organization_id", name="uq_uip_fb_org"),
        db.UniqueConstraint("organization_id", "year_start", "category", name="uq_uip_fb_category"),
        db.CheckConstraint("EXTRACT(MONTH FROM year_start) = 3 AND EXTRACT(DAY FROM year_start) = 1", name="ck_uip_fb_march"),
    )


class UipFinanceBudgetRevision(db.Model):
    __tablename__ = "uip_finance_budget_revision"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    budget_line_id = db.Column(db.Integer, nullable=False)
    revision = db.Column(db.Integer, nullable=False)
    approved_amount = db.Column(db.Numeric(16, 2), nullable=False)
    description = db.Column(db.Text, nullable=False)
    revised_date = db.Column(db.Date, nullable=False)
    governance_decision_id = db.Column(db.Integer)
    recorded_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    recorded_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    __table_args__ = (
        db.ForeignKeyConstraint(["budget_line_id", "organization_id"], ["uip_finance_budget_line.id", "uip_finance_budget_line.organization_id"], name="fk_uip_fbr_line"),
        db.ForeignKeyConstraint(["governance_decision_id", "organization_id"], ["uip_resolution.id", "uip_resolution.organization_id"], name="fk_uip_fbr_decision"),
        db.UniqueConstraint("budget_line_id", "revision", name="uq_uip_fbr_revision"),
        db.CheckConstraint("approved_amount >= 0 AND revision > 0", name="ck_uip_fbr_amount"),
    )


class UipFinanceCommitmentRevision(db.Model):
    __tablename__ = "uip_finance_commitment_revision"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    commitment_id = db.Column(db.Integer, nullable=False)
    revision = db.Column(db.Integer, nullable=False)
    amount = db.Column(db.Numeric(16, 2), nullable=False)
    cancelled = db.Column(db.Boolean, nullable=False, default=False)
    effective_date = db.Column(db.Date, nullable=False)
    reason = db.Column(db.Text, nullable=False)
    recorded_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    recorded_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    __table_args__ = (
        db.ForeignKeyConstraint(["commitment_id", "organization_id"], ["uip_finance_commitment.id", "uip_finance_commitment.organization_id"], name="fk_uip_fcr_commitment"),
        db.UniqueConstraint("commitment_id", "revision", name="uq_uip_fcr_revision"),
        db.CheckConstraint("amount > 0 AND revision > 0", name="ck_uip_fcr_amount"),
    )
