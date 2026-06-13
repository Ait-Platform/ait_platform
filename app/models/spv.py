from datetime import datetime

from app.extensions import db


class Spv(db.Model):
    __tablename__ = "spvs"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)  # Dale, Auto Worx
    code = db.Column(db.String(50), unique=True)      # dale_spv, auto_worx_spv

    description = db.Column(db.Text)

    # structure
    spv_type = db.Column(db.String(50))  # rental, development, mixed

    # financials
    target_raise = db.Column(db.Numeric(12, 2))
    projected_roi = db.Column(db.Float)

    # lifecycle
    status = db.Column(db.String(50))  # draft, fundraising, active, closed

    # draft → fundraising → closed → active → exited
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    lifecycle_stage = db.Column(db.String(50))

    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvInvestor(db.Model):
    __tablename__ = "spv_investors"

    id = db.Column(db.Integer, primary_key=True)

    spv_id = db.Column(db.Integer, db.ForeignKey("spvs.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)

    amount_invested = db.Column(db.Numeric(12, 2), default=0)
    equity_percentage = db.Column(db.Float)
    pseudonym = db.Column(db.String(100))

    joined_at = db.Column(db.DateTime, default=datetime.utcnow)

    spv = db.relationship("Spv", backref="investors")
    #user = db.relationship("User", backref="spv_investments")

    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvTransaction(db.Model):
    __tablename__ = "spv_transactions"

    id = db.Column(db.Integer, primary_key=True)

    spv_id = db.Column(db.Integer, db.ForeignKey("spvs.id"), nullable=False)
    investor_id = db.Column(db.Integer, db.ForeignKey("spv_investors.id"))

    amount = db.Column(db.Numeric(12, 2), nullable=False)

    transaction_type = db.Column(db.String(50))  
    # investment, payout, expense, loan

    reference = db.Column(db.String(255))
    notes = db.Column(db.Text)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    spv = db.relationship("Spv", backref="transactions")
    investor = db.relationship("SpvInvestor", backref="transactions")

    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvDocument(db.Model):
    __tablename__ = "spv_documents"

    id = db.Column(db.Integer, primary_key=True)

    spv_id = db.Column(db.Integer, db.ForeignKey("spvs.id"), nullable=False)

    title = db.Column(db.String(255))
    file_path = db.Column(db.String(255))

    document_type = db.Column(db.String(50))  
    # agreement, financials, report

    uploaded_at = db.Column(db.DateTime, default=datetime.utcnow)

    spv = db.relationship("Spv", backref="documents")
    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvPayout(db.Model):
    __tablename__ = "spv_payouts"

    id = db.Column(db.Integer, primary_key=True)

    spv_id = db.Column(db.Integer, db.ForeignKey("spvs.id"), nullable=False)

    total_amount = db.Column(db.Numeric(12, 2), nullable=False)

    payout_date = db.Column(db.DateTime, default=datetime.utcnow)
    notes = db.Column(db.Text)

    spv = db.relationship("Spv", backref="payouts")
    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvPayoutDistribution(db.Model):
    __tablename__ = "spv_payout_distributions"

    id = db.Column(db.Integer, primary_key=True)

    payout_id = db.Column(db.Integer, db.ForeignKey("spv_payouts.id"), nullable=False)
    investor_id = db.Column(db.Integer, db.ForeignKey("spv_investors.id"), nullable=False)

    amount = db.Column(db.Numeric(12, 2), nullable=False)

    payout = db.relationship("SpvPayout", backref="distributions")
    investor = db.relationship("SpvInvestor", backref="payouts")
    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvFinancialModel(db.Model):
    __tablename__ = "spv_financial_models"

    id = db.Column(db.Integer, primary_key=True)

    spv_id = db.Column(db.Integer, db.ForeignKey("spvs.id"), nullable=False)

    # CAPITAL
    purchase_price = db.Column(db.Numeric(12, 2))
    development_cost = db.Column(db.Numeric(12, 2))
    total_project_cost = db.Column(db.Numeric(12, 2))

    # RENTAL (monthly)
    gross_rental_income = db.Column(db.Numeric(12, 2))
    operating_expenses = db.Column(db.Numeric(12, 2))
    net_rental_income = db.Column(db.Numeric(12, 2))

    # SALES (exit)
    projected_sale_value = db.Column(db.Numeric(12, 2))

    # TIMELINE
    hold_period_months = db.Column(db.Integer)

    # OUTPUTS (calculated)
    total_profit = db.Column(db.Numeric(12, 2))
    roi_percentage = db.Column(db.Float)

    spv = db.relationship("Spv", backref="financial_model", uselist=False)
    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvDealProfile(db.Model):
    __tablename__ = "spv_deal_profiles"

    id = db.Column(db.Integer, primary_key=True)

    spv_id = db.Column(db.Integer, db.ForeignKey("spvs.id"), nullable=False)

    headline = db.Column(db.String(255))  # “Student Housing Development – Durban”
    summary = db.Column(db.Text)

    investment_highlights = db.Column(db.Text)  # bullet-style text
    risks = db.Column(db.Text)

    minimum_investment = db.Column(db.Numeric(12, 2))
    target_raise = db.Column(db.Numeric(12, 2))

    open_date = db.Column(db.DateTime)
    close_date = db.Column(db.DateTime)

    spv = db.relationship("Spv", backref="deal_profile", uselist=False)
    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvCommitment(db.Model):
    __tablename__ = "spv_commitments"

    id = db.Column(db.Integer, primary_key=True)

    spv_id = db.Column(db.Integer, db.ForeignKey("spvs.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)

    amount = db.Column(db.Numeric(12, 2), nullable=False)

    status = db.Column(db.String(50))  
    # pending, confirmed, cancelled

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    spv = db.relationship("Spv", backref="commitments")
    #user = db.relationship("User", backref="spv_commitments")
    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvInvestment(db.Model):
    __tablename__ = "spv_investments"

    id = db.Column(db.Integer, primary_key=True)

    spv_id = db.Column(db.Integer, db.ForeignKey("spvs.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)

    commitment_id = db.Column(db.Integer, db.ForeignKey("spv_commitments.id"))

    total_amount = db.Column(db.Numeric(12, 2), nullable=False)

    status = db.Column(db.String(50))  
    # pending_payment, active, cancelled

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    spv = db.relationship("Spv", backref="investments")
    #user = db.relationship("User", backref="spv_investments")
    commitment = db.relationship("SpvCommitment", backref="investment", uselist=False)
    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvPayment(db.Model):
    __tablename__ = "spv_payments"

    id = db.Column(db.Integer, primary_key=True)

    investment_id = db.Column(db.Integer, db.ForeignKey("spv_investments.id"), nullable=False)

    amount = db.Column(db.Numeric(12, 2), nullable=False)

    payment_date = db.Column(db.DateTime, default=datetime.utcnow)
    reference = db.Column(db.String(255))

    investment = db.relationship("SpvInvestment", backref="payments")
    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvAgreement(db.Model):
    __tablename__ = "spv_agreements"

    id = db.Column(db.Integer, primary_key=True)

    investment_id = db.Column(db.Integer, db.ForeignKey("spv_investments.id"), nullable=False)

    document_path = db.Column(db.String(255))

    signed = db.Column(db.Boolean, default=False)
    signed_at = db.Column(db.DateTime)

    investment = db.relationship("SpvInvestment", backref="agreement")
    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvFund(db.Model):
    __tablename__ = "spv_funds"

    id = db.Column(db.Integer, primary_key=True)

    name = db.Column(db.String(255), nullable=False)
    code = db.Column(db.String(50), unique=True)

    description = db.Column(db.Text)

    target_raise = db.Column(db.Numeric(12, 2))

    status = db.Column(db.String(50))  
    # raising, active, closed

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvFundSpv(db.Model):
    __tablename__ = "spv_fund_spvs"

    id = db.Column(db.Integer, primary_key=True)

    fund_id = db.Column(db.Integer, db.ForeignKey("spv_funds.id"), nullable=False)
    spv_id = db.Column(db.Integer, db.ForeignKey("spvs.id"), nullable=False)

    allocation_percentage = db.Column(db.Float)  # how fund allocates capital

    #fund = db.relationship("SpvFund", backref="fund_spvs")
    spv = db.relationship("Spv", backref="fund_links")
    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvFundInvestor(db.Model):
    __tablename__ = "spv_fund_investors"

    id = db.Column(db.Integer, primary_key=True)

    fund_id = db.Column(db.Integer, db.ForeignKey("spv_funds.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)

    amount_invested = db.Column(db.Numeric(12, 2), default=0)

    #fund = db.relationship("Fund", backref="investors")
    #user = db.relationship("User", backref="fund_investments")
    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvUserRole(db.Model):
    __tablename__ = "spv_user_roles"

    id = db.Column(db.Integer, primary_key=True)

    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    role = db.Column(db.String(50))  
    # admin, manager, investor

    user = db.relationship("User", backref="spv_roles")

    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvApproval(db.Model):
    __tablename__ = "spv_approvals"

    id = db.Column(db.Integer, primary_key=True)

    entity_type = db.Column(db.String(50))  
    # spv, payout, investment, fund

    entity_id = db.Column(db.Integer)

    status = db.Column(db.String(50))  
    # pending, approved, rejected

    requested_by = db.Column(db.Integer, db.ForeignKey("user.id"))
    approved_by = db.Column(db.Integer, db.ForeignKey("user.id"))

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvAuditLog(db.Model):
    __tablename__ = "spv_audit_logs"

    id = db.Column(db.Integer, primary_key=True)

    user_id = db.Column(db.Integer, db.ForeignKey("user.id"))

    action = db.Column(db.String(255))
    entity_type = db.Column(db.String(50))
    entity_id = db.Column(db.Integer)

    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvInvestorStatement(db.Model):
    __tablename__ = "spv_investor_statements"

    id = db.Column(db.Integer, primary_key=True)

    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    spv_id = db.Column(db.Integer, db.ForeignKey("spvs.id"))

    period = db.Column(db.String(20))  # "2026-04"

    total_invested = db.Column(db.Numeric(12, 2))
    total_returns = db.Column(db.Numeric(12, 2))
    roi = db.Column(db.Float)

    generated_at = db.Column(db.DateTime, default=datetime.utcnow)
    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvPerformanceAlert(db.Model):
    __tablename__ = "spv_performance_alerts"

    id = db.Column(db.Integer, primary_key=True)

    spv_id = db.Column(db.Integer, db.ForeignKey("spvs.id"))

    alert_type = db.Column(db.String(50))  
    # underperformance, overbudget, delay

    message = db.Column(db.Text)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvBankTransaction(db.Model):
    __tablename__ = "spv_bank_transactions"

    id = db.Column(db.Integer, primary_key=True)

    amount = db.Column(db.Numeric(12, 2))
    reference = db.Column(db.String(255))
    transaction_date = db.Column(db.DateTime)

    matched = db.Column(db.Boolean, default=False)
    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvDocumentTemplate(db.Model):
    __tablename__ = "spv_document_templates"

    id = db.Column(db.Integer, primary_key=True)

    name = db.Column(db.String(255))
    content = db.Column(db.Text)  # template text with placeholders
    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"), nullable=False)

class SpvTenant(db.Model):
    __tablename__ = "spv_tenants"

    id = db.Column(db.Integer, primary_key=True)

    name = db.Column(db.String(255), nullable=False)
    code = db.Column(db.String(50), unique=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    brand_name = db.Column(db.String(255))
    logo_url = db.Column(db.String(255))
    primary_color = db.Column(db.String(20))

class SpvTenantUser(db.Model):
    __tablename__ = "spv_tenant_users"

    id = db.Column(db.Integer, primary_key=True)

    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"))
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"))

    role = db.Column(db.String(50))  
    # owner, manager, investor

class SpvManagementFee(db.Model):
    __tablename__ = "spv_management_fees"

    id = db.Column(db.Integer, primary_key=True)

    spv_id = db.Column(db.Integer, db.ForeignKey("spvs.id"))

    percentage = db.Column(db.Float)  # e.g. 2% annually

class SpvPerformanceFee(db.Model):
    __tablename__ = "spv_performance_fees"

    id = db.Column(db.Integer, primary_key=True)

    spv_id = db.Column(db.Integer, db.ForeignKey("spvs.id"))

    percentage = db.Column(db.Float)  # e.g. 20% of profit

class SpvTenantSubscription(db.Model):
    __tablename__ = "spv_tenant_subscriptions"

    id = db.Column(db.Integer, primary_key=True)

    tenant_id = db.Column(db.Integer, db.ForeignKey("spv_tenants.id"))

    plan = db.Column(db.String(50))  
    # basic, pro, enterprise

    monthly_fee = db.Column(db.Numeric(12, 2))

    active = db.Column(db.Boolean, default=True)

class SpvDeal(db.Model):
    __tablename__ = "spv_deals"

    id = db.Column(db.Integer, primary_key=True)

    title = db.Column(db.String(255), nullable=False)
    slug = db.Column(db.String(255), unique=True, nullable=False)

    summary = db.Column(db.Text, nullable=False)
    description = db.Column(db.Text)

    location = db.Column(db.String(255))
    project_type = db.Column(db.String(255))

    founder_position = db.Column(db.Numeric(12, 2), default=0)
    capital_position = db.Column(db.Numeric(12, 2), default=0)
    combined_structure = db.Column(db.Numeric(12, 2), default=0)

    status = db.Column(db.String(50), default="open")

    cover_image = db.Column(db.String(500))

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    participations = db.relationship(
        "SpvParticipation",
        backref="deal",
        lazy=True
    )
    
    '''
    is_active = db.Column(
        db.Boolean,
        default=True
    )
    ''' 
    
class SpvParticipation(db.Model):
    __tablename__ = "spv_participations"

    id = db.Column(db.Integer, primary_key=True)

    user_id = db.Column(
        db.Integer,
        db.ForeignKey("user.id"),
        nullable=False
    )

    deal_id = db.Column(
        db.Integer,
        db.ForeignKey("spv_deals.id"),
        nullable=False
    )

    amount = db.Column(
        db.Numeric(12, 2),
        nullable=False
    )
    
    pseudonym = db.Column(db.String(100))

    status = db.Column(
        db.String(50),
        default="pending"
    )

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

'''
class SpvSection(db.Model):

    __tablename__ = "spv_sections"

    id = db.Column(db.Integer, primary_key=True)

    deal_id = db.Column(
        db.Integer,
        db.ForeignKey("spv_deals.id"),
        nullable=False
    )

    slug = db.Column(db.String(120), nullable=False)

    title = db.Column(db.String(255), nullable=False)

    summary = db.Column(db.Text)

    content = db.Column(db.Text)

    sort_order = db.Column(db.Integer, default=0)

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    assets = db.relationship(
        "SpvAsset",
        back_populates="section",
        lazy=True,
        order_by="SpvAsset.sort_order"
    )
''' 

class SpvAsset(db.Model):

    __tablename__ = "spv_assets"

    id = db.Column(db.Integer, primary_key=True)

    section_id = db.Column(
        db.Integer,
        db.ForeignKey("spv_sections.id"),
        nullable=False
    )

    title = db.Column(db.String(255), nullable=False)

    file_path = db.Column(db.String(500))

    asset_type = db.Column(db.String(50))

    external_url = db.Column(db.String(500))

    sort_order = db.Column(db.Integer, default=0)

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    section = db.relationship(
        "SpvSection",
        back_populates="assets"
    )

class SpvSection(db.Model):

    __tablename__ = "spv_sections"

    id = db.Column(db.Integer, primary_key=True)

    deal_id = db.Column(
        db.Integer,
        db.ForeignKey("spv_deals.id"),
        nullable=False
    )

    slug = db.Column(db.String(120), nullable=False)

    title = db.Column(db.String(255), nullable=False)

    summary = db.Column(db.Text)

    content = db.Column(db.Text)

    sort_order = db.Column(
        db.Integer,
        default=0
    )

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    deal = db.relationship(
        "SpvDeal",
        backref="sections"
    )

    assets = db.relationship(
        "SpvAsset",
        back_populates="section",
        lazy=True,
        order_by="SpvAsset.sort_order"
    )




