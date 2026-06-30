# models_billing.py


from app.extensions import db
from sqlalchemy import (
    Column, Integer, String, Float, Date,
    Boolean, ForeignKey, CheckConstraint, Index, func)

from datetime import date, datetime
from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, SubmitField
from wtforms.validators import DataRequired
from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import validates, relationship
from sqlalchemy.orm import synonym
from app.extensions import db  # adjust import



class BilProperty(db.Model):
    __tablename__ = 'bil_property'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    address = db.Column(db.String(200))
    municipal_bill_number = db.Column(db.String(100))
    description = db.Column(db.Text)
    
    # Financials / Arrangements
    metro_arrangement_amount = db.Column(db.Float, default=0.0)
    metro_arrangement_duration = db.Column(db.Integer, default=0)
    metro_rates_amount = db.Column(db.Float, default=0.0)

    # 👤 Link to the manager who owns this property
    manager_id = db.Column(
        db.Integer,
        db.ForeignKey('user.id'),
        nullable=True
    )

    # Link to the user enrollment record
    enrollment_id = db.Column(
        db.Integer,
        db.ForeignKey('user_enrollment.id'),
        nullable=True
    )

    # Onboarding Status & Progress
    onboarding_status = db.Column(db.String(50), default='completed') # e.g. draft_extracting, draft_collating, draft_readings, completed
    expected_bills = db.Column(db.Integer, default=1)
    expected_tenants = db.Column(db.Integer, default=1)
    is_bulk_metered = db.Column(db.Integer, default=0)
    expected_sub_meters = db.Column(db.Integer, default=0)

    sectional_units = db.relationship('BilSectionalUnit', backref='property', lazy=True)
    manager = db.relationship('User', backref='managed_properties')

class BilMeter(db.Model):
    __tablename__ = 'bil_meter'
    __table_args__ = {'extend_existing': True}

    id = db.Column(db.Integer, primary_key=True)
    meter_number = db.Column(db.String(50), nullable=False)
    utility_type = db.Column(db.String(50), nullable=False)  # ✅ Add this line
    parent_meter_id = db.Column(db.Integer, db.ForeignKey('bil_meter.id'), nullable=True)
    pointing_to = db.Column(db.String(100), nullable=True)
    municipal_bill_number = db.Column(db.String(100), nullable=True)

    # BilMeter
    sectional_unit_id = db.Column(db.Integer, db.ForeignKey('bil_sectional_unit.id'), nullable=False)
    sectional_unit = db.relationship("BilSectionalUnit", back_populates="meters")



    def __repr__(self):
        return f'<BilMeter {self.meter_number}>'

class BilMeterReading(db.Model):
    __tablename__ = 'bil_meter_reading'
    id = db.Column(db.Integer, primary_key=True)
    meter_id = db.Column(db.Integer, db.ForeignKey('bil_meter.id'), nullable=False)
    reading_date = db.Column(db.Date, nullable=False)
    reading_value = db.Column(db.Float, nullable=False)

    meter = db.relationship('BilMeter', backref='readings')

class BilTariff(db.Model):
    __tablename__ = 'bil_tariff'

    id = db.Column(db.Integer, primary_key=True)
    utility_type = db.Column(db.String, nullable=False)      # e.g. 'electricity', 'water', 'sanitation'
    code = db.Column(db.String, nullable=False)              # e.g. 'ElecRate', 'Tier1_W&S'
    description = db.Column(db.String)                       # For display purposes
    rate = db.Column(db.Float, nullable=False)               # The actual rate (unit or fixed)
    block_start = db.Column(db.Float, default=0.0)           # Start of tier (for block/tiered rates)
    block_end = db.Column(db.Float, default=0.0)             # End of tier
    effective_date = db.Column(db.String, nullable=False)    # e.g. '2025-06-01'
    reduction_factor = db.Column(db.Float, default=1.0)
    unit = db.Column(db.String(20))

class BilFixedItem(db.Model):
    __tablename__ = 'bil_fixed_item'
    id = Column(Integer, primary_key=True)
    description = Column(String(100), nullable=False)
    utility_type = Column(String(10), nullable=False)  # ELE, WTR, etc.
    default_amount = Column(Float, nullable=False)
    charge_frequency = Column(String(20), default='monthly')  # monthly, once-off

class BilMeterFixedCharge(db.Model):
    __tablename__ = "bil_meter_fixed_charge"

    id = db.Column(db.Integer, primary_key=True)
    meter_id = db.Column(db.Integer, db.ForeignKey("bil_meter.id"), nullable=False)
    month = db.Column(db.String(7), nullable=False)  # Format: 'YYYY-MM'
    description = db.Column(db.String(100), nullable=False)  # e.g. 'Water Loss Levy'
    utility_type = db.Column(db.String(10))  # 'water', 'sanitation', 'refuse', etc.
    amount = db.Column(db.Float, nullable=False)
    rate = db.Column(db.Float)      # Optional: for dynamic charges
    cons = db.Column(db.Float)      # Optional: consumption used for charge calculation

    # Relationships
    meter = db.relationship("BilMeter", backref="fixed_charges")

    def __repr__(self):
        return f"<FixedCharge {self.description} for meter {self.meter_id} @ {self.amount}>"

class BilPayment(db.Model):
    __tablename__ = 'bil_municipal_payment'
    id = Column(Integer, primary_key=True)
    property_id = Column(Integer, ForeignKey('bil_property.id'), nullable=False)
    payment_date = Column(Date, nullable=False)
    metro_account_number = Column(String(100))
    deposit_amount = Column(Float)
    arrears = Column(Float)
    due_date = Column(Date)

    property = relationship('BilProperty')

class BilConsumption(db.Model):
    __tablename__ = 'bil_consumption'
    id = Column(Integer, primary_key=True)
    meter_id = Column(Integer, ForeignKey('bil_meter.id'), nullable=False)
    meter_number = db.Column(db.String, nullable=False)  # ✅ Add this
    last_date = Column(Date, nullable=False)
    new_date = Column(Date, nullable=False)
    last_read = Column(Float, nullable=False)
    new_read = Column(Float, nullable=False)
    days = Column(Integer, nullable=False)
    consumption = Column(Float, nullable=False)
    month = db.Column(db.String)  # 🔧 Add this line to match the table
    meter = relationship('BilMeter')

class PropertyForm(FlaskForm):
    name = StringField("Property Name", validators=[DataRequired()])
    location = StringField("Location", validators=[DataRequired()])
    type = SelectField("Property Type", choices=[
        ("residential", "Residential"),
        ("commercial", "Commercial"),
        ("mixed_use", "Mixed-Use")
    ])
    submit = SubmitField("Add Property")   

class BilLease(db.Model):
    __tablename__ = "bil_lease"

    id = db.Column(db.Integer, primary_key=True)
    tenant_id = db.Column(db.Integer, db.ForeignKey("bil_tenant.id"), nullable=False)
    sectional_unit_id = db.Column(db.Integer, db.ForeignKey("bil_sectional_unit.id"))

    start_date   = db.Column(db.Text)
    end_date     = db.Column(db.Text)
    rent_amount  = db.Column(db.Numeric, nullable=True)
    day_of_month = db.Column(db.Integer, nullable=True)
    notes        = db.Column(db.Text, nullable=True)
    
    # Pass-Through Charges
    tenant_arrangement_charge = db.Column(db.Float, default=0.0)
    tenant_rates_charge = db.Column(db.Float, default=0.0)
    tenant_arrears_total = db.Column(db.Float, default=0.0)
    tenant_arrears_installment = db.Column(db.Float, default=0.0)
    agent_fee_amount = db.Column(db.Float, default=0.0)
    agent_fee_target = db.Column(db.String(50), default='owner') # 'tenant' or 'owner'

    tenant = db.relationship("BilTenant", back_populates="leases", lazy="joined")

    # legacy aliases (optional)
    lease_start = synonym("start_date")
    lease_end   = synonym("end_date")

    @property
    def is_active(self):
        today = datetime.utcnow().date()
        return self.lease_start <= today and (self.lease_end is None or self.lease_end >= today)

    def __repr__(self):
        return f"<BilLease id={self.id} tenant_id={self.tenant_id} unit_id={self.sectional_unit_id}>"

class BilSectionalUnit(db.Model):
    __tablename__ = "bil_sectional_unit"

    id   = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False, unique=True, index=True)
    property_id = db.Column(db.Integer, db.ForeignKey('bil_property.id'), nullable=True)

    # Match BilMeter.sectional_unit  (cascade delete meters when a unit is removed)
    meters = db.relationship(
        "BilMeter",
        back_populates="sectional_unit",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    # Match BilTenant.sectional_unit  (don’t cascade-delete tenants)
    tenants = db.relationship(
        "BilTenant",
        back_populates="sectional_unit",
    )

    def __repr__(self):
        return f"<BilSectionalUnit id={self.id} name={self.name!r}>"


class BilBankDetail(db.Model):
    __tablename__ = "bil_bank_detail"

    id = db.Column(db.Integer, primary_key=True)
    bank_name = db.Column(db.String(100), nullable=False)
    branch_name = db.Column(db.String(100))
    branch_code = db.Column(db.String(50))
    account_number = db.Column(db.String(100), nullable=False)
    account_holder = db.Column(db.String(150))
    account_type = db.Column(db.String(50))

    def __repr__(self):
        return f"<BilBankDetail {self.bank_name} - {self.account_number}>"

class BilTenant(db.Model):
    __tablename__ = "bil_tenant"

    id   = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)

    sectional_unit_id = db.Column(
        db.Integer, db.ForeignKey('bil_sectional_unit.id'), nullable=False, index=True
    )
    sectional_unit = db.relationship("BilSectionalUnit", back_populates="tenants")

    metro_account_no     = db.Column(db.String(64), index=True)
    rent_includes_metro  = db.Column(db.Integer, default=0, nullable=False)  # 0/1
    email                = db.Column(db.String(255), index=True)
    email_statements     = db.Column(db.Boolean, default=False)
    phone                = db.Column(db.String(50), index=True)
    bank_detail_id = db.Column(db.Integer, db.ForeignKey('bil_bank_detail.id'), nullable=True)
    bank_detail = db.relationship("BilBankDetail")

    notes                = db.Column(db.Text)

    leases = db.relationship(
        "BilLease",
        back_populates="tenant",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="BilLease.lease_start.desc()",
    )

    def __repr__(self):
        return f"<BilTenant id={self.id} name={self.name!r}>"



    # If statements are per-tenant (keep if you have BilStatement)
    statements = relationship(
        "BilStatement",
        back_populates="tenant",
        passive_deletes=True
    )

    # If meters are attached to the UNIT (your current design), you’ll traverse via unit:
    # meters = association_proxy('unit', 'meters')  # only if you use association_proxy

class BilStatement(db.Model):
    __tablename__ = "bil_statement"

    id = db.Column(db.Integer, primary_key=True)
    tenant_id = db.Column(db.Integer, db.ForeignKey("bil_tenant.id"), nullable=False)

    # add your other columns here, e.g. period_start, period_end, total_due, etc.

    tenant = db.relationship("BilTenant", back_populates="statements")


class BilTenantLedger(db.Model):
    __tablename__ = "bil_tenant_ledger"

    id = db.Column(db.Integer, primary_key=True)
    tenant_id = db.Column(db.Integer, db.ForeignKey('bil_tenant.id'), nullable=False, index=True)
    txn_date = db.Column(db.DateTime)
    month = db.Column(db.String(7))
    description = db.Column(db.String(255))
    kind = db.Column(db.String(50))
    amount = db.Column(db.Numeric(10, 2))
    ref = db.Column(db.String(255))
    
    tenant = db.relationship("BilTenant", backref="ledger_entries")


class BilTenantRecurring(db.Model):
    __tablename__ = "bil_tenant_recurring"

    id = db.Column(db.Integer, primary_key=True)
    tenant_id = db.Column(db.Integer, db.ForeignKey('bil_tenant.id'), nullable=False, index=True)
    description = db.Column(db.String(255))
    kind = db.Column(db.String(50))
    amount = db.Column(db.Numeric(10, 2))
    day_of_month = db.Column(db.Integer, default=1)
    is_active = db.Column(db.Integer, default=1)

    tenant = db.relationship("BilTenant", backref="recurring_items")

class BilMetsoaTenantMonth(db.Model):
    __tablename__ = "bil_metsoa_tenant_month"
    id = db.Column(db.Integer, primary_key=True)
    tenant_id = db.Column(db.Integer, nullable=False)
    month = db.Column(db.String(7), nullable=False)
    ws_total = db.Column(db.Float, default=0.0)
    sd_total = db.Column(db.Float, default=0.0)
    water_total = db.Column(db.Float, default=0.0)
    updated_at = db.Column(db.DateTime)
    __table_args__ = (db.UniqueConstraint('tenant_id', 'month', name='uq_metsoa_tenant_month'),)

class BilMetsoaMeterMonth(db.Model):
    __tablename__ = "bil_metsoa_meter_month"
    id = db.Column(db.Integer, primary_key=True)
    tenant_id = db.Column(db.Integer, nullable=False)
    meter_id = db.Column(db.Integer, nullable=False)
    month = db.Column(db.String(7), nullable=False)
    utility_type = db.Column(db.String(50))
    prev_date = db.Column(db.String(50))
    prev_read = db.Column(db.Float)
    curr_date = db.Column(db.String(50))
    curr_read = db.Column(db.Float)
    days = db.Column(db.Integer)
    consumption = db.Column(db.Float)
    elec_rate = db.Column(db.Float)
    elec_due = db.Column(db.Float)
    ws_total = db.Column(db.Float, default=0.0)
    sd_total = db.Column(db.Float, default=0.0)
    water_cost = db.Column(db.Float, default=0.0)
    total_due = db.Column(db.Float, default=0.0)
    updated_at = db.Column(db.DateTime)
    __table_args__ = (db.UniqueConstraint('tenant_id', 'meter_id', 'month', name='uq_metsoa_meter_month'),)

def __repr__(self):
        return f"<BilTenant id={self.id} name={self.name!r} active={self.is_active}>"

    # --- Validators (db-agnostic, safer than regex CHECK in SQLite) ---
@validates("email")
def _validate_email(self, key, value):
        if value:
            v = value.strip()
            # extremely light check; do heavy validation in forms
            if "@" not in v or "." not in v:
                raise ValueError("Invalid email address")
            return v
        return value

@validates("phone")
def _validate_phone(self, key, value):
        return value.strip() if value else value



# --- Municipal Accounts & Ledger ---

class RefMuniOwner(db.Model):
    __tablename__ = "ref_muni_owner"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)

class BilMuniAccount(db.Model):
    __tablename__ = "bil_muni_account"
    id = db.Column(db.Integer, primary_key=True)
    account_number = db.Column(db.String(50), nullable=False, unique=True)
    owner_id = db.Column(db.Integer, db.ForeignKey('ref_muni_owner.id', ondelete='RESTRICT'), nullable=False)
    muni_email = db.Column(db.String(255))
    water_meter_id = db.Column(db.Integer, db.ForeignKey('bil_meter.id', ondelete='SET NULL'))
    elec_meter_id = db.Column(db.Integer, db.ForeignKey('bil_meter.id', ondelete='SET NULL'))
    muni_water_meter_no = db.Column(db.String(50))
    muni_water_ref = db.Column(db.String(50))
    muni_elec_meter_no = db.Column(db.String(50))
    muni_elec_ref = db.Column(db.String(50))

class BilMuniCycleTotals(db.Model):
    __tablename__ = "bil_muni_cycle_totals"
    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey('bil_muni_account.id', ondelete='CASCADE'), nullable=False)
    period = db.Column(db.String(7), nullable=False)
    balance = db.Column(db.Float, default=0.0, nullable=False)
    due = db.Column(db.Float, default=0.0, nullable=False)
    arrears = db.Column(db.Float, default=0.0, nullable=False)
    paid = db.Column(db.Float, default=0.0, nullable=False)
    __table_args__ = (db.UniqueConstraint('account_id', 'period', name='uq_muni_cycle_account_period'),)

class BilMetsoaCycle(db.Model):
    __tablename__ = "bil_metsoa_cycle"
    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey('bil_muni_account.id', ondelete='CASCADE'), nullable=False)
    period = db.Column(db.String(7), nullable=False)
    metsoa_due = db.Column(db.Float, default=0.0, nullable=False)
    __table_args__ = (db.UniqueConstraint('account_id', 'period', name='uq_metsoa_cycle_account_period'),)

class BilMeterChargeMap(db.Model):
    __tablename__ = "bil_meter_charge_map"
    id = db.Column(db.Integer, primary_key=True)
    meter_id = db.Column(db.Integer, db.ForeignKey('bil_meter.id', ondelete='CASCADE'), nullable=False)
    charge_code = db.Column(db.String(50), nullable=False)
    utility_type = db.Column(db.String(50))
    effective_start = db.Column(db.Date)
    effective_end = db.Column(db.Date)
    is_enabled = db.Column(db.Integer, default=1)
    tariff_code_override = db.Column(db.String(50))

class BilMuniPayment(db.Model):
    __tablename__ = "bil_muni_payment"
    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey('bil_muni_account.id', ondelete='CASCADE'), nullable=False)
    month = db.Column(db.String(7), nullable=False) # e.g. '2026-06'
    payment_date = db.Column(db.Date, nullable=False)
    amount = db.Column(db.Float, default=0.0, nullable=False)
    reference = db.Column(db.String(100))
    account = db.relationship('BilMuniAccount', backref=db.backref('payments', lazy=True, cascade='all, delete-orphan'))

class BilMetroReadingLog(db.Model):
    __tablename__ = "bil_metro_reading_log"
    id = db.Column(db.Integer, primary_key=True)
    meter_number = db.Column(db.String(50), nullable=False)
    meter_id = db.Column(db.Integer, db.ForeignKey('bil_meter.id', ondelete='CASCADE'), nullable=False)
    account_number = db.Column(db.String(50), nullable=False)
    reading_date = db.Column(db.Date, nullable=False)
    reading_value = db.Column(db.Float, nullable=False)
    billing_period = db.Column(db.String(7), nullable=False) # e.g. '2026-06'
    metro_email = db.Column(db.String(255))
    sent_at = db.Column(db.DateTime, default=datetime.utcnow)

class BilPlatformSettings(db.Model):
    __tablename__ = 'bil_platform_settings'
    id = db.Column(db.Integer, primary_key=True)
    base_price_cents = db.Column(db.Integer, default=10000)
    included_meters = db.Column(db.Integer, default=2)
    extra_meter_price_cents = db.Column(db.Integer, default=1500)

class BilStatementPayment(db.Model):
    __tablename__ = 'bil_statement_payment'
    id = db.Column(db.Integer, primary_key=True)
    manager_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    month = db.Column(db.String(7), nullable=False)  # 'YYYY-MM'
    meters_billed = db.Column(db.Integer, nullable=False, default=0)
    amount_paid_cents = db.Column(db.Integer, nullable=False, default=0)
    paid_at = db.Column(db.DateTime, default=func.now())
    
    __table_args__ = (db.UniqueConstraint('manager_id', 'month', name='uq_manager_month_payment'),)

class BilExtractionLog(db.Model):
    __tablename__ = 'bil_extraction_log'
    id = db.Column(db.Integer, primary_key=True)
    manager_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    property_name = db.Column(db.String(255), nullable=True)
    address = db.Column(db.Text, nullable=True)
    metro_account_no = db.Column(db.String(100), nullable=True)
    muni_email = db.Column(db.String(255), nullable=True)
    has_rates = db.Column(db.Boolean, default=False)
    rates_amount = db.Column(db.Float, default=0.0)
    amount_due = db.Column(db.Float, default=0.0)
    raw_json = db.Column(db.JSON, nullable=True)
    created_at = db.Column(db.DateTime, default=func.now())
