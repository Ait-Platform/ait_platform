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




class BilProperty(db.Model):
    __tablename__ = 'bil_property'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    address = db.Column(db.String(200))
    description = db.Column(db.Text)

    # 👤 Link to the manager who owns this property
    manager_id = db.Column(
        db.Integer,
        db.ForeignKey('user.id'),
        nullable=True
    )

    manager = db.relationship('User', backref='managed_properties')
'''
class BilSectionalUnit(db.Model):
    __tablename__ = 'bil_sectional_unit'
    __table_args__ = {'extend_existing': True}

    id = db.Column(db.Integer, primary_key=True)
    unit_number = db.Column(db.String(50), nullable=False)
    property_id = db.Column(db.Integer, db.ForeignKey('bil_property.id'), nullable=False)

    # 🧭 Reverse relationship
    meters = db.relationship('BilMeter', back_populates='sectional_unit')

    def __repr__(self):
        return f'<BilSectionalUnit {self.unit_number}>'

class BilTenant(db.Model):
    __tablename__ = 'bil_tenant'
    __table_args__ = {'extend_existing': True}

    id = db.Column(db.Integer, primary_key=True)

    # 🔗 Link to the sectional unit this tenant occupies
    sectional_unit_id = db.Column(
        db.Integer,
        db.ForeignKey('bil_sectional_unit.id'),
        nullable=False
    )

    # 🧾 Tenant details
    name = db.Column(db.String(100), nullable=False)
    unit_label = db.Column(db.String(50))
    rent_amount = db.Column(db.Float)

    # 📅 Lease info
    start_date = db.Column(db.Date, default=date.today)
    lease_duration_months = db.Column(db.Integer, default=12)  # e.g., 12-month lease
    end_date = db.Column(db.Date)  # Can be auto-set from start_date + duration
    reminder_date = db.Column(db.Date)  # Optional — 3 months before end_date

    # 👤 Link to the user account for login
    user_id = db.Column(
        db.Integer,
        db.ForeignKey('user.id'),
        nullable=False
    )

    # ↔️ Relationships
    sectional_unit = db.relationship('BilSectionalUnit', backref='tenants')
    user = db.relationship('User', backref='tenant_profile')

    def __repr__(self):
        label = self.unit_label or self.name
        return f"<BilTenant {label}>"

    # 🔹 Auto-set end_date & reminder_date when saving
    def set_lease_dates(self):
        if self.start_date and self.lease_duration_months:
            self.end_date = self.start_date + relativedelta(months=self.lease_duration_months)
            self.reminder_date = self.end_date - relativedelta(months=3)
'''
class BilMeter(db.Model):
    __tablename__ = 'bil_meter'
    __table_args__ = {'extend_existing': True}

    id = db.Column(db.Integer, primary_key=True)
    meter_number = db.Column(db.String(50), nullable=False, unique=True)
    utility_type = db.Column(db.String(50), nullable=False)  # ✅ Add this line

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
''''''
# 🏗️ Property Onboarding Form
class PropertyForm(FlaskForm):
    name = StringField("Property Name", validators=[DataRequired()])
    location = StringField("Location", validators=[DataRequired()])
    type = SelectField("Property Type", choices=[
        ("residential", "Residential"),
        ("commercial", "Commercial"),
        ("mixed_use", "Mixed-Use")
    ])
    submit = SubmitField("Add Property")   

# models/lease.py



# or from sqlalchemy.orm import relationship
# but here we'll keep db.relationship for consistency

# models.py (or wherever BilLease is defined)
from sqlalchemy.orm import synonym
from app.extensions import db  # adjust import



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


# models/sectional_unit.py


# models/billing/sectional_unit.py
class BilSectionalUnit(db.Model):
    __tablename__ = "bil_sectional_unit"

    id   = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False, unique=True, index=True)

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
    phone                = db.Column(db.String(50), index=True)
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

__table_args__ = (
        CheckConstraint("rent_includes_metro IN (0, 1)", name="ck_tenant_rent_includes_metro"),
        # Optional: prevent empty string names
        CheckConstraint("length(trim(coalesce(name, ''))) > 0", name="ck_tenant_name_not_empty"),
        # Helpful composite index for admin search
        Index("ix_tenant_name_unit", "name", "sectional_unit_id"),
    )

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



# models/billing/tenant.py  (unchanged)

class BilStatement(db.Model):
    __tablename__ = "bil_statement"

    id = db.Column(db.Integer, primary_key=True)
    tenant_id = db.Column(db.Integer, db.ForeignKey("bil_tenant.id"), nullable=False)

    # add your other columns here, e.g. period_start, period_end, total_due, etc.

    tenant = db.relationship("BilTenant", back_populates="statements")








