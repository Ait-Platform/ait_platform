from app.extensions import db
from datetime import datetime


class MechShop(db.Model):
    __tablename__ = 'mech_shops'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    business_name = db.Column(db.String(150))
    address = db.Column(db.Text)
    phone = db.Column(db.String(50))
    email = db.Column(db.String(150))
    registration_number = db.Column(db.String(100))
    tax_number = db.Column(db.String(100))
    logo_url = db.Column(db.String(255))
    terms_and_conditions = db.Column(db.Text)
    onboarding_status = db.Column(db.String(50), default='draft_setup') # draft_setup, draft_review, active
    wallet_balance_cents = db.Column(db.Integer, default=0, nullable=False)
    trial_ends_at = db.Column(db.DateTime, nullable=True)
    shadow_spent_cents = db.Column(db.Integer, default=0, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class MechCatalogPart(db.Model):
    __tablename__ = 'mech_catalog_parts'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True) # Null for global pre-populated, specific for learned
    part_name = db.Column(db.String(150), nullable=False)
    category = db.Column(db.String(50)) # e.g. Engine, Brakes, Suspension
    default_price = db.Column(db.Float, default=0.0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class MechClient(db.Model):
    __tablename__ = 'mech_clients'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True) # Optional linking to main auth
    name = db.Column(db.String(150), nullable=False)
    phone = db.Column(db.String(50))
    email = db.Column(db.String(150))
    address = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    vehicles = db.relationship('MechVehicle', backref='client', lazy=True)

class MechVehicle(db.Model):
    __tablename__ = 'mech_vehicles'
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey('mech_clients.id'), nullable=False)
    make = db.Column(db.String(50))
    model = db.Column(db.String(50))
    year = db.Column(db.Integer)
    vin = db.Column(db.String(50), unique=True)
    license_plate = db.Column(db.String(20))
    mileage = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    job_cards = db.relationship('MechJobCard', backref='vehicle', lazy=True)

class MechJobCard(db.Model):
    __tablename__ = 'mech_job_cards'
    id = db.Column(db.Integer, primary_key=True)
    job_number = db.Column(db.String(50), unique=True, nullable=False)
    vehicle_id = db.Column(db.Integer, db.ForeignKey('mech_vehicles.id'), nullable=False)
    status = db.Column(db.String(50), default='Intake') # Intake, Diagnosis, In Progress, Quality Check, Ready, Billed
    description = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime)
    
    labor_lines = db.relationship('MechLaborLine', backref='job_card', lazy=True)
    part_lines = db.relationship('MechPartLine', backref='job_card', lazy=True)
    invoices = db.relationship('MechInvoice', backref='job_card', lazy=True)

class MechLaborLine(db.Model):
    __tablename__ = 'mech_labor_lines'
    id = db.Column(db.Integer, primary_key=True)
    job_card_id = db.Column(db.Integer, db.ForeignKey('mech_job_cards.id'), nullable=False)
    mechanic_name = db.Column(db.String(100))
    description = db.Column(db.String(255))
    hours = db.Column(db.Float, default=0.0)
    rate_per_hour = db.Column(db.Float, default=0.0)

class MechPartLine(db.Model):
    __tablename__ = 'mech_part_lines'
    id = db.Column(db.Integer, primary_key=True)
    job_card_id = db.Column(db.Integer, db.ForeignKey('mech_job_cards.id'), nullable=False)
    part_number = db.Column(db.String(100))
    description = db.Column(db.String(255))
    quantity = db.Column(db.Integer, default=1)
    unit_cost = db.Column(db.Float, default=0.0)
    markup_price = db.Column(db.Float, default=0.0)

class MechInvoice(db.Model):
    __tablename__ = 'mech_invoices'
    id = db.Column(db.Integer, primary_key=True)
    job_card_id = db.Column(db.Integer, db.ForeignKey('mech_job_cards.id'), nullable=False)
    subtotal = db.Column(db.Float, default=0.0)
    tax = db.Column(db.Float, default=0.0)
    total = db.Column(db.Float, default=0.0)
    status = db.Column(db.String(50), default='Unpaid') # Unpaid, Paid, Void
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
