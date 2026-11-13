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
    vat_rate = db.Column(db.Float, default=0.0)
    logo_url = db.Column(db.String(255))
    letterhead_url = db.Column(db.String(255))
    use_custom_letterhead = db.Column(db.Boolean, default=False)
    terms_and_conditions = db.Column(db.Text)
    bank_details = db.Column(db.Text)
    trade = db.Column(db.String(50), default='mechanic')
    onboarding_status = db.Column(db.String(50), default='draft_setup') # draft_setup, draft_review, active
    shadow_spent_cents = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class MechCatalogPart(db.Model):
    __tablename__ = 'mech_catalog_parts'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True) # Null for global pre-populated, specific for learned
    shop_id = db.Column(db.Integer, db.ForeignKey('mech_shops.id'), nullable=True)
    trade = db.Column(db.String(50), nullable=True) # NULL denotes legacy mechanic pricing
    part_name = db.Column(db.String(150), nullable=False)
    category = db.Column(db.String(50)) # e.g. Engine, Brakes, Suspension
    default_price = db.Column(db.Float, default=0.0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class ProTradeBoqItem(db.Model):
    """Reusable per-unit BOQ pricing; enabled for Carpentry first."""
    __tablename__ = 'protrade_boq_items'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    shop_id = db.Column(db.Integer, db.ForeignKey('mech_shops.id'), nullable=False)
    trade = db.Column(db.String(50), nullable=False)
    code = db.Column(db.String(50), nullable=False)
    description = db.Column(db.Text, nullable=False)
    unit = db.Column(db.String(8), nullable=False)
    material_rate = db.Column(db.Numeric(14, 2), nullable=False)
    labour_rate = db.Column(db.Numeric(14, 2), nullable=False)
    total_rate = db.Column(db.Numeric(15, 2), db.Computed('material_rate + labour_rate', persisted=True))
    quantity = db.Column(db.Numeric(14, 3), nullable=False, default=1, server_default='1')
    total_amount = db.Column(db.Numeric(28, 2), db.Computed('quantity * (material_rate + labour_rate)', persisted=True))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    __table_args__ = (
        db.UniqueConstraint('shop_id', 'trade', 'code', name='uq_protrade_boq_shop_trade_code'),
        db.CheckConstraint('quantity > 0 AND quantity <= 99999999999.999', name='ck_protrade_boq_quantity'),
        db.CheckConstraint("unit <> 'Each' OR quantity = trunc(quantity)", name='ck_protrade_boq_each_quantity'),
        db.CheckConstraint("code = upper(trim(code)) AND length(code) > 0", name='ck_protrade_boq_code'),
        db.CheckConstraint("length(trim(description)) > 0", name='ck_protrade_boq_description'),
        db.CheckConstraint("unit IN ('Each', 'm', 'm\u00b2', 'm\u00b3', 'Hour')", name='ck_protrade_boq_unit'),
        db.CheckConstraint('material_rate BETWEEN 0 AND 999999999999.99 AND labour_rate BETWEEN 0 AND 999999999999.99', name='ck_protrade_boq_rates'),
    )


class ProTradeBoqQuote(db.Model):
    """Saved BOQ quote snapshots, independent of vehicle job cards."""
    __tablename__ = 'protrade_boq_quotes'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    shop_id = db.Column(db.Integer, db.ForeignKey('mech_shops.id'), nullable=False)
    debtor_id = db.Column(db.Integer, db.ForeignKey('debtor.id'), nullable=True)
    client_id = db.Column(db.Integer, db.ForeignKey('mech_clients.id'), nullable=True)
    is_draft = db.Column(db.Boolean, nullable=True)
    trade = db.Column(db.String(50), nullable=False)
    quote_number = db.Column(db.String(50), nullable=False, unique=True)
    quotation_seq = db.Column(db.Integer, server_default=db.FetchedValue())

    @property
    def display_quote_number(self):
        return f'Q-{self.quotation_seq:04d}' if self.quotation_seq is not None else self.quote_number
    client_name = db.Column(db.String(150), nullable=False)
    client_phone = db.Column(db.String(50))
    client_email = db.Column(db.String(150))
    client_address = db.Column(db.Text)
    items = db.Column(db.JSON, nullable=False)
    grand_total = db.Column(db.Numeric(28, 2), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    __table_args__ = (
        db.UniqueConstraint('shop_id','quotation_seq',name='uq_boq_shop_quote_number'),
        db.Index('uq_protrade_boq_open_draft','shop_id','debtor_id',unique=True,postgresql_where=db.text('is_draft IS TRUE AND debtor_id IS NOT NULL')),
        db.CheckConstraint("trade = 'carpentry'", name='ck_protrade_boq_quote_trade'),
        db.CheckConstraint('grand_total >= 0', name='ck_protrade_boq_quote_total'),
    )


class ProTradeOutputEmail(db.Model):
    """Durable at-most-once SMTP attempt for a confirmed Output preview."""
    __tablename__ = 'protrade_output_email'
    id = db.Column(db.Integer, primary_key=True)
    request_key = db.Column(db.String(64), nullable=False, unique=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    shop_id = db.Column(db.Integer, db.ForeignKey('mech_shops.id'), nullable=False)
    debtor_id = db.Column(db.Integer, db.ForeignKey('debtor.id'), nullable=False)
    selection = db.Column(db.String(100), nullable=False)
    recipient = db.Column(db.String(150), nullable=False)
    status = db.Column(db.String(30), nullable=False, default='attempted')
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class MechClient(db.Model):
    __tablename__ = 'mech_clients'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True) # Optional linking to main auth
    shop_id = db.Column(db.Integer, db.ForeignKey('mech_shops.id'), nullable=True)
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
    vin = db.Column(db.String(50))
    license_plate = db.Column(db.String(20))
    engine_no = db.Column(db.String(50))
    gvm = db.Column(db.String(20))
    tare = db.Column(db.String(20))
    disk_license_no = db.Column(db.String(50))
    mileage = db.Column(db.Integer)
    license_disk_url = db.Column(db.String(500))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    job_cards = db.relationship('MechJobCard', backref='vehicle', lazy=True)

class MechJobCard(db.Model):
    __tablename__ = 'mech_job_cards'
    id = db.Column(db.Integer, primary_key=True)
    shop_id = db.Column(db.Integer, db.ForeignKey('mech_shops.id'), nullable=True)
    debtor_id = db.Column(db.Integer, db.ForeignKey('debtor.id'), nullable=True)
    is_draft = db.Column(db.Boolean, nullable=True)
    job_number = db.Column(db.String(50), unique=True, nullable=False)
    quotation_seq = db.Column(db.Integer, server_default=db.FetchedValue())

    @property
    def display_quote_number(self):
        return f'Q-{self.quotation_seq:04d}' if self.quotation_seq is not None else self.job_number

    @property
    def display_document_number(self):
        return self.display_quote_number if self.status in ('Quote','Rejected') else self.job_number
    vehicle_id = db.Column(db.Integer, db.ForeignKey('mech_vehicles.id'), nullable=False)
    status = db.Column(db.String(50), default='Quote') # Quote, Approved, In Progress, Quality Check, Ready, Billed
    description = db.Column(db.Text)
    vat_rate = db.Column(db.Float, default=0.0)
    deposit_amount = db.Column(db.Float, default=0.0)
    payment_method = db.Column(db.String(50), default='EFT')
    next_service_due = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime)
    
    __table_args__ = (db.UniqueConstraint('shop_id','quotation_seq',name='uq_mechanic_shop_quote_number'), db.Index('uq_protrade_mech_open_draft','shop_id','debtor_id',unique=True,postgresql_where=db.text("status = 'Quote' AND is_draft IS TRUE AND debtor_id IS NOT NULL")),)

    labor_lines = db.relationship('MechLaborLine', backref='job_card', lazy=True)
    part_lines = db.relationship('MechPartLine', backref='job_card', lazy=True)
    invoices = db.relationship('MechInvoice', backref='job_card', lazy=True)

class MechLaborLine(db.Model):
    __tablename__ = 'mech_labor_lines'
    id = db.Column(db.Integer, primary_key=True)
    job_card_id = db.Column(db.Integer, db.ForeignKey('mech_job_cards.id'), nullable=False)
    mechanic_name = db.Column(db.String(100))
    description = db.Column(db.String(255))
    time_in = db.Column(db.String(10)) # e.g. 08:00
    time_out = db.Column(db.String(10)) # e.g. 10:30
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




class MechCommunication(db.Model):
    __tablename__ = 'mech_communications'
    id = db.Column(db.Integer, primary_key=True)
    job_card_id = db.Column(db.Integer, db.ForeignKey('mech_job_cards.id'), nullable=False)
    comm_type = db.Column(db.String(50), nullable=False) # e.g. "Email", "Phone", "WhatsApp"
    recipient = db.Column(db.String(150), nullable=True) # e.g. email address or phone number
    message = db.Column(db.Text, nullable=True)
    status = db.Column(db.String(50), default='Logged')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    job_card = db.relationship('MechJobCard', backref=db.backref('communications', lazy=True))


class ProTradeQuoteCounter(db.Model):
    __tablename__ = 'protrade_quote_counter'
    shop_id = db.Column(db.Integer, db.ForeignKey('mech_shops.id'), primary_key=True)
    last_number = db.Column(db.Integer, nullable=False)
    __table_args__ = (db.CheckConstraint('last_number > 0', name='ck_protrade_quote_counter'),)


class ProTradeQuoteNumber(db.Model):
    __tablename__ = 'protrade_quote_number'
    shop_id = db.Column(db.Integer, db.ForeignKey('mech_shops.id'), primary_key=True)
    number = db.Column(db.Integer, primary_key=True)
    document_kind = db.Column(db.String(16), nullable=False)
    document_id = db.Column(db.Integer, nullable=False)
    __table_args__ = (
        db.UniqueConstraint('document_kind','document_id',name='uq_protrade_quote_document'),
        db.CheckConstraint("number > 0 AND document_kind IN ('boq','mechanic')", name='ck_protrade_quote_number'),
    )
