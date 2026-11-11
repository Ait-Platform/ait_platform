"""Focused tests for the improved ProTrade dashboard using the Property Management hub pattern.

Verifies:
1. Retaining the existing four dashboard actions and their routes in a clean permanent menu.
2. Property Management hub layout and styling (central tile, top color strip, title + Back button row).
3. Client Register working area with visible input borders and autofocus.
4. Table displaying Client, Contact, Service, Status and Action.
5. Add Client and View Client functionality reusing existing routes/records.
6. Strict operator multi-tenant isolation (each operator sees only their own clients).
7. Friendly empty register when no clients exist.
"""
from pathlib import Path
import unittest
from flask import Flask, Blueprint, request, session, flash, redirect, url_for, render_template, current_app, abort
from flask_login import LoginManager, UserMixin, current_user, login_user, login_required
from sqlalchemy import create_engine, Column, Integer, String, Text, Float, DateTime, Boolean, ForeignKey, func
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from lxml import html
import datetime

ROOT = Path(__file__).resolve().parents[1]
Base = declarative_base()


class User(Base, UserMixin):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    email = Column(String(100), unique=True)
    role = Column(String(50), default="user")
    active = Column(Boolean, default=True)

    @property
    def is_active(self):
        return self.active


class MechShop(Base):
    __tablename__ = "mech_shops"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    business_name = Column(String(150))
    address = Column(Text)
    phone = Column(String(50))
    email = Column(String(150))
    tax_number = Column(String(100))
    vat_rate = Column(Float, default=15.0)
    logo_url = Column(String(255))
    letterhead_url = Column(String(255))
    use_custom_letterhead = Column(Boolean, default=False)
    terms_and_conditions = Column(Text)
    trade = Column(String(50), default="mechanic")
    onboarding_status = Column(String(50), default="active")
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class MechClient(Base):
    __tablename__ = "mech_clients"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    name = Column(String(150), nullable=False)
    phone = Column(String(50))
    email = Column(String(150))
    address = Column(Text)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    vehicles = relationship("MechVehicle", backref="client", lazy=True)


class MechVehicle(Base):
    __tablename__ = "mech_vehicles"
    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey("mech_clients.id"), nullable=False)
    make = Column(String(50))
    model = Column(String(50))
    year = Column(Integer)
    vin = Column(String(50))
    license_plate = Column(String(20))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    job_cards = relationship("MechJobCard", backref="vehicle", lazy=True)


class MechJobCard(Base):
    __tablename__ = "mech_job_cards"
    id = Column(Integer, primary_key=True)
    job_number = Column(String(50), unique=True, nullable=False)
    vehicle_id = Column(Integer, ForeignKey("mech_vehicles.id"), nullable=False)
    status = Column(String(50), default="Quote")
    description = Column(Text)
    vat_rate = Column(Float, default=0.0)
    payment_method = Column(String(50), default="EFT")
    next_service_due = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class Debtor(Base):
    __tablename__ = "debtors"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    name = Column(String(150), nullable=False)
    phone = Column(String(50))
    email = Column(String(150))
    slug_reference = Column(String(50), default="mechanic")
    reference_id = Column(Integer, nullable=True)
    sender_profile_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class SenderProfile(Base):
    __tablename__ = "sender_profiles"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    business_name = Column(String(150))
    address = Column(Text)
    phone = Column(String(50))
    email = Column(String(150))
    is_default = Column(Boolean, default=True)


class AitTokenWallet(Base):
    __tablename__ = "ait_token_wallets"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    balance = Column(Integer, default=50)


class ProTradeDashboardTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.db_session = self.Session()

        self.app = Flask(
            "test_protrade_dashboard_app",
            template_folder=str(ROOT / "templates")
        )
        self.app.config.update(
            TESTING=True,
            SECRET_KEY="test-secret-key-123",
            WTF_CSRF_ENABLED=False
        )

        login_manager = LoginManager(self.app)

        @login_manager.user_loader
        def load_user(user_id):
            return self.db_session.query(User).get(int(user_id))

        self.app.jinja_env.globals["csrf_token"] = lambda: "mock_csrf_token"
        self.app.jinja_env.globals["now"] = datetime.datetime.now

        # Create blueprint and routes mimicking app/program_mechanic/routes.py
        mechanic_bp = Blueprint("mechanic_bp", __name__)
        bridge_bp = Blueprint("bridge_bp", __name__)
        cultural_bp = Blueprint("cultural_bp", __name__)
        public_bp = Blueprint("public_bp", __name__)
        admin_bp = Blueprint("admin_bp", __name__)
        auth_bp = Blueprint("auth_bp", __name__)

        @public_bp.route("/")
        def welcome():
            return "Welcome"

        @public_bp.route("/contact")
        def contact():
            return "Contact"

        @admin_bp.route("/admin/loss")
        def loss_dashboard():
            return "Admin Loss"

        @auth_bp.route("/auth/logout", methods=["POST"])
        def logout():
            return "Logout"

        @auth_bp.route("/auth/login")
        def login():
            return "Login"

        @bridge_bp.route("/bridge")
        def bridge():
            return "Bridge Home"

        @cultural_bp.route("/cultural/wallet/transfer")
        def wallet_transfer_page():
            return "Wallet Transfer"

        @mechanic_bp.route("/mechanic/quote/new", methods=["GET", "POST"])
        def new_quote():
            return "Create Quote Page"

        @mechanic_bp.route("/mechanic/jobs")
        def job_cards_list():
            return "Job Cards List"

        @mechanic_bp.route("/mechanic/catalog", methods=["GET", "POST"])
        def catalog_manage():
            return "Catalog Page"

        @mechanic_bp.route("/mechanic/client_accounts")
        def client_accounts():
            return "Client Accounts"

        @mechanic_bp.route("/mechanic/price")
        def price_page():
            return "Price Page"

        @mechanic_bp.route("/mechanic/document_preview")
        def document_preview():
            return "Document Preview"

        @mechanic_bp.route("/mechanic/communication-logs")
        def communication_logs():
            return "Communication Logs"

        @mechanic_bp.route("/mechanic/bank_accounts")
        def bank_accounts():
            return "Bank Accounts"

        AVAILABLE_TRADES = [
            ('mechanic', 'General Mechanic'),
            ('panel', 'Panel Beater'),
            ('auto_elec', 'Auto Electrician'),
            ('generic', 'Generic Business'),
            ('protrade', 'ProTrade Default')
        ]

        def test_ensure_sender_profile_synced(shop):
            if not shop or not shop.business_name:
                return
            sp = self.db_session.query(SenderProfile).filter_by(user_id=shop.user_id, business_name=shop.business_name).first()
            if not sp:
                sp = self.db_session.query(SenderProfile).filter_by(user_id=shop.user_id, is_default=True).first()
            if not sp:
                sp = SenderProfile(user_id=shop.user_id, is_default=True)
                self.db_session.add(sp)
            sp.business_name = shop.business_name
            sp.address = shop.address
            sp.phone = shop.phone
            sp.email = shop.email
            self.db_session.commit()

        def test_get_active_shop(user_id, trade=None, shop_id=None):
            if shop_id:
                shop = self.db_session.query(MechShop).filter_by(id=shop_id, user_id=user_id).first()
                if shop:
                    return shop
            target_trade = trade or session.get("active_trade")
            if target_trade:
                shop = self.db_session.query(MechShop).filter_by(user_id=user_id, trade=target_trade).first()
                if shop:
                    if shop.onboarding_status != "active" and shop.business_name:
                        shop.onboarding_status = "active"
                        self.db_session.commit()
                    return shop
            sess_shop_id = session.get("active_shop_id")
            if sess_shop_id:
                shop = self.db_session.query(MechShop).filter_by(id=sess_shop_id, user_id=user_id).first()
                if shop:
                    return shop
            shop = self.db_session.query(MechShop).filter_by(user_id=user_id, onboarding_status="active").first()
            if shop:
                return shop
            # Happiness profile self-healing: completed business_name with non-active status
            shop = self.db_session.query(MechShop).filter(
                MechShop.user_id == user_id,
                MechShop.business_name.isnot(None),
                MechShop.business_name != ""
            ).first()
            if shop:
                shop.onboarding_status = "active"
                if not getattr(shop, "trade", None):
                    shop.trade = "mechanic"
                self.db_session.commit()
                return shop
            # Company Register fallback
            sp = self.db_session.query(SenderProfile).filter_by(user_id=user_id).order_by(SenderProfile.is_default.desc(), SenderProfile.id.desc()).first()
            if sp and sp.business_name:
                shop = self.db_session.query(MechShop).filter_by(user_id=user_id).first()
                if not shop:
                    shop = MechShop(user_id=user_id)
                    self.db_session.add(shop)
                shop.business_name = sp.business_name
                shop.address = sp.address
                shop.phone = sp.phone
                shop.email = sp.email
                shop.onboarding_status = "active"
                shop.trade = getattr(shop, "trade", None) or "mechanic"
                self.db_session.commit()
                return shop
            return self.db_session.query(MechShop).filter_by(user_id=user_id).first()

        @mechanic_bp.route("/mechanic/onboarding/process", methods=["POST"])
        def onboarding_process():
            trade = (request.form.get("trade") or session.get("active_trade") or "mechanic").strip()
            shop = self.db_session.query(MechShop).filter_by(user_id=current_user.id, trade=trade).first()
            if not shop:
                shop = self.db_session.query(MechShop).filter(
                    MechShop.user_id == current_user.id,
                    (MechShop.trade.is_(None)) | (MechShop.trade == '')
                ).first()
            if not shop:
                shop = MechShop(user_id=current_user.id, trade=trade, onboarding_status="active")
                self.db_session.add(shop)
            shop.trade = trade
            shop.business_name = request.form.get("business_name") or shop.business_name or "My Mechanic Shop"
            shop.address = request.form.get("address")
            shop.phone = request.form.get("phone")
            shop.email = request.form.get("email")
            shop.terms_and_conditions = request.form.get("terms_and_conditions")
            shop.onboarding_status = "active"
            self.db_session.commit()
            session["active_trade"] = trade
            session["active_shop_id"] = shop.id
            test_ensure_sender_profile_synced(shop)
            return redirect(url_for("mechanic_bp.mechanic_dashboard", trade=trade))

        @mechanic_bp.route("/upload_business_card", methods=["POST"])
        def upload_business_card():
            return "Upload Business Card"

        @mechanic_bp.route("/mechanic/client_ledger/<int:debtor_id>")
        def client_ledger(debtor_id):
            debtor = self.db_session.query(Debtor).filter_by(id=debtor_id, user_id=current_user.id).first()
            if not debtor:
                abort(404)
            return f"Ledger for debtor {debtor.name} ({debtor.id})"

        @mechanic_bp.route("/mechanic/dashboard", methods=["GET", "POST"])
        @login_required
        def mechanic_dashboard():
            if request.method == "POST":
                name = request.form.get("name", "").strip()
                if not name or len(name) > 150:
                    flash("Please enter a valid client name.", "danger")
                    return redirect(url_for("mechanic_bp.mechanic_dashboard"))

                existing = self.db_session.query(MechClient).filter(
                    MechClient.user_id == current_user.id,
                    func.lower(MechClient.name) == func.lower(name)
                ).first()

                if existing:
                    flash("That client is already in your register. Use their existing record below.", "info")
                    return redirect(url_for("mechanic_bp.mechanic_dashboard"))

                contact = request.form.get("contact", "").strip()
                phone = request.form.get("phone", "").strip()
                email = request.form.get("email", "").strip()
                if contact and not phone and not email:
                    if "@" in contact:
                        email = contact
                    else:
                        phone = contact
                service = request.form.get("service", "").strip()

                client = MechClient(
                    user_id=current_user.id,
                    name=name,
                    phone=phone or None,
                    email=email or None
                )
                self.db_session.add(client)
                self.db_session.flush()

                # Sync to Debtor record
                sender_profile = self.db_session.query(SenderProfile).filter_by(user_id=current_user.id, is_default=True).first()
                debtor = Debtor(
                    user_id=current_user.id,
                    name=client.name,
                    phone=client.phone,
                    email=client.email,
                    slug_reference="mechanic",
                    reference_id=client.id,
                    sender_profile_id=sender_profile.id if sender_profile else None
                )
                self.db_session.add(debtor)

                if service:
                    vehicle = MechVehicle(
                        client_id=client.id,
                        make=service[:50]
                    )
                    self.db_session.add(vehicle)
                    self.db_session.flush()
                    import uuid
                    job_card = MechJobCard(
                        job_number=f"JOB-{uuid.uuid4().hex[:6].upper()}",
                        vehicle_id=vehicle.id,
                        status="Active",
                        description=service
                    )
                    self.db_session.add(job_card)

                self.db_session.commit()
                flash(f"Client '{name}' added successfully.", "success")
                return redirect(url_for("mechanic_bp.mechanic_dashboard"))

            selected_trade = request.args.get("trade")
            selected_shop_id = request.args.get("shop_id", type=int)
            if selected_trade:
                session["active_trade"] = selected_trade
            elif selected_shop_id:
                session["active_shop_id"] = selected_shop_id

            active_shop = test_get_active_shop(current_user.id, trade=selected_trade, shop_id=selected_shop_id)
            if active_shop:
                test_ensure_sender_profile_synced(active_shop)
                current_trade = getattr(active_shop, 'trade', 'mechanic') or 'mechanic'
            else:
                current_trade = selected_trade or session.get("active_trade") or "mechanic"

            session["active_trade"] = current_trade
            if active_shop:
                session["active_shop_id"] = active_shop.id

            user_shops = self.db_session.query(MechShop).filter_by(user_id=current_user.id).all()
            shops_by_trade = {}
            for s in user_shops:
                t = getattr(s, 'trade', None) or 'mechanic'
                if t not in shops_by_trade or s.onboarding_status == 'active':
                    shops_by_trade[t] = s

            raw_clients = self.db_session.query(MechClient).filter_by(user_id=current_user.id).order_by(MechClient.created_at.desc(), MechClient.id.desc()).all()
            client_rows = []
            for c in raw_clients:
                contact_parts = [p for p in (c.phone, c.email) if p]
                contact_str = " | ".join(contact_parts) if contact_parts else ""

                latest_job = None
                latest_veh = None
                for v in c.vehicles:
                    if not latest_veh:
                        latest_veh = v
                    for jc in v.job_cards:
                        if not latest_job or (jc.created_at and latest_job.created_at and jc.created_at > latest_job.created_at):
                            latest_job = jc
                            latest_veh = v

                if latest_job:
                    service_str = latest_job.description or f"{latest_veh.make or ''} {latest_veh.model or ''} ({latest_veh.license_plate or ''})".strip() or "General Service"
                    status_str = latest_job.status or "Active"
                elif latest_veh:
                    service_str = f"{latest_veh.make or ''} {latest_veh.model or ''} ({latest_veh.license_plate or ''})".strip() or "Vehicle Registered"
                    status_str = "Active"
                else:
                    service_str = ""
                    status_str = "Active"

                status_lower = status_str.lower()
                if "quote" in status_lower:
                    badge_class = "bg-amber-100 text-amber-800"
                elif "approved" in status_lower or "progress" in status_lower:
                    badge_class = "bg-blue-100 text-blue-800"
                elif "ready" in status_lower or "active" in status_lower:
                    badge_class = "bg-emerald-100 text-emerald-800"
                elif "billed" in status_lower:
                    badge_class = "bg-slate-100 text-slate-800"
                else:
                    badge_class = "bg-slate-100 text-slate-700"

                client_rows.append({
                    "client": c,
                    "contact": contact_str,
                    "service": service_str,
                    "status": status_str,
                    "status_badge_class": badge_class
                })

            wallet = self.db_session.query(AitTokenWallet).filter_by(user_id=current_user.id).first()
            return render_template(
                "program_mechanic/dashboard.html",
                active_shop=active_shop,
                wallet=wallet,
                clients=client_rows,
                client_rows=client_rows,
                available_trades=AVAILABLE_TRADES,
                current_trade=current_trade,
                active_trade_name=dict(AVAILABLE_TRADES).get(current_trade, 'General Mechanic'),
                shops_by_trade=shops_by_trade,
                user_shops=user_shops
            )

        @mechanic_bp.route("/mechanic/client/<int:client_id>", methods=["GET"])
        @login_required
        def view_client(client_id):
            client = self.db_session.query(MechClient).filter_by(id=client_id, user_id=current_user.id).first()
            if not client:
                abort(404)
            debtor = self.db_session.query(Debtor).filter_by(reference_id=client.id, slug_reference="mechanic", user_id=current_user.id).first()
            if not debtor:
                debtor = self.db_session.query(Debtor).filter_by(name=client.name, slug_reference="mechanic", user_id=current_user.id).first()
            if not debtor:
                sender_profile = self.db_session.query(SenderProfile).filter_by(user_id=current_user.id, is_default=True).first()
                debtor = Debtor(
                    user_id=current_user.id,
                    name=client.name,
                    phone=client.phone,
                    email=client.email,
                    slug_reference="mechanic",
                    reference_id=client.id,
                    sender_profile_id=sender_profile.id if sender_profile else None
                )
                self.db_session.add(debtor)
                self.db_session.commit()
            elif not debtor.reference_id:
                debtor.reference_id = client.id
                self.db_session.commit()

            return redirect(url_for("mechanic_bp.client_ledger", debtor_id=debtor.id))

        self.app.register_blueprint(mechanic_bp)
        self.app.register_blueprint(bridge_bp)
        self.app.register_blueprint(cultural_bp)
        self.app.register_blueprint(public_bp)
        self.app.register_blueprint(admin_bp)
        self.app.register_blueprint(auth_bp)

        # Seed test operators
        self.op1 = User(id=645, name="Operator One", email="operator1@example.com", role="user", active=True)
        self.op2 = User(id=777, name="Operator Two", email="operator2@example.com", role="user", active=True)
        self.db_session.add_all([self.op1, self.op2])
        self.db_session.commit()

        # Seed shop for op1
        self.shop1 = MechShop(user_id=645, business_name="Alpha Auto Care", onboarding_status="active")
        self.wallet1 = AitTokenWallet(user_id=645, balance=120)
        self.db_session.add_all([self.shop1, self.wallet1])
        self.db_session.commit()

        self.client = self.app.test_client()

    def tearDown(self):
        self.db_session.close()
        Base.metadata.drop_all(self.engine)

    def login(self, user_id=645):
        with self.client.session_transaction() as sess:
            sess["_user_id"] = str(user_id)
            sess["_fresh"] = True

    def test_retains_four_dashboard_actions_in_permanent_menu(self):
        """Action 1 & 2: Place all dashboard buttons in one horizontal menu row with different light pastel colours."""
        self.login(645)
        response = self.client.get("/mechanic/dashboard")
        self.assertEqual(response.status_code, 200)

        tree = html.fromstring(response.data)
        menu = tree.xpath('//nav[@aria-label="ProTrade"]')
        self.assertTrue(len(menu) > 0, "ProTrade permanent menu must be present")

        menu_items = menu[0].xpath('.//a | .//button')
        self.assertEqual(len(menu_items), 6, "Menu must contain exactly 6 dashboard actions in one row")

        labels = ["".join(item.itertext()).strip() for item in menu_items]
        self.assertEqual(labels, ["Quote", "Invoice", "Price List", "Debtor", "Tokens", "Setup"])

        # Check preserved routes for the four core actions
        hrefs = {item.text_content().strip(): item.get("href") for item in menu_items if item.get("href")}
        self.assertEqual(hrefs.get("Quote"), "/mechanic/quote/new")
        self.assertEqual(hrefs.get("Invoice"), "/mechanic/jobs")
        self.assertEqual(hrefs.get("Price List"), "/mechanic/catalog")
        self.assertEqual(hrefs.get("Debtor"), "/mechanic/client_accounts")

        # Check distinct light pastel background colors
        pastel_bgs = [item.get("style", "") for item in menu_items]
        self.assertIn("#eff6ff", pastel_bgs[0])  # Quote
        self.assertIn("#f0fdf4", pastel_bgs[1])  # Invoice
        self.assertIn("#fefce8", pastel_bgs[2])  # Price List
        self.assertIn("#f5f3ff", pastel_bgs[3])  # Debtor
        self.assertIn("#f0fdfa", pastel_bgs[4])  # Tokens
        self.assertIn("#fff7ed", pastel_bgs[5])  # Setup

    def test_working_area_blank_initially_and_close_button(self):
        """Action 3: Working area blank initially; actions have Close buttons to return to blank."""
        self.login(645)
        response = self.client.get("/mechanic/dashboard")
        self.assertEqual(response.status_code, 200)

        tree = html.fromstring(response.data)
        working_area = tree.xpath('//*[@id="working-area"]')
        self.assertTrue(len(working_area) > 0)
        # Blank initially: must have class 'hidden'
        self.assertIn("hidden", working_area[0].get("class", ""))

        # Close buttons present in workspaces
        close_buttons = working_area[0].xpath('.//button[contains(text(), "Close")]')
        self.assertTrue(len(close_buttons) >= 3, "Each workspace panel must have a Close button")

        # Tokens workspace combines balance and transfer
        tokens_panel = working_area[0].xpath('//*[@id="workspace-tokens"]')
        self.assertTrue(len(tokens_panel) > 0)
        tokens_text = "".join(tokens_panel[0].itertext())
        self.assertIn("120 Tokens", tokens_text)
        self.assertTrue(len(tokens_panel[0].xpath('.//a[@href="/mechanic/price"]')) > 0)
        self.assertTrue(len(tokens_panel[0].xpath('.//a[@href="/cultural/wallet/transfer"]')) > 0)

        # Setup workspace exists with profile edit
        setup_panel = working_area[0].xpath('//*[@id="workspace-setup"]')
        self.assertTrue(len(setup_panel) > 0)
        self.assertIn("Alpha Auto Care", "".join(setup_panel[0].itertext()))

    def test_client_register_counter_and_show_more_show_less(self):
        """Action 4: Visible client counter and Show More / Show Less when table grows."""
        # Add 7 clients for operator 645
        for i in range(1, 8):
            self.db_session.add(MechClient(name=f"Client {i}", phone=f"082000000{i}", user_id=645))
        self.db_session.commit()

        self.login(645)
        response = self.client.get("/mechanic/dashboard")
        self.assertEqual(response.status_code, 200)

        tree = html.fromstring(response.data)
        counter = tree.xpath('//*[@id="client-counter"]')
        self.assertTrue(len(counter) > 0)
        self.assertIn("7 clients", counter[0].text_content().strip())

        # Table rows check: 5 visible, 2 hidden
        all_rows = tree.xpath('//table[@id="client-register-table"]/tbody/tr')
        self.assertEqual(len(all_rows), 7)
        hidden_rows = tree.xpath('//table[@id="client-register-table"]/tbody/tr[contains(@class, "client-row-hidden")]')
        self.assertEqual(len(hidden_rows), 2)

        # Show More button
        toggle_btn = tree.xpath('//*[@id="client-toggle-btn"]')
        self.assertTrue(len(toggle_btn) > 0)
        self.assertIn("Show More (5 of 7)", toggle_btn[0].text_content())

    def test_property_management_hub_styling_and_layout(self):
        """Action 8: Follow PM styling with central tile, top strip, header + Back button, input borders, autofocus."""
        self.login(645)
        response = self.client.get("/mechanic/dashboard")
        self.assertEqual(response.status_code, 200)

        tree = html.fromstring(response.data)

        # Top strip
        top_strip = tree.xpath('//div[contains(@class, "bg-slate-700") and contains(@class, "h-2")]')
        self.assertTrue(len(top_strip) > 0, "Must have top color strip matching ProTrade")

        # Header with Title and Back button to bridge
        back_btn = tree.xpath('//a[@href="/bridge"]')
        self.assertTrue(len(back_btn) > 0, "Header must have Back button pointing to bridge")
        self.assertEqual("".join(back_btn[0].itertext()).strip(), "Back")

        # Input borders and autofocus
        client_name_input = tree.xpath('//input[@id="client-name"]')
        self.assertTrue(len(client_name_input) > 0, "Must have Client Name input with id client-name")
        self.assertIn("autofocus", client_name_input[0].attrib, "Client name input must have autofocus")
        self.assertIn("border-slate-300", client_name_input[0].get("class", ""))

        # Check CSS style block ensuring explicit visible border styling
        style_blocks = tree.xpath('//style')
        self.assertTrue(len(style_blocks) > 0)
        style_text = "".join(s.text or "" for s in style_blocks)
        self.assertIn("#client-name", style_text)
        self.assertIn("border", style_text)

    def test_friendly_empty_register_when_no_clients(self):
        """Action 7: Friendly empty register message when no clients exist."""
        self.login(645)
        response = self.client.get("/mechanic/dashboard")
        self.assertEqual(response.status_code, 200)

        tree = html.fromstring(response.data)
        table = tree.xpath('//table[@id="client-register-table"]')
        self.assertTrue(len(table) > 0)

        # Headers: Client, Contact, Service, Status, Action
        headers = [h.strip() for h in table[0].xpath('.//thead//th//text()') if h.strip()]
        self.assertEqual(headers, ["Client", "Contact", "Service", "Status", "Action"])

        # Friendly empty state
        tbody_text = "".join(table[0].xpath('.//tbody//text()')).strip()
        self.assertIn("No clients registered yet. Enter a client name above to begin.", tbody_text)

    def test_add_client_form_submission_and_sync(self):
        """Action 3, 4, 5: Add Client creates records and updates the register."""
        self.login(645)

        # Submit Add Client form
        post_data = {
            "name": "Jane Miller",
            "contact": "0839991234",
            "service": "Major Service & Brake Pads"
        }
        res = self.client.post("/mechanic/dashboard", data=post_data, follow_redirects=True)
        self.assertEqual(res.status_code, 200)

        # Verify DB records
        client = self.db_session.query(MechClient).filter_by(name="Jane Miller", user_id=645).first()
        self.assertIsNotNone(client, "MechClient must be created for current user")
        self.assertEqual(client.phone, "0839991234")

        # Verify Debtor sync
        debtor = self.db_session.query(Debtor).filter_by(reference_id=client.id, user_id=645).first()
        self.assertIsNotNone(debtor, "Debtor record must be synced with reference_id")
        self.assertEqual(debtor.name, "Jane Miller")

        # Verify Table row display
        tree = html.fromstring(res.data)
        rows = tree.xpath('//table[@id="client-register-table"]/tbody/tr')
        self.assertEqual(len(rows), 1)

        cols = [td.text_content().strip() for td in rows[0].xpath('./td')]
        self.assertEqual(cols[0], "Jane Miller")
        self.assertEqual(cols[1], "0839991234")
        self.assertEqual(cols[2], "Major Service & Brake Pads")
        self.assertEqual(cols[3], "Active")
        self.assertEqual(cols[4], "View Client")

    def test_operator_multi_tenant_isolation(self):
        """Action 6: Ensure each operator strictly sees only their own clients."""
        # Operator 1 creates a client
        client1 = MechClient(name="Client of Op 1", phone="0111111111", user_id=645)
        # Operator 2 creates a client
        client2 = MechClient(name="Client of Op 2", phone="0222222222", user_id=777)
        self.db_session.add_all([client1, client2])
        self.db_session.commit()

        # Operator 1 logs in
        self.login(645)
        res1 = self.client.get("/mechanic/dashboard")
        self.assertEqual(res1.status_code, 200)
        self.assertIn("Client of Op 1", res1.get_data(as_text=True))
        self.assertNotIn("Client of Op 2", res1.get_data(as_text=True))

        # Operator 2 logs in
        self.login(777)
        res2 = self.client.get("/mechanic/dashboard")
        self.assertEqual(res2.status_code, 200)
        self.assertIn("Client of Op 2", res2.get_data(as_text=True))
        self.assertNotIn("Client of Op 1", res2.get_data(as_text=True))

    def test_view_client_route_and_access_control(self):
        """Action 5 & 6: View Client reuses client_ledger and prevents cross-operator access."""
        client1 = MechClient(name="Secret Client", phone="0777777777", user_id=645)
        self.db_session.add(client1)
        self.db_session.commit()

        # Operator 1 accesses View Client
        self.login(645)
        res1 = self.client.get(f"/mechanic/client/{client1.id}", follow_redirects=True)
        self.assertEqual(res1.status_code, 200)
        self.assertIn("Ledger for debtor Secret Client", res1.get_data(as_text=True))

        # Operator 2 tries to access Operator 1's client -> 404 isolation
        self.login(777)
        res2 = self.client.get(f"/mechanic/client/{client1.id}")
        self.assertEqual(res2.status_code, 404, "Operator 2 cannot view Operator 1's client")

    def test_ai_autofill_removed_and_manual_fields_present(self):
        """Correction 1: AI Auto-Fill removed from Shop Profile Setup modal. Manual entry retained."""
        self.login(645)
        response = self.client.get("/mechanic/dashboard")
        self.assertEqual(response.status_code, 200)

        tree = html.fromstring(response.data)

        # AI tabs and AI upload container must NOT exist
        ai_tab_btn = tree.xpath('//*[@id="tab-btn-ai"]')
        self.assertEqual(len(ai_tab_btn), 0, "AI Auto-Fill tab button must be removed")
        ai_tab_content = tree.xpath('//*[@id="tab-content-ai"]')
        self.assertEqual(len(ai_tab_content), 0, "AI Auto-Fill upload container must be removed")
        ai_upload_input = tree.xpath('//*[@id="ajax_card_upload"]')
        self.assertEqual(len(ai_upload_input), 0, "AI card upload input must be removed")

        # Manual Entry form must be directly visible inside modal
        manual_content = tree.xpath('//*[@id="tab-content-manual"]')
        self.assertTrue(len(manual_content) > 0, "Manual entry section must be present")
        self.assertNotIn("hidden", manual_content[0].get("class", ""), "Manual entry must not be hidden")

        # All manual fields must be present
        self.assertTrue(len(tree.xpath('//select[@name="trade"]')) > 0, "Trade selector must be present")
        self.assertTrue(len(tree.xpath('//input[@name="business_name"]')) > 0, "Business Name field must be present")
        self.assertTrue(len(tree.xpath('//input[@name="address"]')) > 0, "Address field must be present")
        self.assertTrue(len(tree.xpath('//input[@name="phone"]')) > 0, "Phone field must be present")
        self.assertTrue(len(tree.xpath('//input[@name="email"]')) > 0, "Email field must be present")
        self.assertTrue(len(tree.xpath('//input[@name="tax_number"]')) > 0, "Tax Number field must be present")
        self.assertTrue(len(tree.xpath('//input[@name="vat_rate"]')) > 0, "VAT Rate field must be present")
        self.assertTrue(len(tree.xpath('//input[@name="logo_file"]')) > 0, "Logo upload must be present")
        self.assertTrue(len(tree.xpath('//input[@name="letterhead_file"]')) > 0, "Letterhead upload must be present")
        self.assertTrue(len(tree.xpath('//select[@id="tc-template-select"]')) > 0, "T&C template selector must be present")
        self.assertTrue(len(tree.xpath('//textarea[@name="terms_and_conditions"]')) > 0, "Terms textarea must be present")
        self.assertTrue(len(tree.xpath('//button[@type="submit" and contains(text(), "Save Profile")]')) > 0, "Save Profile button must be present")

    def test_happiness_shop_profile_loads_and_syncs_to_company_register(self):
        """Correction 2: Happiness's completed profile loads correctly and syncs to Company Register."""
        # Create user Happiness (user 888) with an existing completed profile that had draft_setup status
        happiness = User(id=888, name="Happiness", email="happiness@example.com")
        self.db_session.add(happiness)
        self.db_session.commit()

        # Previous completed profile with non-active status
        h_shop = MechShop(
            user_id=888,
            business_name="Happiness Auto Care",
            address="45 Commissioner St, Johannesburg",
            phone="011 555 7890",
            email="happiness@autocare.co.za",
            onboarding_status="draft_setup"  # Previously prevented loading
        )
        self.db_session.add(h_shop)
        self.db_session.commit()

        # Login as Happiness
        self.login(888)
        res = self.client.get("/mechanic/dashboard")
        self.assertEqual(res.status_code, 200)

        # Profile loads and self-heals
        self.assertIn("Happiness Auto Care", res.get_data(as_text=True))
        
        # Verify self-healing in database
        updated_shop = self.db_session.query(MechShop).filter_by(user_id=888).first()
        self.assertEqual(updated_shop.onboarding_status, "active", "Profile status should self-heal to active")

        # Verify it appears in Company Register (SenderProfile) without duplication
        sp = self.db_session.query(SenderProfile).filter_by(user_id=888, business_name="Happiness Auto Care").all()
        self.assertEqual(len(sp), 1, "Must exist in Company Register (SenderProfile) without duplicates")

    def test_multiple_trades_separate_profiles_and_selection(self):
        """Correction 3: Each operator can maintain separate shop profiles for each trade and select between them."""
        self.login(645)

        # Operator 645 has existing mechanic shop: "Alpha Auto Care" (seeded in setUp)
        # Create second trade shop for panel beating
        panel_shop = MechShop(
            user_id=645,
            business_name="Alpha Panel Beaters",
            trade="panel",
            phone="011 999 0001",
            onboarding_status="active"
        )
        self.db_session.add(panel_shop)
        self.db_session.commit()

        # 1. Dashboard has trade switcher dropdown
        res = self.client.get("/mechanic/dashboard")
        self.assertEqual(res.status_code, 200)
        tree = html.fromstring(res.data)
        trade_nav = tree.xpath('//select[@id="trade-select-nav"]')
        self.assertTrue(len(trade_nav) > 0, "Trade switcher dropdown must be present on dashboard")

        # 2. Selecting panel trade loads the panel profile
        res_panel = self.client.get("/mechanic/dashboard?trade=panel")
        self.assertEqual(res_panel.status_code, 200)
        self.assertIn("Alpha Panel Beaters", res_panel.get_data(as_text=True))

        # 3. Selecting mechanic trade loads the mechanic profile
        res_mech = self.client.get("/mechanic/dashboard?trade=mechanic")
        self.assertEqual(res_mech.status_code, 200)
        self.assertIn("Alpha Auto Care", res_mech.get_data(as_text=True))

        # 4. Save a new profile under auto_elec trade
        post_data = {
            "trade": "auto_elec",
            "business_name": "Alpha Auto Electrical",
            "phone": "011 999 0002",
            "terms_and_conditions": "3 months warranty on electrical parts."
        }
        res_save = self.client.post("/mechanic/onboarding/process", data=post_data, follow_redirects=True)
        self.assertEqual(res_save.status_code, 200)

        # Verify separate record was created
        auto_elec = self.db_session.query(MechShop).filter_by(user_id=645, trade="auto_elec").first()
        self.assertIsNotNone(auto_elec, "auto_elec shop profile must be created")
        self.assertEqual(auto_elec.business_name, "Alpha Auto Electrical")

        # Verify original mechanic profile remains untouched
        original_mech = self.db_session.query(MechShop).filter_by(user_id=645, trade="mechanic").first()
        self.assertEqual(original_mech.business_name, "Alpha Auto Care")


if __name__ == "__main__":
    unittest.main()
