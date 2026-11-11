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

        @mechanic_bp.route("/mechanic/onboarding/process", methods=["POST"])
        def onboarding_process():
            return "Onboarding Process"

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

            active_shop = self.db_session.query(MechShop).filter_by(user_id=current_user.id, onboarding_status="active").first()
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
                client_rows=client_rows
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


if __name__ == "__main__":
    unittest.main()
