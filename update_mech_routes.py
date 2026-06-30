import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''@mechanic_bp.route("/mechanic/dashboard")
@login_required
def mechanic_dashboard():
    # Placeholder for the mechanic dashboard
    # Will display active job cards, recent invoices, and quick actions
    job_cards = MechJobCard.query.order_by(MechJobCard.created_at.desc()).limit(10).all()
    return render_template("program_mechanic/dashboard.html", job_cards=job_cards)'''

injection = '''from app.models.mechanic import MechShop, MechCatalogPart

@mechanic_bp.route("/mechanic/dashboard")
@login_required
def mechanic_dashboard():
    draft_shop = MechShop.query.filter(
        MechShop.user_id == current_user.id,
        MechShop.onboarding_status.like('draft_%')
    ).first()
    
    active_shop = MechShop.query.filter(
        MechShop.user_id == current_user.id,
        MechShop.onboarding_status == 'active'
    ).first()

    job_cards = MechJobCard.query.order_by(MechJobCard.created_at.desc()).limit(10).all()
    
    # Seed some default parts if none exist
    if MechCatalogPart.query.count() == 0:
        default_parts = [
            MechCatalogPart(part_name='Brake Pads', category='Brakes', default_price=450.0),
            MechCatalogPart(part_name='Oil Filter', category='Engine', default_price=120.0),
            MechCatalogPart(part_name='Spark Plug', category='Engine', default_price=80.0),
            MechCatalogPart(part_name='Air Filter', category='Engine', default_price=150.0),
            MechCatalogPart(part_name='Wiper Blades', category='Exterior', default_price=200.0),
            MechCatalogPart(part_name='Battery', category='Electrical', default_price=1200.0)
        ]
        db.session.bulk_save_objects(default_parts)
        db.session.commit()

    return render_template("program_mechanic/dashboard.html", 
                           job_cards=job_cards, 
                           draft_shop=draft_shop, 
                           active_shop=active_shop)

@mechanic_bp.route("/mechanic/onboarding/start", methods=["POST"])
@login_required
def onboarding_start():
    import time
    time.sleep(2) # Simulate AI processing
    
    draft_shop = MechShop.query.filter(
        MechShop.user_id == current_user.id,
        MechShop.onboarding_status.like('draft_%')
    ).first()
    
    if not draft_shop:
        draft_shop = MechShop(
            user_id=current_user.id,
            business_name="Extracted Business Name",
            address="123 Extracted Street",
            phone="555-1234",
            email="extracted@example.com",
            terms_and_conditions="Payment strictly within 30 days.",
            onboarding_status='draft_review'
        )
        db.session.add(draft_shop)
        db.session.commit()
    
    return redirect(url_for("mechanic_bp.mechanic_dashboard", view='review'))

@mechanic_bp.route("/mechanic/onboarding/process", methods=["POST"])
@login_required
def onboarding_process():
    draft_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='draft_review').first()
    if draft_shop:
        draft_shop.business_name = request.form.get("business_name")
        draft_shop.address = request.form.get("address")
        draft_shop.phone = request.form.get("phone")
        draft_shop.email = request.form.get("email")
        draft_shop.terms_and_conditions = request.form.get("terms_and_conditions")
        draft_shop.onboarding_status = 'active'
        db.session.commit()
        flash("Shop profile successfully activated!", "success")
        
    return redirect(url_for("mechanic_bp.mechanic_dashboard"))
'''

content = content.replace(target, injection)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
