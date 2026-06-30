import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

injection = '''
@mechanic_bp.route("/mechanic/quote/new", methods=["GET", "POST"])
@login_required
def new_quote():
    active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
    if not active_shop:
        flash("You must complete your shop setup first.", "warning")
        return redirect(url_for("mechanic_bp.mechanic_dashboard"))
        
    # Get global parts + user's learned parts
    catalog_parts = MechCatalogPart.query.filter(
        (MechCatalogPart.user_id == None) | (MechCatalogPart.user_id == current_user.id)
    ).all()
    
    if request.method == "POST":
        customer_name = request.form.get("customer_name")
        vehicle_reg = request.form.get("vehicle_reg")
        # In a real app, we'd save this to MechJobCard and MechClient.
        # For now, if they typed a new part, let's learn it!
        new_part_name = request.form.get("new_part_name")
        new_part_price = request.form.get("new_part_price")
        
        if new_part_name and new_part_price:
            learned_part = MechCatalogPart(
                user_id=current_user.id,
                part_name=new_part_name,
                category='Custom',
                default_price=float(new_part_price)
            )
            db.session.add(learned_part)
            db.session.commit()
            flash(f"Learned new part: {new_part_name}", "success")
            
        flash("Quote created successfully!", "success")
        return redirect(url_for("mechanic_bp.mechanic_dashboard"))
        
    return render_template("program_mechanic/quote_form.html", catalog_parts=catalog_parts, shop=active_shop)
'''

content = content + "\n" + injection

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
