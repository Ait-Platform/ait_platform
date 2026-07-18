with open('app/program_billing/routes.py', 'a', encoding='utf-8') as f:
    f.write('''

@billing_bp.route("/billing/delete_property/<int:property_id>", methods=["POST"])
@login_required
def delete_property(property_id):
    from app.extensions import db
    prop = BilProperty.query.filter_by(id=property_id, manager_id=current_user.id).first()
    if prop:
        db.session.delete(prop)
        db.session.commit()
        flash("Property permanently deleted.", "success")
    return redirect(url_for('billing_bp.learner_dashboard'))
''')
    print("Appended delete_property route")
