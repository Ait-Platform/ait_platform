new_route = '''
@billing_bp.route("/billing/utilities/<int:property_id>/consumption/<month>/email", methods=["POST"])
@login_required
def email_consumption(property_id, month):
    from app.models.billing import BilProperty
    from app.utils.mailer import send_email
    
    data = request.get_json()
    email = data.get("email") if data else None
    
    if not email:
        return {"success": False, "error": "Email address is required"}, 400
        
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        return {"success": False, "error": "Unauthorized"}, 403
        
    subject = f"Consumption Review - {prop.name} - {month}"
    body = f"Hello,\\n\\nPlease log in to the AIT platform to view the consumption review for {prop.name} for the billing month of {month}.\\n\\nRegards,\\nAIT Platform"
    
    try:
        success = send_email(subject, [email], body)
        if success:
            return {"success": True}
        else:
            return {"success": False, "error": "Mailer returned false."}
    except Exception as e:
        return {"success": False, "error": str(e)}
'''

with open('app/program_billing/routes.py', 'a', encoding='utf-8') as f:
    f.write('\n' + new_route)
print('Done adding email endpoint')
