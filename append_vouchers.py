import os

code_to_add = '''
from app.models.payment import VoucherToken
from app.models.auth import AuthSubject
import uuid

@admin_bp.route("/vouchers", methods=["GET", "POST"], endpoint="manage_vouchers")
def manage_vouchers():
    if not is_admin():
        abort(403)
        
    if request.method == "POST":
        subject_id = request.form.get("subject_id", type=int)
        value_amount = request.form.get("value_amount", type=int)
        code = request.form.get("code")
        
        if not subject_id or not value_amount:
            flash("Subject and Value Amount are required.", "danger")
        else:
            if not code:
                # Generate a random 8-character uppercase code
                code = str(uuid.uuid4()).upper()[:8]
            
            # Check if code exists
            exists = VoucherToken.query.filter_by(code=code).first()
            if exists:
                flash("That voucher code already exists!", "danger")
            else:
                v = VoucherToken(
                    code=code, 
                    value_amount=value_amount, 
                    subject_id=subject_id,
                    created_by_user_id=current_user.id
                )
                db.session.add(v)
                db.session.commit()
                flash(f"Voucher {code} generated successfully!", "success")
        return redirect(url_for("admin_bp.manage_vouchers"))

    # GET request
    vouchers = VoucherToken.query.order_by(VoucherToken.created_at.desc()).all()
    subjects = AuthSubject.query.order_by(AuthSubject.name.asc()).all()
    
    return render_template("admin/vouchers.html", vouchers=vouchers, subjects=subjects)
'''

with open('app/admin/routes.py', 'a', encoding='utf-8') as f:
    f.write(code_to_add)
