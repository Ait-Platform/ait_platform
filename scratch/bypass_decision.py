import re

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_code = '    # If they reach here, there is NO free access and NO trial, so we prompt them to pay or enter a voucher\n    return render_template("auth/checkout_decision.html", email=user_email, subject=subject)'
new_code = '    # If they reach here, there is NO free access and NO trial, and we no longer use vouchers.\n    # Route directly to the payment gateway.\n    return redirect(url_for("paystack_bp.paystack_start", email=user_email, subject=subject, debug=0))'

if old_code in text:
    text = text.replace(old_code, new_code)
    with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Bypassed checkout_decision.html")
else:
    print("Could not find code to replace.")
