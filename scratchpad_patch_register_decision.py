import os

def patch_routes():
    file_path = r"d:\Users\yeshk\Documents\ait_platform\app\auth\routes.py"
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # The point to intercept is around line 383:
    #     # 3) Normal paid flow: keep your existing pricing + PayFast logic here
    #     q = ctx.get("quote")

    search_str = """    # 3) Normal paid flow: keep your existing pricing + PayFast logic here
    q = ctx.get("quote")"""

    replacement_str = """    # 3) Normal paid flow: keep your existing pricing + PayFast logic here
    q = ctx.get("quote")

    # ----- COUNTRY PRICE CHECKER INTERCEPT -----
    if subject not in ("sms", "cultural_fire"):
        from app.enrollment.logic import get_quote_for_subject_country
        from app.models.auth import AuthSubject
        
        established_country = db.session.execute(
            db.text("SELECT country_code FROM user_enrollment WHERE user_id = :uid AND country_code IS NOT NULL AND status != 'archived' ORDER BY id ASC LIMIT 1"),
            {"uid": user_id}
        ).scalar()

        if established_country:
            established_country = established_country.strip().upper()
            quote_country = None
            if q and q.get("country_code"):
                quote_country = q.get("country_code").strip().upper()
            elif session.get("subject_slug") == subject and session.get("country_code"):
                quote_country = session.get("country_code").strip().upper()
            elif request.form.get("country"):
                quote_country = request.form.get("country").strip().upper()

            # If mismatch and not yet acknowledged
            if quote_country and quote_country != established_country and request.form.get("acknowledge_mismatch") != "1":
                subj_obj = AuthSubject.query.filter(db.func.lower(AuthSubject.slug) == subject).first()
                if subj_obj:
                    # Recalculate using established country
                    new_quote_row = get_quote_for_subject_country(subj_obj.id, established_country)
                    if new_quote_row:
                        # Get country names
                        from app.models.payment import RefCountryCurrency
                        q_country_obj = db.session.execute(db.text("SELECT name FROM ref_country_currency WHERE alpha2 = :c LIMIT 1"), {"c": quote_country}).scalar() or quote_country
                        e_country_obj = db.session.execute(db.text("SELECT name FROM ref_country_currency WHERE alpha2 = :c LIMIT 1"), {"c": established_country}).scalar() or established_country
                        
                        # Get other enrolled slugs
                        other_enrolls = db.session.execute(
                            db.text("SELECT s.slug FROM user_enrollment e JOIN auth_subject s ON e.subject_id = s.id WHERE e.user_id = :uid AND e.status != 'archived' AND s.slug != :slug"),
                            {"uid": user_id, "slug": subject}
                        ).scalars().all()

                        return render_template(
                            "auth/country_price_checker.html",
                            subject=subject,
                            quote_country_code=quote_country,
                            quote_country_name=q_country_obj,
                            user_country_code=established_country,
                            user_country_name=e_country_obj,
                            local_currency=new_quote_row.local_currency,
                            local_amount_cents=new_quote_row.local_amount_cents,
                            zar_amount_cents=new_quote_row.zar_amount_cents,
                            other_slugs=other_enrolls
                        )

            # If acknowledged or matched, FORCE the established country
            if quote_country and quote_country != established_country:
                subj_obj = AuthSubject.query.filter(db.func.lower(AuthSubject.slug) == subject).first()
                if subj_obj:
                    new_quote_row = get_quote_for_subject_country(subj_obj.id, established_country)
                    if new_quote_row:
                        q = {
                            "country_code": new_quote_row.country_code,
                            "currency": new_quote_row.local_currency,
                            "amount_cents": new_quote_row.local_amount_cents,
                            "zar_amount_cents": new_quote_row.zar_amount_cents,
                            "est_zar_cents": new_quote_row.zar_amount_cents,
                            "version": new_quote_row.price_version,
                        }
                        ctx["quote"] = q
                        session.modified = True
    # ----- END COUNTRY PRICE CHECKER INTERCEPT -----"""

    if search_str in content:
        content = content.replace(search_str, replacement_str)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        print("Patched routes.py successfully.")
    else:
        print("Could not find search_str in routes.py")

if __name__ == "__main__":
    patch_routes()
