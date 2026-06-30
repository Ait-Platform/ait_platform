with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

import re

target = '''        "is_discount": getattr(row, "is_discount", False),
                "has_quote": True,
            })

    return render_template("program_mechanic/price.html", price=price_ctx, subject=subject)'''

injection = '''        "is_discount": getattr(row, "is_discount", False),
            })
            price_ctx["has_quote"] = True
        else:
            flash("No pricing found for that country yet.", "warning")

    countries = db.session.execute(
        text("""
            SELECT r.alpha2 AS code, r.name
              FROM ref_country_currency r
        """)
    ).mappings().all()

    return render_template("program_mechanic/price.html", price=price_ctx, subject=subject, countries=countries)'''

if target in content:
    new_content = content.replace(target, injection)
    with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
        f.write(new_content)
    print("Successfully updated routes.py")
else:
    print("Could not find the target string in routes.py")
