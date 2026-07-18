with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

import re
old_commit = """        prop.onboarding_status = 'draft_manual'
        
        db.session.commit()
        return jsonify({"message": "Architecture saved successfully!"}), 200"""

new_commit = """        prop.onboarding_status = 'draft_manual'
        
        # Save Draft JSON so frontend wizard can restore it!
        from app.models.billing import BilArchitectureDraft
        draft = BilArchitectureDraft.query.filter_by(property_id=prop.id).first()
        if not draft:
            draft = BilArchitectureDraft(property_id=prop.id)
            db.session.add(draft)
        draft.draft_json = data
        
        db.session.commit()
        return jsonify({"message": "Architecture saved successfully!"}), 200"""

text = text.replace(old_commit, new_commit)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
print("Updated routes.py to save draft_json!")

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    html = f.read()

html = html.replace('class="rate-reference w-full rounded', 'class="rate-reference w-full rounded capitalize')
html = html.replace('class="rate-erf w-full rounded', 'class="rate-erf w-full rounded capitalize')
html = html.replace('class="rate-category w-full rounded', 'class="rate-category w-full rounded capitalize')

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(html)
print("Updated setup_wizard.html for rates capitalization!")
