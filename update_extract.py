import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''                from app.extensions import db
                db.session.add(log_entry)
                db.session.commit()
        except Exception as inner_e:'''

injection = '''                from app.extensions import db
                db.session.add(log_entry)
                
                # Check if we should upgrade the draft property to collation
                prop_id = request.form.get("property_id")
                if prop_id:
                    draft = BilProperty.query.get(prop_id)
                    if draft and draft.onboarding_status == 'draft_extracting':
                        count = BilExtractionLog.query.filter_by(property_name=draft.name).count()
                        # We count the current one too because it's in the session but maybe not returned by count yet?
                        # Actually we can just do count + 1
                        if count + 1 >= draft.expected_bills:
                            draft.onboarding_status = 'draft_collating'
                            
                db.session.commit()
        except Exception as inner_e:'''

content = content.replace(target, injection)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
