import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

find_block = '''        body = f"Hello,\\n\\nYour {doc_type} #{job_card.job_number} is ready. You can view it here: {doc_url}\\n\\nThank you for choosing us!"
        html = f"<p>Hello,</p><p>Your {doc_type} <strong>#{job_card.job_number}</strong> is ready. You can view it here: <a href='{doc_url}'>{doc_url}</a></p><p>Thank you for choosing us!</p>"'''

replace_block = '''        body = f"Hello,\\n\\nYour {doc_type} #{job_card.job_number} is ready. You can view it here: {doc_url}\\n\\nThank you for choosing us!"
        
        active_shop = MechShop.query.filter_by(user_id=current_user.id, onboarding_status='active').first()
        letterhead_html = ""
        if active_shop and active_shop.use_custom_letterhead and active_shop.letterhead_url:
            lh_url = url_for('static', filename=f'uploads/mechanic/{active_shop.letterhead_url}', _external=True)
            letterhead_html = f'<div style="text-align: center; margin-bottom: 20px;"><img src="{lh_url}" alt="Shop Letterhead" style="max-width: 100%; height: auto; max-height: 150px; border-radius: 8px;"></div><hr style="border: 0; border-top: 1px solid #e2e8f0; margin-bottom: 20px;">'

        html = f"""{letterhead_html}
        <div style="font-family: sans-serif; color: #334155; max-width: 600px; margin: 0 auto;">
            <p>Hello,</p>
            <p>Your {doc_type} <strong>#{job_card.job_number}</strong> is ready. You can securely view and download it here:</p>
            <p style="text-align: center; margin: 30px 0;">
                <a href='{doc_url}' style="background-color: #4f46e5; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block;">View {doc_type}</a>
            </p>
            <p>Thank you for choosing us!</p>
        </div>"""'''

if find_block in content:
    content = content.replace(find_block, replace_block)
    print("SUCCESS")
else:
    print("FAILED")

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
