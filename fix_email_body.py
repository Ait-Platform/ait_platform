import sys

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''        body = f"Hello,\\n\\nYour {doc_type} #{job_card.job_number} is ready. You can view it here: {doc_url}\\n\\nWe have also attached a PDF copy for your records.\\n\\nThank you for choosing us!"'''

new_target = '''        body = f"Hello,\\n\\nYour {doc_type} #{job_card.job_number} is ready. We have attached a PDF copy for your records.\\n\\nThank you for choosing us!"'''
content = content.replace(target, new_target)

target_html = '''        html = f\"\"\"{letterhead_html}
        <div style="font-family: sans-serif; color: #334155; max-width: 600px; margin: 0 auto;">
            <p>Hello,</p>
            <p>Your {doc_type} <strong>#{job_card.job_number}</strong> is ready. You can securely view and download it here:</p>
            <p style="text-align: center; margin: 30px 0;">
                <a href='{doc_url}' style="background-color: #4f46e5; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block;">View {doc_type}</a>
            </p>
            <p>We have also attached a PDF copy for your convenience.</p>
            <p>Thank you for choosing us!</p>
        </div>\"\"\"'''

new_html = '''        html = f\"\"\"{letterhead_html}
        <div style="font-family: sans-serif; color: #334155; max-width: 600px; margin: 0 auto;">
            <p>Hello,</p>
            <p>Your {doc_type} <strong>#{job_card.job_number}</strong> is ready. We have attached a PDF copy for your records.</p>
            <br>
            <p>Thank you for choosing us!</p>
        </div>\"\"\"'''
content = content.replace(target_html, new_html)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
