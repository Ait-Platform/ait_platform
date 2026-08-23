import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Add Edit Quote button
content = re.sub(
    r"{% if job_card\.status == 'Quote' %}\s*<form method=\"POST\" action=\"\{\{ url_for\('mechanic_bp\.accept_quote'",
    "{% if job_card.status == 'Quote' %}\n              <a href=\"{{ url_for('mechanic_bp.edit_quote', id=job_card.id) }}\" class=\"px-4 py-2 bg-slate-800 text-white font-bold rounded-lg hover:bg-slate-900 shadow-sm transition text-sm mr-2\">Edit Quote</a>\n              <form method=\"POST\" action=\"{{ url_for('mechanic_bp.accept_quote'",
    content,
    flags=re.MULTILINE
)

# 2. Add "Send to Client" button next to Edit Quote
content = re.sub(
    r"<a href=\"\{\{ url_for\('mechanic_bp\.edit_quote', id=job_card\.id\) \}\}\" class=\"(.*?)\">Edit Quote</a>",
    "<a href=\"{{ url_for('mechanic_bp.edit_quote', id=job_card.id) }}\" class=\"\\1\">Edit Quote</a>\n              <a href=\"{{ url_for('mechanic_bp.email_document', id=job_card.id) }}\" class=\"px-4 py-2 bg-indigo-600 text-white font-bold rounded-lg hover:bg-indigo-700 shadow-sm transition text-sm mr-2\"><i class=\"fas fa-paper-plane mr-2\"></i>Send to Client</a>",
    content
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
