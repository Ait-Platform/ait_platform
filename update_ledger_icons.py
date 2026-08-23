import re

with open('templates/program_mechanic/client_ledger.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''                      <td class="px-4 py-3 whitespace-nowrap text-right text-sm font-medium">
                          <a href="{{ url_for('mechanic_bp.job_card_detail', id=jc.id) }}" title="View Hub" class="w-8 h-8 inline-flex items-center justify-center rounded-full bg-indigo-50 text-indigo-600 hover:bg-indigo-100 transition mr-2"><i class="fas fa-eye"></i></a>
                          <a href="{{ url_for('mechanic_bp.download_document', id=jc.id) }}" title="Download PDF" class="w-8 h-8 inline-flex items-center justify-center rounded-full bg-green-50 text-green-600 hover:bg-green-100 transition mr-2"><i class="fas fa-file-pdf"></i></a>
                          <a href="{{ url_for('mechanic_bp.email_document', id=jc.id) }}" title="Email Invoice" class="w-8 h-8 inline-flex items-center justify-center rounded-full bg-blue-50 text-blue-600 hover:bg-blue-100 transition"><i class="fas fa-envelope"></i></a>
                      </td>'''

content = re.sub(
    r"<td class=\"px-4 py-3 whitespace-nowrap text-right text-sm font-medium\">\s*<a href=\"\{\{ url_for\('mechanic_bp\.job_card_detail', id=jc\.id\) \}\}\" class=\"text-indigo-600 hover:text-indigo-900 mr-3\">View Hub</a>\s*<a href=\"\{\{ url_for\('mechanic_bp\.download_document', id=jc\.id\) \}\}\" class=\"text-green-600 hover:text-green-900 mr-3\">Download PDF</a>\s*<a href=\"\{\{ url_for\('mechanic_bp\.email_document', id=jc\.id\) \}\}\" class=\"text-blue-600 hover:text-blue-900\">Email</a>\s*</td>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/client_ledger.html', 'w', encoding='utf-8') as f:
    f.write(content)
