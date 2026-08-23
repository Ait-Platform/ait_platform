import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''          <!-- RIGHT: Shop Profile & Preview Block -->
          <div class="flex flex-col gap-2 h-full">
            <button type="button" onclick="document.getElementById('manual-setup-modal').classList.remove('hidden')" class="block text-left w-full rounded-xl border-2 p-5 shadow-sm transition hover:shadow-md bg-blue-50 border-blue-200 hover:border-blue-400 group flex-1">
              <div class="font-bold text-slate-900 group-hover:text-blue-700 text-lg">Update Shop Profile</div>
              <div class="mt-1 text-sm text-slate-700">Edit business details, terms, and logo.</div>
            </button>
            <a href="{{ url_for('mechanic_bp.document_preview') }}" class="block text-left w-full rounded-xl border-2 p-5 shadow-sm transition hover:shadow-md bg-teal-50 border-teal-200 hover:border-teal-400 group flex-1">
              <div class="font-bold text-slate-900 group-hover:text-teal-700 text-lg">Document Preview</div>
              <div class="mt-1 text-sm text-slate-700">See exactly what clients see on their PDFs.</div>
            </a>
          </div>'''

content = re.sub(
    r"<!-- RIGHT: Shop Profile & Preview Block -->\s*<div class=\"flex flex-col gap-2\">\s*<button type=\"button\".*?Document Preview\s*</a>\s*</div>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
