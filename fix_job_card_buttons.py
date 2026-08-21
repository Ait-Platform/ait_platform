import sys

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''          <div class="flex items-center gap-3">
            {% if not job_card.vehicle.client.email or not job_card.vehicle.client.phone %}'''

new_target = '''          <div class="flex items-center gap-3">
              <a href="{{ url_for('mechanic_bp.download_document', id=job_card.id) }}" class="inline-flex items-center rounded-lg border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-700 shadow-sm hover:bg-slate-50 transition">
                <i class="fas fa-file-pdf mr-2"></i> Download PDF
              </a>
            {% if not job_card.vehicle.client.email or not job_card.vehicle.client.phone %}'''

content = content.replace(target, new_target)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
