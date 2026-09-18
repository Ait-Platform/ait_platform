import re
with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    text = f.read()

old_header = """  <!-- RESOLUTIONS REGISTER -->
  <div class="mb-10">
      <div class="mb-4 flex justify-between items-end border-b border-slate-200 pb-2">
          <h2 class="text-sm font-bold text-slate-500 uppercase tracking-widest"><i class="fas fa-file-signature mr-2 text-indigo-500"></i> Resolutions Register</h2>
      </div>"""

new_header = """  <!-- RESOLUTIONS REGISTER -->
  <div class="mb-10">
      <div class="mb-4 flex justify-between items-end border-b border-slate-200 pb-2">
          <h2 class="text-sm font-bold text-slate-500 uppercase tracking-widest"><i class="fas fa-file-signature mr-2 text-indigo-500"></i> Resolutions Register</h2>
          <a href="{{ url_for('uip_bp.draft_resolution', org_slug=org.slug) }}" class="ui-btn ui-btn-primary ui-btn-sm font-bold shadow-sm" style="padding: 0.25rem 0.75rem; font-size: 0.75rem;"><i class="fas fa-pen-nib mr-1"></i> Draft Resolution</a>
      </div>"""

text = text.replace(old_header, new_header)

with open("templates/program_uip/dashboards/committee.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Added draft button to committee dashboard")
