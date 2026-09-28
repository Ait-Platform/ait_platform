import re

filepath = 'templates/program_uip/dashboards/secretary_workspace.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Replace Tile 5 link and text
old_tile_5 = r'<!-- Tile 5: Meetings & Agendas -->.*?</a>'

new_tile_5 = """<!-- Tile 5: Mandate Recording -->
        <a href="{{ url_for('uip_bp.mandate_recording_desk', org_slug=org.slug) }}" class="group block relative overflow-hidden rounded-2xl border bg-emerald-50 border-emerald-100 hover:border-emerald-300 hover:shadow-md transition-all duration-300">
            <div class="p-6">
                <div class="flex justify-between items-start mb-4">
                    <div class="w-12 h-12 rounded-full flex items-center justify-center text-xl bg-emerald-100 text-emerald-600">
                        <i class="fas fa-file-signature"></i>
                    </div>
                </div>
                <h2 class="text-xl font-black text-emerald-900 mb-1">Mandate Recording Desk</h2>
                <p class="text-sm text-emerald-600/70 font-medium">Continuously record foundational mandates and resolutions into the public register.</p>
            </div>
            <div class="h-1.5 w-full bg-emerald-400 absolute bottom-0 left-0 opacity-0 group-hover:opacity-100 transition-opacity"></div>
        </a>"""

content = re.sub(old_tile_5, new_tile_5, content, flags=re.DOTALL)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("secretary_workspace updated")
