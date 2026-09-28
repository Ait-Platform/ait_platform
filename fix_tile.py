import re

filepath = 'templates/program_uip/dashboards/secretary_workspace.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

tile8_pattern = re.compile(
    r'<!-- Tile 8: Members Directory -->\s*<a href="#" class="group block relative overflow-hidden rounded-2xl border bg-cyan-50 border-cyan-100 hover:border-cyan-300 hover:shadow-md transition-all duration-300">.*?<h2 class="text-xl font-black text-cyan-900 mb-1">Committee Roster</h2>\s*<p class="text-sm text-cyan-600/70 font-medium">View the active committee health and ExCo members\.</p>\s*</div>\s*<div class="h-1\.5 w-full bg-cyan-400 absolute bottom-0 left-0 opacity-0 group-hover:opacity-100 transition-opacity"></div>\s*</a>',
    re.DOTALL
)

tile8_replacement = """<!-- Tile 8: Public Mandates -->
        <a href="{{ url_for('uip_bp.public_mandates', org_slug=org.slug) }}" class="group block relative overflow-hidden rounded-2xl border bg-cyan-50 border-cyan-100 hover:border-cyan-300 hover:shadow-md transition-all duration-300">
            <div class="p-6">
                <div class="flex justify-between items-start mb-6">
                    <div class="w-12 h-12 rounded-full flex items-center justify-center text-xl bg-cyan-100 text-cyan-500">
                        <i class="fas fa-scroll"></i>
                    </div>
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-[10px] font-bold bg-white text-cyan-400 border border-cyan-100 uppercase tracking-widest">
                        Register
                    </span>
                </div>
                <h2 class="text-xl font-black text-cyan-900 mb-1">Public Mandates</h2>
                <p class="text-sm text-cyan-600/70 font-medium">View the transparent registry of all officially adopted Mandates.</p>
            </div>
            <div class="h-1.5 w-full bg-cyan-400 absolute bottom-0 left-0 opacity-0 group-hover:opacity-100 transition-opacity"></div>
        </a>"""

content = tile8_pattern.sub(tile8_replacement, content)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("Tile 8 replaced successfully")
