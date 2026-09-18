import re

with open("templates/program_uip/router.html", "r", encoding="utf-8") as f:
    text = f.read()

# The block to remove
block_to_remove = """                <!-- Public Tile (Anonymous Reporting - Left isolated at bottom) -->
                <a href="{{ url_for('uip_bp.verify_public', org_slug=org.slug) }}" class="p-4 block mt-8 rounded-xl shadow-sm border border-rose-100 transition-transform hover:-translate-y-1 flex items-center justify-between" style="background-color: #fff1f2; text-decoration: none;">
                    <div class="flex items-center">
                        <i class="fas fa-bullhorn text-2xl w-10 text-rose-600 mr-4 text-center"></i>
                        <div>
                            <h2 class="text-lg font-bold text-slate-900 mb-0">Report an Issue</h2>
                            <p class="text-xs text-slate-500">Anonymous community reporting of public faults.</p>
                        </div>
                    </div>
                    <i class="fas fa-chevron-right text-rose-200 ml-2"></i>
                </a>"""

if block_to_remove in text:
    text = text.replace(block_to_remove, "")
    with open("templates/program_uip/router.html", "w", encoding="utf-8") as f:
        f.write(text)
    print("Removed 'Report an Issue' tile successfully.")
else:
    print("Block not found!")
