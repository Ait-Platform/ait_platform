import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = """
                    <!-- App View: Generic Fallback (Look at Projector) -->
                    <div id="app-view-1" class="app-view hidden text-center">
                        <div class="h-24 w-24 bg-slate-100 rounded-full flex items-center justify-center mb-6 mx-auto">
                            <i class="fas fa-chalkboard-teacher text-5xl text-slate-400"></i>
                        </div>
                        <h3 class="text-2xl font-bold text-slate-800 mb-2">Eyes on the Projector</h3>
                        <p class="text-slate-600 mb-6">The facilitator is currently presenting. Please direct your attention to the main screen.</p>
                        <div class="inline-flex items-center bg-indigo-50 text-indigo-700 px-4 py-2 rounded-full text-sm font-bold border border-indigo-100 shadow-sm">
                            <i class="fas fa-sync-alt fa-spin mr-2"></i> Waiting for next activity...
                        </div>
                    </div>
"""

# Replace old app-view-1
content = re.sub(r'<!-- App View: Slide 1 -->\s*<div id="app-view-1".*?</div>\s*</div>', replacement.strip(), content, flags=re.DOTALL)

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
