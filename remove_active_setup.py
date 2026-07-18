import re

with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# We need to replace the entire `<!-- Tile 1: Set Property Map -->` block up to `<!-- Property Data Table -->`
# Actually, I can just replace the whole `<div class="grid grid-cols-1 md:grid-cols-2 gap-6">` ... `</div>` block
# Let's use a regex to grab it.
pattern = re.compile(r'<div class="grid grid-cols-1 md:grid-cols-2 gap-6">.*?</div>\s*</div>\s*<!-- Setup Modal -->', re.DOTALL)

new_content = """<div class="grid grid-cols-1 gap-6 max-w-xl mx-auto">
          <button onclick="document.getElementById('setupModal').classList.remove('hidden')" class="text-left block rounded-xl border p-8 shadow-sm transition hover:shadow-md bg-white border-slate-200 hover:border-blue-300 group">
              <div class="font-bold text-slate-800 text-xl group-hover:text-blue-700 flex items-center">
                <span class="w-8 h-8 rounded-full bg-blue-100 text-blue-700 flex items-center justify-center mr-3 text-sm">+</span>
                Start New Property Setup
              </div>
              <div class="mt-3 text-sm text-slate-600 leading-relaxed">Initialize a new property by defining the total meters, expected accounts, and bulk configurations. Once initiated, you can complete the 11-step wizard from the table below.</div>
          </button>
        </div>
      </div>
      
      <!-- Setup Modal -->"""

content = pattern.sub(new_content, content)

with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Removed active setup tiles from manager_dashboard.html")
