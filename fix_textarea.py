import re

filepath = 'templates/program_uip/dashboards/ratification_desk.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Remove the static block
static_block_pattern = re.compile(
    r'<div class="bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden mb-8">\s*<div class="p-8">\s*<h2 class="text-lg font-bold text-slate-500 uppercase tracking-widest mb-4">Mandate Content</h2>\s*<div class="prose prose-slate max-w-none font-medium text-slate-700 whitespace-pre-wrap">\{\{ resolution\.description \}\}</div>\s*</div>\s*</div>',
    re.DOTALL
)
content = static_block_pattern.sub('', content)

# 2. Add textarea inside form
form_pattern = r'<div class="grid grid-cols-1 md:grid-cols-2 gap-6">'
mandate_content = '''<div class="mb-6">
                  <label class="block text-xs font-bold text-slate-400 uppercase tracking-widest mb-2">Mandate Content</label>
                  <textarea name="description" rows="12" class="w-full bg-slate-800 border border-slate-700 rounded-lg px-4 py-3 text-white focus:outline-none focus:border-amber-500 font-mono text-sm whitespace-pre-wrap">{{ resolution.description }}</textarea>
                  <p class="text-[10px] text-slate-500 mt-2">Review or adjust the final recorded wording before saving.</p>
              </div>
              
              <div class="grid grid-cols-1 md:grid-cols-2 gap-6">'''
content = content.replace('<div class="grid grid-cols-1 md:grid-cols-2 gap-6">', mandate_content)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("ratification_desk textarea fixed")
