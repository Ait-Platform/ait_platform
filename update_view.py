import re

filepath = 'templates/program_uip/dashboards/resolution_view.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Remove the tally block
tally_block = '''            <div>
                <div class="text-[10px] uppercase tracking-widest font-bold text-slate-400 mb-1">Live Vote Tally</div>
                <div class="font-bold text-slate-700 text-sm">
                    <span class="text-emerald-600">Y: {{ rat.votes_yea }}</span> &middot; 
                    <span class="text-rose-600">N: {{ rat.votes_nay }}</span> &middot; 
                    <span class="text-amber-600">A: {{ rat.votes_abstain }}</span>
                </div>
            </div>'''
content = content.replace(tally_block, '')

# Adjust grid cols from md:grid-cols-4 to md:grid-cols-3
content = content.replace('<div class="grid grid-cols-2 md:grid-cols-4 gap-6">', '<div class="grid grid-cols-2 md:grid-cols-3 gap-6">')

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated resolution_view")
