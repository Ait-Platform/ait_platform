import re
with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Bigger Title
old_title = """        <div>
            <div class="text-xs font-bold text-slate-500 uppercase tracking-widest mb-3">
                <i class="fas fa-landmark text-indigo-500 mr-2"></i> 
                {% if resolution.status == 'ADOPTED' %}Historical Resolution Record{% elif resolution.status == 'DRAFT' %}Drafting Desk{% else %}Digital Voting Room{% endif %}
            </div>"""

new_title = """        <div class="w-full">
            <div class="text-lg font-black text-slate-800 uppercase tracking-widest mb-4 border-b border-slate-200 pb-2 flex items-center">
                <i class="fas fa-landmark text-indigo-600 mr-3 text-xl"></i> 
                {% if resolution.status == 'ADOPTED' or resolution.status == 'REJECTED' %}Historical Resolution Record{% elif resolution.status == 'DRAFT' %}Drafting Desk{% else %}Digital Voting Room{% endif %}
            </div>"""
text = text.replace(old_title, new_title)

# 2. Remove Debate Thread
pattern = re.compile(r'\s*<!-- Debate Thread -->\s*<div class="bg-white border border-slate-200 rounded-xl shadow-sm flex flex-col h-96">.*?</div>\s*</div>\s*</div>', re.DOTALL)
match = pattern.search(text)
if match:
    text = text.replace(match.group(0), "\n            </div>")
    print("Removed debate thread.")
else:
    print("Could not find Debate Thread block with regex.")
    
# Let's do a more robust debate thread removal if the above fails
if "<!-- Debate Thread -->" in text:
    print("Still found Debate Thread! Trying alternate removal.")
    start = text.find("<!-- Debate Thread -->")
    # find the end of the debate thread block. It's right before </div>\n        </div>\n        \n        <!-- Right Column"
    end_marker = "        <!-- Right Column: Voting & Debate -->"
    if end_marker in text:
        end = text.find(end_marker)
        # we want to keep the closing divs for the left column
        text = text[:start] + "            </div>\n        </div>\n        \n" + text[end:]
        print("Removed debate thread via string slicing.")

# 3. Fix Cast Your Vote Buttons
old_buttons = """                  <div class="mt-4 pt-4 border-t border-indigo-200">
                      <form method="POST" action="{{ url_for('uip_bp.vote_resolution', org_slug=org.slug, res_id=resolution.id) }}" class="flex gap-2">
                          <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                          <button type="submit" name="vote" value="YEA" class="flex-1 bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-2 rounded transition text-sm text-center">YEA</button>
                          <button type="submit" name="vote" value="NAY" class="flex-1 bg-red-600 hover:bg-red-500 text-white font-bold py-2 rounded transition text-sm text-center">NAY</button>
                          <button type="submit" name="vote" value="ABSTAIN" class="flex-1 bg-slate-500 hover:bg-slate-400 text-white font-bold py-2 rounded transition text-sm text-center">ABSTAIN</button>
                      </form>
                  </div>"""

new_buttons = """                  <div class="mt-4 pt-4 border-t border-indigo-200">
                      <form method="POST" action="{{ url_for('uip_bp.vote_resolution', org_slug=org.slug, res_id=resolution.id) }}" class="flex flex-row justify-between w-full gap-3">
                          <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                          <button type="submit" name="vote" value="YEA" class="flex-1 bg-emerald-600 hover:bg-emerald-500 text-white font-black py-3 rounded-lg transition text-sm text-center shadow-sm">YEA</button>
                          <button type="submit" name="vote" value="NAY" class="flex-1 bg-red-600 hover:bg-red-500 text-white font-black py-3 rounded-lg transition text-sm text-center shadow-sm">NAY</button>
                          <button type="submit" name="vote" value="ABSTAIN" class="flex-1 bg-slate-500 hover:bg-slate-400 text-white font-black py-3 rounded-lg transition text-sm text-center shadow-sm">ABSTAIN</button>
                      </form>
                  </div>"""
text = text.replace(old_buttons, new_buttons)

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated view file")
