import re
with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

old_form = """                  <form method="POST" action="{{ url_for('uip_bp.vote_resolution', org_slug=org.slug, res_id=resolution.id) }}" class="space-y-2">
                      <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                      <button type="submit" name="vote" value="YEA" class="w-full py-3 rounded-lg font-black text-white bg-emerald-600 hover:bg-emerald-700 transition shadow-sm">
                          YEA
                      </button>
                      <button type="submit" name="vote" value="NAY" class="w-full py-3 rounded-lg font-black text-white bg-red-600 hover:bg-red-700 transition shadow-sm">
                          NAY
                      </button>
                      <button type="submit" name="vote" value="ABSTAIN" class="w-full py-2 rounded-lg font-bold text-slate-600 bg-white border border-slate-300 hover:bg-slate-50 transition">
                          ABSTAIN
                      </button>
                  </form>"""

new_form = """                  <form method="POST" action="{{ url_for('uip_bp.vote_resolution', org_slug=org.slug, res_id=resolution.id) }}" class="flex flex-row gap-3 w-full">
                      <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                      <button type="submit" name="vote" value="YEA" class="flex-1 py-3 rounded-lg font-black text-white bg-emerald-600 hover:bg-emerald-700 transition shadow-sm text-center">
                          YEA
                      </button>
                      <button type="submit" name="vote" value="NAY" class="flex-1 py-3 rounded-lg font-black text-white bg-red-600 hover:bg-red-700 transition shadow-sm text-center">
                          NAY
                      </button>
                      <button type="submit" name="vote" value="ABSTAIN" class="flex-1 py-3 rounded-lg font-black text-slate-600 bg-white border border-slate-300 hover:bg-slate-50 transition shadow-sm text-center">
                          ABSTAIN
                      </button>
                  </form>"""

if old_form in text:
    text = text.replace(old_form, new_form)
    print("Replaced voting form successfully.")
else:
    print("Voting form not found! Trying regex.")
    # let's try a more robust regex if spacing doesn't match perfectly
    pattern = re.compile(r'<form method="POST".*?class="space-y-2">.*?</form>', re.DOTALL)
    text = pattern.sub(new_form, text)
    print("Replaced via regex.")

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)
