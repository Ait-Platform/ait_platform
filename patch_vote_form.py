with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

import re

# 1. Update colors for YEA
text = re.sub(r'<button type="submit" name="vote" value="YEA" class=".*?"',
              r'<button type="submit" name="vote" value="YEA" class="flex-1 py-3 rounded-lg font-black text-green-800 bg-green-100 border border-green-300 hover:bg-green-200 transition shadow-sm text-center"', text)

# Update colors for NAY
text = re.sub(r'<button type="submit" name="vote" value="NAY" class=".*?"',
              r'<button type="submit" name="vote" value="NAY" class="flex-1 py-3 rounded-lg font-black text-red-800 bg-red-100 border border-red-300 hover:bg-red-200 transition shadow-sm text-center"', text)

# Update colors for ABSTAIN
text = re.sub(r'<button type="submit" name="vote" value="ABSTAIN" class=".*?"',
              r'<button type="submit" name="vote" value="ABSTAIN" class="flex-1 py-3 rounded-lg font-black text-amber-800 bg-amber-100 border border-amber-300 hover:bg-amber-200 transition shadow-sm text-center"', text)


# 2. Hide the form once a vote is cast
# Currently:
# {% if has_voted %}
# <div class="mb-4 text-sm text-indigo-700 bg-indigo-100 p-2 rounded text-center font-bold">
#     You securely voted: {{ my_vote.vote }} or similar
# </div>
# {% endif %}
# <form ...> ... </form>

# We need to wrap the <form> in {% if not has_voted %}
form_pattern = r'(<form method="POST" action="{{ vote_url.*?</form>)'
replacement = r'{% if not has_voted %}\n                \1\n                {% endif %}'

text = re.sub(form_pattern, replacement, text, flags=re.DOTALL)

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)

print("Applied colors and hid form on vote cast")
