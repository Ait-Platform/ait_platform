with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

import re

# We will just change {% if my_vote %} to {% if has_voted or my_vote %}
text = text.replace("{% if my_vote %}", "{% if has_voted or my_vote %}")
# And change "You securely voted: {{ my_vote.vote }}" to "You have securely cast your vote."
text = text.replace("You securely voted: {{ my_vote.vote }}", "You have securely cast your vote.")

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)

print("Updated template to show has_voted correctly")
