import re

filepath = 'templates/program_uip/dashboards/ratification_desk.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Rename "Live Ratification Desk" to "Mandate Recording Desk"
content = content.replace('Live Ratification Desk', 'Mandate Recording Desk')

# 2. Change button text
content = content.replace('Save Ratification Record', 'Save to Mandate Register')

# 3. Just to be thorough, if there are any other places with "Post-Meeting Ratification", we could rename it too.
content = content.replace('Post-Meeting Ratification', 'Mandate Recording')
content = content.replace('The digital temperature check is complete. After the live meeting concludes, formally record the final outcome and the live vote tally here.', 'Formally record the outcome of the meeting or the external mandate to lock it into the register.')

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("ratification_desk updated")
