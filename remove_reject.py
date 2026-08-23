import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Remove the Reject button form
reject_btn_regex = r'<form method="POST" action="\{\{ url_for\(\'mechanic_bp\.reject_quote\', id=job\.id\) \}\}" class="inline m-0 ml-1">.*?</form>'
content = re.sub(reject_btn_regex, '', content, flags=re.DOTALL)

# 2. Remove the Rejected Quotes tab button
rejected_tab_regex = r'<button onclick="switchTab\(\'rejected\'\)".*?Rejected<br>Quotes.*?</button>'
content = re.sub(rejected_tab_regex, '', content, flags=re.DOTALL)

# 3. Remove the Rejected Quotes table block
rejected_content_regex = r'<div id="tab-content-rejected" class="tab-pane hidden">.*?</div>'
content = re.sub(rejected_content_regex, '', content, flags=re.DOTALL)

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
