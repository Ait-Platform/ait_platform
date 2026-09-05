import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

text = re.sub(r'<h3 class="font-bold text-indigo-900 mb-2 text-lg"><i class="fas fa-users mr-2 text-indigo-600"></i>Waiting for Participants</h3>\s*<p class="text-slate-700 text-base leading-relaxed">Wait for participants to log in\. Click Start to begin the Reading Activity\.</p>',
              '<h3 class="font-bold text-indigo-900 mb-2 text-lg"><i class="fas fa-play-circle mr-2 text-green-600"></i>Workshop Ready</h3>\n                              <p class="text-slate-700 text-base leading-relaxed">Click Start to begin the Reading Activity.</p>',
              text)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
print("Regex replaced")
