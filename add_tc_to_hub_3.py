import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

tc_html = '''
        <!-- Terms and Conditions -->
        {% if shop and shop.terms_and_conditions %}
        <div class="mt-8 bg-slate-50 border border-slate-200 rounded-xl p-5 shadow-sm">
          <h3 class="text-sm font-bold text-slate-700 uppercase tracking-wider mb-2">Terms & Conditions</h3>
          <div class="text-sm text-slate-600 whitespace-pre-line">{{ shop.terms_and_conditions }}</div>
          <p class="text-xs text-slate-400 mt-3 italic">This is automatically attached to the bottom of the PDF sent to the client. You can edit this in your Shop Settings on the Dashboard.</p>
        </div>
        {% endif %}
'''

content = content.replace(
    '    <!-- Contact Client Modal -->',
    tc_html + '\n    <!-- Contact Client Modal -->'
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
