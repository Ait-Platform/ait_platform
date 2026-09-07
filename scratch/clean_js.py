import re
html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

# I'll just remove all <script> function openPledgeModal blocks and put one clean one at the very bottom
text = re.sub(r'<script>\s*function openPledgeModal.*?</script>', '', text, flags=re.DOTALL)

clean_script = '''
<script>
function openPledgeModal() {
    var modal = document.getElementById('pledge-modal');
    if(modal) modal.classList.remove('hidden');
}
function closePledgeModal() {
    var modal = document.getElementById('pledge-modal');
    if(modal) modal.classList.add('hidden');
}
</script>
{% endblock %}
'''

text = text.replace('{% endblock %}', '') + clean_script

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
