import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

if '<script>' not in html:
    script_block = '''
<script>
function openPledgeModal() {
    document.getElementById('pledge-modal').classList.remove('hidden');
}
function closePledgeModal() {
    document.getElementById('pledge-modal').classList.add('hidden');
}
</script>
'''
    html = html.replace('{% endblock %}', script_block + '{% endblock %}')

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
