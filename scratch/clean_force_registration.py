html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

start_tag = '<!-- Post-Pledge Registration Modal -->'
end_tag = '{% endblock %}'

start_idx = html.find(start_tag)
if start_idx != -1:
    end_idx = html.rfind(end_tag)
    
    new_modal = '''<!-- Post-Pledge Registration Modal -->
{% if has_pledged and not current_user.is_authenticated %}
<div id="reg-modal" class="fixed inset-0 z-[70] bg-slate-900 bg-opacity-75 flex items-center justify-center p-4 backdrop-blur-sm pointer-events-auto">
    <div class="bg-white rounded-xl shadow-2xl w-full max-w-md overflow-hidden border border-slate-200 text-center relative">
        <div class="p-8">
            <div class="w-16 h-16 bg-emerald-100 rounded-full flex items-center justify-center mx-auto mb-4">
                <i class="fas fa-check text-emerald-600 text-3xl"></i>
            </div>
            <h3 class="font-black text-2xl text-slate-800 mb-2">Pledge Accepted!</h3>
            <p class="text-slate-600 mb-6">Thank you for acknowledging the IP Pledge. To generate your secure access links and return to this dashboard in the future, you must register a free account now.<br><br>Going forward, you will access this Control Centre via the standard AIT Sign In page.</p>
            <a href="{{ url_for('auth_bp.register', next=request.path) }}" class="block w-full py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-md transition text-lg mb-1">
                Register My Account
            </a>
        </div>
    </div>
</div>
{% endif %}

{% endblock %}'''
    
    html = html[:start_idx] + new_modal

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
