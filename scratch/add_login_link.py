import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_links = '''            <a href="{{ url_for('auth_bp.register', subject='sace', next=request.path) }}" class="block w-full py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-md transition text-lg mb-1">
                Register My Account
            </a>
        </div>'''

new_links = '''            <a href="{{ url_for('auth_bp.register', subject='sace', next=request.path) }}" class="block w-full py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-md transition text-lg mb-3">
                Register My Account
            </a>
            <a href="{{ url_for('auth_bp.login', next=request.path) }}" class="block w-full py-3 bg-slate-100 hover:bg-slate-200 text-slate-700 font-bold rounded-lg shadow-sm transition text-lg mb-1">
                Already registered? Sign In
            </a>
        </div>'''

text = text.replace(old_links, new_links)

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
