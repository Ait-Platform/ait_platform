import re

file_path = 'templates/auth/login.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_link = '''<a
          href="{{ url_for('public_bp.welcome') }}"
          class="text-sky-600 hover:underline"
        >Create one</a>'''

new_link = '''<a
          href="{{ url_for('auth_bp.register', next=request.args.get('next')) }}"
          class="text-sky-600 hover:underline font-bold"
        >Create one</a>'''

text = text.replace(old_link, new_link)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
