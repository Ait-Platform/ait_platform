import re

file_path = 'templates/program_sace/post_test/test.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Insert the CSRF token field right after the form tag opens
form_tag = '''<form action="{{ url_for('sace_bp.submit_post_test') }}" method="POST" class="p-8 space-y-8">'''
csrf_field = '''<form action="{{ url_for('sace_bp.submit_post_test') }}" method="POST" class="p-8 space-y-8">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>'''

text = text.replace(form_tag, csrf_field)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
