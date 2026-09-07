import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_import = 'from flask import render_template, request, redirect, url_for, flash, current_app, jsonify, send_from_directory'
new_import = 'from flask import render_template, request, redirect, url_for, flash, current_app, jsonify, send_from_directory, session, abort'

if old_import in text:
    text = text.replace(old_import, new_import, 1)
else:
    # Fallback if the exact string doesn't match
    text = "from flask import session\n" + text

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
