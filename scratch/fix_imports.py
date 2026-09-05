import re

with open('app/uip/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('from flask import render_template, redirect, url_for, flash, request', 'from flask import render_template, redirect, url_for, flash, request, abort')

with open('app/uip/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
