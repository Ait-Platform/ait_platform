with open("app/auth/forms.py", "r", encoding="utf-8") as f:
    text = f.read()

# Remove Email() from LoginForm
old_login = """class LoginForm(FlaskForm):
    email = StringField("Email", validators=[DataRequired(), Email(), Length(max=255)])"""

new_login = """class LoginForm(FlaskForm):
    email = StringField("Email", validators=[DataRequired(), Length(max=255)])"""

if old_login in text:
    text = text.replace(old_login, new_login)
    with open("app/auth/forms.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched LoginForm")
else:
    print("Could not find LoginForm")
