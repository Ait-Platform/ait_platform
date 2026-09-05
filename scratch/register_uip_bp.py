import re

with open('app/__init__.py', 'r', encoding='utf-8') as f:
    text = f.read()

if "from app.uip import uip_bp" not in text:
    old_register = "app.register_blueprint(public_bp)"
    new_register = "app.register_blueprint(public_bp)\n\n    from app.uip import uip_bp\n    app.register_blueprint(uip_bp)"
    text = text.replace(old_register, new_register)
    with open('app/__init__.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Registered uip_bp")
else:
    print("uip_bp already registered")
