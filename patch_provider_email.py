import sys

with open("templates/program_uip/dashboards/provider_dashboard.html", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    'value="{{ provider.contact_email }}"',
    'value="{{ provider.contact_email or current_user.email }}"'
)

with open("templates/program_uip/dashboards/provider_dashboard.html", "w", encoding="utf-8") as f:
    f.write(c)
