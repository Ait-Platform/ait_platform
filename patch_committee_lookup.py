with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    text = f.read()

old_list = """{% set escalation_list = org.member_profiles|selectattr("eligibility_status", "equalto", "unverified")|selectattr("invite_wave", "equalto", 3)|list %}"""

# We can't easily filter by nested attributes in Jinja without custom filters. 
# So we'll iterate and build the list.
new_list = """{% set escalation_list = [] %}
    {% for rp in org.member_profiles if rp.eligibility_status == 'unverified' and rp.campaign_status and rp.campaign_status.invite_wave == 3 %}
        {% set _ = escalation_list.append(rp) %}
    {% endfor %}"""

text = text.replace(old_list, new_list)

with open("templates/program_uip/dashboards/committee.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated committee.html nested attribute check")
