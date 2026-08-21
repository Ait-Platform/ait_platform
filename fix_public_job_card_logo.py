import sys

with open('templates/program_mechanic/public_job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''            {% if shop and shop.use_custom_letterhead and shop.letterhead_url %}
            <img src="{{ url_for('static', filename='uploads/mechanic/' + shop.letterhead_url, _external=True) }}" alt="Shop Letterhead">
            {% else %}
            <h1>{{ shop.business_name if shop else 'AIT ProTrade' }}</h1>
            {% endif %}'''

new_target = '''            {% if shop and shop.use_custom_letterhead and shop.letterhead_url %}
            <img src="{{ url_for('static', filename='uploads/mechanic/' + shop.letterhead_url, _external=True) }}" alt="Shop Letterhead" style="max-height: 120px; width: 100%; object-fit: cover;">
            {% elif shop and shop.logo_url %}
            <img src="{{ url_for('static', filename='uploads/mechanic/' + shop.logo_url, _external=True) }}" alt="Shop Logo" style="max-height: 120px;">
            <h1>{{ shop.business_name }}</h1>
            {% else %}
            <h1>{{ shop.business_name if shop else 'AIT ProTrade' }}</h1>
            {% endif %}'''

content = content.replace(target, new_target)

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
