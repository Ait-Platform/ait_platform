import sys

with open('templates/program_mechanic/public_job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''            {% if shop and shop.use_custom_letterhead and shop.letterhead_url %}
            <img src="{{ url_for('static', filename='uploads/mechanic/' + shop.letterhead_url, _external=True) }}" alt="Shop Letterhead" style="max-height: 120px; width: 100%; object-fit: cover;">
            {% elif shop and shop.logo_url %}
            <img src="{{ url_for('static', filename='uploads/mechanic/' + shop.logo_url, _external=True) }}" alt="Shop Logo" style="max-height: 120px;">
            <h1>{{ shop.business_name }}</h1>
            {% else %}'''

new_target = '''            {% if shop and shop.use_custom_letterhead and shop.letterhead_url %}
              {% if shop.letterhead_url.startswith('http') %}
                <img src="{{ shop.letterhead_url }}" alt="Shop Letterhead" style="max-height: 120px; width: 100%; object-fit: cover;">
              {% else %}
                <img src="{{ url_for('static', filename='uploads/mechanic/' + shop.letterhead_url, _external=True) }}" alt="Shop Letterhead" style="max-height: 120px; width: 100%; object-fit: cover;">
              {% endif %}
            {% elif shop and shop.logo_url %}
              {% if shop.logo_url.startswith('http') %}
                <img src="{{ shop.logo_url }}" alt="Shop Logo" style="max-height: 120px;">
              {% else %}
                <img src="{{ url_for('static', filename='uploads/mechanic/' + shop.logo_url, _external=True) }}" alt="Shop Logo" style="max-height: 120px;">
              {% endif %}
              <h1>{{ shop.business_name }}</h1>
            {% else %}'''

content = content.replace(target, new_target)

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
