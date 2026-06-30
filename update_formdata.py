import re

with open('templates/program_billing/ai_onboarding.html', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''        if (this.subMetersExpected > 0) {
          formData.append('sub_meters', this.subMetersExpected);
        }'''

injection = '''        if (this.subMetersExpected > 0) {
          formData.append('sub_meters', this.subMetersExpected);
        }
        {% if draft_property %}
        formData.append('property_id', '{{ draft_property.id }}');
        {% endif %}'''

content = content.replace(target, injection)

with open('templates/program_billing/ai_onboarding.html', 'w', encoding='utf-8') as f:
    f.write(content)
