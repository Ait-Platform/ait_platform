import re

with open('templates/program_mechanic/public_job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''        <div style="margin-top: 30px; font-size: 12px; color: #4b5563; text-align: center; font-weight: bold;">
            Thank you for your business! Only genuine parts used. Professional services guaranteed.
        </div>
        <div style="margin-top: 10px; font-size: 10px; color: #9ca3af; font-style: italic; text-align: left;">E.&O.E.</div>

        <div class="footer">
            {% if shop %}
            <p>{{ shop.business_name }} | {{ shop.email }} | {{ shop.phone }}</p>
            {% endif %}
        </div>
    </div>
</body>
</html>'''

content = re.sub(
    r"        <div style=\"margin-top: 30px; font-size: 10px; color: #9ca3af; font-style: italic;\">E\.&O\.E\.</div>.*?</html>",
    replacement,
    content,
    flags=re.DOTALL
)

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
