import re

with open('templates/program_billing/consumption_table.html', 'r', encoding='utf-8') as f:
    text = f.read()

email_button = '''<button onclick="sendEmail()" class="inline-flex items-center px-4 py-2 bg-blue-600 text-white text-sm font-semibold rounded-lg hover:bg-blue-700 transition shadow-sm">
            <svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"></path></svg>
            Email
          </button>'''

text = re.sub(
    r'<a href="mailto:.*?</a>',
    email_button,
    text,
    flags=re.DOTALL
)

js_script = '''
{% block scripts %}
<script>
async function sendEmail() {
    const email = prompt("Enter email address to send this consumption review to:", "{{ current_user.email }}");
    if (!email) return;
    
    try {
        const response = await fetch("{{ url_for('billing_bp.email_consumption', property_id=property.id, month=month) }}", {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': '{{ csrf_token() }}'
            },
            body: JSON.stringify({ email: email })
        });
        
        const result = await response.json();
        if (result.success) {
            alert("Email sent successfully to " + email);
        } else {
            alert("Failed to send email: " + result.error);
        }
    } catch(err) {
        alert("Error sending email.");
    }
}
</script>
{% endblock %}
'''

if '{% block scripts %}' not in text:
    text = text + js_script

with open('templates/program_billing/consumption_table.html', 'w', encoding='utf-8') as f:
    f.write(text)

print('Updated consumption_table.html with standardized email JS')
