import re

with open('templates/payments/quote.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Add the Murphy's Law message to the left panel right before "Price (your currency)"
old_html = """        <div class="mt-6">
          <div class="text-base font-bold text-slate-800 mb-2">Price (your currency)</div>"""

new_html = """        {% if 'sace' in subject_slug %}
        <div class="mt-6 mb-4 p-4 bg-emerald-50 border border-emerald-200 rounded-lg">
          <p class="text-sm text-emerald-800 font-semibold flex items-start">
            <svg class="w-5 h-5 text-emerald-600 mr-2 mt-0.5 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
            <span><strong>Are you a SACE or DBE sponsored educator?</strong><br>Don't worry about the price below! Continue below with the email address provided to you by your institution, and the system will automatically recognize you and grant you free access.</span>
          </p>
        </div>
        {% endif %}

        <div class="mt-6">
          <div class="text-base font-bold text-slate-800 mb-2">Price (your currency)</div>"""

if old_html in text:
    text = text.replace(old_html, new_html)
    with open('templates/payments/quote.html', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Added message to quote.html")
else:
    print("Could not find html block in quote.html")
