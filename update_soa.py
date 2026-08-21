import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

# We need to replace everything from <!DOCTYPE html> to <body class="..."> with:
# {% extends 'layout.html' %}
# {% block head %}
# <style> ... </style>
# {% endblock %}
# {% block flashes %}{% endblock %}
# {% block page_wrap_classes %}mx-auto w-full max-w-6xl px-4 py-8{% endblock %}
# {% block content %}

top_replacement = '''{% extends 'layout.html' %}
{% block head %}
    <style>
        @page {
            size: A4;
            margin: 1cm;
        }
        @media print {
            body { background-color: white !important; }
            .no-print { display: none !important; }
            .print-container { box-shadow: none !important; max-width: none !important; width: 100% !important; margin: 0 !important; padding: 40px !important; box-sizing: border-box !important; border: none !important; }
        }
    
        /* Custom CSS to replace Tailwind for SOA */
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: #f3f4f6; color: #1f2937; line-height: 1.5; }
        .max-w-4xl { max-width: 56rem; margin-left: auto; margin-right: auto; }
        .mb-8 { margin-bottom: 2rem; }
        .mt-6 { margin-top: 1.5rem; }
        .bg-white { background-color: #ffffff; }
        .rounded-xl { border-radius: 0.75rem; }
        .shadow { box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06); }
        .shadow-lg { box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05); }
        .border { border-width: 1px; border-style: solid; }
        .border-slate-200 { border-color: #e2e8f0; }
        .border-gray-200 { border-color: #e5e7eb; }
        .border-gray-300 { border-color: #d1d5db; }
        .border-gray-800 { border-color: #1f2937; }
        .border-b-2 { border-bottom-width: 2px; border-bottom-style: solid; }
        .border-b { border-bottom-width: 1px; border-bottom-style: solid; }
        .overflow-hidden { overflow: hidden; }
        .h-2 { height: 0.5rem; }
        .h-3 { height: 0.75rem; }
        .bg-blue-600 { background-color: #2563eb; }
        .text-blue-600 { color: #2563eb; }
        .bg-blue-50 { background-color: #eff6ff; }
        .border-blue-200 { border-color: #bfdbfe; }
        .text-blue-800 { color: #1e40af; }
        .p-6 { padding: 1.5rem; }
        .p-4 { padding: 1rem; }
        .p-10 { padding: 2.5rem; }
        .py-8 { padding-top: 2rem; padding-bottom: 2rem; }
        .py-2 { padding-top: 0.5rem; padding-bottom: 0.5rem; }
        .px-3 { padding-left: 0.75rem; padding-right: 0.75rem; }
        .px-4 { padding-left: 1rem; padding-right: 1rem; }
        .pb-6 { padding-bottom: 1.5rem; }
        .pb-4 { padding-bottom: 1rem; }
        .flex { display: flex; }
        .justify-between { justify-content: space-between; }
        .justify-end { justify-content: flex-end; }
        .items-center { align-items: center; }
        .items-start { align-items: flex-start; }
        .items-end { align-items: flex-end; }
        .mb-4 { margin-bottom: 1rem; }
        .mb-2 { margin-bottom: 0.5rem; }
        .mb-1 { margin-bottom: 0.25rem; }
        .mt-4 { margin-top: 1rem; }
        .mt-2 { margin-top: 0.5rem; }
        .mt-1 { margin-top: 0.25rem; }
        .text-3xl { font-size: 1.875rem; line-height: 2.25rem; }
        .text-2xl { font-size: 1.5rem; line-height: 2rem; }
        .text-lg { font-size: 1.125rem; line-height: 1.75rem; }
        .text-sm { font-size: 0.875rem; line-height: 1.25rem; }
        .text-xs { font-size: 0.75rem; line-height: 1rem; }
        .font-bold { font-weight: 700; }
        .font-semibold { font-weight: 600; }
        .font-medium { font-weight: 500; }
        .font-extrabold { font-weight: 800; }
        .text-slate-800 { color: #1e293b; }
        .text-gray-900 { color: #111827; }
        .text-gray-800 { color: #1f2937; }
        .text-gray-700 { color: #374151; }
        .text-gray-600 { color: #4b5563; }
        .text-gray-500 { color: #6b7280; }
        .text-gray-400 { color: #9ca3af; }
        .text-red-600 { color: #dc2626; }
        .text-green-600 { color: #16a34a; }
        .bg-gray-200 { background-color: #e5e7eb; }
        .bg-gray-100 { background-color: #f3f4f6; }
        .bg-gray-50 { background-color: #f9fafb; }
        .text-white { color: #ffffff; }
        .rounded { border-radius: 0.25rem; }
        .shadow-sm { box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.05); }
        .transition { transition-property: color, background-color, border-color, text-decoration-color, fill, stroke, opacity, box-shadow, transform, filter, backdrop-filter; transition-timing-function: cubic-bezier(0.4, 0, 0.2, 1); transition-duration: 150ms; }
        .w-1\/3 { width: 33.333333%; }
        .w-2\/3 { width: 66.666667%; }
        .w-full { width: 100%; }
        .w-24 { width: 6rem; }
        .w-28 { width: 7rem; }
        .w-32 { width: 8rem; }
        .text-right { text-align: right; }
        .text-center { text-align: center; }
        .uppercase { text-transform: uppercase; }
        .tracking-wide { letter-spacing: 0.025em; }
        .tracking-wider { letter-spacing: 0.05em; }
        .tracking-widest { letter-spacing: 0.1em; }
        .leading-tight { line-height: 1.25; }
        .whitespace-pre-line { white-space: pre-line; }
        .whitespace-nowrap { white-space: nowrap; }
        .object-contain { object-fit: contain; }
        .max-h-24 { max-height: 6rem; }
        .h-24 { height: 6rem; }
        .w-48 { width: 12rem; }
        .border-collapse { border-collapse: collapse; }
        .italic { font-style: italic; }
        .min-h-\[1056px\] { min-height: 1056px; }
        .relative { position: relative; }
        .block { display: block; }
        .inline-block { display: inline-block; }
        a { text-decoration: none; cursor: pointer; }
        
        /* Modal specific */
        .fixed { position: fixed; }
        .inset-0 { top: 0; right: 0; bottom: 0; left: 0; }
        .z-50 { z-index: 50; }
        .bg-black { background-color: #000000; }
        .bg-opacity-50 { background-color: rgba(0, 0, 0, 0.5); }
        .hidden { display: none !important; }
        .max-w-md { max-width: 28rem; }
        .focus\:outline-none:focus { outline: 2px solid transparent; outline-offset: 2px; }
        .focus\:border-blue-500:focus { border-color: #3b82f6; }
        
        button { border: none; cursor: pointer; font-family: inherit; }

    </style>
{% endblock %}

{% block flashes %}{% endblock %}
{% block page_wrap_classes %}mx-auto w-full max-w-6xl px-4 py-8{% endblock %}

{% block content %}
<div class="max-w-4xl mx-auto mb-8 mt-6 no-print">
  <div class="bg-white rounded-xl shadow overflow-hidden border border-slate-200">
    <div class="h-3 bg-blue-600"></div>
    <div class="p-6">
      <!-- Row 1: Title and Back button -->
      <div class="flex justify-between items-center mb-4 border-b pb-4">
        <h1 class="text-2xl font-bold text-slate-800">Statement Preview</h1>
        {% if return_url %}
          <a href="{{ return_url }}" class="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300 font-semibold text-sm shadow-sm transition">Back</a>
        {% else %}
          <a href="{{ url_for('debtors_bp.debtor_view', debtor_id=debtor.id) }}" class="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300 font-semibold text-sm shadow-sm transition">Back</a>
        {% endif %}
      </div>
      
      <!-- Row 2: Actions -->
      <div class="mb-6 text-sm text-gray-600 bg-blue-50 p-4 rounded-lg border border-blue-200">
        <p><strong>Print / Save PDF:</strong> Uses your browser to print the statement or save it as a PDF.</p>
        <p class="mt-2"><strong>Email Statement:</strong> Instantly sends this exact statement directly to the client's email inbox.</p>
      </div>
      <div class="flex flex-col sm:flex-row justify-end items-center bg-gray-50 p-4 rounded-lg border border-gray-200 gap-4">
        <button onclick="window.print()" class="w-full sm:w-auto px-6 py-3 bg-green-600 text-white rounded-lg hover:bg-green-700 font-bold shadow-md transition flex justify-center items-center">
           <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 17h2a2 2 0 002-2v-4a2 2 0 00-2-2H5a2 2 0 00-2 2v4a2 2 0 002 2h2m2 4h6a2 2 0 002-2v-4a2 2 0 00-2-2H9a2 2 0 00-2 2v4a2 2 0 002 2zm8-12V5a2 2 0 00-2-2H9a2 2 0 00-2 2v4h10z"></path></svg>
           Print / Save PDF
        </button>
        
        <button onclick="document.getElementById('email-soa-modal').classList.remove('hidden')" class="w-full sm:w-auto px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 font-bold shadow-md transition flex justify-center items-center">
           <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"></path></svg>
           Email Statement
        </button>
      </div>
    </div>
  </div>
</div>
<div class="max-w-4xl mx-auto mb-4 no-print">
    {% include "partials/flash_messages.html" %}
</div>
'''

# Find the end of the original top section (the line just before <div class="max-w-4xl mx-auto bg-white p-10...)
end_str = '<div class="max-w-4xl mx-auto bg-white p-10 shadow-lg print-container min-h-[1056px] border border-gray-200 relative">'
idx = content.find(end_str)

if idx != -1:
    content = top_replacement + content[idx:]
    
    # Also replace </body></html> at the bottom with {% endblock %}
    content = content.replace('</body>\n</html>', '{% endblock %}')
    content = content.replace('</body></html>', '{% endblock %}')
    
    # Fix the email modal inputs to have outlines and autofocus
    content = content.replace('class="w-full border-gray-300 rounded focus:border-blue-500 focus:ring-1 focus:ring-blue-500"', 'class="w-full border border-slate-300 rounded focus:border-blue-500 focus:ring-2 focus:ring-blue-500" autofocus')
    content = content.replace('id="to_email"', 'id="to_email" class="w-full border border-slate-300 rounded focus:border-blue-500 focus:ring-2 focus:ring-blue-500" autofocus')

    # Wait, autofocus should ONLY be on the first input (email). The second is message?
    # Actually, I should use regex to specifically target the email input.
    
    with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Successfully replaced top section.")
else:
    print("Could not find the print container start string.")

