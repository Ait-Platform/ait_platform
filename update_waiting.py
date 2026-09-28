import re

filepath = 'templates/program_uip/ratepayer_waiting.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

bad_buttons = '''<a class="ui-btn" href="{{ url_for('uip_bp.verify_ratepayer', org_slug=org.slug) }}">Check again</a>
<a class="ui-btn" href="{{ url_for('uip_bp.router_page', org_slug=org.slug, force=1) }}">Role Selection</a>'''

good_buttons = '''
<div class="mt-6 flex flex-col sm:flex-row gap-4">
    <a class="bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-2 px-6 rounded-lg shadow-sm transition inline-flex items-center justify-center" href="{{ url_for('uip_bp.verify_ratepayer', org_slug=org.slug) }}">
        <i class="fas fa-sync-alt mr-2"></i> Check Vault Again
    </a>
    <form action="{{ url_for('uip_bp.claim_ratepayer', org_slug=org.slug) }}" method="POST">
        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
        <button type="submit" class="bg-amber-500 hover:bg-amber-600 text-white font-bold py-2 px-6 rounded-lg shadow-sm transition inline-flex items-center justify-center w-full">
            <i class="fas fa-hand-paper mr-2"></i> Submit Manual Claim
        </button>
    </form>
    <a class="bg-slate-100 hover:bg-slate-200 text-slate-700 font-bold py-2 px-6 rounded-lg shadow-sm transition inline-flex items-center justify-center border border-slate-300" href="{{ url_for('uip_bp.router_page', org_slug=org.slug, force=1) }}">
        <i class="fas fa-arrow-left mr-2"></i> Role Selection
    </a>
</div>
'''

if 'Submit Manual Claim' not in content:
    content = content.replace(bad_buttons, good_buttons)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
print("ratepayer_waiting.html updated")
