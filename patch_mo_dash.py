new_mo_html = """{% extends "program_uip/base.html" %}
{% block title %}{{ org.name }} - Municipal Officer Dashboard{% endblock %}

{% block content %}
<style>
    .ui-sidebar { display: none !important; }
    .ui-shell { grid-template-columns: 1fr !important; display: block !important; }
    .ui-workspace { padding-left: 0 !important; margin-left: 0 !important; max-width: 1200px; margin: 0 auto !important; width: 100%; }
</style>

<div class="max-w-7xl mx-auto px-4 mt-8 mb-16">
    <header class="ui-header-2row">
        <!-- Row 1: Title & Back Button -->
        <div class="ui-header-2row-top">
            <h1 class="text-3xl font-extrabold text-slate-900 tracking-tight ui-header-2row-title">Municipal Action Board</h1>
            <a href="{{ url_for('uip_bp.router_page', org_slug=org.slug) }}" class="inline-flex items-center text-sm font-bold text-slate-500 hover:text-indigo-600 transition" style="white-space: nowrap;">
                <i class="fas fa-hat-cowboy mr-2"></i> Switch Hat
            </a>
        </div>
        <!-- Row 2: Subtitle & Action Buttons -->
        <div class="ui-header-2row-bottom">
            <p class="text-slate-500 font-medium ui-header-2row-subtitle">Logged in as: <strong>{{ current_user.name }}</strong> ({{ current_user.email }})</p>
            <div class="ui-header-actions">
                <button class="px-4 py-2 bg-white border border-slate-200 hover:bg-slate-50 hover:border-slate-300 text-slate-700 text-sm font-bold rounded-lg shadow-sm transition"><i class="fas fa-file-upload mr-2"></i> Upload RP Vault</button>
                <button onclick="document.getElementById('photoModal').classList.remove('hidden')" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-bold rounded-lg shadow-sm transition" style="white-space: nowrap;">
                    <i class="fas fa-camera mr-2"></i> Upload Organogram Photo
                </button>
            </div>
        </div>
    </header>

    {% include "partials/flash_messages.html" %}

    <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
        <div class="col-span-1 bg-white p-6 rounded-xl border border-slate-200 shadow-sm">
            <h2 class="text-lg font-bold text-slate-900 mb-2"><i class="fas fa-chart-line text-indigo-500 mr-2"></i> Oversight Metrics</h2>
            <p class="text-sm text-slate-500 mb-4">Monthly service delivery SLA compliance</p>
            <div class="space-y-3">
                <div class="flex justify-between items-center border-b border-slate-100 pb-2">
                    <span class="text-sm font-medium text-slate-600">Potholes Repaired</span>
                    <span class="text-sm font-bold text-emerald-600">92%</span>
                </div>
                <div class="flex justify-between items-center border-b border-slate-100 pb-2">
                    <span class="text-sm font-medium text-slate-600">Streetlights Fixed</span>
                    <span class="text-sm font-bold text-amber-500">74%</span>
                </div>
                <div class="flex justify-between items-center pb-2">
                    <span class="text-sm font-medium text-slate-600">Water Leaks</span>
                    <span class="text-sm font-bold text-emerald-600">88%</span>
                </div>
            </div>
        </div>
        
        <div class="col-span-2 bg-white p-6 rounded-xl border border-slate-200 shadow-sm">
            <h2 class="text-lg font-bold text-slate-900 mb-4"><i class="fas fa-exclamation-triangle text-amber-500 mr-2"></i> Escalated Master Tickets</h2>
            <p class="text-sm text-slate-500 mb-6">Deduplicated, verified municipal issues escalated by the UIP Operations Desk.</p>
            
            <div class="overflow-x-auto">
                <table class="w-full text-left border-collapse">
                    <thead>
                        <tr class="border-b-2 border-slate-200 bg-slate-50">
                            <th class="p-4 font-semibold text-slate-700 text-sm">Ticket ID</th>
                            <th class="p-4 font-semibold text-slate-700 text-sm">Actions</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for referral in escalations %}
                        <tr class="border-b border-slate-100 hover:bg-slate-50 transition">
                            <td class="p-4 font-mono text-sm text-slate-500">#{{ referral.interaction.id }}</td>
                            <td class="p-4">
                                <span class="text-xs text-slate-400">Placeholder Action</span>
                            </td>
                        </tr>
                        {% else %}
                        <tr>
                            <td colspan="2" class="p-8 text-center text-slate-500">
                                <i class="fas fa-check-circle text-3xl text-slate-200 mb-2 block"></i>
                                No active escalations. Your board is clear!
                            </td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
</div>

<!-- Photo Upload Modal -->
<div id="photoModal" class="hidden fixed inset-0 bg-slate-900/50 backdrop-blur-sm z-50 flex items-center justify-center p-4">
    <div class="bg-white rounded-2xl shadow-xl w-full max-w-md overflow-hidden">
        <div class="px-6 py-4 border-b border-slate-100 flex justify-between items-center bg-slate-50">
            <h3 class="font-bold text-slate-800"><i class="fas fa-camera text-indigo-500 mr-2"></i> Update Organogram Photo</h3>
            <button onclick="document.getElementById('photoModal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600"><i class="fas fa-times"></i></button>
        </div>
        <div class="p-6">
            <p class="text-sm text-slate-600 mb-4">In compliance with the POPI Act, you must upload your own photo for the public-facing Organogram.</p>
            <form method="POST" action="">
                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                <input type="hidden" name="action" value="upload_photo"/>
                <div class="mb-4">
                    <label class="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-2">Photo URL</label>
                    <input type="url" name="photo_url" placeholder="https://example.com/photo.jpg" class="w-full px-4 py-2 bg-slate-50 border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:outline-none" required>
                </div>
                <button type="submit" class="w-full py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-sm transition">Save Photo</button>
            </form>
        </div>
    </div>
</div>
{% endblock %}
"""

with open("templates/program_uip/dashboards/municipal_officer.html", "w", encoding="utf-8") as f:
    f.write(new_mo_html)
print("Updated MO dashboard")
