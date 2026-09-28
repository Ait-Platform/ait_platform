import re

filepath = 'templates/program_uip/dashboards/secretary_workspace.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

old_tile6 = '''        <!-- Tile 6: Ratepayer Queries -->
        <a href="{{ url_for('uip_bp.reception_page', org_slug=org.slug) }}" class="group block relative overflow-hidden rounded-2xl border bg-rose-50 border-rose-100 hover:border-rose-300 hover:shadow-md transition-all duration-300">
            <div class="p-6">
                <div class="flex justify-between items-start mb-6">
                    <div class="w-12 h-12 rounded-full flex items-center justify-center text-xl bg-rose-100 text-rose-500">
                        <i class="fas fa-question-circle"></i>
                    </div>
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-[10px] font-bold bg-white text-rose-400 border border-rose-100 uppercase tracking-widest">
                        Inbox
                    </span>
                </div>'''

new_tile6 = '''        <!-- Tile 6: Ratepayer Queries -->
        <a href="{{ url_for('uip_bp.reception_page', org_slug=org.slug) }}" class="group block relative overflow-hidden rounded-2xl border bg-rose-50 border-rose-100 hover:border-rose-300 hover:shadow-md transition-all duration-300">
            <div class="p-6">
                <div class="flex justify-between items-start mb-6">
                    <div class="w-12 h-12 rounded-full flex items-center justify-center text-xl bg-rose-100 text-rose-500">
                        <i class="fas fa-question-circle"></i>
                    </div>
                    {% if open_queries and open_queries > 0 %}
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-[10px] font-bold bg-red-100 text-red-600 border border-red-200 uppercase tracking-widest animate-pulse">
                        {{ open_queries }} Pending
                    </span>
                    {% else %}
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-[10px] font-bold bg-white text-rose-400 border border-rose-100 uppercase tracking-widest">
                        Inbox
                    </span>
                    {% endif %}
                </div>'''

content = content.replace(old_tile6, new_tile6)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

# Now subcomm tools board
filepath_sub = 'templates/program_uip/subcomm_tools/board.html'
with open(filepath_sub, 'r', encoding='utf-8') as f:
    content_sub = f.read()

old_sub_tile4 = '''        <!-- Tile 4: Queries Desk -->
        <a href="{{ url_for('uip_bp.reception_page', org_slug=org.slug) }}" class="group block relative overflow-hidden rounded-2xl border bg-rose-50 border-rose-100 hover:border-rose-300 hover:shadow-md transition-all duration-300">
            <div class="p-6">
                <div class="flex justify-between items-start mb-6">
                    <div class="w-12 h-12 rounded-full flex items-center justify-center text-xl bg-rose-100 text-rose-500">
                        <i class="fas fa-inbox"></i>
                    </div>
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-[10px] font-bold bg-white text-rose-400 border border-rose-100 uppercase tracking-widest">
                        Inbox
                    </span>
                </div>'''

new_sub_tile4 = '''        <!-- Tile 4: Queries Desk -->
        <a href="{{ url_for('uip_bp.reception_page', org_slug=org.slug) }}" class="group block relative overflow-hidden rounded-2xl border bg-rose-50 border-rose-100 hover:border-rose-300 hover:shadow-md transition-all duration-300">
            <div class="p-6">
                <div class="flex justify-between items-start mb-6">
                    <div class="w-12 h-12 rounded-full flex items-center justify-center text-xl bg-rose-100 text-rose-500">
                        <i class="fas fa-inbox"></i>
                    </div>
                    {% if open_queries and open_queries > 0 %}
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-[10px] font-bold bg-red-100 text-red-600 border border-red-200 uppercase tracking-widest animate-pulse">
                        {{ open_queries }} Pending
                    </span>
                    {% else %}
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-[10px] font-bold bg-white text-rose-400 border border-rose-100 uppercase tracking-widest">
                        Inbox
                    </span>
                    {% endif %}
                </div>'''

content_sub = content_sub.replace(old_sub_tile4, new_sub_tile4)

with open(filepath_sub, 'w', encoding='utf-8') as f:
    f.write(content_sub)

print("Updated HTML tiles")
