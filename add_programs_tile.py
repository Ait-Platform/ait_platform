with open('templates/admin/admin_secure/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

new_tile = """
        <!-- Manage Bridge Tiles & Programs -->
        <div class="rounded-xl border border-slate-200 bg-white shadow-sm overflow-hidden flex flex-col h-full">
          <div class="h-1.5 bg-sky-500"></div>
          <div class="p-5 flex flex-col h-full">
            <h2 class="font-medium text-slate-900 flex items-center">
              <svg class="w-5 h-5 mr-2 text-sky-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z"></path></svg>
              Manage Bridge Tiles
            </h2>
            <p class="text-xs text-slate-500 mb-4 mt-2 flex-grow">
              Control which tiles appear on the bridge and edit program types.
            </p>
            <a href="{{ url_for('admin_bp.manage_programs') }}"
               class="inline-block mt-4 rounded-md border border-slate-300 px-4 py-2 text-sm text-slate-700 hover:bg-slate-50 transition bg-white shadow-sm w-full text-center font-medium">
              Manage Bridge
            </a>
          </div>
        </div>
"""

insert_pos = content.rfind('      </div>\n    </div>\n  </div>\n</div>\n{% endblock %}')
if insert_pos != -1:
    content = content[:insert_pos] + new_tile + content[insert_pos:]
    with open('templates/admin/admin_secure/dashboard.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Success")
else:
    print("Insertion point not found")
