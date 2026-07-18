with open('templates/admin/admin_secure/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

new_tiles = """
        <!-- Platform Pricing -->
        <div class="rounded-xl border border-slate-200 bg-white shadow-sm overflow-hidden flex flex-col h-full">
          <div class="h-1.5 bg-orange-500"></div>
          <div class="p-5 flex flex-col h-full">
            <h2 class="font-medium text-slate-900 flex items-center">
              <svg class="w-5 h-5 mr-2 text-orange-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"></path></svg>
              Platform Pricing
            </h2>
            <p class="text-xs text-slate-500 mb-4 mt-2 flex-grow">
              Manage platform fees globally across all subject modules. Updates take effect immediately.
            </p>
            <a href="{{ url_for('admin_bp.global_settings') }}"
               class="inline-block mt-4 rounded-md border border-slate-300 px-4 py-2 text-sm text-slate-700 hover:bg-slate-50 transition bg-white shadow-sm w-full text-center font-medium">
              Manage Pricing
            </a>
          </div>
        </div>

        <!-- Platform Modules Control -->
        <div class="rounded-xl border border-slate-200 bg-white shadow-sm overflow-hidden flex flex-col h-full">
          <div class="h-1.5 bg-indigo-500"></div>
          <div class="p-5 flex flex-col h-full">
            <h2 class="font-medium text-slate-900 flex items-center">
              <svg class="w-5 h-5 mr-2 text-indigo-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"></path></svg>
              Platform Modules Control
            </h2>
            <p class="text-xs text-slate-500 mb-4 mt-2 flex-grow">
              Manage visibility on the Welcome page and toggle Yoco environments (Live/Sandbox) for each program.
            </p>
            <a href="{{ url_for('admin_bp.modules_control') }}"
               class="inline-block mt-4 rounded-md border border-slate-300 px-4 py-2 text-sm text-slate-700 hover:bg-slate-50 transition bg-white shadow-sm w-full text-center font-medium">
              Manage Modules
            </a>
          </div>
        </div>
"""

start = content.find('<!-- Platform Pricing & Controls -->')
if start == -1:
    start = content.find('<!-- Platform Pricing -->')

end_str = '      </div>\n    </div>\n  </div>\n</div>\n{% endblock %}'
end = content.find(end_str)

if start != -1 and end != -1:
    content = content[:start] + new_tiles + '\n' + content[end:]
    with open('templates/admin/admin_secure/dashboard.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Success")
else:
    print("Failed")
