with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Remove the FAB
fab = '''<!-- Universal Help FAB -->
<a href="{{ url_for('mechanic_bp.help_center') }}" class="fixed bottom-6 right-6 w-14 h-14 bg-indigo-600 hover:bg-indigo-700 text-white rounded-full shadow-lg flex items-center justify-center transition transform hover:scale-110 z-50 group">
  <i class="fas fa-question text-2xl"></i>
  <span class="absolute right-16 bg-slate-800 text-white text-xs font-bold px-3 py-1.5 rounded opacity-0 group-hover:opacity-100 transition whitespace-nowrap shadow-md pointer-events-none">Help Center</span>
</a>'''

content = content.replace(fab, '')

# Add to top bar
header_original = '''      <div class="flex flex-col md:flex-row justify-between items-center bg-white p-6 rounded-xl shadow-sm border border-slate-200 mb-8">
        <div>
          <h2 class="text-3xl font-extrabold text-slate-900 tracking-tight">ProTrade</h2>
          <p class="text-slate-500 mt-2">Manage your shop, quotes, and workflow below.</p>
        </div>
      </div>'''

header_new = '''      <div class="flex flex-col md:flex-row justify-between items-center bg-white p-6 rounded-xl shadow-sm border border-slate-200 mb-8">
        <div>
          <h2 class="text-3xl font-extrabold text-slate-900 tracking-tight">ProTrade</h2>
          <p class="text-slate-500 mt-2">Manage your shop, quotes, and workflow below.</p>
        </div>
        <div class="mt-4 md:mt-0">
          <a href="{{ url_for('mechanic_bp.help_center') }}" class="inline-flex items-center gap-2 bg-indigo-50 text-indigo-700 hover:bg-indigo-100 px-4 py-2 rounded-lg font-semibold transition border border-indigo-200 shadow-sm">
            <i class="fas fa-question-circle"></i> Help Center
          </a>
        </div>
      </div>'''

content = content.replace(header_original, header_new)

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated dashboard.html")
