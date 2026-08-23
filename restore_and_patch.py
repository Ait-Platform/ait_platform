import re
import base64

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update the ledger URL
content = content.replace("url_for('debtors_bp.dashboard')", "url_for('mechanic_bp.client_accounts')")

# 2. Inject Tracker button
ledger_btn_str = '''<a href="{{ url_for('mechanic_bp.client_accounts') }}" id="tab-btn-ledger" class="flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200 text-center leading-tight min-w-[80px] ml-4">
            <span class="mb-1">Ledger</span>
            <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full font-black">{{ total_debtors_count }}</span>
          </a>'''

tracker_btn_str = '''
          <button onclick="switchTab('tracker')" id="tab-btn-tracker" class="flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200 text-center leading-tight min-w-[80px]">
            <span class="mb-1">Repair<br>Tracker</span>
            <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full font-black"><i class="fas fa-search"></i></span>
          </button>'''

# Using regex because indentation might differ slightly
content = re.sub(r'(<a href="\{\{ url_for\(\'mechanic_bp\.client_accounts\'\) \}\}".*?</a>)', r'\1' + tracker_btn_str, content, flags=re.DOTALL)

# 3. Inject Tracker pane
tracker_pane = '''
        <div id="tab-content-tracker" class="tab-pane hidden mt-8">
          <div class="bg-white border-2 border-slate-200 rounded-xl shadow-sm p-6 max-w-3xl mx-auto">
            <h2 class="text-xl font-bold text-slate-800 mb-2"><i class="fas fa-satellite-dish text-indigo-500 mr-2"></i> Vehicle Repair Tracker</h2>
            <p class="text-sm text-slate-500 mb-6">Enter a vehicle registration number to instantly view its complete physical and financial timeline on the workshop floor.</p>
            
            <div class="flex space-x-3 mb-8">
              <input type="text" id="tracker-input" placeholder="e.g. XYZ 123" class="flex-1 rounded-lg border-2 border-slate-300 px-4 py-3 font-bold text-lg uppercase focus:border-indigo-500 focus:ring-indigo-500 shadow-sm" onkeypress="if(event.key === 'Enter') searchTracker()">
              <button onclick="searchTracker()" class="bg-indigo-600 text-white px-6 py-3 rounded-lg font-bold shadow-sm hover:bg-indigo-700 transition">Search</button>
            </div>
            
            <div id="tracker-loading" class="hidden text-center py-8">
              <i class="fas fa-spinner fa-spin text-3xl text-indigo-500 mb-3"></i>
              <p class="font-bold text-slate-500">Scanning workshop history...</p>
            </div>
            
            <div id="tracker-results" class="hidden">
              <div class="bg-slate-50 border border-slate-200 rounded-lg p-4 mb-6 flex justify-between items-center">
                <div>
                  <h3 id="tracker-veh-name" class="font-bold text-lg text-slate-900">Toyota Hilux</h3>
                  <p id="tracker-client-name" class="text-sm text-slate-600"><i class="fas fa-user mr-1"></i> Graham</p>
                </div>
                <div class="bg-white border-2 border-slate-300 px-3 py-1 rounded-md shadow-sm font-black text-slate-800 text-lg tracking-wider uppercase" id="tracker-reg-plate">
                  XYZ 123
                </div>
              </div>
              
              <div class="relative border-l-2 border-indigo-200 ml-4 space-y-6 pb-4" id="tracker-timeline">
                <!-- Timeline items injected here -->
              </div>
            </div>
            
            <div id="tracker-empty" class="hidden text-center py-8 bg-slate-50 rounded-lg border border-slate-200">
              <i class="fas fa-search-minus text-3xl text-slate-400 mb-3"></i>
              <p class="font-bold text-slate-600">No history found for that registration number.</p>
            </div>
          </div>
        </div>
'''
content = re.sub(r'(<div id="tab-content-completed" class="tab-pane hidden">.*?</div>)', r'\1\n' + tracker_pane, content, flags=re.DOTALL)

# 4. Update javascript tabs array
content = content.replace("const tabs = ['pending', 'accepted', 'completed', 'rejected'];", "const tabs = ['pending', 'accepted', 'completed', 'tracker', 'rejected'];")

# 5. Inject tracker Javascript using base64 decoding so it isn't tampered with
b64_js = "ICBmdW5jdGlvbiBzZWFyY2hUcmFja2VyKCkgewogICAgICBjb25zdCByZWcgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgndHJhY2tlci1pbnB1dCcpLnZhbHVlLnRyaW0oKTsKICAgICAgaWYoIXJlZykgcmV0dXJuOwogICAgICAKICAgICAgZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ3RyYWNrZXItcmVzdWx0cycpLmNsYXNzTGlzdC5hZGQoJ2hpZGRlbicpOwogICAgICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgndHJhY2tlci1lbXB0eScpLmNsYXNzTGlzdC5hZGQoJ2hpZGRlbicpOwogICAgICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgndHJhY2tlci1sb2FkaW5nJykuY2xhc3NMaXN0LnJlbW92ZSgnaGlkZGVuJyk7CiAgICAgIAogICAgICBmZXRjaChgL21lY2hhbmljL2FwaS90cmFja2VyLyR7ZW5jb2RlVVJJQ29tcG9uZW50KHJlZyl9YCkKICAgICAgICAgIC50aGVuKHIgPT4gci5qc29uKCkpCiAgICAgICAgICAudGhlbihkYXRhID0+IHsKICAgICAgICAgICAgICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgndHJhY2tlci1sb2FkaW5nJykuY2xhc3NMaXN0LmFkZCgnaGlkZGVuJyk7CiAgICAgICAgICAgICAgaWYoZGF0YS5lcnJvcikgewogICAgICAgICAgICAgICAgICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgndHJhY2tlci1lbXB0eScpLmNsYXNzTGlzdC5yZW1vdmUoJ2hpZGRlbicpOwogICAgICAgICAgICAgIH0gZWxzZSB7CiAgICAgICAgICAgICAgICAgIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCd0cmFja2VyLXZlaC1uYW1lJykudGV4dENvbnRlbnQgPSBkYXRhLnZlaGljbGU7CiAgICAgICAgICAgICAgICAgIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCd0cmFja2VyLWNsaWVudC1uYW1lJykuaW5uZXJIVE1MID0gYDxpIGNsYXNzPSJmYXMgZmEtdXNlciBtci0xIj48L2k+ICR7ZGF0YS5jbGllbnR9YDsKICAgICAgICAgICAgICAgICAgZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ3RyYWNrZXItcmVnLXBsYXRlJykudGV4dENvbnRlbnQgPSBkYXRhLnJlZzsKICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgIGNvbnN0IHRDb250YWluZXIgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgndHJhY2tlci10aW1lbGluZScpOwogICAgICAgICAgICAgICAgICB0Q29udGFpbmVyLmlubmVySFRNTCA9ICcnOwogICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgZGF0YS50aW1lbGluZS5mb3JFYWNoKChpdGVtLCBpbmRleCkgPT4gewogICAgICAgICAgICAgICAgICAgICAgY29uc3QgZGl2ID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgnZGl2Jyk7CiAgICAgICAgICAgICAgICAgICAgICBkaXYuY2xhc3NOYW1lID0gInJlbGF0aXZlIHBsLTYiOwogICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICBsZXQgYmFkZ2VDb2xvciA9ICJiZy1zbGF0ZS00MDAiOwogICAgICAgICAgICAgICAgICAgICAgbGV0IGljb25Db2xvciA9ICJ0ZXh0LXNsYXRlLTUwMCI7CiAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgIGlmKGl0ZW0uY29sb3IgPT09ICdibHVlJykgeyBiYWRnZUNvbG9yID0gImJnLWJsdWUtNTAwIjsgaWNvbkNvbG9yID0gInRleHQtYmx1ZS03MDAiOyB9CiAgICAgICAgICAgICAgICAgICAgICBpZihpdGVtLmNvbG9yID09PSAnZW1lcmFsZCcpIHsgYmFkZ2VDb2xvciA9ICJiZy1lbWVyYWxkLTUwMCI7IGljb25Db2xvciA9ICJ0ZXh0LWVtZXJhbGQtNzAwIjsgfQogICAgICAgICAgICAgICAgICAgICAgaWYoaXRlbS5jb2xvciA9PT0gJ2luZGlnbycpIHsgYmFkZ2VDb2xvciA9ICJiZy1pbmRpZ28tNTAwIjsgaWNvbkNvbG9yID0gInRleHQtaW5kaWdvLTcwMCI7IH0KICAgICAgICAgICAgICAgICAgICAgIGlmKGl0ZW0uY29sb3IgPT09ICdncmVlbicpIHsgYmFkZ2VDb2xvciA9ICJiZy1ncmVlbi01MDAiOyBpY29uQ29sb3IgPSAidGV4dC1ncmVlbi03MDAiOyB9CiAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgIGRpdi5pbm5lckhUTUwgPSBgCiAgICAgICAgICAgICAgICAgICAgICAgIDxkaXYgY2xhc3M9ImFic29sdXRlIC1sZWZ0LVs5cHhdIHRvcC0xIGgtNCB3LTQgcm91bmRlZC1mdWxsIGJvcmRlci00IGJvcmRlci13aGl0ZSAke2JhZGdlQ29sb3J9IHNoYWRvdy1zbSB6LTEwIj48L2Rpdj4KICAgICAgICAgICAgICAgICAgICAgICAgPGRpdiBjbGFzcz0iYmctd2hpdGUgYm9yZGVyIGJvcmRlci1zbGF0ZS0yMDAgcm91bmRlZC1sZyBwLTMgc2hhZG93LXNtIGhvdmVyOnNoYWRvdy1tZCB0cmFuc2l0aW9uIj4KICAgICAgICAgICAgICAgICAgICAgICAgICA8ZGl2IGNsYXNzPSJmbGV4IGp1c3RpZnktYmV0d2VlbiBpdGVtcy1zdGFydCBtYi0xIj4KICAgICAgICAgICAgICAgICAgICAgICAgICAgIDxoNCBjbGFzcz0iZm9udC1ib2xkIHRleHQtc2xhdGUtODAwIj48aSBjbGFzcz0iZmFzICR7aXRlbS5pY29ufSAke2ljb25Db2xvcn0gbXItMiI+PC9pPiAke2l0ZW0uZXZlbnR9PC9oND4KICAgICAgICAgICAgICAgICAgICAgICAgICAgIDxzcGFuIGNsYXNzPSJ0ZXh0LXhzIGZvbnQtYm9sZCB0ZXh0LXNsYXRlLTUwMCBiZy1zbGF0ZS0xMDAgcHgtMiBweS0wLjUgcm91bmRlZCI+JHtpdGVtLnRpbWV9PC9zcGFuPgogICAgICAgICAgICAgICAgICAgICAgICAgIDwvZGl2PgogICAgICAgICAgICAgICAgICAgICAgICAgIDxwIGNsYXNzPSJ0ZXh0LXhzIHRleHQtc2xhdGUtNDAwIGZvbnQtbWVkaXVtIj4ke2l0ZW0uZGF0ZX08L3A+CiAgICAgICAgICAgICAgICAgICAgICAgIDwvZGl2PgogICAgICAgICAgICAgICAgICAgICAgYDsKICAgICAgICAgICAgICAgICAgICAgIHRDb250YWluZXIuYXBwZW5kQ2hpbGQoZGl2KTsKICAgICAgICAgICAgICAgICAgfSk7CiAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgndHJhY2tlci1yZXN1bHRzJykuY2xhc3NMaXN0LnJlbW92ZSgnaGlkZGVuJyk7CiAgICAgICAgICAgICAgfQogICAgICAgICAgfSkKICAgICAgICAgIC5jYXRjaChlcnIgPT4gewogICAgICAgICAgICAgIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCd0cmFja2VyLWxvYWRpbmcnKS5jbGFzc0xpc3QuYWRkKCdoaWRkZW4nKTsKICAgICAgICAgICAgICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgndHJhY2tlci1lbXB0eScpLmNsYXNzTGlzdC5yZW1vdmUoJ2hpZGRlbicpOwogICAgICAgICAgfSk7CiAgfQ=="
proper_js = base64.b64decode(b64_js).decode('utf-8')

# CAREFULLY insert it before the closing script tag
content = content.replace("</script>\n{% endblock %}", proper_js + "\n</script>\n{% endblock %}")

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
