import base64

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("url_for('debtors_bp.dashboard')", "url_for('mechanic_bp.client_accounts')")

ledger_block = '''<a href="{{ url_for('mechanic_bp.client_accounts') }}" id="tab-btn-ledger" class="flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200 text-center leading-tight min-w-[80px] ml-4">
              <span class="mb-1">Ledger</span>
              <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full font-black">{{ total_debtors_count }}</span>
            </a>'''
            
tracker_btn = '''
            <button onclick="switchTab('tracker')" id="tab-btn-tracker" class="flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200 text-center leading-tight min-w-[80px]">
              <span class="mb-1">Repair<br>Tracker</span>
              <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full font-black"><i class="fas fa-search"></i></span>
            </button>'''

if 'id="tab-btn-tracker"' not in content:
    content = content.replace(ledger_block, ledger_block + tracker_btn)


js_broken = "function searchTracker() {"
js_end = "  }\n\n</script>"

if js_broken in content:
    idx_start = content.find(js_broken)
    idx_end = content.find(js_end, idx_start) + len(js_end)
    content = content[:idx_start] + content[idx_end:]


b64_js = "ICBmdW5jdGlvbiBzZWFyY2hUcmFja2VyKCkgewogICAgICBjb25zdCByZWcgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgndHJhY2tlci1pbnB1dCcpLnZhbHVlLnRyaW0oKTsKICAgICAgaWYoIXJlZykgcmV0dXJuOwogICAgICAKICAgICAgZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ3RyYWNrZXItcmVzdWx0cycpLmNsYXNzTGlzdC5hZGQoJ2hpZGRlbicpOwogICAgICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgndHJhY2tlci1lbXB0eScpLmNsYXNzTGlzdC5hZGQoJ2hpZGRlbicpOwogICAgICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgndHJhY2tlci1sb2FkaW5nJykuY2xhc3NMaXN0LnJlbW92ZSgnaGlkZGVuJyk7CiAgICAgIAogICAgICBmZXRjaChgL21lY2hhbmljL2FwaS90cmFja2VyLyR7ZW5jb2RlVVJJQ29tcG9uZW50KHJlZyl9YCkKICAgICAgICAgIC50aGVuKHIgPT4gci5qc29uKCkpCiAgICAgICAgICAudGhlbihkYXRhID0+IHsKICAgICAgICAgICAgICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgndHJhY2tlci1sb2FkaW5nJykuY2xhc3NMaXN0LmFkZCgnaGlkZGVuJyk7CiAgICAgICAgICAgICAgaWYoZGF0YS5lcnJvcikgewogICAgICAgICAgICAgICAgICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgndHJhY2tlci1lbXB0eScpLmNsYXNzTGlzdC5yZW1vdmUoJ2hpZGRlbicpOwogICAgICAgICAgICAgIH0gZWxzZSB7CiAgICAgICAgICAgICAgICAgIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCd0cmFja2VyLXZlaC1uYW1lJykudGV4dENvbnRlbnQgPSBkYXRhLnZlaGljbGU7CiAgICAgICAgICAgICAgICAgIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCd0cmFja2VyLWNsaWVudC1uYW1lJykuaW5uZXJIVE1MID0gYDxpIGNsYXNzPSJmYXMgZmEtdXNlciBtci0xIj48L2k+ICR7ZGF0YS5jbGllbnR9YDsKICAgICAgICAgICAgICAgICAgZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ3RyYWNrZXItcmVnLXBsYXRlJykudGV4dENvbnRlbnQgPSBkYXRhLnJlZzsKICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgIGNvbnN0IHRDb250YWluZXIgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgndHJhY2tlci10aW1lbGluZScpOwogICAgICAgICAgICAgICAgICB0Q29udGFpbmVyLmlubmVySFRNTCA9ICcnOwogICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgZGF0YS50aW1lbGluZS5mb3JFYWNoKChpdGVtLCBpbmRleCkgPT4gewogICAgICAgICAgICAgICAgICAgICAgY29uc3QgZGl2ID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgnZGl2Jyk7CiAgICAgICAgICAgICAgICAgICAgICBkaXYuY2xhc3NOYW1lID0gInJlbGF0aXZlIHBsLTYiOwogICAgICAgICAgICAgICAgICAgICAgCiAgICAgICAgICAgICAgICAgICAgICBsZXQgYmFkZ2VDb2xvciA9ICJiZy1zbGF0ZS00MDAiOwogICAgICAgICAgICAgICAgICAgICAgbGV0IGljb25Db2xvciA9ICJ0ZXh0LXNsYXRlLTUwMCI7CiAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgIGlmKGl0ZW0uY29sb3IgPT09ICdibHVlJykgeyBiYWRnZUNvbG9yID0gImJnLWJsdWUtNTAwIjsgaWNvbkNvbG9yID0gInRleHQtYmx1ZS03MDAiOyB9CiAgICAgICAgICAgICAgICAgICAgICBpZihpdGVtLmNvbG9yID09PSAnZW1lcmFsZCcpIHsgYmFkZ2VDb2xvciA9ICJiZy1lbWVyYWxkLTUwMCI7IGljb25Db2xvciA9ICJ0ZXh0LWVtZXJhbGQtNzAwIjsgfQogICAgICAgICAgICAgICAgICAgICAgaWYoaXRlbS5jb2xvciA9PT0gJ2luZGlnbycpIHsgYmFkZ2VDb2xvciA9ICJiZy1pbmRpZ28tNTAwIjsgaWNvbkNvbG9yID0gInRleHQtaW5kaWdvLTcwMCI7IH0KICAgICAgICAgICAgICAgICAgICAgIGlmKGl0ZW0uY29sb3IgPT09ICdncmVlbicpIHsgYmFkZ2VDb2xvciA9ICJiZy1ncmVlbi01MDAiOyBpY29uQ29sb3IgPSAidGV4dC1ncmVlbi03MDAiOyB9CiAgICAgICAgICAgICAgICAgICAgICAKICAgICAgICAgICAgICAgICAgICAgIGRpdi5pbm5lckhUTUwgPSBgCiAgICAgICAgICAgICAgICAgICAgICAgIDxkaXYgY2xhc3M9ImFic29sdXRlIC1sZWZ0LVs5cHhdIHRvcC0xIGgtNCB3LTQgcm91bmRlZC1mdWxsIGJvcmRlci00IGJvcmRlci13aGl0ZSAke2JhZGdlQ29sb3J9IHNoYWRvdy1zbSB6LTEwIj48L2Rpdj4KICAgICAgICAgICAgICAgICAgICAgICAgPGRpdiBjbGFzcz0iYmctd2hpdGUgYm9yZGVyIGJvcmRlci1zbGF0ZS0yMDAgcm91bmRlZC1sZyBwLTMgc2hhZG93LXNtIGhvdmVyOnNoYWRvdy1tZCB0cmFuc2l0aW9uIj4KICAgICAgICAgICAgICAgICAgICAgICAgICA8ZGl2IGNsYXNzPSJmbGV4IGp1c3RpZnktYmV0d2VlbiBpdGVtcy1zdGFydCBtYi0xIj4KICAgICAgICAgICAgICAgICAgICAgICAgICAgIDxoNCBjbGFzcz0iZm9udC1ib2xkIHRleHQtc2xhdGUtODAwIj48aSBjbGFzcz0iZmFzICR7aXRlbS5pY29ufSAke2ljb25Db2xvcn0gbXItMiI+PC9pPiAke2l0ZW0uZXZlbnR9PC9oND4KICAgICAgICAgICAgICAgICAgICAgICAgICAgIDxzcGFuIGNsYXNzPSJ0ZXh0LXhzIGZvbnQtYm9sZCB0ZXh0LXNsYXRlLTUwMCBiZy1zbGF0ZS0xMDAgcHgtMiBweS0wLjUgcm91bmRlZCI+JHtpdGVtLnRpbWV9PC9zcGFuPgogICAgICAgICAgICAgICAgICAgICAgICAgIDwvZGl2PgogICAgICAgICAgICAgICAgICAgICAgICAgIDxwIGNsYXNzPSJ0ZXh0LXhzIHRleHQtc2xhdGUtNDAwIGZvbnQtbWVkaXVtIj4ke2l0ZW0uZGF0ZX08L3A+CiAgICAgICAgICAgICAgICAgICAgICAgIDwvZGl2PgogICAgICAgICAgICAgICAgICAgICAgYDsKICAgICAgICAgICAgICAgICAgICAgIHRDb250YWluZXIuYXBwZW5kQ2hpbGQoZGl2KTsKICAgICAgICAgICAgICAgICAgfSk7CiAgICAgICAgICAgICAgICAgIAogICAgICAgICAgICAgICAgICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgndHJhY2tlci1yZXN1bHRzJykuY2xhc3NMaXN0LnJlbW92ZSgnaGlkZGVuJyk7CiAgICAgICAgICAgICAgfQogICAgICAgICAgfSkKICAgICAgICAgIC5jYXRjaChlcnIgPT4gewogICAgICAgICAgICAgIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCd0cmFja2VyLWxvYWRpbmcnKS5jbGFzc0xpc3QuYWRkKCdoaWRkZW4nKTsKICAgICAgICAgICAgICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgndHJhY2tlci1lbXB0eScpLmNsYXNzTGlzdC5yZW1vdmUoJ2hpZGRlbicpOwogICAgICAgICAgfSk7CiAgfQ=="
proper_js = base64.b64decode(b64_js).decode('utf-8')
content = content.replace("</script>\n{% endblock %}", proper_js + "\n</script>\n{% endblock %}")

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
