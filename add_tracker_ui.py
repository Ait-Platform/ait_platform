import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Add Tracker Tile Button
tile_regex = r'(<a href="\{\{ url_for\(\'debtors_bp\.dashboard\'\) \}\}".*?</a>)'
tracker_btn = '''
          <button onclick="switchTab('tracker')" id="tab-btn-tracker" class="flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200 text-center leading-tight min-w-[80px]">
            <span class="mb-1">Repair<br>Tracker</span>
            <span class="bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full font-black"><i class="fas fa-search"></i></span>
          </button>
'''
content = re.sub(tile_regex, tracker_btn + r'\1', content)

# 2. Add Tracker Tab Pane
pane_regex = r'(<div id="tab-content-completed" class="tab-pane hidden">.*?</div>)'
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
content = re.sub(pane_regex, r'\1\n' + tracker_pane, content, flags=re.DOTALL)

# 3. Add JS to support the Tracker
js_regex = r'(const tabs = \[\'pending\', \'accepted\', \'completed\', \'rejected\'\];)'
js_replacement = r"const tabs = ['pending', 'accepted', 'completed', 'tracker', 'rejected'];"
content = re.sub(js_regex, js_replacement, content)

js_tracker = '''
  function searchTracker() {
      const reg = document.getElementById('tracker-input').value.trim();
      if(!reg) return;
      
      document.getElementById('tracker-results').classList.add('hidden');
      document.getElementById('tracker-empty').classList.add('hidden');
      document.getElementById('tracker-loading').classList.remove('hidden');
      
      fetch(/mechanic/api/tracker/)
          .then(r => r.json())
          .then(data => {
              document.getElementById('tracker-loading').classList.add('hidden');
              if(data.error) {
                  document.getElementById('tracker-empty').classList.remove('hidden');
              } else {
                  document.getElementById('tracker-veh-name').textContent = data.vehicle;
                  document.getElementById('tracker-client-name').innerHTML = <i class="fas fa-user mr-1"></i> ;
                  document.getElementById('tracker-reg-plate').textContent = data.reg;
                  
                  const tContainer = document.getElementById('tracker-timeline');
                  tContainer.innerHTML = '';
                  
                  data.timeline.forEach((item, index) => {
                      const isLast = index === data.timeline.length - 1;
                      const div = document.createElement('div');
                      div.className = "relative pl-6";
                      
                      let badgeColor = "bg-slate-400";
                      let iconColor = "text-slate-500";
                      
                      if(item.color === 'blue') { badgeColor = "bg-blue-500"; iconColor = "text-blue-700"; }
                      if(item.color === 'emerald') { badgeColor = "bg-emerald-500"; iconColor = "text-emerald-700"; }
                      if(item.color === 'indigo') { badgeColor = "bg-indigo-500"; iconColor = "text-indigo-700"; }
                      if(item.color === 'green') { badgeColor = "bg-green-500"; iconColor = "text-green-700"; }
                      
                      div.innerHTML = 
                        <div class="absolute -left-[9px] top-1 h-4 w-4 rounded-full border-4 border-white  shadow-sm z-10"></div>
                        <div class="bg-white border border-slate-200 rounded-lg p-3 shadow-sm hover:shadow-md transition">
                          <div class="flex justify-between items-start mb-1">
                            <h4 class="font-bold text-slate-800"><i class="fas   mr-2"></i> </h4>
                            <span class="text-xs font-bold text-slate-500 bg-slate-100 px-2 py-0.5 rounded"></span>
                          </div>
                          <p class="text-xs text-slate-400 font-medium"></p>
                        </div>
                      ;
                      tContainer.appendChild(div);
                  });
                  
                  document.getElementById('tracker-results').classList.remove('hidden');
              }
          })
          .catch(err => {
              document.getElementById('tracker-loading').classList.add('hidden');
              document.getElementById('tracker-empty').classList.remove('hidden');
          });
  }
'''

content = content.replace("</script>\n{% endblock %}", js_tracker + "\n</script>\n{% endblock %}")

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
