import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Add id's to the poll bars so we can manipulate them
content = content.replace('class="bg-blue-500 h-6 rounded-full" style="width: 25%"', 'id="poll-res" class="bg-blue-500 h-6 rounded-full transition-all duration-1000" style="width: 0%"')
content = content.replace('class="bg-amber-500 h-6 rounded-full" style="width: 15%"', 'id="poll-class" class="bg-amber-500 h-6 rounded-full transition-all duration-1000" style="width: 0%"')
content = content.replace('class="bg-rose-500 h-6 rounded-full" style="width: 45%"', 'id="poll-lang" class="bg-rose-500 h-6 rounded-full transition-all duration-1000" style="width: 0%"')
content = content.replace('class="bg-green-500 h-6 rounded-full" style="width: 15%"', 'id="poll-meth" class="bg-green-500 h-6 rounded-full transition-all duration-1000" style="width: 0%"')

# Inside fetchState(), handle poll_data
poll_update_js = """
                // Update Roster Modal
                const rosterList = document.getElementById('roster-list');
                if (rosterList && data.roster) {
                    if (data.roster.length === 0) {
                        rosterList.innerHTML = '<li class="p-6 text-center text-gray-400 text-sm italic">No participants have checked in yet.</li>';
                    } else {
                        rosterList.innerHTML = data.roster.map(name => '<li class="p-4 px-6 flex items-center bg-white"><div class="h-8 w-8 rounded-full bg-indigo-100 text-indigo-600 flex items-center justify-center font-bold text-sm mr-4"><i class="fas fa-user"></i></div><span class="font-semibold text-gray-700">' + name + '</span></li>').join('');
                    }
                }
                
                // Update Poll Data Dynamically
                if (data.poll_data) {
                    const totalVotes = data.poll_data.root_cause.resources + data.poll_data.root_cause.class_size + data.poll_data.root_cause.language + data.poll_data.root_cause.methods;
                    if (totalVotes > 0) {
                        const resPct = Math.round((data.poll_data.root_cause.resources / totalVotes) * 100);
                        const classPct = Math.round((data.poll_data.root_cause.class_size / totalVotes) * 100);
                        const langPct = Math.round((data.poll_data.root_cause.language / totalVotes) * 100);
                        const methPct = Math.round((data.poll_data.root_cause.methods / totalVotes) * 100);
                        
                        if(document.getElementById('poll-res')) document.getElementById('poll-res').style.width = resPct + '%';
                        if(document.getElementById('poll-class')) document.getElementById('poll-class').style.width = classPct + '%';
                        if(document.getElementById('poll-lang')) document.getElementById('poll-lang').style.width = langPct + '%';
                        if(document.getElementById('poll-meth')) document.getElementById('poll-meth').style.width = methPct + '%';
                    }
                }
"""

content = re.sub(r'// Update Roster Modal.*?}\s*}', poll_update_js.strip(), content, flags=re.DOTALL)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
