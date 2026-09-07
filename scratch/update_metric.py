import re

file_path = 'templates/uip/dashboards/manager.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Replace Levy Health Metric with Municipal Escalations
old_metric = '''<!-- Metric 1: Financial Health -->
            <div class="bg-gradient-to-br from-emerald-50 to-teal-50 rounded-xl shadow-sm border border-emerald-100 p-5 flex flex-col justify-between relative overflow-hidden">
                <div class="absolute -right-4 -bottom-4 text-emerald-200 text-6xl opacity-30"><i class="fas fa-coins"></i></div>
                <div class="flex justify-between items-start mb-2 relative z-10">
                    <p class="text-emerald-800 text-xs font-bold uppercase tracking-wide">Levy Health</p>
                    <span class="px-2 py-1 bg-emerald-200 text-emerald-800 text-[10px] font-bold rounded">LIVE</span>
                </div>
                <div class="relative z-10">
                    <p class="text-3xl font-black text-emerald-900 mt-1">94%</p>
                    <p class="text-xs text-emerald-600 font-bold mt-1"><i class="fas fa-arrow-up mr-1"></i> Paid in full (30 Days)</p>
                </div>
            </div>'''

new_metric = '''<!-- Metric 1: Municipal Escalations -->
            <div class="bg-gradient-to-br from-emerald-50 to-teal-50 rounded-xl shadow-sm border border-emerald-100 p-5 flex flex-col justify-between relative overflow-hidden">
                <div class="absolute -right-4 -bottom-4 text-emerald-200 text-6xl opacity-30"><i class="fas fa-city"></i></div>
                <div class="flex justify-between items-start mb-2 relative z-10">
                    <p class="text-emerald-800 text-xs font-bold uppercase tracking-wide">eThekwini Escalations</p>
                    <span class="px-2 py-1 bg-emerald-200 text-emerald-800 text-[10px] font-bold rounded">LIVE</span>
                </div>
                <div class="relative z-10">
                    <p class="text-3xl font-black text-emerald-900 mt-1">4</p>
                    <p class="text-xs text-emerald-600 font-bold mt-1"><i class="fas fa-link mr-1"></i> Sent to Municipality</p>
                </div>
            </div>'''

text = text.replace(old_metric, new_metric)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
