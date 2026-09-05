import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Update the reset classes in switchTab
old_reset = '''btn.className = "flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200 text-center leading-tight min-w-[80px]";
            // Update badge color
            const badge = btn.querySelector('span');
            if (badge) badge.className = "bg-slate-300 text-slate-700 py-0.5 px-2 rounded-full text-xs ml-1";'''

new_reset = '''btn.className = "flex flex-col items-center justify-center px-4 py-3 rounded-lg text-sm font-bold shadow-sm transition bg-white text-slate-600 border-2 border-slate-200 hover:bg-slate-100 hover:text-slate-800 hover:border-slate-300 text-center w-full group";
            // Update badge color
            const badge = btn.querySelectorAll('span')[1];
            if (badge) badge.className = "bg-slate-100 text-slate-600 py-1 px-3 rounded-full font-black text-lg group-hover:bg-slate-200";'''

content = content.replace(old_reset, new_reset)

# Update the active classes in switchTab
old_active = '''btn.className = "flex flex-col items-center justify-center px-3 py-2 rounded-lg text-xs font-bold shadow-sm transition bg-indigo-600 text-white border-2 border-indigo-700 text-center leading-tight min-w-[80px]";
        // Update badge color
        const badge = btn.querySelector('span');
        if (badge) badge.className = "bg-white text-indigo-700 py-0.5 px-2 rounded-full text-xs ml-1";'''

new_active = '''btn.className = "flex flex-col items-center justify-center px-4 py-3 rounded-lg text-sm font-bold shadow-sm transition bg-indigo-600 text-white border-2 border-indigo-700 text-center w-full";
        // Update badge color
        const badge = btn.querySelectorAll('span')[1];
        if (badge) badge.className = "bg-white text-indigo-700 py-1 px-3 rounded-full font-black text-lg";'''

content = content.replace(old_active, new_active)

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
