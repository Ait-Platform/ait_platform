import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

new_js = """    // Reset buttons to look "shut" and disabled
    const inactiveClass = "flex items-center px-4 py-2 bg-slate-800 text-slate-500 font-bold rounded-t-lg transition border-b-2 border-transparent opacity-50 hover:opacity-100 hover:bg-slate-700";
    document.getElementById('btn-tab-a').className = inactiveClass;
    document.getElementById('btn-tab-f').className = inactiveClass;
    document.getElementById('btn-tab-p').className = inactiveClass;
    
    // Reset lights to RED (shut)
    const redLight = "w-3 h-3 rounded-full bg-red-600 shadow-[0_0_8px_rgba(220,38,38,0.5)] mr-2"; 
    const greenLight = "w-3 h-3 rounded-full bg-green-500 shadow-[0_0_12px_rgba(34,197,94,1)] mr-2";
    document.getElementById('light-f').className = redLight;
    document.getElementById('light-p').className = redLight;"""

text = re.sub(r'// Reset buttons to look "shut" and disabled.*?document\.getElementById\(\'light-p\'\)\.className = redLight;', new_js, text, flags=re.DOTALL)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
