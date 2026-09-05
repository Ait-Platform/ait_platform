import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Update the switchTab function to make inactive tabs look very "disabled" (opacity-50, grayscale)
new_js = """    // Reset buttons to look "shut" and disabled
    const inactiveClass = "flex items-center px-4 py-2 bg-slate-900 text-slate-500 font-bold rounded-t-lg transition border-b-2 border-transparent opacity-60 hover:opacity-100";
    document.getElementById('btn-tab-a').className = inactiveClass;
    document.getElementById('btn-tab-f').className = inactiveClass;
    document.getElementById('btn-tab-p').className = inactiveClass;
    
    // Reset lights to RED (shut)
    const redLight = "w-3 h-3 rounded-full bg-slate-700 shadow-inner mr-2"; // changed from bright red to dim/shut
    const greenLight = "w-3 h-3 rounded-full bg-green-500 shadow-[0_0_12px_rgba(34,197,94,1)] mr-2";
    document.getElementById('light-f').className = redLight;
    document.getElementById('light-p').className = redLight;"""

text = re.sub(r'// Reset buttons.*?document\.getElementById\(\'light-p\'\)\.className = redLight;', new_js, text, flags=re.DOTALL)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
