import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Add Evaluator Mode button to header
old_header = """<button onclick="location.href='{{ url_for('sace.reading_hub') }}'" class="px-4 py-2 bg-slate-800 hover:bg-slate-700 rounded font-bold shadow transition">Exit</button>"""
new_header = """<button id="eval-mode-btn" onclick="toggleEvaluatorMode()" class="px-4 py-2 bg-slate-700 hover:bg-slate-600 rounded font-bold shadow transition mr-2"><i class="fas fa-user-shield mr-2"></i>Evaluator Mode</button>
                <button onclick="location.href='{{ url_for('sace.reading_hub') }}'" class="px-4 py-2 bg-slate-800 hover:bg-slate-700 rounded font-bold shadow transition">Exit</button>"""

if old_header in content:
    content = content.replace(old_header, new_header)

# 2. Add Evaluator Mode JS logic
js_injection = """
    let evaluatorMode = false;
    function toggleEvaluatorMode() {
        evaluatorMode = !evaluatorMode;
        const btn = document.getElementById('eval-mode-btn');
        if (evaluatorMode) {
            alert("SACE Evaluator Mode ENABLED.\\n\\nThis dashboard is now DISCONNECTED from the live database. You can click 'Next Slide' and 'Previous Slide' to freely explore the Facilitator flow without affecting any live participants.");
            btn.classList.replace('bg-slate-700', 'bg-amber-500');
            btn.classList.replace('hover:bg-slate-600', 'hover:bg-amber-600');
            btn.innerHTML = '<i class="fas fa-unlink mr-2"></i>Offline Mode';
        } else {
            alert("Evaluator Mode DISABLED. Reconnecting to live server...");
            btn.classList.replace('bg-amber-500', 'bg-slate-700');
            btn.classList.replace('hover:bg-amber-600', 'hover:bg-slate-600');
            btn.innerHTML = '<i class="fas fa-user-shield mr-2"></i>Evaluator Mode';
            fetchState();
        }
    }
"""

content = content.replace("let sessionState = 'lobby';", "let sessionState = 'lobby';\n" + js_injection)

# 3. Block fetchState and changeSlide in Evaluator Mode
content = content.replace(
    "function fetchState() {",
    "function fetchState() {\n        if(evaluatorMode) return;"
)

change_slide_injection = """
        if(evaluatorMode) {
            currentSlide = newSlide;
            updateView();
            return;
        }
"""
content = re.sub(r'(function changeSlide\(direction\) \{.*?if\(newSlide > totalSlides\) newSlide = totalSlides;)', r'\1\n' + change_slide_injection, content, flags=re.DOTALL)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
