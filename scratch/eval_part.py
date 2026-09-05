import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Add Evaluator floating header and controls
eval_ui = """
<!-- EVALUATOR CONTROLS -->
<div id="eval-mode-banner" class="hidden fixed top-0 left-0 right-0 bg-amber-500 text-white text-center py-2 font-bold z-50 shadow-md flex justify-between px-4 items-center">
    <span><i class="fas fa-user-shield mr-2"></i> SACE Evaluator Mode (Offline)</span>
    <div>
        <button onclick="evalChangeSlide(-1)" class="px-3 py-1 bg-amber-700 hover:bg-amber-800 rounded mr-2 text-sm"><i class="fas fa-arrow-left"></i> Prev</button>
        <button onclick="evalChangeSlide(1)" class="px-3 py-1 bg-amber-700 hover:bg-amber-800 rounded text-sm">Next <i class="fas fa-arrow-right"></i></button>
    </div>
</div>
<button id="eval-toggle-btn" onclick="toggleEvaluatorMode()" class="fixed bottom-4 left-4 bg-slate-800 text-white p-3 rounded-full shadow-lg z-50 hover:bg-slate-700 transition" title="Enable Evaluator Mode">
    <i class="fas fa-user-shield"></i>
</button>
"""

content = content.replace("<!-- WAITING VIEW (-0.5) -->", eval_ui + "\n                    <!-- WAITING VIEW (-0.5) -->")

# 2. Add JS logic
js_eval = """
    let evaluatorMode = false;
    function toggleEvaluatorMode() {
        evaluatorMode = !evaluatorMode;
        if(evaluatorMode) {
            alert("SACE Evaluator Mode ENABLED.\\n\\nThis app is now DISCONNECTED from the Facilitator. Use the orange banner at the top to click through and evaluate the Participant App slides manually.");
            document.getElementById('eval-mode-banner').classList.remove('hidden');
            document.getElementById('eval-toggle-btn').classList.replace('bg-slate-800', 'bg-amber-500');
            hasJoinedLocally = true;
            sessionState = 'active';
            updateView();
        } else {
            alert("Evaluator Mode DISABLED. Reconnecting to live server...");
            document.getElementById('eval-mode-banner').classList.add('hidden');
            document.getElementById('eval-toggle-btn').classList.replace('bg-amber-500', 'bg-slate-800');
            // let next fetch handle state
        }
    }
    
    function evalChangeSlide(dir) {
        currentSlide += dir;
        if(currentSlide < 0) currentSlide = 0;
        if(currentSlide > 18) currentSlide = 18;
        sessionState = 'active';
        updateView();
    }
"""

content = content.replace("let hasJoinedLocally = localStorage.getItem('sace_joined') === 'true';", "let hasJoinedLocally = localStorage.getItem('sace_joined') === 'true';\n" + js_eval)

# 3. Block interval fetch in Evaluator Mode
content = content.replace(
    "setInterval(() => {",
    "setInterval(() => {\n        if (evaluatorMode) return;"
)
content = content.replace(
    "fetch('/sace/workshop/get_state')",
    "if(evaluatorMode) return;\n        fetch('/sace/workshop/get_state')"
)


with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
