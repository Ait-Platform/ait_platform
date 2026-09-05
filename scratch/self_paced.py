import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Remove the setInterval sync logic
content = re.sub(r'// Sync with facilitator.*?setInterval\(\(\) => \{.*?\}\);\n', '', content, flags=re.DOTALL)

# 2. Make hasJoinedLocally default to true and sessionState default to active
content = content.replace("let sessionState = 'unknown';", "let sessionState = 'active';")
content = content.replace("let hasJoinedLocally = localStorage.getItem('sace_joined') === 'true';", "let hasJoinedLocally = true;")

# 3. Add Next/Prev buttons to the bottom of the screen permanently
button_html = """
    <!-- Self-Paced Navigation Controls -->
    <div class="fixed bottom-0 left-0 right-0 bg-white border-t border-slate-200 p-4 shadow-[0_-4px_6px_-1px_rgba(0,0,0,0.05)] z-[60] flex justify-between items-center">
        <button onclick="changeSlide(-1)" class="px-6 py-3 bg-slate-100 hover:bg-slate-200 text-slate-700 font-bold rounded-xl transition">
            <i class="fas fa-arrow-left mr-2"></i> Previous
        </button>
        <div class="text-slate-500 font-bold text-sm tracking-widest uppercase">
            Step <span id="nav-step">1</span> of 18
        </div>
        <button onclick="changeSlide(1)" class="px-6 py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-xl transition shadow-md">
            Next <i class="fas fa-arrow-right ml-2"></i>
        </button>
    </div>
    
    <script>
    function changeSlide(dir) {
        currentSlide += dir;
        if(currentSlide < 0) currentSlide = 0;
        if(currentSlide > 18) currentSlide = 18;
        document.getElementById('nav-step').innerText = currentSlide || 1;
        updateView();
    }
    </script>
"""

# Insert right before </body> or {% endblock %}
content = content.replace("{% endblock %}", button_html + "\n{% endblock %}")

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Made Participant App self-paced")
