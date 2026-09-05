import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Remove all injected navigation blocks and scripts
content = re.sub(r'<!-- Self-Paced Navigation Controls -->.*?</script>', '', content, flags=re.DOTALL)

# Find the end of the content block and insert it right before the closing </div> of the main body, or before the floating button
button_html = """
    <!-- Self-Paced Navigation Controls -->
    <div class="fixed bottom-0 left-0 right-0 bg-white border-t border-slate-300 p-4 shadow-[0_-10px_20px_rgba(0,0,0,0.1)] z-[9999] flex justify-between items-center">
        <button onclick="changeSlide(-1)" class="px-6 py-3 bg-slate-200 hover:bg-slate-300 text-slate-800 font-bold rounded-xl transition shadow-sm border border-slate-400">
            <i class="fas fa-arrow-left mr-2"></i> Previous
        </button>
        <div class="text-slate-600 font-bold text-sm tracking-widest uppercase">
            Step <span id="nav-step">1</span> of 18
        </div>
        <button onclick="changeSlide(1)" class="px-6 py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-xl transition shadow-md border border-indigo-700">
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

content = content.replace("<!-- Floating SACE Guide Button -->", button_html + "\n\n<!-- Floating SACE Guide Button -->")

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Fixed navigation buttons")
