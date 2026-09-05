import re

with open('scratch/fd.html', 'r', encoding='utf-8') as f:
    html = f.read()

# Fix the duplicate Facilitator Controls block
# It looks like the entire <div class="flex flex-col"> ... </div> for Facilitator Controls was duplicated.
# Let's find "Room Controls"
parts = html.split('<!-- Facilitator Controls -->')
if len(parts) > 2:
    # There's more than one "Facilitator Controls" comment?
    pass

# A safer way: just use string replace on the duplicate block if it exists
match = re.search(r'(<!-- Facilitator Controls -->\s*<div class="flex flex-col">.*?</div>\s*</div>\s*</div>\s*</div>)', html, re.DOTALL)
if match:
    # Replace any duplicate occurrences
    html = html.replace(match.group(1) + '\n' + match.group(1), match.group(1))
    
    # Wait, the duplication might be slightly different due to my previous regex. 
    # Let's just find the first "Room Controls" and everything after it up to the script tag, and replace it all with a clean block.

html = re.sub(r'<!-- Facilitator Controls -->.*?(?=<script>)', '''<!-- Facilitator Controls -->
            <div class="flex flex-col">
                <h2 class="text-xl font-bold mb-4 text-slate-300">Room Controls</h2>
                
                <div class="bg-slate-800 rounded-xl p-6 border border-slate-700 space-y-6">
                    <div class="p-4 bg-indigo-900/30 border border-indigo-500/50 rounded-lg">
                        <h3 class="font-bold text-indigo-300 mb-2"><i class="fas fa-lightbulb text-yellow-400 mr-2"></i>Facilitator Note: The Activity</h3>
                        <p class="text-slate-300 text-sm leading-relaxed">Wait for the room to log their answers. If you see participants voting "FALSE", use this data to spark a debate before you reveal Slide 1 (The 2030 Panel Report).</p>
                    </div>
                    
                    <div>
                        <button onclick="pollData()" class="w-full py-3 bg-slate-700 hover:bg-slate-600 font-bold rounded-lg transition text-slate-200 border border-slate-600 mb-3">
                            <i class="fas fa-sync-alt mr-2"></i> Manual Refresh Data
                        </button>
                        <button onclick="currentSlide = 1; updateView();" class="w-full py-3 bg-indigo-600 hover:bg-indigo-500 font-bold rounded-lg transition shadow-[0_0_15px_rgba(79,70,229,0.5)]">
                            <i class="fas fa-play mr-2"></i> Reveal Slide 1 to Projector
                        </button>
                    </div>
                    
                    <!-- Dummy Data Injector (For Testing) -->
                    <div class="mt-8 p-4 border-t border-slate-700">
                        <h4 class="text-sm font-bold text-slate-500 uppercase tracking-wider mb-4">Evaluator Tools (Simulate Room)</h4>
                        <div class="flex space-x-2">
                            <button onclick="simulateVote('True')" class="flex-1 py-2 bg-green-900/50 hover:bg-green-800 text-green-400 border border-green-700 rounded text-sm font-bold transition">
                                +1 True Vote
                            </button>
                            <button onclick="simulateVote('False')" class="flex-1 py-2 bg-red-900/50 hover:bg-red-800 text-red-400 border border-red-700 rounded text-sm font-bold transition">
                                +1 False Vote
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>

''', html, flags=re.DOTALL)

# Fix duplicated JS at the end
html = re.sub(r'        }\s*}\s*function nextSlide\(\) {\s*if \(currentSlide < totalSlides\) {\s*currentSlide\+\+;\s*updateView\(\);\s*}\s*}\s*// Initialize\s*updateView\(\);\s*</script>\s*{% endblock %}\s*}*\s*}*\s*function nextSlide\(\) {.*', '''        }
    }
    
    // Initialize
    updateView();

</script>
{% endblock %}
''', html, flags=re.DOTALL)


with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(html)
