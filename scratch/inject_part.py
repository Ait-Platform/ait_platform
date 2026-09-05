import re

new_card = """            <div class="bg-white p-4 rounded-xl border border-slate-200 shadow-sm border-l-4 border-l-amber-500 mt-5">
                <h4 class="font-bold text-amber-800 mb-2"><i class="fas fa-bolt mr-2"></i>Loadshedding & Offline Contingency</h4>
                <p class="mb-2 text-slate-700 text-sm"><strong>Tech Auto-Recovery:</strong> If the Wi-Fi drops, the app safely pauses. Upon reconnection, all devices instantly re-sync to the current slide without any data loss.</p>
                <p class="mb-2 text-slate-700 text-sm"><strong>Physical Pivot:</strong> Our core methodologies (Vowel Hops, A-Z Flashcards) are deeply tactile. If the projector fails, the Facilitator simply switches to their printed flip-book and the physical activities continue uninterrupted.</p>
                <p class="text-slate-700 text-sm"><strong>Paper Assessments:</strong> The Facilitator carries physical paper copies of the Annexure D final assessment to ensure certification capture even during a total blackout.</p>
            </div>
"""

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

target = '<div class="bg-white p-4 rounded-xl border border-slate-200 shadow-sm border-l-4 border-l-rose-500">'

if target in content and "Loadshedding" not in content:
    content = content.replace(target, new_card + "            " + target)
    with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Injected into participant app")
