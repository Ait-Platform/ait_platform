import re

new_card = """            <div class="bg-white p-4 rounded-xl border border-slate-200 shadow-sm border-l-4 border-l-amber-500">
                <h4 class="font-bold text-amber-800 mb-2"><i class="fas fa-bolt mr-2"></i>Loadshedding & Offline Contingency</h4>
                <p class="mb-2"><strong>Tech Auto-Recovery:</strong> If the Wi-Fi drops, the app safely pauses. Upon reconnection, all devices instantly re-sync to the current slide without any data loss.</p>
                <p class="mb-2"><strong>Physical Pivot:</strong> Our core methodologies (Vowel Hops, A-Z Flashcards) are deeply tactile. If the projector fails, the Facilitator simply switches to their printed flip-book and the physical activities continue uninterrupted.</p>
                <p><strong>Paper Assessments:</strong> The Facilitator carries physical paper copies of the Annexure D final assessment to ensure certification capture even during a total blackout.</p>
            </div>
"""

# For interactive_workshop.html
with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content1 = f.read()

# Insert before the last div in the modal (the Got It button)
insert_point_1 = """        <div class="p-4 border-t border-gray-100 bg-gray-50 text-center">"""
if insert_point_1 not in content1: # Try alternative insert point
    insert_point_1 = """            <div class="bg-white p-4 rounded-xl border border-slate-200 shadow-sm border-l-4 border-l-rose-500">"""
    if insert_point_1 in content1:
        content1 = content1.replace(insert_point_1, new_card + insert_point_1)

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content1)

# For facilitator_dashboard.html
with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    content2 = f.read()

insert_point_2 = """            <div class="bg-white p-4 rounded-xl border border-slate-200 shadow-sm border-l-4 border-l-rose-500">"""
if insert_point_2 in content2:
    content2 = content2.replace(insert_point_2, new_card + insert_point_2)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content2)
