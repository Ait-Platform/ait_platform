import re
with open("templates/program_uip/dashboards/process_claims.html", "r", encoding="utf-8") as f:
    text = f.read()

# I will add the term fields into the form, right after the "Resolution Target" block.
term_fields = """
    <!-- Term of Office -->
    <div class="ui-form-section mb-8 mt-6">
        <h2 class="text-lg font-bold text-slate-900 mb-4"><i class="fas fa-calendar-alt text-indigo-500 mr-2"></i> Term of Office</h2>
        <div class="grid grid-cols-2 gap-4">
            <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Commencement Date</label>
                <input type="date" name="term_start_date" class="ui-input w-full" value="{{ datetime.now().strftime('%Y-%m-%d') }}" required>
            </div>
            <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Duration (Months)</label>
                <input type="number" name="term_duration_months" class="ui-input w-full" value="12" min="1" max="60" required>
            </div>
        </div>
        <p class="text-xs text-slate-500 mt-2">This sets the official term limit for the members included in this resolution.</p>
    </div>
"""

# Insert it before <!-- 2. Role Assignments Included -->
text = text.replace("    <!-- 2. Role Assignments Included -->", term_fields + "\n    <!-- 2. Role Assignments Included -->")

with open("templates/program_uip/dashboards/process_claims.html", "w", encoding="utf-8") as f:
    f.write(text)
