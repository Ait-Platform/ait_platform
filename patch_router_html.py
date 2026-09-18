import re

with open("templates/program_uip/router.html", "r", encoding="utf-8") as f:
    text = f.read()

# Replace Chairman button
old_chair = """                    <button type="submit" class="w-full p-4 text-left rounded-xl shadow-sm border border-emerald-200 transition-transform hover:-translate-y-1 bg-emerald-50 text-emerald-900 font-bold text-lg cursor-pointer flex items-center justify-between">
                        <span class="flex items-center"><i class="fas fa-crown text-2xl w-10 text-emerald-600 mr-3 text-center"></i> Chairman</span>
                        <i class="fas fa-chevron-right text-emerald-300"></i>
                    </button>"""
new_chair = """                    {% if 'chairman' in occupied_seats %}
                    <button disabled class="w-full p-4 text-left rounded-xl border border-slate-200 bg-slate-100 text-slate-400 font-bold text-lg cursor-not-allowed flex items-center justify-between opacity-75">
                        <span class="flex items-center"><i class="fas fa-crown text-2xl w-10 text-slate-400 mr-3 text-center"></i> Chairman</span>
                        <span class="text-xs uppercase tracking-widest font-bold">Occupied</span>
                    </button>
                    {% else %}
                    <button type="submit" class="w-full p-4 text-left rounded-xl shadow-sm border border-emerald-200 transition-transform hover:-translate-y-1 bg-emerald-50 text-emerald-900 font-bold text-lg cursor-pointer flex items-center justify-between">
                        <span class="flex items-center"><i class="fas fa-crown text-2xl w-10 text-emerald-600 mr-3 text-center"></i> Chairman</span>
                        <i class="fas fa-chevron-right text-emerald-300"></i>
                    </button>
                    {% endif %}"""
text = text.replace(old_chair, new_chair)

# Replace Vice Chair button
old_vice = """                    <button type="submit" class="w-full p-4 text-left rounded-xl shadow-sm border border-emerald-200 transition-transform hover:-translate-y-1 bg-emerald-50 text-emerald-900 font-bold text-lg cursor-pointer flex items-center justify-between">
                        <span class="flex items-center"><i class="fas fa-user-tie text-2xl w-10 text-emerald-600 mr-3 text-center"></i> Vice Chair</span>
                        <i class="fas fa-chevron-right text-emerald-300"></i>
                    </button>"""
new_vice = """                    {% if 'vice chairman' in occupied_seats or 'vice chair' in occupied_seats %}
                    <button disabled class="w-full p-4 text-left rounded-xl border border-slate-200 bg-slate-100 text-slate-400 font-bold text-lg cursor-not-allowed flex items-center justify-between opacity-75">
                        <span class="flex items-center"><i class="fas fa-user-tie text-2xl w-10 text-slate-400 mr-3 text-center"></i> Vice Chair</span>
                        <span class="text-xs uppercase tracking-widest font-bold">Occupied</span>
                    </button>
                    {% else %}
                    <button type="submit" class="w-full p-4 text-left rounded-xl shadow-sm border border-emerald-200 transition-transform hover:-translate-y-1 bg-emerald-50 text-emerald-900 font-bold text-lg cursor-pointer flex items-center justify-between">
                        <span class="flex items-center"><i class="fas fa-user-tie text-2xl w-10 text-emerald-600 mr-3 text-center"></i> Vice Chair</span>
                        <i class="fas fa-chevron-right text-emerald-300"></i>
                    </button>
                    {% endif %}"""
text = text.replace(old_vice, new_vice)

# Replace Secretary button
old_sec = """                    <button type="submit" class="w-full p-4 text-left rounded-xl shadow-sm border border-emerald-200 transition-transform hover:-translate-y-1 bg-emerald-50 text-emerald-900 font-bold text-lg cursor-pointer flex items-center justify-between">
                        <span class="flex items-center"><i class="fas fa-clipboard-check text-2xl w-10 text-emerald-600 mr-3 text-center"></i> Secretary</span>
                        <i class="fas fa-chevron-right text-emerald-300"></i>
                    </button>"""
new_sec = """                    {% if 'secretary' in occupied_seats %}
                    <button disabled class="w-full p-4 text-left rounded-xl border border-slate-200 bg-slate-100 text-slate-400 font-bold text-lg cursor-not-allowed flex items-center justify-between opacity-75">
                        <span class="flex items-center"><i class="fas fa-clipboard-check text-2xl w-10 text-slate-400 mr-3 text-center"></i> Secretary</span>
                        <span class="text-xs uppercase tracking-widest font-bold">Occupied</span>
                    </button>
                    {% else %}
                    <button type="submit" class="w-full p-4 text-left rounded-xl shadow-sm border border-emerald-200 transition-transform hover:-translate-y-1 bg-emerald-50 text-emerald-900 font-bold text-lg cursor-pointer flex items-center justify-between">
                        <span class="flex items-center"><i class="fas fa-clipboard-check text-2xl w-10 text-emerald-600 mr-3 text-center"></i> Secretary</span>
                        <i class="fas fa-chevron-right text-emerald-300"></i>
                    </button>
                    {% endif %}"""
text = text.replace(old_sec, new_sec)

# Replace Treasurer button
old_treasurer = """                    <button type="submit" class="w-full p-4 text-left rounded-xl shadow-sm border border-emerald-200 transition-transform hover:-translate-y-1 bg-emerald-50 text-emerald-900 font-bold text-lg cursor-pointer flex items-center justify-between">
                        <span class="flex items-center"><i class="fas fa-coins text-2xl w-10 text-emerald-600 mr-3 text-center"></i> Treasurer</span>
                        <i class="fas fa-chevron-right text-emerald-300"></i>
                    </button>"""
new_treasurer = """                    {% if 'treasurer' in occupied_seats %}
                    <button disabled class="w-full p-4 text-left rounded-xl border border-slate-200 bg-slate-100 text-slate-400 font-bold text-lg cursor-not-allowed flex items-center justify-between opacity-75">
                        <span class="flex items-center"><i class="fas fa-coins text-2xl w-10 text-slate-400 mr-3 text-center"></i> Treasurer</span>
                        <span class="text-xs uppercase tracking-widest font-bold">Occupied</span>
                    </button>
                    {% else %}
                    <button type="submit" class="w-full p-4 text-left rounded-xl shadow-sm border border-emerald-200 transition-transform hover:-translate-y-1 bg-emerald-50 text-emerald-900 font-bold text-lg cursor-pointer flex items-center justify-between">
                        <span class="flex items-center"><i class="fas fa-coins text-2xl w-10 text-emerald-600 mr-3 text-center"></i> Treasurer</span>
                        <i class="fas fa-chevron-right text-emerald-300"></i>
                    </button>
                    {% endif %}"""
text = text.replace(old_treasurer, new_treasurer)

with open("templates/program_uip/router.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated UI occupied seats logic")
