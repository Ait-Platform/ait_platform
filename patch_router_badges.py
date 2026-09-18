import re
with open("templates/program_uip/router.html", "r", encoding="utf-8") as f:
    text = f.read()

# Replace vacant buttons with VACANT status
# Chairman Vacant
old_chair_vacant = """                    <button type="submit" class="w-full p-4 text-left rounded-xl shadow-sm border border-emerald-200 transition-transform hover:-translate-y-1 bg-emerald-50 text-emerald-900 font-bold text-lg cursor-pointer flex items-center justify-between">
                        <span class="flex items-center"><i class="fas fa-crown text-2xl w-10 text-emerald-600 mr-3 text-center"></i> Chairman</span>
                        <i class="fas fa-chevron-right text-emerald-300"></i>
                    </button>"""
new_chair_vacant = """                    <button type="submit" class="w-full p-4 text-left rounded-xl shadow-sm border border-emerald-200 transition-transform hover:-translate-y-1 bg-emerald-50 text-emerald-900 font-bold text-lg cursor-pointer flex items-center justify-between">
                        <span class="flex items-center"><i class="fas fa-crown text-2xl w-10 text-emerald-600 mr-3 text-center"></i> Chairman</span>
                        <span class="text-xs uppercase tracking-widest font-bold text-emerald-600 border border-emerald-300 rounded px-2 py-1 bg-white">Vacant</span>
                    </button>"""
text = text.replace(old_chair_vacant, new_chair_vacant)

# Vice Chair Vacant
old_vice_vacant = """                    <button type="submit" class="w-full p-4 text-left rounded-xl shadow-sm border border-emerald-200 transition-transform hover:-translate-y-1 bg-emerald-50 text-emerald-900 font-bold text-lg cursor-pointer flex items-center justify-between">
                        <span class="flex items-center"><i class="fas fa-user-tie text-2xl w-10 text-emerald-600 mr-3 text-center"></i> Vice Chair</span>
                        <i class="fas fa-chevron-right text-emerald-300"></i>
                    </button>"""
new_vice_vacant = """                    <button type="submit" class="w-full p-4 text-left rounded-xl shadow-sm border border-emerald-200 transition-transform hover:-translate-y-1 bg-emerald-50 text-emerald-900 font-bold text-lg cursor-pointer flex items-center justify-between">
                        <span class="flex items-center"><i class="fas fa-user-tie text-2xl w-10 text-emerald-600 mr-3 text-center"></i> Vice Chair</span>
                        <span class="text-xs uppercase tracking-widest font-bold text-emerald-600 border border-emerald-300 rounded px-2 py-1 bg-white">Vacant</span>
                    </button>"""
text = text.replace(old_vice_vacant, new_vice_vacant)

# Secretary Vacant
old_sec_vacant = """                    <button type="submit" class="w-full p-4 text-left rounded-xl shadow-sm border border-emerald-200 transition-transform hover:-translate-y-1 bg-emerald-50 text-emerald-900 font-bold text-lg cursor-pointer flex items-center justify-between">
                        <span class="flex items-center"><i class="fas fa-clipboard-check text-2xl w-10 text-emerald-600 mr-3 text-center"></i> Secretary</span>
                        <i class="fas fa-chevron-right text-emerald-300"></i>
                    </button>"""
new_sec_vacant = """                    <button type="submit" class="w-full p-4 text-left rounded-xl shadow-sm border border-emerald-200 transition-transform hover:-translate-y-1 bg-emerald-50 text-emerald-900 font-bold text-lg cursor-pointer flex items-center justify-between">
                        <span class="flex items-center"><i class="fas fa-clipboard-check text-2xl w-10 text-emerald-600 mr-3 text-center"></i> Secretary</span>
                        <span class="text-xs uppercase tracking-widest font-bold text-emerald-600 border border-emerald-300 rounded px-2 py-1 bg-white">Vacant</span>
                    </button>"""
text = text.replace(old_sec_vacant, new_sec_vacant)

# Treasurer Vacant
old_treasurer_vacant = """                    <button type="submit" class="w-full p-4 text-left rounded-xl shadow-sm border border-emerald-200 transition-transform hover:-translate-y-1 bg-emerald-50 text-emerald-900 font-bold text-lg cursor-pointer flex items-center justify-between">
                        <span class="flex items-center"><i class="fas fa-coins text-2xl w-10 text-emerald-600 mr-3 text-center"></i> Treasurer</span>
                        <i class="fas fa-chevron-right text-emerald-300"></i>
                    </button>"""
new_treasurer_vacant = """                    <button type="submit" class="w-full p-4 text-left rounded-xl shadow-sm border border-emerald-200 transition-transform hover:-translate-y-1 bg-emerald-50 text-emerald-900 font-bold text-lg cursor-pointer flex items-center justify-between">
                        <span class="flex items-center"><i class="fas fa-coins text-2xl w-10 text-emerald-600 mr-3 text-center"></i> Treasurer</span>
                        <span class="text-xs uppercase tracking-widest font-bold text-emerald-600 border border-emerald-300 rounded px-2 py-1 bg-white">Vacant</span>
                    </button>"""
text = text.replace(old_treasurer_vacant, new_treasurer_vacant)

with open("templates/program_uip/router.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated router vacant badges")
