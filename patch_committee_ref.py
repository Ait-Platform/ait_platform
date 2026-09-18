import re
with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    text = f.read()

old_td = """                    <td class="py-3 px-6">
                        <div class="font-medium text-slate-900">{{ res.title }}</div>
                        <div class="text-xs text-slate-500 mt-1">{{ res.description }}</div>
                    </td>"""

new_td = """                    <td class="py-3 px-6">
                        <div class="text-[10px] font-black text-indigo-600 mb-1 tracking-widest uppercase">{{ res.reference }}</div>
                        <div class="font-medium text-slate-900">{{ res.title }}</div>
                        <div class="text-xs text-slate-500 mt-1 line-clamp-1">{{ res.description }}</div>
                    </td>"""

text = text.replace(old_td, new_td)

with open("templates/program_uip/dashboards/committee.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated committee.html with reference")
