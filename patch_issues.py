# coding: utf-8
import re

with open("templates/program_uip/operations/issues.html", "r", encoding="utf-8") as f:
    text = f.read()

old_inc = "{% include 'program_uip/issue_table.html' %}"
new_inc = """
<form method="POST" action="{{ url_for('uip_bp.merge_tickets', org_slug=org.slug) }}">
    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
    
    <div class="p-4 bg-slate-50 border-b border-slate-200 flex justify-between items-center" style="margin-bottom:1rem;">
        <div>
            <h3 class="text-sm font-bold text-slate-700"><i class="fas fa-layer-group text-indigo-500 mr-2"></i>Triage Valve</h3>
            <p class="text-xs text-slate-500">Select duplicate public faults below to merge them into a single Master Ticket for municipal escalation.</p>
        </div>
        <div class="flex space-x-2" style="display:flex; gap:0.5rem;">
            <input type="text" name="master_title" required placeholder="Master Incident Title..." class="border-slate-300 rounded text-sm px-3 py-1">
            <button type="submit" class="ui-btn ui-btn-primary bg-indigo-600 text-white font-bold text-sm">Merge Selected</button>
        </div>
    </div>
    
    {% set allow_merge = True %}
    {% include 'program_uip/issue_table.html' %}
</form>
"""
text = text.replace(old_inc, new_inc)
with open("templates/program_uip/operations/issues.html", "w", encoding="utf-8") as f:
    f.write(text)

with open("templates/program_uip/issue_table.html", "r", encoding="utf-8") as f:
    text2 = f.read()

old_head = "<th>Date</th>"
new_head = "{% if allow_merge %}<th class='w-12 text-center'>Merge</th>{% endif %}<th>Date</th>"
text2 = text2.replace(old_head, new_head)

old_row = "<tr>\n<td>{{ ix.created_at"
new_row = "<tr>\n{% if allow_merge %}<td class='text-center'><input type='checkbox' name='ticket_ids[]' value='{{ ix.id }}' class='rounded border-slate-300 text-indigo-600 focus:ring-indigo-500'></td>{% endif %}<td>{{ ix.created_at"
text2 = text2.replace(old_row, new_row)

old_empty = '<td colspan="7" class="ui-empty">'
new_empty = '<td colspan="{% if allow_merge %}8{% else %}7{% endif %}" class="ui-empty">'
text2 = text2.replace(old_empty, new_empty)

with open("templates/program_uip/issue_table.html", "w", encoding="utf-8") as f:
    f.write(text2)
print("Updated HTML files safely")
