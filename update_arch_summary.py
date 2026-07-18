import re

with open('templates/program_billing/architecture_summary.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_water = """                                <div class="flex items-center space-x-2">
                                    <span class="w-2 h-2 rounded-full bg-sky-400"></span>
                                    <span class="font-medium">{{ acc.water_meter.meter_number }}</span>
                                </div>"""

new_water = """                                <div class="flex flex-col">
                                    <div class="flex items-center space-x-2">
                                        <span class="w-2 h-2 rounded-full bg-sky-400"></span>
                                        <span class="font-medium">{{ acc.water_meter.meter_number }}</span>
                                    </div>
                                    {% if acc.water_meter.readings and acc.water_meter.readings|length > 0 %}
                                    <div class="text-[10px] text-slate-400 ml-4">
                                        Start: {{ acc.water_meter.readings[0].reading_value }} ({{ acc.water_meter.readings[0].reading_date.strftime('%Y-%m-%d') }})
                                    </div>
                                    {% endif %}
                                </div>"""

content = content.replace(old_water, new_water)

old_elec = """                                <div class="flex items-center space-x-2">
                                    <span class="w-2 h-2 rounded-full bg-indigo-400"></span>
                                    <span class="font-medium">{{ acc.elec_meter.meter_number }}</span>
                                </div>"""

new_elec = """                                <div class="flex flex-col">
                                    <div class="flex items-center space-x-2">
                                        <span class="w-2 h-2 rounded-full bg-indigo-400"></span>
                                        <span class="font-medium">{{ acc.elec_meter.meter_number }}</span>
                                    </div>
                                    {% if acc.elec_meter.readings and acc.elec_meter.readings|length > 0 %}
                                    <div class="text-[10px] text-slate-400 ml-4">
                                        Start: {{ acc.elec_meter.readings[0].reading_value }} ({{ acc.elec_meter.readings[0].reading_date.strftime('%Y-%m-%d') }})
                                    </div>
                                    {% endif %}
                                </div>"""

content = content.replace(old_elec, new_elec)

with open('templates/program_billing/architecture_summary.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated architecture_summary.html")
