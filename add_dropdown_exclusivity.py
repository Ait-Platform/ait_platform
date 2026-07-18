import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Add the refreshDropdownExclusivity function right before buildMappingDashboard
function_code = """
  function refreshDropdownExclusivity() {
      // Handle Water Dropdowns
      const waterSelects = document.querySelectorAll('.map-sub-water');
      const selectedWater = Array.from(waterSelects).map(s => s.value).filter(v => v !== "");
      
      waterSelects.forEach(sel => {
          Array.from(sel.options).forEach(opt => {
              if (opt.value === "") return;
              if (selectedWater.includes(opt.value) && sel.value !== opt.value) {
                  opt.disabled = true;
                  opt.style.display = 'none'; // visually hide
              } else {
                  opt.disabled = false;
                  opt.style.display = ''; // restore
              }
          });
      });

      // Handle Elec Dropdowns
      const elecSelects = document.querySelectorAll('.map-sub-elec');
      const selectedElec = Array.from(elecSelects).map(s => s.value).filter(v => v !== "");
      
      elecSelects.forEach(sel => {
          Array.from(sel.options).forEach(opt => {
              if (opt.value === "") return;
              if (selectedElec.includes(opt.value) && sel.value !== opt.value) {
                  opt.disabled = true;
                  opt.style.display = 'none';
              } else {
                  opt.disabled = false;
                  opt.style.display = '';
              }
          });
      });
  }

  function buildMappingDashboard() {"""

content = content.replace("  function buildMappingDashboard() {", function_code)

# Add onchange to HTML selects
old_water_select = '<select class="map-sub-water w-full rounded border-slate-300 px-3 py-2 text-sm focus:border-sky-500 bg-white" style="border-width:1px;" data-saved="${savedMap.water}">'
new_water_select = '<select class="map-sub-water w-full rounded border-slate-300 px-3 py-2 text-sm focus:border-sky-500 bg-white" style="border-width:1px;" data-saved="${savedMap.water}" onchange="refreshDropdownExclusivity()">'

old_elec_select = '<select class="map-sub-elec w-full rounded border-slate-300 px-3 py-2 text-sm focus:border-indigo-500 bg-white" style="border-width:1px;" data-saved="${savedMap.elec}">'
new_elec_select = '<select class="map-sub-elec w-full rounded border-slate-300 px-3 py-2 text-sm focus:border-indigo-500 bg-white" style="border-width:1px;" data-saved="${savedMap.elec}" onchange="refreshDropdownExclusivity()">'

content = content.replace(old_water_select, new_water_select)
content = content.replace(old_elec_select, new_elec_select)

# Add the initial call to refreshDropdownExclusivity at the end of buildMappingDashboard
old_end_build = """    // Apply saved selections
    document.querySelectorAll('.map-sub-water').forEach(sel => { if(sel.dataset.saved) sel.value = sel.dataset.saved; });
    document.querySelectorAll('.map-sub-elec').forEach(sel => { if(sel.dataset.saved) sel.value = sel.dataset.saved; });
  }"""

new_end_build = """    // Apply saved selections
    document.querySelectorAll('.map-sub-water').forEach(sel => { if(sel.dataset.saved) sel.value = sel.dataset.saved; });
    document.querySelectorAll('.map-sub-elec').forEach(sel => { if(sel.dataset.saved) sel.value = sel.dataset.saved; });
    
    // Filter dropdowns to remove already mapped meters
    refreshDropdownExclusivity();
  }"""

content = content.replace(old_end_build, new_end_build)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Added dropdown exclusivity.")
